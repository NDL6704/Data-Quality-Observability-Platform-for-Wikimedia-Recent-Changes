"""Real PostgreSQL tests for persistence, concurrent delivery and rollback."""

import base64
import json
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import psycopg
import pytest

from schemas.models import CuratedRecord, WikimediaRecentChange
from src.common.config import get_settings
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection
from src.storage.repository import (
    QualityRepository,
    decode_quarantine_payload,
    encode_quarantine_payload,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def connection() -> Generator[psycopg.Connection[Any], None, None]:
    """Rollback all fixture writes, including when assertions fail."""
    with get_db_connection() as conn, conn.transaction(force_rollback=True):
        yield conn


@pytest.fixture
def record() -> CuratedRecord:
    """Unique, valid source event; never collides with live ingestion."""
    now = datetime.now(UTC)
    payload = {
        "wiki": "test_repository",
        "type": "edit",
        "server_name": "example.org",
        "title": "Unicode tiếng Việt 'quoted'",
        "namespace": 0,
        "meta": {"id": str(uuid4()), "dt": now.isoformat(), "stream": "mediawiki.recentchange"},
    }
    event = WikimediaRecentChange.model_validate(payload)
    return CuratedRecord.from_validated_event(event, payload, now)


def _evaluate_record(record: CuratedRecord) -> Any:
    """Use the production rule set and the original receipt time."""
    engine = QualityEngine()
    engine.register_rules(build_default_rule_set())
    return engine.evaluate_payload(record.raw_payload, received_at=record.received_timestamp)


def test_upsert_roundtrip_and_duplicate(
    connection: psycopg.Connection[Any],
    record: CuratedRecord,
) -> None:
    repo = QualityRepository(connection)
    assert repo.upsert_curated(record)
    original = connection.execute(
        "SELECT * FROM curated_changes WHERE event_key=%s", (record.event_key,)
    ).fetchone()
    assert original is not None
    changed = record.model_copy(update={"title": "changed"})
    assert not repo.upsert_curated(changed)
    rows = connection.execute(
        "SELECT * FROM curated_changes WHERE event_key=%s", (record.event_key,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["title"] == record.title  # First source payload wins.
    assert rows[0]["raw_payload"] == record.raw_payload
    assert rows[0]["event_timestamp"] == record.event_timestamp
    assert rows[0]["processed_timestamp"] >= original["processed_timestamp"]


def test_persist_accepted_and_duplicate_ledger(
    connection: psycopg.Connection[Any],
    record: CuratedRecord,
) -> None:
    repo = QualityRepository(connection)
    summary = _evaluate_record(record)
    assert not repo.persist_evaluation(summary, record.raw_payload, curated=record).duplicate
    assert repo.persist_evaluation(summary, record.raw_payload, curated=record).duplicate
    rows = connection.execute(
        "SELECT * FROM quality_results WHERE failure_details->0->>'event_key'=%s",
        (record.event_key,),
    ).fetchall()
    assert len(rows) == 22
    duplicates = [r for r in rows if r["rule_id"] == "R-DED-02"]
    assert sorted(r["rows_failed"] for r in duplicates) == [0, 1]
    assert all(r["rows_checked"] == 1 for r in rows)
    assert all(r["failure_details"][0]["event_key"] == record.event_key for r in rows)
    assert len(summary.results) == 10  # No mutation by persistence.


@pytest.mark.parametrize(
    "payload",
    [b"\xff\x00broken", '{"meta":', {"meta": None}, {"value": float("nan")}, {"value": "\x00"}],
)
def test_quarantine_preserves_rejected_input(
    connection: psycopg.Connection[Any],
    payload: Any,
) -> None:
    engine = QualityEngine()
    engine.register_rules(build_default_rule_set())
    summary = engine.evaluate_payload(payload)
    assert summary.should_quarantine
    outcome = QualityRepository(connection).persist_evaluation(summary, payload)
    row = connection.execute(
        "SELECT * FROM quarantined_events WHERE id=%s", (outcome.quarantine_id,)
    ).fetchone()
    assert row is not None and row["replay_status"] == "PENDING"
    assert row["failure_codes"] == summary.failure_codes
    assert row["failure_details"][0]["failure_code"] == summary.failure_codes[0]
    if isinstance(payload, bytes):
        assert base64.b64decode(row["raw_payload"]["data"]) == payload
    elif isinstance(payload, str):
        assert base64.b64decode(row["raw_payload"]["data"]).decode() == payload
    elif "encoding" not in row["raw_payload"]:
        assert row["raw_payload"] == payload
    else:
        assert row["raw_payload"]["source_kind"] == "serialized_json"
    assert (
        connection.execute(
            "SELECT 1 FROM curated_changes WHERE event_key=%s", (summary.event_key,)
        ).fetchone()
        is None
    )


def test_warning_stays_curated(connection: psycopg.Connection[Any], record: CuratedRecord) -> None:
    record.raw_payload["title"] = None
    record.title = None
    summary = _evaluate_record(record)
    assert summary.has_warnings
    result = QualityRepository(connection).persist_evaluation(
        summary, record.raw_payload, curated=record
    )
    assert result.quarantine_id is None
    rows = connection.execute(
        "SELECT * FROM quality_results WHERE rule_id='R-CMP-02' "
        "AND failure_details->0->>'event_key'=%s",
        (record.event_key,),
    ).fetchall()
    assert rows[0]["rows_failed"] == 1 and rows[0]["failure_rate"] == 1.0


@pytest.mark.parametrize("rejected", [False, True])
def test_ledger_failure_rolls_back_destination(
    connection: psycopg.Connection[Any],
    record: CuratedRecord,
    rejected: bool,
) -> None:
    payload: Any = record.raw_payload if not rejected else {"test_marker": str(uuid4())}
    engine = QualityEngine()
    engine.register_rules(build_default_rule_set())
    summary = engine.evaluate_payload(payload, received_at=record.received_timestamp)
    repo = QualityRepository(connection)
    original = repo.record_quality_results

    def fail_after_ledger_write(event_key: str, results: Any) -> None:
        original(event_key, results)
        # Actual SQL failure after ledger INSERT, not only a mock exception.
        connection.execute("SELECT 1 / 0")

    with patch.object(repo, "record_quality_results", side_effect=fail_after_ledger_write):
        with pytest.raises(psycopg.errors.DivisionByZero):
            repo.persist_evaluation(summary, payload, curated=None if rejected else record)
    for table in ("curated_changes", "quarantined_events"):
        # Table names are constants from this test, never user input.
        from psycopg import sql

        assert (
            connection.execute(
                sql.SQL("SELECT 1 FROM {} WHERE event_key=%s").format(sql.Identifier(table)),
                (summary.event_key,),
            ).fetchone()
            is None
        )
    assert (
        connection.execute(
            "SELECT 1 FROM quality_results WHERE failure_details->0->>'event_key'=%s",
            (summary.event_key,),
        ).fetchone()
        is None
    )
    row = connection.execute("SELECT 1 AS ok").fetchone()
    assert row is not None and row["ok"] == 1


def test_mismatched_routing_rejected(
    connection: psycopg.Connection[Any],
    record: CuratedRecord,
) -> None:
    repo = QualityRepository(connection)
    summary = _evaluate_record(record)
    with pytest.raises(ValueError, match="matching curated"):
        repo.persist_evaluation(summary, record.raw_payload)
    with pytest.raises(ValueError, match="matching curated"):
        repo.persist_evaluation(
            summary, record.raw_payload, curated=record.model_copy(update={"event_key": "wrong"})
        )
    with pytest.raises(ValueError, match="raw payload"):
        repo.persist_evaluation(summary, {"other": True}, curated=record)
    with pytest.raises(ValueError, match="Quarantine requires"):
        repo.insert_quarantine(summary, record.raw_payload)


def test_outer_rollback_and_commit_visibility(record: CuratedRecord) -> None:
    key = record.event_key
    try:
        with pytest.raises(RuntimeError, match="crash before commit"):
            with get_db_connection() as conn:
                QualityRepository(conn).persist_evaluation(
                    _evaluate_record(record), record.raw_payload, curated=record
                )
                with get_db_connection() as observer:
                    assert (
                        observer.execute(
                            "SELECT 1 FROM curated_changes WHERE event_key=%s", (key,)
                        ).fetchone()
                        is None
                    )
                raise RuntimeError("crash before commit")
        with get_db_connection() as observer:
            assert (
                observer.execute(
                    "SELECT 1 FROM curated_changes WHERE event_key=%s", (key,)
                ).fetchone()
                is None
            )
        with get_db_connection() as conn:
            QualityRepository(conn).persist_evaluation(
                _evaluate_record(record), record.raw_payload, curated=record
            )
        with get_db_connection() as observer:
            assert (
                observer.execute(
                    "SELECT 1 FROM curated_changes WHERE event_key=%s", (key,)
                ).fetchone()
                is not None
            )
    finally:
        _cleanup_record(key)


def _cleanup_record(key: str) -> None:
    """Remove only committed rows belonging to this unique fixture."""
    with get_db_connection() as conn:
        conn.execute(
            "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=%s", (key,)
        )
        conn.execute("DELETE FROM curated_changes WHERE event_key=%s", (key,))


def test_two_connections_concurrently_insert_one_row(record: CuratedRecord) -> None:
    barrier = Barrier(2)

    def write() -> bool:
        with get_db_connection() as conn:
            conn.execute("SET LOCAL lock_timeout = '5s'")
            barrier.wait(timeout=5)
            return QualityRepository(conn).upsert_curated(record)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: write(), range(2)))
        assert sorted(results) == [False, True]
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM curated_changes WHERE event_key=%s", (record.event_key,)
            ).fetchone()
            assert row is not None and row["n"] == 1
    finally:
        _cleanup_record(record.event_key)


def test_autocommit_connection_rejected(record: CuratedRecord) -> None:
    settings = get_settings()
    with psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        autocommit=True,
    ) as conn:
        with pytest.raises(ValueError, match="caller-owned transaction"):
            QualityRepository(conn).upsert_curated(record)


@pytest.mark.parametrize("namespace", ["abc", 10001])
def test_prepared_input_to_database_without_reparse(
    connection: psycopg.Connection[Any],
    record: CuratedRecord,
    namespace: Any,
) -> None:
    record.raw_payload["namespace"] = namespace
    instance = QualityEngine()
    instance.register_rules(build_default_rule_set())
    # Text input must share exactly the same normalization as dictionary input.
    prepared = instance.prepare_payload(
        json.dumps(record.raw_payload), received_at=record.received_timestamp
    )
    repo = QualityRepository(connection)
    assert not repo.persist_prepared(prepared).duplicate
    assert repo.persist_prepared(prepared).duplicate
    row = connection.execute(
        "SELECT namespace, raw_payload FROM curated_changes WHERE event_key=%s",
        (prepared.summary.event_key,),
    ).fetchone()
    assert row is not None and row["namespace"] is None
    assert row["raw_payload"]["namespace"] == namespace


def test_quarantine_decode_re_evaluate_same_failure(
    connection: psycopg.Connection[Any],
) -> None:
    instance = QualityEngine()
    instance.register_rules(build_default_rule_set())
    raw = b"\xffbroken"
    prepared = instance.prepare_payload(raw)
    outcome = QualityRepository(connection).persist_prepared(prepared)
    row = connection.execute(
        "SELECT raw_payload FROM quarantined_events WHERE id=%s", (outcome.quarantine_id,)
    ).fetchone()
    assert row is not None
    assert row["raw_payload"]["received_at"] == prepared.received_at.isoformat()
    decoded = decode_quarantine_payload(row["raw_payload"])
    assert decoded == raw
    assert instance.prepare_payload(decoded).summary.event_key == prepared.summary.event_key


def test_versioned_encoding_does_not_confuse_source_wrapper() -> None:
    original = {"encoding": "base64", "source_kind": "bytes", "data": "YWJj"}
    encoded = encode_quarantine_payload(original)
    assert decode_quarantine_payload(encoded) == original
    # Old rows remain readable; only old exact-shape wrappers are ambiguous.
    assert decode_quarantine_payload(original) == b"abc"
    assert decode_quarantine_payload({"meta": None}) == {"meta": None}
    encoded["version"] = 999
    with pytest.raises(ValueError, match="Unsupported"):
        decode_quarantine_payload(encoded)
