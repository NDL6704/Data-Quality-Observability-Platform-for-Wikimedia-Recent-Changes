"""Real-DB replay atomicity/concurrency and reconciliation accounting tests."""

import json
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest

from src.faults.scenarios import StrictTitleRule
from src.ingestion.raw_archiver import RawArchiver
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.recovery.reconciler import reconcile_keys, reconcile_manifest
from src.recovery.replayer import replay_quarantine
from src.storage.db import get_db_connection
from src.storage.repository import QualityRepository

pytestmark = pytest.mark.integration


@pytest.fixture
def pending() -> Generator[dict[str, Any], None, None]:
    receipt = datetime.now(UTC) - timedelta(days=2)
    raw = json.dumps(
        {
            "meta": {
                "id": str(uuid4()),
                "dt": receipt.isoformat(),
                "stream": "mediawiki.recentchange",
            },
            "wiki": "recovery_test",
            "server_name": "example.invalid",
            "type": "edit",
            "namespace": 0,
            "title": "Replay original receipt",
        }
    ).encode()
    engine = QualityEngine()
    engine.register_rules([*build_default_rule_set(), StrictTitleRule()])
    prepared = engine.prepare_payload(raw, received_at=receipt)
    key = prepared.summary.event_key
    with get_db_connection() as conn:
        outcome = QualityRepository(conn).persist_prepared(prepared)
    assert outcome.quarantine_id is not None
    state = {"id": outcome.quarantine_id, "key": key, "receipt": receipt, "raw": raw}
    try:
        yield state
    finally:
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT replay_run_id FROM quarantined_events WHERE id=%s", (state["id"],)
            ).fetchone()
            conn.execute(
                "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=%s", (key,)
            )
            conn.execute("DELETE FROM curated_changes WHERE event_key=%s", (key,))
            conn.execute("DELETE FROM quarantined_events WHERE id=%s", (state["id"],))
            if row and row["replay_run_id"]:
                conn.execute("DELETE FROM pipeline_runs WHERE run_id=%s", (row["replay_run_id"],))


def test_concurrent_replay_is_idempotent(pending: dict[str, Any]) -> None:
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(replay_quarantine, [pending["id"], pending["id"]]))
    assert all(outcome.status == "REPLAYED" for outcome in outcomes)
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM curated_changes WHERE event_key=%s", (pending["key"],)
        ).fetchall()
        assert len(rows) == 1 and rows[0]["received_timestamp"] == pending["receipt"]
        results = conn.execute(
            """SELECT count(*) AS n FROM quality_results
               WHERE rule_id='R-DED-02' AND failure_details->0->>'event_key'=%s""",
            (pending["key"],),
        ).fetchone()
        assert results is not None and results["n"] == 1


def test_replay_rolls_back_promotion_on_failure(pending: dict[str, Any]) -> None:
    original = QualityRepository.persist_prepared

    def fail(self: Any, prepared: Any, **kwargs: Any) -> Any:
        original(self, prepared, **kwargs)
        raise RuntimeError("Injected failure after curated write")

    with patch.object(QualityRepository, "persist_prepared", fail):
        with pytest.raises(RuntimeError, match="Injected failure"):
            replay_quarantine(pending["id"])
    with get_db_connection() as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM curated_changes WHERE event_key=%s", (pending["key"],)
            ).fetchone()
            is None
        )
        row = conn.execute(
            "SELECT * FROM quarantined_events WHERE id=%s", (pending["id"],)
        ).fetchone()
        assert row is not None and row["replay_status"] == "PENDING"
        assert row["replay_run_id"] is None and row["replayed_at"] is None


@pytest.mark.parametrize("receipt", [None, "2026-10-25T00:00:00"])
def test_replay_rejects_missing_or_naive_receipt(
    pending: dict[str, Any], receipt: str | None
) -> None:
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE quarantined_events SET raw_payload=jsonb_set(raw_payload, '{received_at}', %s) "
            "WHERE id=%s",
            (json.dumps(receipt), pending["id"]),
        )
    with pytest.raises(ValueError, match="received_at"):
        replay_quarantine(pending["id"])


def test_reconciliation_counts_unique_keys_and_missing(pending: dict[str, Any]) -> None:
    missing = "test-missing-" + uuid4().hex
    report = reconcile_keys([pending["key"], pending["key"], missing])
    assert report.raw_deliveries == 3 and report.unique_raw == 2
    assert report.duplicate_deliveries == 1 and report.quarantine_only == 1
    assert report.curated == 0 and report.missing == (missing,) and not report.overlap
    replay_quarantine(pending["id"])
    promoted = reconcile_keys([pending["key"]])
    assert promoted.curated == 1 and promoted.quarantine_only == 0
    assert promoted.overlap == (pending["key"],) and not promoted.missing


def test_reconcile_empty_input() -> None:
    report = reconcile_keys([])
    assert report.unique_raw == report.raw_deliveries == report.curated == 0
    assert not report.missing


@pytest.mark.parametrize("fault", ["count", "missing-file", "invalid-envelope"])
def test_manifest_reconciliation_rejects_inadequate_evidence(tmp_path: Path, fault: str) -> None:
    payload = {
        "meta": {
            "id": str(uuid4()),
            "dt": datetime.now(UTC).isoformat(),
            "stream": "mediawiki.recentchange",
        },
        "wiki": "archive_test",
        "type": "edit",
        "server_name": "example.invalid",
    }
    if fault == "invalid-envelope":
        payload.pop("meta")
    archiver = RawArchiver(base_dir=tmp_path, batch_size=1)
    manifest = archiver.append(payload)
    assert manifest is not None
    try:
        if fault == "count":
            with get_db_connection() as conn:
                conn.execute(
                    "UPDATE archive_manifests SET record_count=2 WHERE manifest_id=%s",
                    (manifest.manifest_id,),
                )
        elif fault == "missing-file":
            Path(manifest.file_path).unlink()  # Only the file this test just created.
        with pytest.raises(ValueError):
            reconcile_manifest(manifest.manifest_id)
    finally:
        with get_db_connection() as conn:
            conn.execute(
                "DELETE FROM archive_manifests WHERE manifest_id=%s", (manifest.manifest_id,)
            )


def test_discarded_quarantine_is_not_promoted(pending: dict[str, Any]) -> None:
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE quarantined_events SET replay_status='DISCARDED' WHERE id=%s", (pending["id"],)
        )
    assert replay_quarantine(pending["id"]).status == "DISCARDED"
    assert reconcile_keys([pending["key"]]).curated == 0
