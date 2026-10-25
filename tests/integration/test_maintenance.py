"""Real DB/filesystem maintenance atomicity, idempotency and source preservation."""

import json
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

from src.ingestion.raw_archiver import RawArchiver
from src.maintenance.jobs import (
    compact_manifest,
    daily_quality_report,
    maintenance_batch,
    reconciliation_job,
)
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection
from src.storage.repository import QualityRepository

pytestmark = pytest.mark.integration


@pytest.fixture
def archive(tmp_path: Path) -> Generator[dict[str, Any], None, None]:
    raw = {
        "meta": {
            "id": str(uuid4()),
            "dt": datetime.now(UTC).isoformat(),
            "stream": "mediawiki.recentchange",
        },
        "wiki": "maintenance_test",
        "type": "edit",
        "server_name": "example.invalid",
        "extra": {"array": [None, 1, "text"]},
    }
    archiver = RawArchiver(base_dir=tmp_path, batch_size=2)
    archiver.append(raw)
    manifest = archiver.append(raw)
    assert manifest is not None
    engine = QualityEngine()
    engine.register_rules(build_default_rule_set())
    prepared = engine.prepare_payload(raw)
    state = {"manifest": manifest, "raw": raw, "prepared": prepared, "root": tmp_path}
    try:
        yield state
    finally:
        with get_db_connection() as conn:
            links = conn.execute(
                "SELECT parquet_manifest_id FROM archive_compactions WHERE source_manifest_id=%s",
                (manifest.manifest_id,),
            ).fetchall()
            conn.execute(
                "DELETE FROM archive_compactions WHERE source_manifest_id=%s",
                (manifest.manifest_id,),
            )
            conn.execute(
                "DELETE FROM reconciliation_reports WHERE manifest_id=%s", (manifest.manifest_id,)
            )
            conn.execute(
                "DELETE FROM incidents WHERE alert_labels->>'manifest_id'=%s",
                (manifest.manifest_id,),
            )
            for link in links:
                conn.execute(
                    "DELETE FROM archive_manifests WHERE manifest_id=%s",
                    (link["parquet_manifest_id"],),
                )
            conn.execute(
                "DELETE FROM archive_manifests WHERE manifest_id=%s", (manifest.manifest_id,)
            )
            key = prepared.summary.event_key
            conn.execute(
                "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=%s", (key,)
            )
            conn.execute("DELETE FROM curated_changes WHERE event_key=%s", (key,))


def test_compaction_concurrent_roundtrip_and_source_immutable(archive: dict[str, Any]) -> None:
    source = archive["manifest"]
    original = Path(source.file_path).read_bytes()
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(
            pool.map(lambda _: compact_manifest(source.manifest_id, root=archive["root"]), range(2))
        )
    assert {row["status"] for row in outcomes} == {"COMPACTED", "ALREADY_COMPACTED"}
    path = Path(source.file_path).with_suffix(".parquet")
    rows = pq.read_table(path).column("payload_json").to_pylist()
    assert [json.loads(row) for row in rows] == [archive["raw"]] * 2
    assert Path(source.file_path).read_bytes() == original
    assert pq.read_metadata(path).row_group(0).column(0).compression == "SNAPPY"
    assert RawArchiver.verify_manifest_in_db(source.manifest_id)["valid"]


def test_compaction_corruption_refused(archive: dict[str, Any]) -> None:
    source = archive["manifest"]
    Path(source.file_path).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        compact_manifest(source.manifest_id, root=archive["root"])


def test_compaction_outside_root_refused(archive: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="outside"):
        compact_manifest(archive["manifest"].manifest_id, root=archive["root"] / "different")


def test_compaction_db_failure_leaves_retryable_owned_output(archive: dict[str, Any]) -> None:
    source = archive["manifest"]

    @contextmanager
    def failing() -> Generator[Any, None, None]:
        with get_db_connection() as conn:

            class Proxy:
                def execute(self, sql: str, args: Any = None) -> Any:
                    if sql.startswith("UPDATE archive_manifests SET status="):
                        raise RuntimeError("Injected DB failure after output rename")
                    return conn.execute(sql, args)

            yield Proxy()

    with patch("src.maintenance.jobs.get_db_connection", failing):
        with pytest.raises(RuntimeError, match="after output rename"):
            compact_manifest(source.manifest_id, root=archive["root"])
    assert Path(source.file_path).with_suffix(".parquet").exists()
    with get_db_connection() as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM archive_compactions WHERE source_manifest_id=%s",
                (source.manifest_id,),
            ).fetchone()
            is None
        )
    assert compact_manifest(source.manifest_id, root=archive["root"])["status"] == "COMPACTED"


def test_reconciliation_batch_rotates_and_scopes_root(archive: dict[str, Any]) -> None:
    archiver = RawArchiver(base_dir=archive["root"], batch_size=1)
    second = archiver.append(archive["raw"])
    assert second is not None
    try:
        with patch("src.maintenance.jobs.get_settings") as cfg:
            cfg.return_value.raw_archive_dir = archive["root"]
            assert len(maintenance_batch("reconciliation", limit=1)) == 1
            assert len(maintenance_batch("reconciliation", limit=1)) == 1
        with get_db_connection() as conn:
            row = conn.execute(
                """SELECT count(DISTINCT manifest_id) AS n FROM reconciliation_reports
                   WHERE manifest_id=ANY(%s)""",
                ([second.manifest_id, archive["manifest"].manifest_id],),
            ).fetchone()
        assert row and row["n"] == 2
    finally:
        with get_db_connection() as conn:
            conn.execute(
                "DELETE FROM reconciliation_reports WHERE manifest_id=%s", (second.manifest_id,)
            )
            conn.execute(
                "DELETE FROM incidents WHERE alert_labels->>'manifest_id'=%s", (second.manifest_id,)
            )
            conn.execute(
                "DELETE FROM archive_manifests WHERE manifest_id=%s", (second.manifest_id,)
            )


def test_compaction_batch_requires_mature_source(archive: dict[str, Any]) -> None:
    with patch("src.maintenance.jobs.get_settings") as cfg:
        cfg.return_value.raw_archive_dir = archive["root"]
        assert maintenance_batch("compaction", manifest_id=archive["manifest"].manifest_id) == []
        with get_db_connection() as conn:
            conn.execute(
                "UPDATE archive_manifests SET start_time=%s,end_time=%s WHERE manifest_id=%s",
                (
                    datetime.now(UTC) - timedelta(days=2),
                    datetime.now(UTC) - timedelta(days=2),
                    archive["manifest"].manifest_id,
                ),
            )
        rows = maintenance_batch("compaction", manifest_id=archive["manifest"].manifest_id)
        assert len(rows) == 1 and rows[0]["status"] == "COMPACTED"


def test_crash_after_file_before_db_commit_can_retry(archive: dict[str, Any]) -> None:
    source = archive["manifest"]
    # A deterministic orphan is the filesystem state after file rename but before DB commit.
    output = Path(source.file_path).with_suffix(".parquet")
    output.write_bytes(b"orphan-incomplete-output")
    outcome = compact_manifest(source.manifest_id, root=archive["root"])
    assert outcome["status"] == "COMPACTED"
    assert pq.read_table(output).num_rows == 2


def test_reconciliation_idempotent_missing_then_resolved(archive: dict[str, Any]) -> None:
    source = archive["manifest"]
    first = reconciliation_job(source.manifest_id)
    again = reconciliation_job(source.manifest_id)
    assert first["report_id"] == again["report_id"] and len(first["missing"]) == 1
    with get_db_connection() as conn:
        QualityRepository(conn).persist_prepared(archive["prepared"])
    complete = reconciliation_job(source.manifest_id)
    assert not complete["missing"] and complete["curated"] == 1
    with get_db_connection() as conn:
        reports = conn.execute(
            "SELECT count(*) AS n FROM reconciliation_reports WHERE manifest_id=%s",
            (source.manifest_id,),
        ).fetchone()
        incident = conn.execute(
            "SELECT status FROM incidents WHERE alert_labels->>'manifest_id'=%s",
            (source.manifest_id,),
        ).fetchone()
    assert reports and reports["n"] == 2
    assert incident and incident["status"] == "RESOLVED"


def test_daily_summary_repeat_replaces_snapshot(archive: dict[str, Any]) -> None:
    day = datetime(2001, 1, 2, tzinfo=UTC)
    key = archive["prepared"].summary.event_key
    with get_db_connection() as conn:
        QualityRepository(conn).persist_prepared(archive["prepared"])
        conn.execute(
            "UPDATE quality_results SET evaluated_at=%s WHERE failure_details->0->>'event_key'=%s",
            (day, key),
        )
    try:
        first = daily_quality_report(day.date())
        again = daily_quality_report(day.date())
        assert first == again and first["groups"] > 0
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT sum(rows_checked) AS n FROM daily_quality_summary WHERE summary_day=%s",
                (day.date(),),
            ).fetchone()
        assert row and row["n"] == len(archive["prepared"].summary.results) + 1
    finally:
        with get_db_connection() as conn:
            conn.execute("DELETE FROM daily_quality_summary WHERE summary_day=%s", (day.date(),))
