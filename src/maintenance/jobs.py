"""Idempotent maintenance boundaries: DB checkpoints and immutable source archives."""

import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg.types.json import Jsonb

from src.common.config import get_settings
from src.ingestion.raw_archiver import RawArchiver, compute_file_sha256
from src.observability.incidents import IncidentManager, parse_notifications
from src.recovery.reconciler import reconcile_manifest
from src.storage.db import get_db_connection


def owned_archive(path: str, root: Path) -> Path:
    """Resolve host-relative or container-mounted paths within configured root."""
    candidate = Path(path).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError("Archive path is outside configured root")
    return candidate


def compact_manifest(manifest_id: str, *, root: Path | None = None) -> dict[str, Any]:
    """Lock source; write/verify deterministic Parquet; commit derived manifest/link.

    A crash before DB commit can leave an owned orphan Parquet file. Retry replaces
    that deterministic destination; original JSONL/manifest are never deleted.
    The payload_json column preserves heterogeneous source fields without inference.
    """
    archive_root = root or get_settings().raw_archive_dir
    # Cold pandas/numpy integration initialization must not hold a DB row lock.
    pa.array([], type=pa.string())
    with get_db_connection() as conn:
        conn.execute("SET LOCAL lock_timeout='5s'")
        conn.execute("SET LOCAL statement_timeout='15s'")
        source = conn.execute(
            "SELECT * FROM archive_manifests WHERE manifest_id=%s FOR UPDATE", (manifest_id,)
        ).fetchone()
        if source is None or source["format"] != "jsonl":
            raise ValueError("A JSONL source manifest is required")
        path = owned_archive(source["file_path"], archive_root)
        if not RawArchiver.verify_file_checksum(path, source["checksum_sha256"]):
            raise ValueError("Source archive checksum failed")
        if path.stat().st_size != source["file_size_bytes"] or path.stat().st_size > 100_000_000:
            raise ValueError("Source size mismatch or exceeds 100MB bound")
        existing = conn.execute(
            "SELECT parquet_manifest_id FROM archive_compactions WHERE source_manifest_id=%s",
            (manifest_id,),
        ).fetchone()
        if existing:
            derived = conn.execute(
                "SELECT * FROM archive_manifests WHERE manifest_id=%s",
                (existing["parquet_manifest_id"],),
            ).fetchone()
            if derived is None or not RawArchiver.verify_file_checksum(
                owned_archive(derived["file_path"], archive_root), derived["checksum_sha256"]
            ):
                raise ValueError("Existing Parquet integrity failed; explicit repair required")
            return {
                "source_manifest_id": manifest_id,
                "parquet_manifest_id": derived["manifest_id"],
                "status": "ALREADY_COMPACTED",
                "record_count": derived["record_count"],
            }
        lines: list[str] = []
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                if not isinstance(json.loads(line), dict):
                    raise ValueError("Archive row must be an object")
                lines.append(line.rstrip("\r\n"))
        if len(lines) != source["record_count"]:
            raise ValueError("Source record count mismatch")
        output = path.with_suffix(".parquet")
        temporary = path.with_name(".compact-" + uuid4().hex + ".tmp")
        try:
            pq.write_table(
                pa.table({"payload_json": pa.array(lines, type=pa.string())}),
                temporary,
                compression="snappy",
            )
            with temporary.open("rb+") as physical:
                os.fsync(physical.fileno())
            restored = pq.read_table(temporary).column("payload_json").to_pylist()
            if restored != lines or not RawArchiver.verify_file_checksum(
                path, source["checksum_sha256"]
            ):
                raise ValueError("Roundtrip failed or source changed during compaction")
            checksum = compute_file_sha256(temporary)
            temporary.replace(output)
            derived_id = hashlib.sha256(("parquet:" + manifest_id).encode()).hexdigest()
            # Keep the same path convention as source (relative host/container-compatible path).
            stored_output = str(Path(source["file_path"]).with_suffix(".parquet")).replace(
                "\\", "/"
            )
            conn.execute(
                """INSERT INTO archive_manifests
                (manifest_id,file_path,format,record_count,start_time,end_time,
                 checksum_sha256,file_size_bytes,status)
                VALUES (%s,%s,'parquet',%s,%s,%s,%s,%s,'COMPACTED')""",
                (
                    derived_id,
                    stored_output,
                    len(lines),
                    source["start_time"],
                    source["end_time"],
                    checksum,
                    output.stat().st_size,
                ),
            )
            conn.execute(
                """INSERT INTO archive_compactions(source_manifest_id,parquet_manifest_id)
                   VALUES (%s,%s)""",
                (manifest_id, derived_id),
            )
            conn.execute(
                "UPDATE archive_manifests SET status='COMPACTED' WHERE manifest_id=%s",
                (manifest_id,),
            )
        finally:
            temporary.unlink(missing_ok=True)
        return {
            "source_manifest_id": manifest_id,
            "parquet_manifest_id": derived_id,
            "status": "COMPACTED",
            "record_count": len(lines),
        }


def reconciliation_job(manifest_id: str) -> dict[str, Any]:
    """Persist one result per source-manifest checksum/destination snapshot.

    Missing outcomes produce a deterministic incident, and a later complete result
    resolves the same source-generation episode. Exceptions fail the task visibly.
    """
    report = reconcile_manifest(manifest_id).to_dict()
    with get_db_connection() as conn:
        source = conn.execute(
            "SELECT * FROM archive_manifests WHERE manifest_id=%s", (manifest_id,)
        ).fetchone()
        assert source is not None
        identity = json.dumps(
            [manifest_id, source["checksum_sha256"], report], sort_keys=True
        ).encode()
        report_id = hashlib.sha256(identity).hexdigest()
        conn.execute(
            """INSERT INTO reconciliation_reports(report_id,manifest_id,report)
                     VALUES (%s,%s,%s) ON CONFLICT(report_id)
                     DO UPDATE SET last_checked_at=clock_timestamp()""",
            (report_id, manifest_id, Jsonb(report)),
        )
    start = source["created_at"].astimezone(UTC).isoformat()
    alert = {
        "version": "4",
        "alerts": [
            {
                "status": "firing" if report["missing"] else "resolved",
                "startsAt": start,
                "endsAt": datetime.now(UTC).isoformat(),
                "labels": {
                    "alertname": "ReconciliationMissing",
                    "severity": "HIGH",
                    "component": "reconciliation",
                    "manifest_id": manifest_id,
                },
                "annotations": {"summary": "Source keys lack curated or quarantine outcome"},
            }
        ],
    }
    # Do not create resolved-only rows for a clean source never known to be missing.
    notifications = parse_notifications(alert)
    with get_db_connection() as conn:
        prior = conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id=%s", (notifications[0].incident_id,)
        ).fetchone()
    if report["missing"] or prior:
        IncidentManager().apply(notifications)
    return {"report_id": report_id, **report}


def maintenance_batch(
    kind: str, *, limit: int = 10, manifest_id: str | None = None
) -> list[dict[str, Any]]:
    """Bound hourly/daily work; DAG retry can repeat any manifest safely."""
    if kind not in {"reconciliation", "compaction"} or not 1 <= limit <= 100:
        raise ValueError("Invalid maintenance kind/limit")
    root = get_settings().raw_archive_dir.resolve()
    absolute_prefix = root.as_posix()
    try:
        relative_prefix = Path(os.path.relpath(root, Path.cwd())).as_posix()
    except ValueError:
        relative_prefix = absolute_prefix

    def pattern(value: str) -> str:
        return value.replace("!", "!!").replace("%", "!%").replace("_", "!_") + "/%"

    with get_db_connection() as conn:
        rows = conn.execute(
            """SELECT a.manifest_id,a.file_path FROM archive_manifests a
            WHERE a.format='jsonl' AND (%s::text IS NULL OR a.manifest_id=%s)
            AND (a.file_path LIKE %s ESCAPE '!' OR a.file_path LIKE %s ESCAPE '!')
            AND (%s='reconciliation' OR a.end_time < clock_timestamp()-interval '24 hours')
            AND (%s='reconciliation' OR NOT EXISTS
                (SELECT 1 FROM archive_compactions c WHERE c.source_manifest_id=a.manifest_id))
            ORDER BY (SELECT max(r.last_checked_at) FROM reconciliation_reports r
                      WHERE r.manifest_id=a.manifest_id)
            NULLS FIRST,a.end_time,a.manifest_id LIMIT %s""",
            (
                manifest_id,
                manifest_id,
                pattern(absolute_prefix),
                pattern(relative_prefix),
                kind,
                kind,
                limit,
            ),
        ).fetchall()
    results = []
    for row in rows:
        if not Path(row["file_path"]).resolve().is_relative_to(root):
            continue
        results.append(
            reconciliation_job(row["manifest_id"])
            if kind == "reconciliation"
            else compact_manifest(row["manifest_id"])
        )
    return results


def daily_quality_report(day: date) -> dict[str, Any]:
    """Attempt-weighted UTC-day aggregates; unknown wiki is explicitly unattributed."""
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    with get_db_connection() as conn:
        # Serialize repeat runs for the same day, then replace that day's snapshot atomically.
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("quality:" + day.isoformat(),))
        conn.execute("DELETE FROM daily_quality_summary WHERE summary_day=%s", (day,))
        rows = conn.execute(
            """INSERT INTO daily_quality_summary
            (summary_day,wiki,rule_id,rule_version,rows_checked,rows_failed)
            SELECT %s,COALESCE(c.wiki,'unattributed'),q.rule_id,q.rule_version,
                   sum(q.rows_checked),sum(q.rows_failed)
            FROM quality_results q LEFT JOIN curated_changes c
              ON c.event_key=q.failure_details->0->>'event_key'
            WHERE q.evaluated_at >= %s AND q.evaluated_at < %s
            GROUP BY c.wiki,q.rule_id,q.rule_version RETURNING rule_id""",
            (day, start, start + timedelta(days=1)),
        ).fetchall()
    return {"day": day.isoformat(), "groups": len(rows)}


def retention_report(days: int = 30, *, apply: bool = False) -> dict[str, Any]:
    """Weekly inventory; deletion requires an explicit manual DAG-run opt-in."""
    if days < 7:
        raise ValueError("Retention inventory requires at least seven days")
    with get_db_connection() as conn:
        row = conn.execute(
            """SELECT
            (SELECT count(*) FROM raw_events WHERE received_at
              < clock_timestamp()-(%s*interval '1 day')) AS raw_candidates,
            (SELECT count(*) FROM incidents WHERE status='RESOLVED'
             AND resolved_at < clock_timestamp()-(%s*interval '1 day')) AS resolved_candidates""",
            (days, days),
        ).fetchone()
        deleted: dict[str, int] = {}
        if apply:
            raw = conn.execute(
                """DELETE FROM raw_events WHERE received_at
                < clock_timestamp()-(%s*interval '1 day')""",
                (days,),
            )
            deleted["raw_deleted"] = raw.rowcount
            incidents = conn.execute(
                """DELETE FROM incidents WHERE status='RESOLVED'
                AND resolved_at < clock_timestamp()-(%s*interval '1 day')""",
                (days,),
            )
            deleted["resolved_deleted"] = incidents.rowcount
    return {"dry_run": not apply, "retention_days": days, **(row or {}), **deleted}
