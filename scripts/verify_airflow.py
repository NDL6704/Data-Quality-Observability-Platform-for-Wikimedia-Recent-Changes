"""Run inside airflow-scheduler: parse, local DAG tests and actual scheduled run.

Only UUID-owned synthetic platform rows are removed. Airflow run/task history is
retained; configured test-manifest Variable and paused state are restored.
"""

import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from airflow.models import DagBag, DagRun, Variable
from airflow.utils.session import create_session
from sqlalchemy import desc

from src.common.config import get_settings
from src.ingestion.raw_archiver import RawArchiver
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection
from src.storage.repository import QualityRepository


def cli(*args: str, log: Path) -> None:
    """Use argument arrays, not shell-composed DAG conf or credentials."""
    with log.open("w") as stream:
        result = subprocess.run(
            ["airflow", *args], stdout=stream, stderr=subprocess.STDOUT, timeout=120
        )
    if result.returncode:
        raise RuntimeError(f"airflow command failed; see {log.name}")


def main() -> None:
    run_id = uuid4().hex
    directory = get_settings().raw_archive_dir / ("phase6-" + run_id)
    directory.mkdir(parents=True)
    result: dict[str, Any] = {"run_id": run_id, "checks": {}, "error": None}
    checks = result["checks"]
    source_id = None
    key = None
    previous = Variable.get("wikimedia_maintenance_manifest", default_var=None)
    started = datetime.now(UTC)
    try:
        bag = DagBag("/workspace/dags", include_examples=False)
        checks["four_dags_parse"] = not bag.import_errors and set(bag.dags) == {
            "dq_reconciliation",
            "archive_compaction",
            "daily_quality_report",
            "retention_cleanup",
        }
        if not checks["four_dags_parse"]:
            raise RuntimeError(str(bag.import_errors))
        raw = {
            "meta": {
                "id": run_id,
                "dt": datetime.now(UTC).isoformat(),
                "stream": "mediawiki.recentchange",
            },
            "wiki": "airflow_test",
            "type": "edit",
            "title": "Airflow acceptance",
            "server_name": "example.invalid",
        }
        engine = QualityEngine()
        engine.register_rules(build_default_rule_set())
        prepared = engine.prepare_payload(raw)
        key = prepared.summary.event_key
        archive = RawArchiver(base_dir=directory, batch_size=1)
        manifest = archive.append(raw)
        assert manifest is not None
        source_id = manifest.manifest_id
        result["source_manifest_id"] = source_id
        with get_db_connection() as conn:
            QualityRepository(conn).persist_prepared(prepared)
            conn.execute(
                "UPDATE archive_manifests SET start_time=%s,end_time=%s WHERE manifest_id=%s",
                (started - timedelta(days=2), started - timedelta(days=2), source_id),
            )
        conf = json.dumps({"manifest_id": source_id})
        for dag_id in (
            "dq_reconciliation",
            "archive_compaction",
            "daily_quality_report",
            "retention_cleanup",
        ):
            print("TEST " + dag_id, flush=True)
            cli(
                "dags",
                "test",
                dag_id,
                "2026-10-24T00:00:00+00:00",
                "--conf",
                conf,
                log=directory / (dag_id + ".log"),
            )
            checks[dag_id + "_local_test"] = True
        # Retry the compaction DAG: no duplicate lineage/manifest.
        cli(
            "dags",
            "test",
            "archive_compaction",
            "2026-10-24T01:00:00+00:00",
            "--conf",
            conf,
            log=directory / "compaction-retry.log",
        )
        with get_db_connection() as conn:
            links = conn.execute(
                "SELECT * FROM archive_compactions WHERE source_manifest_id=%s", (source_id,)
            ).fetchall()
            reports = conn.execute(
                "SELECT report FROM reconciliation_reports WHERE manifest_id=%s", (source_id,)
            ).fetchall()
        checks["compaction_unique_link"] = len(links) == 1
        checks["reconciliation_complete"] = bool(reports) and not reports[0]["report"]["missing"]
        checks["source_jsonl_intact"] = RawArchiver.verify_manifest_in_db(source_id)["valid"]
        # Observe a scheduler-created (not manually triggered/local-test) successful run.
        Variable.set("wikimedia_maintenance_manifest", source_id)
        cli("dags", "unpause", "dq_reconciliation", log=directory / "unpause.log")
        deadline = time.monotonic() + 150
        print("WAIT scheduler-created reconciliation", flush=True)
        while time.monotonic() < deadline:
            with create_session() as session:
                runs = (
                    session.query(DagRun)
                    .filter(
                        DagRun.dag_id == "dq_reconciliation",
                        DagRun.run_type == "scheduled",
                        DagRun.start_date >= started,
                    )
                    .order_by(desc(DagRun.start_date))
                    .all()
                )
                if runs and runs[0].state == "success":
                    result["scheduled_run_id"] = runs[0].run_id
                    checks["actual_scheduler_run_success"] = True
                    break
                if runs and runs[0].state == "failed":
                    raise RuntimeError("Scheduled reconciliation failed")
            time.sleep(2)
        if not checks.get("actual_scheduler_run_success"):
            raise TimeoutError("No successful scheduler-created run observed")
        result["passed"] = all(checks.values())
        if not result["passed"]:
            raise RuntimeError("Acceptance checks failed")
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["passed"] = False
        raise
    finally:
        cli("dags", "pause", "dq_reconciliation", log=directory / "pause.log")
        if previous is None:
            Variable.delete("wikimedia_maintenance_manifest")
        else:
            Variable.set("wikimedia_maintenance_manifest", previous)
        with get_db_connection() as conn:
            if source_id:
                links = conn.execute(
                    """SELECT parquet_manifest_id FROM archive_compactions
                       WHERE source_manifest_id=%s""",
                    (source_id,),
                ).fetchall()
                conn.execute(
                    "DELETE FROM archive_compactions WHERE source_manifest_id=%s", (source_id,)
                )
                conn.execute(
                    "DELETE FROM reconciliation_reports WHERE manifest_id=%s", (source_id,)
                )
                conn.execute(
                    "DELETE FROM incidents WHERE alert_labels->>'manifest_id'=%s", (source_id,)
                )
                for link in links:
                    conn.execute(
                        "DELETE FROM archive_manifests WHERE manifest_id=%s",
                        (link["parquet_manifest_id"],),
                    )
                conn.execute("DELETE FROM archive_manifests WHERE manifest_id=%s", (source_id,))
            if key:
                conn.execute(
                    "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=%s", (key,)
                )
                conn.execute("DELETE FROM curated_changes WHERE event_key=%s", (key,))
        (directory / "airflow-report.json").write_text(json.dumps(result, indent=2, default=str))
        print("Evidence: " + str(directory / "airflow-report.json"), flush=True)


if __name__ == "__main__":
    main()
