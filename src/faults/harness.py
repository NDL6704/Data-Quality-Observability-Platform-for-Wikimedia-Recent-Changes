"""Execute isolated, bounded faults and retain evidence even when assertions fail."""

import json
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

from src.faults.contracts import SCENARIOS, ScenarioReport
from src.faults.environment import FaultEnvironment
from src.faults.network import source_disconnect
from src.observability.metrics import get_metrics
from src.storage.db import get_db_connection


def run_child(env: FaultEnvironment, mode: str, report: ScenarioReport) -> None:
    """Stop an owned process/DB connection at an observed transaction boundary."""
    marker = env.directory / f"{mode}.json"
    overrides = {
        "KAFKA_TOPIC_RAW": env.settings.kafka_topic_raw,
        "KAFKA_TOPIC_VALIDATED": env.settings.kafka_topic_validated,
        "KAFKA_TOPIC_DLQ": env.settings.kafka_topic_dlq,
        "KAFKA_CONSUMER_GROUP_QUALITY": env.settings.kafka_consumer_group_quality,
        "QUALITY_MAX_RETRIES": 0,
    }
    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NO_WINDOW
    with (env.directory / f"{mode}.log").open("w", encoding="utf-8") as log:
        child = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "src.faults.worker",
                "--mode",
                mode,
                "--settings",
                json.dumps(overrides),
                "--barrier",
                str(marker.resolve()),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )
        try:
            deadline = time.monotonic() + 40
            while not marker.exists():
                if child.poll() is not None:
                    raise RuntimeError(f"Worker exited early ({child.returncode}); see {mode}.log")
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Worker did not reach {mode} barrier")
                time.sleep(0.05)
            barrier = json.loads(marker.read_text(encoding="utf-8"))
            before = env.committed()
            rows = env.curated_count(env.keys[-1])
            report.evidence[mode] = {
                "barrier": barrier,
                "offset_before": before,
                "curated_before": rows,
                "child_pid": child.pid,
            }
            if mode == "db-drop":
                with get_db_connection() as conn:
                    row = conn.execute(
                        """SELECT pg_terminate_backend(pid) AS terminated FROM pg_stat_activity
                           WHERE pid=%s AND application_name=%s AND pid<>pg_backend_pid()""",
                        (barrier["pid"], env.settings.kafka_consumer_group_quality),
                    ).fetchone()
                    if not row or not row["terminated"]:
                        raise RuntimeError("Owned DB session was not terminated")
                marker.with_suffix(".release").touch()
                child.wait(timeout=20)
                report.checks["db_failure_exits"] = child.returncode != 0
                lines = (env.directory / f"{mode}.log").read_text(encoding="utf-8").splitlines()
                stats_lines = [line for line in lines if line.startswith("FAULT_STATS=")]
                fault_stats = json.loads(stats_lines[-1].split("=", 1)[1]) if stats_lines else {}
                report.evidence[mode]["failure_stats"] = fault_stats
                report.checks["db_error_observed"] = fault_stats.get("db_errors", 0) >= 1
            else:
                # TerminateProcess on Windows / SIGKILL on POSIX.
                child.kill()
                child.wait(timeout=10)
                report.checks[f"{mode}_process_killed"] = child.returncode != 0
            after = env.committed()
            expected_prior = len(env.keys) - 1
            report.checks[f"{mode}_no_ack"] = after == before and (
                before < 0 if expected_prior == 0 else before == expected_prior
            )
            report.checks[f"{mode}_db_boundary"] = rows == int(mode == "after-db")
            recovery_start = time.monotonic()
            stats = env.consume(1)
            report.evidence[mode].update(
                child_exit=child.returncode,
                offset_after_fault=after,
                recovery_stats=stats,
                recovery_duration_seconds=time.monotonic() - recovery_start,
            )
            report.checks[f"{mode}_recovered"] = (
                stats["processed"] == 1
                and env.committed() == max(0, before) + 1
                and env.curated_count(env.keys[-1]) == 1
                and stats["duplicates"] == int(mode == "after-db")
            )
            if report.checks[f"{mode}_recovered"]:
                get_metrics().recovery.labels(report.scenario_id).observe(
                    report.evidence[mode]["recovery_duration_seconds"]
                )
            outputs = env.outputs(len(env.keys))
            report.checks[f"{mode}_sink_recovered"] = len(outputs) == len(env.keys) and all(
                out["topic"] == env.settings.kafka_topic_validated for out in outputs
            )
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)


def run_scenario(
    scenario_id: str, *, output_dir: Path = Path("data/fault_runs")
) -> tuple[ScenarioReport, Path]:
    """Save checks, timing and cleanup outcome using an atomic report rename."""
    if scenario_id not in SCENARIOS:
        raise ValueError("Unknown scenario")
    run_id = uuid4().hex
    directory = output_dir.resolve() / run_id
    directory.mkdir(parents=True, exist_ok=False)
    report = ScenarioReport(scenario_id, run_id)
    started = time.monotonic()
    environment: FaultEnvironment | None = None
    try:
        if scenario_id == "source-disconnect":
            source_disconnect(report)
        else:
            environment = FaultEnvironment(run_id, directory)
            environment.create()
            if scenario_id == "consumer-crash-before-commit":
                for mode in ("before-db", "after-db"):
                    environment.produce([environment.payload()])
                    run_child(environment, mode, report)
            elif scenario_id == "database-outage":
                environment.produce([environment.payload()])
                run_child(environment, "db-drop", report)
                report.evidence["scope"] = (
                    "Owned PostgreSQL connection drop; no shared container stop"
                )
            else:
                from src.faults.scenarios import run_data_scenario

                run_data_scenario(environment, report)
    except Exception as exc:
        report.error = f"{type(exc).__name__}: {exc}"
    finally:
        if environment is not None:
            try:
                environment.cleanup()
                report.checks["owned_resources_cleaned"] = True
            except Exception as exc:
                report.checks["owned_resources_cleaned"] = False
                report.evidence["cleanup_error"] = f"{type(exc).__name__}: {exc}"
        report.duration_seconds = time.monotonic() - started
    path = directory / "report.json"
    temporary = directory / "report.tmp"
    temporary.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
    temporary.replace(path)
    return report, path
