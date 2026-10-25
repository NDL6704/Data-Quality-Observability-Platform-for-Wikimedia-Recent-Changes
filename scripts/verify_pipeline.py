"""Bounded live end-to-end smoke and sustained baseline with retained evidence."""

import argparse
import json
import os
import platform
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from uuid import uuid4

from prometheus_client import generate_latest

from src.faults.environment import FaultEnvironment
from src.ingestion.kafka_producer import WikimediaKafkaProducer
from src.ingestion.raw_archiver import RawArchiver, compute_file_sha256
from src.ingestion.run_ingestor import IngestionRunner
from src.ingestion.sse_client import WikimediaSSEClient
from src.observability.metrics import Metrics
from src.processing.consumer import QualityConsumer
from src.quality.engine import QualityEngine
from src.storage.db import get_db_connection


def verify_pipeline(count: int = 100, timeout: int = 120) -> dict[str, Any]:
    """Keep live DB/archive evidence; remove UUID Kafka resources only on success.

    Consumer runs concurrently with live ingestion. Timer bounds reconnect loops;
    network read/connect timeouts may add a short shutdown grace period.
    """
    if count < 1 or timeout < 1:
        raise ValueError("count and timeout must be positive")
    directory = Path("data/pipeline_runs") / uuid4().hex
    directory.mkdir(parents=True)
    env = FaultEnvironment(directory.name, directory)
    ingest, quality = Metrics(), Metrics()
    report: dict[str, Any] = {
        "run_id": directory.name,
        "count": count,
        "checks": {},
        "error": None,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "mode": "live concurrent source-limited baseline; one partition, one consumer",
    }
    started = time.monotonic()
    runner: IngestionRunner | None = None
    timer: threading.Timer | None = None
    try:
        env.create()
        root = Path("data/raw_archive") / ("phase7-" + directory.name)
        runner = IngestionRunner(
            sse_client=WikimediaSSEClient(read_timeout=5, connect_timeout=5, max_backoff=2),
            kafka_producer=WikimediaKafkaProducer(
                default_topic=env.settings.kafka_topic_raw, auto_create_topic=False
            ),
            raw_archiver=RawArchiver(base_dir=root, batch_size=100),
            auto_handle_signals=False,
            metrics=ingest,
        )
        consumer = QualityConsumer(
            settings=env.settings, metrics=quality, auto_create_topics=False,
            reuse_connections=True,
        )
        timer = threading.Timer(timeout, runner.stop)
        timer.start()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                consumer.run, max_messages=count, max_duration_sec=timeout + 10
            )
            report["ingestion"] = runner.run(max_events=count, max_duration_sec=timeout)
            report["consumer"] = future.result(timeout=timeout + 20)
        elapsed = time.monotonic() - started
        with get_db_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM archive_manifests WHERE file_path LIKE %s",
                (root.as_posix() + "/%",),
            ).fetchall()
            keys = []
            engine = QualityEngine()
            for row in rows:
                for line in Path(row["file_path"]).read_text(encoding="utf-8").splitlines():
                    keys.append(engine.prepare_payload(line.encode()).summary.event_key)
            curated = conn.execute(
                "SELECT count(*) AS n FROM curated_changes WHERE event_key=ANY(%s)", (keys,)
            ).fetchone()
            rejected = conn.execute(
                "SELECT count(DISTINCT event_key) AS n FROM quarantined_events "
                "WHERE event_key=ANY(%s)",
                (keys,),
            ).fetchone()
            covered = conn.execute(
                "SELECT count(*) AS n FROM ("
                "SELECT event_key FROM curated_changes WHERE event_key=ANY(%s) UNION "
                "SELECT event_key FROM quarantined_events WHERE event_key=ANY(%s)) AS covered",
                (keys, keys),
            ).fetchone()
        samples = {
            sample.name + json.dumps(sample.labels, sort_keys=True): sample.value
            for family in quality.registry.collect()
            for sample in family.samples
            if not sample.name.endswith("_created")
        }
        report.update(
            {
                "elapsed_seconds": elapsed,
                "acknowledged_events_per_second": report["consumer"]["processed"] / elapsed,
                "committed_offset": env.committed(),
                "remaining_lag": env.lag(),
                "curated_unique": curated["n"] if curated else 0,
                "quarantined_unique": rejected["n"] if rejected else 0,
                "unique_input_keys": len(set(keys)),
                "covered_unique_keys": covered["n"] if covered else 0,
                "quality_metrics": samples,
                "manifest_ids": [row["manifest_id"] for row in rows],
            }
        )
        report["checks"] = {
            "live_received": report["ingestion"]["received"] == count,
            "delivered_archived": report["ingestion"]["produced"] == count
            and sum(row["record_count"] for row in rows) == count,
            "archive_integrity": bool(rows)
            and all(
                compute_file_sha256(row["file_path"]) == row["checksum_sha256"]
                and Path(row["file_path"]).stat().st_size == row["file_size_bytes"]
                for row in rows
            ),
            "acknowledged": report["consumer"]["processed"] == count
            and report["committed_offset"] == count
            and report["remaining_lag"] == 0,
            "db_coverage": report["covered_unique_keys"] == len(set(keys)) > 0
            and report["curated_unique"] > 0,
            "confirmed_sinks": len(env.outputs(count)) == count,
            "metric_ack_count": sum(
                s.value
                for f in quality.registry.collect()
                for s in f.samples
                if s.name == "wikimedia_consumer_throughput_total"
            )
            == count,
        }
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if timer:
            timer.cancel()
        if runner:
            runner.stop()
        complete = bool(report["checks"]) and all(report["checks"].values())
        if complete and report["error"] is None:
            try:
                env.cleanup()  # no DB keys registered: live records intentionally retained
            except Exception as exc:
                report["cleanup_error"] = str(exc)
        else:
            report["retained_kafka_resources"] = {
                "topics": env.topics, "group": env.settings.kafka_consumer_group_quality
            }
        for name, metrics in (("ingestion", ingest), ("quality", quality)):
            (directory / (name + ".prom")).write_bytes(generate_latest(metrics.registry))
        report["passed"] = (
            bool(report["checks"])
            and all(report["checks"].values())
            and report["error"] is None
            and "cleanup_error" not in report
        )
        (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Evidence: {directory}/report.json; passed={report['passed']}", flush=True)
    return report


def main() -> None:
    """Run a finite live workload; nonzero exit preserves failed evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    raise SystemExit(0 if verify_pipeline(args.count, args.timeout)["passed"] else 1)


if __name__ == "__main__":
    main()
