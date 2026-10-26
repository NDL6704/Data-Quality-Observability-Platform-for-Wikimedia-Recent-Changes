"""Bounded synthetic load or live SSE soak, with per-run reconciliation evidence."""

import argparse
import json
import os
import platform
import shutil
import threading
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import generate_latest

from src.common.config import Settings
from src.ingestion.kafka_producer import WikimediaKafkaProducer
from src.ingestion.raw_archiver import RawArchiver, compute_file_sha256
from src.ingestion.run_ingestor import IngestionRunner
from src.ingestion.sse_client import RawStreamEvent, WikimediaSSEClient
from src.observability.metrics import Metrics
from src.processing.consumer import QualityConsumer, build_consumer_config
from src.quality.engine import QualityEngine
from src.storage.db import get_db_connection


class PacedSource(WikimediaSSEClient):
    """Reproducible mix: 10% repeated deliveries, 5% missing server identity."""

    def __init__(self, run_id: str, count: int, rate: float) -> None:
        super().__init__()
        self.run_id, self.count, self.rate = run_id, count, rate

    def stream_events(self) -> Generator[RawStreamEvent, None, None]:
        """Offer messages against an absolute schedule; never run indefinitely."""
        started = time.monotonic()
        previous: dict[str, Any] = {}
        for index in range(self.count):
            delay = started + index / self.rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            if not self._is_running:
                return
            now = datetime.now(UTC)
            payload = {
                "meta": {
                    "id": f"{self.run_id}-{index}",
                    "dt": now.isoformat(),
                    "stream": "mediawiki.recentchange",
                },
                "wiki": "load_test",
                "type": "edit",
                "namespace": 0,
                "title": "Controlled load " + "x" * 400,
                "server_name": "example.invalid",
            }
            if index % 20 == 0:
                payload.pop("server_name")
            if index % 10 == 9:
                payload = previous
            previous = payload
            yield RawStreamEvent(
                str(index), "message", payload, json.dumps(payload), now.timestamp()
            )


def rss_bytes() -> int | None:
    """Read Linux process RSS; explicitly unavailable on other platforms."""
    status = Path("/proc/self/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return None


def run(args: argparse.Namespace) -> dict[str, Any]:
    """Run isolated three-partition/three-worker traffic and retain all evidence."""
    run_id = uuid4().hex
    directory = Path(args.output) / run_id
    directory.mkdir(parents=True)
    if shutil.disk_usage(directory).free < args.min_free_gb * 1024**3:
        raise RuntimeError("Insufficient free disk space before starting the workload")
    root = directory / "raw"
    settings = Settings(
        KAFKA_TOPIC_RAW=f"sustained.{run_id}.raw",
        KAFKA_TOPIC_VALIDATED=f"sustained.{run_id}.valid",
        KAFKA_TOPIC_DLQ=f"sustained.{run_id}.dlq",
        KAFKA_CONSUMER_GROUP_QUALITY=f"sustained.{run_id}",
    )
    topics = [settings.kafka_topic_raw, settings.kafka_topic_validated, settings.kafka_topic_dlq]
    report: dict[str, Any] = {
        "run_id": run_id,
        "mode": args.mode,
        "config": vars(args),
        "started_at": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "topics": topics,
        "group": settings.kafka_consumer_group_quality,
        "checks": {},
        "error": None,
    }
    (directory / "report.json").write_text(json.dumps(report, indent=2))
    print(f"EVIDENCE {directory}", flush=True)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    observer = Consumer(build_consumer_config(settings))
    workers: list[QualityConsumer] = []
    metrics = Metrics()
    sampler_stop = threading.Event()
    samples: list[dict[str, Any]] = []
    started = time.monotonic()
    runner: IngestionRunner | None = None
    timer: threading.Timer | None = None

    def offsets() -> list[dict[str, int]]:
        partitions = [TopicPartition(settings.kafka_topic_raw, p) for p in range(3)]
        committed = observer.committed(partitions, timeout=5)
        result = []
        for partition, ack in zip(partitions, committed, strict=True):
            low, high = observer.get_watermark_offsets(partition, timeout=5, cached=False)
            result.append(
                {
                    "partition": partition.partition,
                    "low": low,
                    "high": high,
                    "committed": max(0, ack.offset),
                    "lag": high - max(0, ack.offset),
                }
            )
        return result

    def sample() -> None:
        while not sampler_stop.is_set():
            row: dict[str, Any] = {
                "seconds": time.monotonic() - started,
                "cpu_seconds": time.process_time(),
                "rss_bytes": rss_bytes(),
                "disk_free_bytes": shutil.disk_usage(directory).free,
            }
            try:
                row["partitions"] = offsets()
                row["lag"] = sum(p["lag"] for p in row["partitions"])
                row["processed"] = sum(w.stats["processed"] for w in workers)
            except Exception as exc:
                row["sampling_error"] = str(exc)
            samples.append(row)
            with (directory / "samples.jsonl").open("a") as output:
                output.write(json.dumps(row) + "\n")
            if row["disk_free_bytes"] < args.min_free_gb * 1024**3 and runner:
                report["disk_guard_triggered"] = True
                runner.stop()
            sampler_stop.wait(5)

    sampler: threading.Thread | None = None
    try:
        for future in admin.create_topics([NewTopic(t, 3, 1) for t in topics]).values():
            future.result(timeout=20)
        workers = [
            QualityConsumer(
                settings=settings, metrics=metrics, reuse_connections=True, auto_create_topics=False
            )
            for _ in range(3)
        ]
        source = (
            PacedSource(run_id, args.count, args.rate)
            if args.mode == "synthetic"
            else WikimediaSSEClient(connect_timeout=5, read_timeout=5, max_backoff=5)
        )
        producer = WikimediaKafkaProducer(
            default_topic=settings.kafka_topic_raw, auto_create_topic=False
        )
        runner = IngestionRunner(
            sse_client=source,
            kafka_producer=producer,
            raw_archiver=RawArchiver(base_dir=root, batch_size=500),
            auto_handle_signals=True,
            metrics=Metrics(),
        )
        timer = threading.Timer(args.seconds, runner.stop)
        timer.start()
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(w.run, max_duration_sec=args.seconds + args.drain + 15)
                for w in workers
            ]
            try:
                report["ingestion"] = runner.run(
                    max_events=args.count, max_duration_sec=args.seconds, log_interval=1000
                )
                report["producer"] = producer.stats
                report["ingestion_elapsed"] = time.monotonic() - started
                deadline = time.monotonic() + args.drain
                while time.monotonic() < deadline:
                    if any(f.done() and f.exception() for f in futures):
                        raise RuntimeError("A quality worker failed")
                    parts = offsets()
                    if sum(p["high"] for p in parts) == report["ingestion"]["received"] and all(
                        p["lag"] == 0 for p in parts
                    ):
                        break
                    time.sleep(1)
                report["partitions"] = offsets()
                report["drain_seconds"] = time.monotonic() - started - report["ingestion_elapsed"]
            finally:
                for worker in workers:
                    worker.stop()
            report["workers"] = [f.result(timeout=20) for f in futures]
        sampler_stop.set()
        if sampler:
            sampler.join(timeout=20)
        keys = []
        rejected_keys: set[str] = set()
        engine = QualityEngine()
        manifests = []
        with get_db_connection() as conn:
            manifests = conn.execute(
                "SELECT * FROM archive_manifests WHERE file_path LIKE %s", (root.as_posix() + "/%",)
            ).fetchall()
            integrity = bool(manifests)
            for manifest in manifests:
                path = Path(manifest["file_path"])
                lines = path.read_text(encoding="utf-8").splitlines()
                integrity &= (
                    compute_file_sha256(path) == manifest["checksum_sha256"]
                    and len(lines) == manifest["record_count"]
                    and path.stat().st_size == manifest["file_size_bytes"]
                )
                for line in lines:
                    key = engine.prepare_payload(line.encode()).summary.event_key
                    keys.append(key)
                    if args.mode == "synthetic" and "server_name" not in json.loads(line):
                        rejected_keys.add(key)
            curated = {
                r["event_key"]
                for r in conn.execute(
                    "SELECT event_key FROM curated_changes WHERE event_key=ANY(%s)",
                    (list(set(keys)),),
                ).fetchall()
            }
            rejected = {
                r["event_key"]
                for r in conn.execute(
                    "SELECT DISTINCT event_key FROM quarantined_events WHERE event_key=ANY(%s)",
                    (list(set(keys)),),
                ).fetchall()
            }
        # Read every sink delivery independently, without joining the processing group.
        observer.assign([TopicPartition(t, p, 0) for t in topics[1:] for p in range(3)])
        sink_ids: list[tuple[int, int]] = []
        sink_keys: set[str] = set()
        deadline = time.monotonic() + 45
        while len(sink_ids) < len(keys) and time.monotonic() < deadline:
            message = observer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            body = json.loads(message.value() or b"{}")
            sink_ids.append((body["source"]["partition"], body["source"]["offset"]))
            sink_keys.add(body["event_key"])
        expected_ids = {(p["partition"], i) for p in report["partitions"] for i in range(p["high"])}
        received = report["ingestion"]["received"]
        processed = sum(w["processed"] for w in report["workers"])
        report.update(
            {
                "received": received,
                "processed": processed,
                "unique_input_keys": len(set(keys)),
                "curated_unique": len(curated),
                "quarantined_unique": len(rejected),
                "sink_deliveries": len(sink_ids),
                "manifest_count": len(manifests),
                "archive_bytes": sum(m["file_size_bytes"] for m in manifests),
                "elapsed_seconds": time.monotonic() - started,
            }
        )
        report["checks"] = {
            "nonempty": received > 0,
            "confirmed_input": report["producer"]["delivered"] == received
            and report["producer"]["failed"] == 0,
            "archive_integrity": integrity and len(keys) == received,
            "acknowledged": processed == received
            and all(p["lag"] == 0 for p in report["partitions"]),
            "db_coverage": curated | rejected == set(keys),
            "sink_coverage": set(sink_ids) == expected_ids
            and len(sink_ids) == received
            and sink_keys == set(keys),
            "no_worker_errors": all(
                w[k] == 0
                for w in report["workers"]
                for k in [
                    "db_errors",
                    "publish_errors",
                    "commit_errors",
                    "poll_errors",
                    "receipt_missing",
                    "receipt_invalid",
                ]
            ),
            "disk_guard_not_triggered": not report.get("disk_guard_triggered", False),
        }
        if args.mode == "synthetic":
            report["checks"]["target_count"] = received == args.count
            report["checks"]["expected_routing"] = rejected == rejected_keys and curated == (
                set(keys) - rejected_keys
            )
            report["checks"]["expected_duplicates"] = sum(
                w["duplicates"] for w in report["workers"]
            ) == len(keys) - len(set(keys))
        else:
            report["checks"]["soak_duration"] = report["ingestion"]["duration_seconds"] >= (
                args.seconds - 5
            )
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if timer:
            timer.cancel()
        if runner:
            runner.stop()
        for worker in workers:
            worker.stop()
            worker.close()
        sampler_stop.set()
        if sampler:
            sampler.join(timeout=20)
        observer.close()
        report["max_sampled_lag"] = max((s.get("lag", 0) for s in samples), default=0)
        report["max_sampled_rss_bytes"] = max((s.get("rss_bytes") or 0 for s in samples), default=0)
        report["passed"] = (
            bool(report["checks"]) and all(report["checks"].values()) and (report["error"] is None)
        )
        report["ended_at"] = datetime.now(UTC).isoformat()
        if report["passed"]:
            try:
                for future in admin.delete_topics(topics).values():
                    future.result(timeout=15)
                admin.delete_consumer_groups([settings.kafka_consumer_group_quality])[
                    settings.kafka_consumer_group_quality
                ].result(timeout=15)
            except Exception as exc:
                report["cleanup_error"] = str(exc)
                report["passed"] = False
        (directory / "quality.prom").write_bytes(generate_latest(metrics.registry))
        (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(
            json.dumps(
                {
                    "evidence": str(directory),
                    "passed": report["passed"],
                    "error": report["error"],
                    "checks": report["checks"],
                }
            ),
            flush=True,
        )
    return report


def main() -> None:
    """Require finite caps for both source modes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["synthetic", "live"], required=True)
    parser.add_argument("--count", type=int, default=20000)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--seconds", type=int, default=240)
    parser.add_argument("--drain", type=int, default=120)
    parser.add_argument("--min-free-gb", type=float, default=5)
    parser.add_argument("--output", default="data/sustained_runs")
    args = parser.parse_args()
    if not (
        1 <= args.count <= 100000
        and 1 <= args.seconds <= 1800
        and 1 <= args.drain <= 600
        and 0 < args.rate <= 1000
        and args.min_free_gb > 0
    ):
        parser.error("Limits outside bounded verification range")
    raise SystemExit(0 if run(args)["passed"] else 1)


if __name__ == "__main__":
    main()
