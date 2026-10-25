"""Finite real-stack acceptance: workload -> metrics -> Prometheus -> incidents.

Requires monitoring profile running. Owns temporary host endpoints 8000..8002,
UUID Kafka/DB resources and one synthetic archive. Never uses operational group.
"""

import json
import threading
import time
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import uuid4

from src.common.config import get_settings
from src.faults.environment import FaultEnvironment
from src.ingestion.kafka_producer import WikimediaKafkaProducer
from src.ingestion.raw_archiver import RawArchiver
from src.ingestion.run_ingestor import IngestionRunner
from src.ingestion.sse_client import RawStreamEvent, WikimediaSSEClient
from src.observability.collector import StateCollector
from src.observability.metrics import Metrics, get_metrics, start_metrics_endpoint
from src.observability.service import make_server
from src.processing.consumer import QualityConsumer
from src.storage.db import get_db_connection


def fetch(url: str) -> Any:
    """Fetch a bounded JSON HTTP API response."""
    with urlopen(url, timeout=10) as response:
        return json.loads(response.read())


def query(expression: str) -> list[Any]:
    """Query actual Prometheus samples, not a local registry substitute."""
    return fetch("http://127.0.0.1:9090/api/v1/query?" + urlencode({"query": expression}))["data"][
        "result"
    ]


def wait_for(predicate: Any, label: str, timeout: int = 100) -> Any:
    """Bound asynchronous scrape/evaluate/webhook timing; log meaningful transitions."""
    print("WAIT " + label, flush=True)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            print("PASS " + label, flush=True)
            return value
        time.sleep(1)
    raise TimeoutError(label)


def main() -> None:
    """Retain a summary JSON and metrics snapshots even on failed acceptance."""
    run_id = uuid4().hex
    started = datetime.now(UTC)
    directory = Path("data/observability_runs") / run_id
    directory.mkdir(parents=True)
    env = FaultEnvironment(run_id, directory)
    ingest_metrics, quality_metrics, state_metrics = Metrics(), get_metrics(), Metrics()
    endpoints: list[Any] = []
    server: Any = None
    collector: Any = None
    stop = threading.Event()
    poller: Any = None
    summary: dict[str, Any] = {"run_id": run_id, "checks": {}, "error": None}
    checks = summary["checks"]
    archive_path: Path | None = None
    original: bytes | None = None
    try:
        env.create()
        # Only this run's nested archive path is eligible for the test scanner.
        archive_root = get_settings().raw_archive_dir / ("phase5-" + run_id)
        settings = env.settings.model_copy(update={"raw_archive_dir": archive_root})
        collector = StateCollector(settings, state_metrics)
        endpoints.append(start_metrics_endpoint(8000, metrics=ingest_metrics))
        endpoints.append(start_metrics_endpoint(8001, metrics=quality_metrics))
        server = make_server("0.0.0.0", 8002, state_metrics, settings.observability_webhook_token)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        def refresh() -> None:
            while not stop.is_set():
                errors = collector.poll()
                if errors:
                    print("PROBE " + str(errors), flush=True)
                stop.wait(1)

        poller = threading.Thread(target=refresh, daemon=True)
        poller.start()
        wait_for(
            lambda: len(query('up{job=~"ingestor|quality|observability"} == 1')) == 3,
            "three scrape targets UP",
        )
        health = fetch("http://127.0.0.1:3000/api/health")
        checks["grafana_healthy"] = health["database"] == "ok"
        dashboard = fetch("http://127.0.0.1:3000/api/dashboards/uid/wikimedia-pipeline")
        checks["dashboard_provisioned"] = len(dashboard["dashboard"]["panels"]) == 16
        print("PASS Grafana dashboard 16 panels", flush=True)

        # Real runner/producer; finite source avoids public stream dependency.
        payloads = [env.payload() for _ in range(120)]
        producer = WikimediaKafkaProducer(
            default_topic=settings.kafka_topic_raw, auto_create_topic=False
        )
        archiver = RawArchiver(base_dir=archive_root, batch_size=500)

        class FiniteSource(WikimediaSSEClient):
            def __init__(self) -> None:
                self._is_running = True

            def stop(self) -> None:
                self._is_running = False

            def stream_events(self) -> Generator[RawStreamEvent, None, None]:
                for raw in payloads:
                    yield RawStreamEvent(
                        None, "message", json.loads(raw), raw.decode(), time.time()
                    )

        env.keys.extend(quality_key(raw) for raw in payloads)
        runner = IngestionRunner(
            sse_client=FiniteSource(),
            kafka_producer=producer,
            raw_archiver=archiver,
            metrics=ingest_metrics,
            auto_handle_signals=False,
        )
        runner.run(max_events=120)
        with get_db_connection() as conn:
            rows = conn.execute(
                "SELECT manifest_id,file_path FROM archive_manifests WHERE file_path LIKE %s",
                (archive_root.as_posix() + "/%",),
            ).fetchall()
        env.manifests.extend(row["manifest_id"] for row in rows)
        if not rows:
            raise RuntimeError("Synthetic archive manifest missing")
        archive_path = Path(rows[0]["file_path"])
        original = archive_path.read_bytes()
        checks["ingested_120"] = (
            runner.stats["received"] == 120 and producer.stats["delivered"] == 120
        )
        wait_for(
            lambda: query("sum(wikimedia_consumer_lag_records) > 100"), "measured Kafka backlog"
        )
        wait_for(
            lambda: query('ALERTS{alertname="ConsumerLagHigh",alertstate="firing"}'),
            "ConsumerLagHigh firing",
        )

        def incident(name: str, status: str) -> Any:
            with get_db_connection() as conn:
                return conn.execute(
                    """SELECT incident_id,status,occurrence_count FROM incidents
                    WHERE incident_type=%s AND status=%s AND detected_at >= %s
                    ORDER BY detected_at DESC LIMIT 1""",
                    (name, status, started),
                ).fetchone()

        lag_incident = wait_for(lambda: incident("ConsumerLagHigh", "OPEN"), "lag incident OPEN")
        summary["lag_incident"] = lag_incident
        checks["lag_alert_to_incident"] = True
        recover = time.monotonic()
        stats = QualityConsumer(
            settings=settings, metrics=quality_metrics, auto_create_topics=False
        ).run(max_messages=120, max_duration_sec=60)
        quality_metrics.recovery.labels("consumer-lag-surge").observe(time.monotonic() - recover)
        summary["consumer_stats"] = stats
        checks["consumer_120_acknowledged"] = stats["processed"] == 120 and env.committed() == 120
        wait_for(lambda: query("sum(wikimedia_consumer_lag_records) == 0"), "Kafka lag zero")
        wait_for(lambda: incident("ConsumerLagHigh", "RESOLVED"), "lag incident auto-resolved")
        checks["lag_auto_resolved"] = True
        wait_for(
            lambda: query('sum(wikimedia_consumer_throughput_total{status="accepted"}) >= 120'),
            "acknowledged throughput scraped",
        )
        checks["scraped_real_workload"] = True

        # Real one-byte corruption detected by the live collector -> Prometheus -> Alertmanager.
        archive_path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
        wait_for(
            lambda: query('ALERTS{alertname="ArchiveChecksumMismatch",alertstate="firing"}'),
            "checksum alert firing",
        )
        checksum = wait_for(
            lambda: incident("ArchiveChecksumMismatch", "OPEN"), "checksum incident OPEN"
        )
        summary["checksum_incident"] = checksum
        archive_path.write_bytes(original)
        wait_for(
            lambda: incident("ArchiveChecksumMismatch", "RESOLVED"),
            "checksum incident auto-resolved",
        )
        checks["checksum_alert_resolved"] = True
        checks["three_targets_up"] = (
            len(query('up{job=~"ingestor|quality|observability"} == 1')) == 3
        )
        summary["prometheus_rules"] = fetch("http://127.0.0.1:9090/api/v1/rules")
        summary["targets"] = fetch("http://127.0.0.1:9090/api/v1/targets")
        for port in (8000, 8001, 8002):
            with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
                (directory / f"metrics-{port}.txt").write_bytes(response.read())
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if archive_path is not None and original is not None:
            archive_path.write_bytes(original)
        stop.set()
        if poller:
            poller.join(timeout=30)
        if collector:
            collector.close()
        if server:
            server.shutdown()
            server.server_close()
        for endpoint in endpoints:
            endpoint[0].shutdown()
            endpoint[0].server_close()
            endpoint[1].join(timeout=3)
        try:
            env.cleanup()
            checks["owned_resources_cleaned"] = True
        except Exception as exc:
            checks["owned_resources_cleaned"] = False
            summary["cleanup_error"] = str(exc)
        summary["passed"] = not summary["error"] and bool(checks) and all(checks.values())
        (directory / "report.json").write_text(
            json.dumps(summary, indent=2, default=str), encoding="utf-8"
        )
        print(f"Evidence: {directory / 'report.json'}", flush=True)
    if not summary["passed"]:
        raise RuntimeError("Acceptance checks failed")


def quality_key(raw: bytes) -> str:
    """Track only owned keys for exact cleanup."""
    from src.quality.engine import QualityEngine

    return QualityEngine().prepare_payload(raw).summary.event_key


if __name__ == "__main__":
    main()
