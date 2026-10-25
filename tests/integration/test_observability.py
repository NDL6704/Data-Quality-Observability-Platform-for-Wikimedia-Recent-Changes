"""Real DB incident episodes and real Kafka lag; webhooks via loopback HTTP."""

import json
import threading
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import psycopg
import pytest

from src.faults.environment import FaultEnvironment
from src.ingestion.raw_archiver import RawArchiver
from src.observability.collector import StateCollector
from src.observability.incidents import IncidentManager, parse_notifications
from src.observability.metrics import Metrics
from src.observability.service import make_server
from src.storage.db import get_db_connection
from tests.unit.test_incident_notifications import notification

pytestmark = pytest.mark.integration


@pytest.fixture
def episode() -> Generator[dict[str, Any], None, None]:
    body = notification(instance="test-" + uuid4().hex)
    yield body
    with get_db_connection() as conn:
        conn.execute(
            "DELETE FROM incidents WHERE alert_labels->>'instance'=%s",
            (body["alerts"][0]["labels"]["instance"],),
        )


def test_concurrent_dedup_and_terminal_resolve(episode: dict[str, Any]) -> None:
    manager = IncidentManager()
    items = parse_notifications(episode)
    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(lambda _: manager.apply(items), range(4)))
    assert len({row[0] for row in ids}) == 1
    key = ids[0][0]
    assert manager.investigate(key, "operator investigating")
    episode["alerts"][0]["status"] = "resolved"
    manager.apply(parse_notifications(episode))
    episode["alerts"][0]["status"] = "firing"
    manager.apply(parse_notifications(episode))  # Delayed notification cannot reopen.
    with get_db_connection() as conn:
        row = conn.execute("SELECT * FROM incidents WHERE incident_id=%s", (key,)).fetchone()
    assert row is not None and row["status"] == "RESOLVED" and row["occurrence_count"] == 6
    assert row["root_cause"] == "operator investigating"


def test_old_resolve_does_not_close_new_episode(episode: dict[str, Any]) -> None:
    manager = IncidentManager()
    old = parse_notifications(episode)
    manager.apply(old)
    episode["alerts"][0]["startsAt"] = "2026-10-25T01:00:00Z"
    new = parse_notifications(episode)
    manager.apply(new)
    episode["alerts"][0].update(status="resolved", startsAt="2026-10-25T00:00:00Z")
    manager.apply(parse_notifications(episode))
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT status FROM incidents WHERE incident_id=%s", (new[0].incident_id,)
        ).fetchone()
    assert row is not None and row["status"] == "OPEN"


def test_resolved_first_then_delayed_firing(episode: dict[str, Any]) -> None:
    episode["alerts"][0]["status"] = "resolved"
    manager = IncidentManager()
    key = manager.apply(parse_notifications(episode))[0]
    episode["alerts"][0]["status"] = "firing"
    manager.apply(parse_notifications(episode))
    with get_db_connection() as conn:
        row = conn.execute("SELECT status FROM incidents WHERE incident_id=%s", (key,)).fetchone()
    assert row is not None and row["status"] == "RESOLVED"


def test_http_webhook_auth_and_persistence(episode: dict[str, Any]) -> None:
    metrics = Metrics()
    server = make_server("127.0.0.1", 0, metrics, "test-token")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        request = Request(base + "/alerts", data=json.dumps(episode).encode(), method="POST")
        with pytest.raises(HTTPError) as error:
            urlopen(request, timeout=5)
        assert error.value.code == 401
        request.add_header("Authorization", "Bearer test-token")
        with urlopen(request, timeout=5) as response:
            assert json.loads(response.read())["incident_ids"]
        with patch.object(
            IncidentManager, "apply", side_effect=psycopg.OperationalError("injected")
        ):
            with pytest.raises(HTTPError) as failed:
                urlopen(request, timeout=5)
            assert failed.value.code == 503
        assert (
            metrics.registry.get_sample_value(
                "wikimedia_database_errors_total", {"operation": "incident"}
            )
            == 1
        )
        with urlopen(base + "/metrics", timeout=5) as response:
            assert b"wikimedia_active_incidents_count" in response.read()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_lag_reads_durable_offsets_without_ack(tmp_path: Path) -> None:
    env = FaultEnvironment(uuid4().hex, tmp_path)
    collector = StateCollector(env.settings, Metrics())
    try:
        env.create()
        env.produce([env.payload()] * 3)
        collector.poll_kafka()
        assert (
            collector.metrics.registry.get_sample_value(
                "wikimedia_consumer_lag_records",
                {"topic": env.settings.kafka_topic_raw, "partition": "0"},
            )
            == 3
        )
        assert env.committed() < 0
        env.consume(3)
        collector.poll_kafka()
        assert (
            collector.metrics.registry.get_sample_value(
                "wikimedia_consumer_lag_records",
                {"topic": env.settings.kafka_topic_raw, "partition": "0"},
            )
            == 0
        )
    finally:
        collector.close()
        env.cleanup()


def test_archive_detector_persists_and_recovers(tmp_path: Path) -> None:
    archiver = RawArchiver(base_dir=tmp_path, batch_size=1)
    manifest = archiver.append({"test": str(uuid4())})
    assert manifest is not None
    from src.common.config import Settings

    collector = StateCollector(Settings(RAW_ARCHIVE_DIR=tmp_path), Metrics())
    path = Path(manifest.file_path)
    original = path.read_bytes()
    try:
        path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
        collector.check_archives()
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT status FROM archive_manifests WHERE manifest_id=%s", (manifest.manifest_id,)
            ).fetchone()
        assert row is not None and row["status"] == "CORRUPTED"
        path.write_bytes(original)
        collector.check_archives()
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT status FROM archive_manifests WHERE manifest_id=%s", (manifest.manifest_id,)
            ).fetchone()
        assert row is not None and row["status"] == "COMMITTED"
    finally:
        collector.close()
        with get_db_connection() as conn:
            conn.execute(
                "DELETE FROM archive_manifests WHERE manifest_id=%s", (manifest.manifest_id,)
            )
