"""Real Kafka/PostgreSQL tests. Faults are injected boundary exceptions, not process kills."""

import json
import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import Settings
from src.processing.consumer import QualityConsumer, build_consumer_config
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection
from src.storage.repository import QualityRepository, decode_quarantine_payload

pytestmark = pytest.mark.integration


@pytest.fixture
def environment() -> Generator[Any, None, None]:
    suffix = uuid4().hex
    settings = Settings(
        KAFKA_TOPIC_RAW=f"test.quality.{suffix}.raw",
        KAFKA_TOPIC_VALIDATED=f"test.quality.{suffix}.valid",
        KAFKA_TOPIC_DLQ=f"test.quality.{suffix}.dlq",
        KAFKA_CONSUMER_GROUP_QUALITY=f"test.quality.{suffix}",
        QUALITY_MAX_RETRIES=0,
    )
    topics = [settings.kafka_topic_raw, settings.kafka_topic_validated, settings.kafka_topic_dlq]
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for future in admin.create_topics(
        [NewTopic(t, 1, 1) for t in topics], request_timeout=10
    ).values():
        future.result(timeout=15)
    state = SimpleNamespace(settings=settings, keys=[], topics=topics, admin=admin)
    try:
        yield state
    finally:
        with get_db_connection() as conn:
            for key in state.keys:
                conn.execute(
                    "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=%s", (key,)
                )
                conn.execute("DELETE FROM curated_changes WHERE event_key=%s", (key,))
                conn.execute("DELETE FROM quarantined_events WHERE event_key=%s", (key,))
        for future in admin.delete_topics(topics, request_timeout=10).values():
            future.result(timeout=15)
        try:
            admin.delete_consumer_groups([settings.kafka_consumer_group_quality])[
                settings.kafka_consumer_group_quality
            ].result(timeout=10)
        except KafkaException:
            pass  # A never-committed group may already be absent.


def _payload() -> tuple[bytes, datetime]:
    receipt = datetime.now(UTC) - timedelta(seconds=30)
    payload = {
        "meta": {
            "id": str(uuid4()),
            "dt": (receipt - timedelta(seconds=60)).isoformat(),
            "stream": "mediawiki.recentchange",
        },
        "wiki": "test_consumer",
        "type": "edit",
        "title": "Consumer E2E",
        "server_name": "example.org",
        "namespace": "abc",
    }
    return json.dumps(payload).encode(), receipt


def _produce(env: Any, values: list[bytes], receipt: datetime) -> None:
    producer = Producer({"bootstrap.servers": env.settings.kafka_bootstrap_servers})
    errors: list[Any] = []
    engine = QualityEngine()
    engine.register_rules(build_default_rule_set())
    for value in values:
        env.keys.append(engine.prepare_payload(value, received_at=receipt).summary.event_key)
        producer.produce(
            env.settings.kafka_topic_raw,
            value=value,
            partition=0,
            headers=[("received_at_epoch", str(receipt.timestamp()).encode())],
            on_delivery=lambda error, message: errors.append(error),
        )
    assert producer.flush(10) == 0
    assert len(errors) == len(values) and not any(errors)


def _committed(env: Any) -> int:
    consumer = Consumer(build_consumer_config(env.settings))
    try:
        return consumer.committed([TopicPartition(env.settings.kafka_topic_raw, 0)], timeout=10)[
            0
        ].offset
    finally:
        consumer.close()


def _outputs(env: Any, expected: int) -> list[dict[str, Any]]:
    config = build_consumer_config(env.settings)
    config["group.id"] += ".observer"
    consumer = Consumer(config)
    rows: list[dict[str, Any]] = []
    try:
        consumer.assign(
            [
                TopicPartition(env.settings.kafka_topic_validated, 0, 0),
                TopicPartition(env.settings.kafka_topic_dlq, 0, 0),
            ]
        )
        deadline = time.monotonic() + 15
        while len(rows) < expected and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is not None and message.error() is None:
                value = message.value()
                assert value is not None
                rows.append(
                    {
                        "topic": message.topic(),
                        "key": message.key(),
                        "body": json.loads(value),
                    }
                )
    finally:
        consumer.close()
    assert len(rows) == expected
    return rows


@pytest.mark.parametrize("pooled", [False, True])
def test_valid_duplicate_warning_and_invalid_end_to_end(environment: Any, pooled: bool) -> None:
    env = environment
    raw, receipt = _payload()
    invalid = b"\xff" + uuid4().hex.encode()
    _produce(env, [raw, raw, invalid], receipt)
    runner = QualityConsumer(reuse_connections=pooled, settings=env.settings)
    result = runner.run(max_messages=3, max_duration_sec=30)
    assert result["processed"] == 3 and result["curated"] == 2
    assert result["duplicates"] == 1 and result["quarantined"] == 1
    assert _committed(env) == 3
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM curated_changes WHERE event_key=%s", (env.keys[0],)
        ).fetchall()
        assert len(rows) == 1 and rows[0]["namespace"] is None
        assert (
            rows[0]["raw_payload"]["namespace"] == "abc"
            and rows[0]["received_timestamp"] == receipt
        )
        row = conn.execute(
            "SELECT * FROM quarantined_events WHERE event_key=%s", (env.keys[-1],)
        ).fetchone()
        assert row is not None and decode_quarantine_payload(row["raw_payload"]) == invalid
        assert row["raw_payload"]["source"]["offset"] == 2
        assert row["raw_payload"]["source"]["receipt_origin"] == "ingestion_header"
    outputs = _outputs(env, 3)
    assert sum(r["topic"] == env.settings.kafka_topic_validated for r in outputs) == 2
    dlq = next(r for r in outputs if r["topic"] == env.settings.kafka_topic_dlq)
    assert decode_quarantine_payload(dlq["body"]["payload"]) == invalid


@pytest.mark.parametrize("pooled", [False, True])
def test_before_db_commit_failure_rolls_back_and_redelivers(environment: Any, pooled: bool) -> None:
    env = environment
    raw, receipt = _payload()
    _produce(env, [raw], receipt)
    original = QualityRepository.persist_prepared

    def crash(self: Any, prepared: Any, **kwargs: Any) -> Any:
        original(self, prepared, **kwargs)
        raise psycopg.OperationalError("Injected crash before outer commit")

    runner = QualityConsumer(
        reuse_connections=pooled, settings=env.settings, auto_create_topics=False
    )
    with patch.object(QualityRepository, "persist_prepared", crash):
        with pytest.raises(psycopg.OperationalError):
            runner.run(max_messages=1, max_duration_sec=30)
    assert _committed(env) < 0
    with get_db_connection() as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM curated_changes WHERE event_key=%s", (env.keys[0],)
            ).fetchone()
            is None
        )
    resumed = QualityConsumer(
        reuse_connections=pooled, settings=env.settings, auto_create_topics=False
    )
    assert resumed.run(max_messages=1, max_duration_sec=30)["processed"] == 1
    assert _committed(env) == 1


def test_after_db_commit_offset_failure_redelivers_without_curated_duplicate(
    environment: Any,
) -> None:
    env = environment
    raw, receipt = _payload()
    _produce(env, [raw], receipt)
    real = Consumer(build_consumer_config(env.settings))
    failing = Mock(wraps=real)
    failing.commit.side_effect = KafkaException(KafkaError(KafkaError._FAIL, "injected"))
    runner = QualityConsumer(settings=env.settings, consumer=failing, auto_create_topics=False)
    with pytest.raises(KafkaException):
        runner.run(max_messages=1, max_duration_sec=30)
    assert _committed(env) < 0
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM curated_changes WHERE event_key=%s", (env.keys[0],)
        ).fetchone()
        assert row is not None and row["n"] == 1
    resumed = QualityConsumer(settings=env.settings, auto_create_topics=False)
    result = resumed.run(max_messages=1, max_duration_sec=30)
    assert result["processed"] == 1 and result["duplicates"] == 1
    assert _committed(env) == 1
    outputs = _outputs(env, 2)
    assert outputs[0]["key"] == outputs[1]["key"]
    assert (
        outputs[0]["body"]["source"]["delivery_id"] == outputs[1]["body"]["source"]["delivery_id"]
    )
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM curated_changes WHERE event_key=%s", (env.keys[0],)
        ).fetchone()
        assert row is not None and row["n"] == 1


@pytest.mark.parametrize("pooled", [False, True])
def test_dlq_failure_keeps_offset_and_recovers_on_restart(environment: Any, pooled: bool) -> None:
    env = environment
    _, receipt = _payload()
    invalid = b"\xff" + uuid4().hex.encode()
    _produce(env, [invalid], receipt)
    failed_sink = Mock()
    failed_sink.produce.side_effect = BufferError("injected DLQ unavailable")
    failed_sink.flush.return_value = 0
    runner = QualityConsumer(
        reuse_connections=pooled,
        settings=env.settings,
        producer=failed_sink,
        auto_create_topics=False,
    )
    with pytest.raises(BufferError):
        runner.run(max_messages=1, max_duration_sec=30)
    assert _committed(env) < 0
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM quarantined_events WHERE event_key=%s", (env.keys[0],)
        ).fetchone()
        assert row is not None and row["n"] == 1
    resumed = QualityConsumer(
        reuse_connections=pooled, settings=env.settings, auto_create_topics=False
    )
    assert resumed.run(max_messages=1, max_duration_sec=30)["processed"] == 1
    assert _committed(env) == 1
    output = _outputs(env, 1)[0]
    assert output["topic"] == env.settings.kafka_topic_dlq
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM quarantined_events WHERE event_key=%s", (env.keys[0],)
        ).fetchone()
        assert row is not None and row["n"] == 2  # Append-only attempts, documented policy.


@pytest.mark.parametrize(
    "scenario,accepted,failure_code",
    [
        ("clean", True, None),
        ("missing_title", True, "ERR_MISSING_TITLE"),
        ("missing_server", False, "ERR_MISSING_IDENTITY"),
        ("missing_meta", False, "ERR_MISSING_ENVELOPE"),
        ("truncated_json", False, "ERR_INVALID_JSON"),
        ("invalid_timestamp", False, "ERR_INVALID_TIMESTAMP"),
    ],
)
def test_phase3_routing_acceptance(
    environment: Any,
    scenario: str,
    accepted: bool,
    failure_code: str | None,
) -> None:
    """Acceptance matrix uses the complete Kafka -> DB -> sink -> offset path."""
    env = environment
    raw, receipt = _payload()
    payload = json.loads(raw)
    payload["namespace"] = 0
    if scenario == "missing_title":
        payload["title"] = None
    elif scenario == "missing_server":
        payload["server_name"] = None
    elif scenario == "missing_meta":
        del payload["meta"]
    elif scenario == "invalid_timestamp":
        payload["meta"]["dt"] = "invalid-date"
    raw = json.dumps(payload).encode()
    if scenario == "truncated_json":
        raw = raw[:-1]  # Unique payload, real truncated JSON, not just invalid UTF-8.
    _produce(env, [raw], receipt)
    runner = QualityConsumer(settings=env.settings, auto_create_topics=False)
    result = runner.run(max_messages=1, max_duration_sec=30)
    assert result["processed"] == 1 and result["receipt_missing"] == 0
    assert result["receipt_invalid"] == 0 and _committed(env) == 1
    output = _outputs(env, 1)[0]
    expected_topic = (
        env.settings.kafka_topic_validated if accepted else env.settings.kafka_topic_dlq
    )
    assert output["topic"] == expected_topic
    results = output["body"]["quality_results"]
    if failure_code is None:
        assert len(results) == 10 and all(r["passed"] for r in results)
    else:
        assert failure_code in [r["failure_code"] for r in results]
    with get_db_connection() as conn:
        curated = conn.execute(
            "SELECT * FROM curated_changes WHERE event_key=%s", (env.keys[0],)
        ).fetchone()
        quarantined = conn.execute(
            "SELECT * FROM quarantined_events WHERE event_key=%s", (env.keys[0],)
        ).fetchall()
        assert (curated is not None) == accepted
        if accepted:
            assert not quarantined and curated is not None
            assert curated["received_timestamp"] == receipt
            assert curated["raw_payload"] == payload
        else:
            assert len(quarantined) == 1
            assert failure_code in quarantined[0]["failure_codes"]
            assert failure_code in [r["failure_code"] for r in quarantined[0]["failure_details"]]
            assert decode_quarantine_payload(quarantined[0]["raw_payload"]) == raw


def test_phase3_hash_fallback_redelivery_acceptance(environment: Any) -> None:
    """Missing both IDs must stay idempotent through the real consumer path."""
    env = environment
    raw, receipt = _payload()
    payload = json.loads(raw)
    payload["namespace"] = 0
    payload["meta"]["id"] = None
    payload["id"] = None
    raw = json.dumps(payload).encode()
    _produce(env, [raw, raw], receipt)
    runner = QualityConsumer(settings=env.settings, auto_create_topics=False)
    result = runner.run(max_messages=2, max_duration_sec=30)
    assert result["processed"] == 2 and result["duplicates"] == 1
    assert result["quarantined"] == 0 and _committed(env) == 2
    assert ":hash_" in env.keys[0] and env.keys[0] == env.keys[1]
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM curated_changes WHERE event_key=%s", (env.keys[0],)
        ).fetchall()
        assert len(rows) == 1 and rows[0]["event_key"] == env.keys[0]
        ledger = conn.execute(
            "SELECT * FROM quality_results WHERE rule_id='R-DED-02' "
            "AND failure_details->0->>'event_key'=%s",
            (env.keys[0],),
        ).fetchall()
        assert sorted(row["rows_failed"] for row in ledger) == [0, 1]
    outputs = _outputs(env, 2)
    assert all(row["topic"] == env.settings.kafka_topic_validated for row in outputs)
    assert {row["body"]["event_key"] for row in outputs} == {env.keys[0]}
