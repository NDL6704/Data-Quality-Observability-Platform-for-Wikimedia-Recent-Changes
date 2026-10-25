"""Ordering and failure-boundary tests; Kafka/DB adapters are controlled fakes."""

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import psycopg
import pytest
from confluent_kafka import KafkaError, KafkaException

from src.common.config import Settings
from src.observability.metrics import Metrics
from src.processing.consumer import QualityConsumer, build_cli_parser, build_consumer_config
from src.storage.repository import PersistenceOutcome

NOW = datetime(2026, 10, 25, 0, tzinfo=UTC)


def test_metrics_acknowledged_once_after_db_retry(harness: Any) -> None:
    harness.runner.metrics = Metrics()
    payload = json.loads(harness.message.value())
    payload["title"] = None
    harness.message.value.return_value = json.dumps(payload).encode()
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.side_effect = [
            psycopg.OperationalError("injected"),
            PersistenceOutcome("test", duplicate=True),
        ]
        harness.runner.process_message(harness.message)
    registry = harness.runner.metrics.registry
    assert (
        registry.get_sample_value("wikimedia_consumer_throughput_total", {"status": "accepted"})
        == 1
    )
    assert (
        registry.get_sample_value(
            "wikimedia_database_errors_total", {"operation": "quality_transaction"}
        )
        == 1
    )
    assert (
        registry.get_sample_value(
            "wikimedia_quality_rule_failures_total", {"rule_id": "R-CMP-02", "severity": "MEDIUM"}
        )
        == 1
    )
    assert registry.get_sample_value("wikimedia_duplicates_detected_total", {"wiki": "viwiki"}) == 1
    assert (
        registry.get_sample_value("wikimedia_processing_latency_seconds_count", {"stage": "db"})
        == 2
    )


def test_metrics_do_not_count_failed_ack_as_throughput(harness: Any) -> None:
    harness.runner.metrics = Metrics()
    harness.consumer.commit.side_effect = KafkaException(KafkaError(KafkaError._FAIL, "injected"))
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        with pytest.raises(KafkaException):
            harness.runner.process_message(harness.message)
    assert (
        harness.runner.metrics.registry.get_sample_value(
            "wikimedia_consumer_throughput_total", {"status": "accepted"}
        )
        == 0
    )


def test_missing_receipt_does_not_claim_original_ingestion_delay(harness: Any) -> None:
    harness.runner.metrics = Metrics()
    harness.message.headers.return_value = []
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        harness.runner.process_message(harness.message)
    assert (
        harness.runner.metrics.registry.get_sample_value(
            "wikimedia_ingestion_delay_seconds_count", {"wiki": "viwiki"}
        )
        is None
    )


@pytest.fixture
def harness() -> Any:
    events: list[str] = []
    consumer = Mock()
    consumer.assignment.return_value = ["partition0"]

    def commit(**kwargs: Any) -> list[Any]:
        events.append("offset")
        return [SimpleNamespace(error=None)]

    consumer.commit.side_effect = commit
    producer = Mock()
    producer.flush.return_value = 0

    def produce(**kwargs: Any) -> None:
        events.append("sink")
        kwargs["on_delivery"](None, None)

    producer.produce.side_effect = produce

    @contextmanager
    def connection() -> Any:
        events.append("begin")
        try:
            yield Mock()
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("db_commit")

    settings = Settings(QUALITY_MAX_RETRIES=1, QUALITY_RETRY_BACKOFF_SEC=0)
    runner = QualityConsumer(
        settings=settings, consumer=consumer, producer=producer, connection_factory=connection
    )
    message = Mock()
    message.topic.return_value = "rc.raw.v1"
    message.partition.return_value = 0
    message.offset.return_value = 4
    message.key.return_value = b"source-key"
    message.headers.return_value = [("received_at_epoch", str(NOW.timestamp()).encode())]
    message.error.return_value = None
    payload = {
        "meta": {"id": "consumer-test", "dt": NOW.isoformat(), "stream": "mediawiki.recentchange"},
        "type": "edit",
        "wiki": "viwiki",
        "server_name": "vi.wikipedia.org",
        "title": "Test",
    }
    message.value.return_value = json.dumps(payload).encode()
    return SimpleNamespace(
        runner=runner, consumer=consumer, producer=producer, message=message, events=events
    )


def test_manual_offset_configuration() -> None:
    config = build_consumer_config(Settings())
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False


def test_db_commit_then_confirmed_sink_then_offset(harness: Any) -> None:
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("viwiki:consumer-test")
        harness.runner.process_message(harness.message)
    assert harness.events == ["begin", "db_commit", "sink", "offset"]
    prepared = repo.return_value.persist_prepared.call_args.args[0]
    assert prepared.received_at == NOW
    assert harness.runner.stats["processed"] == 1
    body = json.loads(harness.producer.produce.call_args.kwargs["value"])
    assert body["source"]["delivery_id"] == "rc.raw.v1:0:4"
    assert body["payload"]["received_timestamp"] == NOW.isoformat().replace("+00:00", "Z")


def test_outer_commit_failure_never_publishes_or_acks(harness: Any) -> None:
    @contextmanager
    def failing_commit() -> Any:
        yield Mock()
        raise psycopg.OperationalError("commit connection lost")

    harness.runner._connection_factory = failing_commit
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        with pytest.raises(psycopg.OperationalError):
            harness.runner.process_message(harness.message)
    harness.producer.produce.assert_not_called()
    harness.consumer.commit.assert_not_called()
    assert harness.runner.stats["db_errors"] == 2


def test_db_retry_does_not_repeat_rule_evaluation(harness: Any) -> None:
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.side_effect = [
            psycopg.OperationalError("down"),
            PersistenceOutcome("test"),
        ]
        with patch.object(
            harness.runner._engine, "prepare_payload", wraps=harness.runner._engine.prepare_payload
        ) as prepare:
            harness.runner.process_message(harness.message)
            assert prepare.call_count == 1
    assert harness.events == ["begin", "rollback", "begin", "db_commit", "sink", "offset"]
    harness.consumer.pause.assert_called_once()
    harness.consumer.resume.assert_called_once()


def test_publish_failure_keeps_db_but_never_acks(harness: Any) -> None:
    harness.producer.produce.side_effect = BufferError("queue full")
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        with pytest.raises(BufferError):
            harness.runner.process_message(harness.message)
        assert repo.return_value.persist_prepared.call_count == 1
    harness.consumer.commit.assert_not_called()
    assert harness.runner.stats["publish_errors"] == 2


@pytest.mark.parametrize("failure", ["timeout", "callback", "missing_callback"])
def test_delivery_must_be_confirmed(harness: Any, failure: str) -> None:
    if failure == "timeout":
        harness.producer.flush.return_value = 1
    elif failure == "callback":
        harness.producer.produce.side_effect = lambda **kw: kw["on_delivery"](
            KafkaError(KafkaError._MSG_TIMED_OUT, "failed"), None
        )
    else:
        harness.producer.produce.side_effect = None
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        with pytest.raises((TimeoutError, KafkaException)):
            harness.runner.process_message(harness.message)
    harness.consumer.commit.assert_not_called()


def test_commit_retry_does_not_repeat_db_or_sink(harness: Any) -> None:
    harness.consumer.commit.side_effect = [
        KafkaException(KafkaError(KafkaError._FAIL)),
        [SimpleNamespace(error=None)],
    ]
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        harness.runner.process_message(harness.message)
        assert repo.return_value.persist_prepared.call_count == 1
    assert harness.producer.produce.call_count == 1 and harness.consumer.commit.call_count == 2


def test_partial_offset_error_is_not_success(harness: Any) -> None:
    harness.consumer.commit.return_value = [SimpleNamespace(error=KafkaError(KafkaError._FAIL))]
    harness.consumer.commit.side_effect = None
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        with pytest.raises(KafkaException):
            harness.runner.process_message(harness.message)
    assert harness.runner.stats["processed"] == 0


@pytest.mark.parametrize(
    "headers,reason",
    [
        ([], "receipt_missing"),
        ([("received_at_epoch", b"nan")], "receipt_invalid"),
        ([("received_at_epoch", b"bad")], "receipt_invalid"),
        ([("received_at_epoch", b"1"), ("received_at_epoch", b"2")], "receipt_invalid"),
    ],
)
def test_receipt_fallback_observable(harness: Any, headers: Any, reason: str) -> None:
    harness.message.headers.return_value = headers
    receipt, origin = harness.runner._receipt(harness.message)
    assert receipt.tzinfo == UTC and origin == reason
    assert harness.runner.stats[reason] == 1


@pytest.mark.parametrize("payload", [b"\xffbad", None])
def test_rejected_and_tombstone_go_to_dlq(harness: Any, payload: Any) -> None:
    harness.message.value.return_value = payload
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("bad", quarantine_id=1)
        harness.runner.process_message(harness.message)
    call = harness.producer.produce.call_args.kwargs
    assert call["topic"] == harness.runner.settings.kafka_topic_dlq
    body = json.loads(call["value"])
    assert body["payload"]["encoding"] == "base64"
    assert body["source"]["tombstone"] == (payload is None)


def test_exhaustion_stops_before_next_message_and_closes(harness: Any) -> None:
    harness.consumer.poll.return_value = harness.message
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.side_effect = psycopg.OperationalError("down")
        with pytest.raises(psycopg.OperationalError):
            harness.runner.run(max_messages=2)
    assert harness.consumer.poll.call_count == 1
    harness.consumer.close.assert_called_once()
    harness.consumer.commit.assert_not_called()


def test_stop_interrupts_retry_without_ack(harness: Any) -> None:
    harness.consumer.poll.return_value = harness.message

    def stop_on_pause(_: Any) -> None:
        harness.runner.stop()

    harness.consumer.pause.side_effect = stop_on_pause
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.side_effect = psycopg.OperationalError("down")
        result = harness.runner.run(max_messages=1)
    assert result["processed"] == 0
    harness.consumer.commit.assert_not_called()
    harness.consumer.close.assert_called_once()


def test_bounded_run_and_idempotent_close(harness: Any) -> None:
    harness.consumer.poll.return_value = harness.message
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        result = harness.runner.run(max_messages=1)
    harness.runner.close()
    assert result["processed"] == 1
    harness.consumer.close.assert_called_once()
    assert build_cli_parser().parse_args(["--max-messages", "2"]).max_messages == 2


def test_topics_must_be_distinct(harness: Any) -> None:
    settings = Settings(KAFKA_TOPIC_DLQ="rc.raw.v1")
    with pytest.raises(ValueError, match="distinct"):
        QualityConsumer(settings=settings, consumer=harness.consumer, producer=harness.producer)


@pytest.mark.parametrize("duration", [0, -1, float("nan"), float("inf")])
def test_invalid_run_limit_closes_resources(harness: Any, duration: float) -> None:
    with pytest.raises(ValueError):
        harness.runner.run(max_duration_sec=duration)
    harness.consumer.close.assert_called_once()
    harness.consumer.subscribe.assert_not_called()


def test_poll_error_retries_and_eof_is_not_a_record(harness: Any) -> None:
    error = Mock()
    error.error.return_value = KafkaError(KafkaError._TRANSPORT, "temporary broker outage")
    eof = Mock()
    eof.error.return_value = KafkaError(KafkaError._PARTITION_EOF)
    harness.consumer.poll.side_effect = [error, eof, harness.message]
    with patch("src.processing.consumer.QualityRepository") as repo:
        repo.return_value.persist_prepared.return_value = PersistenceOutcome("test")
        result = harness.runner.run(max_messages=1)
    assert result["poll_errors"] == 1 and result["processed"] == 1
    assert harness.consumer.commit.call_count == 1
