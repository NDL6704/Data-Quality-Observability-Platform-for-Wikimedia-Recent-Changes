"""Serial at-least-once quality consumer: DB -> confirmed sink -> offset commit.

No atomic transaction spans PostgreSQL and Kafka. A restart after DB/sink success
may repeat audit rows or sink messages; deterministic source identity enables
reconciliation. A failed record is never skipped to acknowledge a later offset.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import signal
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from threading import Event
from typing import Any, TypeVar

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from src.common.config import Settings, get_settings
from src.common.logging import setup_logger
from src.observability.metrics import (
    FAILURE_CODES,
    RULE_IDS,
    Metrics,
    get_metrics,
    start_metrics_endpoint,
    wiki_label,
)
from src.quality.engine import PreparedEvaluation, QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection
from src.storage.repository import PersistenceOutcome, QualityRepository, encode_quarantine_payload

logger = setup_logger("quality.consumer")
T = TypeVar("T")


def build_consumer_config(settings: Settings) -> dict[str, Any]:
    """Disable both automatic commit and automatic offset storage."""
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group_quality,
        "auto.offset.reset": settings.kafka_auto_offset_reset,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 600000,
        "session.timeout.ms": 45000,
        "socket.timeout.ms": 10000,
    }


def ensure_quality_topics(settings: Settings) -> None:
    """Create missing raw/validated/DLQ topics, fail visibly on admin errors."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = [
        (settings.kafka_topic_raw, 86400000),
        (settings.kafka_topic_validated, 172800000),
        (settings.kafka_topic_dlq, 604800000),
    ]
    futures = admin.create_topics(
        [
            NewTopic(
                name,
                num_partitions=3,
                replication_factor=1,
                config={"retention.ms": str(retention)},
            )
            for name, retention in topics
        ],
        request_timeout=10,
    )
    for future in futures.values():
        try:
            future.result(timeout=15)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


class QualityConsumer:
    """Process one message at a time with bounded, stage-local retries.

    Backoff pauses all assigned partitions and does not poll past the pending
    record. librdkafka heartbeats run in the background. Retry bounds, DB timeouts
    and delivery deadlines keep normal work below max.poll.interval; on exhaustion
    stop/close without ack, letting the group redeliver. Rebalance/commit errors
    also fail safely rather than advancing offsets past unprocessed records.
    """

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        consumer: Any = None,
        producer: Any = None,
        engine: QualityEngine | None = None,
        connection_factory: Callable[
            [], AbstractContextManager[psycopg.Connection[Any]]
        ] = get_db_connection,
        auto_create_topics: bool = True,
        metrics: Metrics | None = None,
        reuse_connections: bool = False,
    ) -> None:
        self.settings = settings or get_settings()
        self.metrics = metrics or get_metrics()
        topics = {
            self.settings.kafka_topic_raw,
            self.settings.kafka_topic_validated,
            self.settings.kafka_topic_dlq,
        }
        if len(topics) != 3 or any(not name.strip() for name in topics):
            raise ValueError("raw, validated and DLQ topics must be distinct nonblank names")
        if not self.settings.kafka_consumer_group_quality.strip():
            raise ValueError("Consumer group must be nonblank")
        if auto_create_topics and consumer is None and producer is None:
            ensure_quality_topics(self.settings)
        self._consumer = (
            consumer if consumer is not None else Consumer(build_consumer_config(self.settings))
        )
        self._producer = (
            producer
            if producer is not None
            else Producer(
                {
                    "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                    "enable.idempotence": True,
                    "acks": "all",
                    "linger.ms": 0,
                    "delivery.timeout.ms": self.settings.quality_delivery_timeout_sec * 1000,
                    "request.timeout.ms": min(
                        5000, self.settings.quality_delivery_timeout_sec * 1000
                    ),
                }
            )
        )
        self._engine = engine if engine is not None else QualityEngine()
        if engine is None:
            self._engine.register_rules(build_default_rule_set())
        self._connection_factory = connection_factory
        self._pool: ConnectionPool[psycopg.Connection[Any]] | None = None
        if reuse_connections:
            if connection_factory is not get_db_connection:
                raise ValueError("Connection reuse requires the default connection factory")
            cfg = self.settings
            self._pool = ConnectionPool(
                make_conninfo(
                    host=cfg.postgres_host,
                    port=cfg.postgres_port,
                    dbname=cfg.postgres_db,
                    user=cfg.postgres_user,
                    password=cfg.postgres_password,
                    connect_timeout=5,
                ),
                min_size=0,
                max_size=2,
                timeout=5,
                kwargs={"row_factory": dict_row},
                open=True,
            )
            self._connection_factory = self._pool.connection
        self._stop = Event()
        self._closed = False
        self.stats = dict.fromkeys(
            (
                "processed",
                "db_committed",
                "curated",
                "quarantined",
                "duplicates",
                "retries",
                "db_errors",
                "publish_errors",
                "commit_errors",
                "poll_errors",
                "receipt_missing",
                "receipt_invalid",
            ),
            0,
        )

    def stop(self) -> None:
        """Finish an in-flight success, or interrupt a retry backoff without ack."""
        self._stop.set()

    def _receipt(self, message: Any) -> tuple[datetime, str]:
        values = [value for name, value in (message.headers() or []) if name == "received_at_epoch"]
        if len(values) == 1 and values[0] is not None:
            try:
                epoch = float(values[0])
                if not math.isfinite(epoch):
                    raise ValueError("Non-finite receipt")
                return datetime.fromtimestamp(epoch, UTC), "ingestion_header"
            except (ValueError, TypeError, OverflowError, OSError):
                pass
        reason = "receipt_missing" if not values else "receipt_invalid"
        self.stats[reason] += 1
        self.metrics.receipt_fallback.labels(reason).inc()
        logger.warning(
            "%s at %s[%s]:%s; using processing-time approximation",
            reason,
            message.topic(),
            message.partition(),
            message.offset(),
        )
        return datetime.now(UTC), reason

    def _source_metadata(self, message: Any, receipt_origin: str) -> dict[str, Any]:
        key = message.key()
        return {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "consumer_group": self.settings.kafka_consumer_group_quality,
            "delivery_id": f"{message.topic()}:{message.partition()}:{message.offset()}",
            "receipt_origin": receipt_origin,
            "tombstone": message.value() is None,
            "key_base64": base64.b64encode(key).decode("ascii") if key is not None else None,
            "headers": [
                [name, base64.b64encode(value).decode("ascii") if value is not None else None]
                for name, value in (message.headers() or [])
            ],
        }

    def _retry(self, stage: str, operation: Callable[[], T]) -> T:
        for attempt in range(self.settings.quality_max_retries + 1):
            started = time.monotonic()
            try:
                result = operation()
            except (psycopg.Error, KafkaException, BufferError, TimeoutError):
                self.metrics.latency.labels(stage).observe(time.monotonic() - started)
                self.stats[f"{stage}_errors"] += 1
                self.metrics.stage_errors.labels(stage).inc()
                if stage == "db":
                    self.metrics.db_errors.labels("quality_transaction").inc()
                if attempt == self.settings.quality_max_retries or self._stop.is_set():
                    raise
                self.stats["retries"] += 1
                self.metrics.retries.labels(stage).inc()
                delay = min(self.settings.quality_retry_backoff_sec * (2**attempt), 5.0)
                logger.warning("Retry stage=%s attempt=%s delay=%.2fs", stage, attempt + 1, delay)
                assigned = self._consumer.assignment()
                if assigned:
                    self._consumer.pause(assigned)
                try:
                    if self._stop.wait(delay):
                        raise InterruptedError("Stopped before retry; offset remains uncommitted")
                finally:
                    if assigned:
                        self._consumer.resume(assigned)
            else:
                self.metrics.latency.labels(stage).observe(time.monotonic() - started)
                return result
        raise AssertionError("Unreachable retry state")

    def _persist(self, prepared: PreparedEvaluation, source: dict[str, Any]) -> PersistenceOutcome:
        with self._connection_factory() as conn:
            conn.execute("SELECT set_config('statement_timeout', '10000', true)")
            conn.execute("SELECT set_config('lock_timeout', '5000', true)")
            outcome = QualityRepository(conn).persist_prepared(prepared, source_metadata=source)
        # Exiting the outer context is part of this operation: commit failures propagate.
        return outcome

    def _publish(self, prepared: PreparedEvaluation, source: dict[str, Any]) -> None:
        rejected = prepared.summary.should_quarantine
        topic = self.settings.kafka_topic_dlq if rejected else self.settings.kafka_topic_validated
        body = {
            "format": "wikimedia-dq.output",
            "version": 1,
            "source": source,
            "event_key": prepared.summary.event_key,
            "received_at": prepared.received_at.isoformat(),
            "quality_results": [r.to_dict() for r in prepared.summary.results],
            "payload": encode_quarantine_payload(
                prepared.source_payload, received_at=prepared.received_at, source_metadata=source
            )
            if rejected
            else prepared.to_curated_record().model_dump(mode="json"),
        }
        deliveries: list[Any] = []

        def delivered(error: Any, message: Any) -> None:
            deliveries.append(error)

        self._producer.produce(
            topic=topic,
            key=source["delivery_id"].encode("utf-8"),
            value=json.dumps(body, allow_nan=False).encode("utf-8"),
            on_delivery=delivered,
        )
        remaining = self._producer.flush(self.settings.quality_delivery_timeout_sec)
        if remaining or not deliveries:
            raise TimeoutError("Sink delivery not confirmed; source offset remains uncommitted")
        if deliveries[0] is not None:
            raise KafkaException(deliveries[0])

    def _commit(self, message: Any) -> None:
        partitions = self._consumer.commit(message=message, asynchronous=False)
        if not partitions or any(part.error is not None for part in partitions):
            raise KafkaException(KafkaError(KafkaError._FAIL, "Source offset commit failed"))

    def process_message(self, message: Any) -> PersistenceOutcome:
        """Prepare once, commit DB, confirm sink delivery, then commit this offset."""
        started = time.monotonic()
        receipt, origin = self._receipt(message)
        source = self._source_metadata(message, origin)
        # Kafka tombstones are rejected empty input; source.tombstone preserves the distinction.
        prepared = self._engine.prepare_payload(message.value() or b"", received_at=receipt)
        wiki = wiki_label((prepared.raw_payload or {}).get("wiki"))
        for result in prepared.summary.results:
            if not result.passed:
                rule = result.rule_id if result.rule_id in RULE_IDS else "other"
                self.metrics.rule_failures.labels(rule, result.severity.value).inc()
        if prepared.event is not None and origin == "ingestion_header":
            delay = (receipt - prepared.event.meta.dt).total_seconds()
            if delay >= 0:
                self.metrics.ingestion_delay.labels(wiki).observe(delay)
            else:
                self.metrics.clock_skew.labels(wiki).inc()
        outcome = self._retry("db", lambda: self._persist(prepared, source))
        self.stats["db_committed"] += 1
        if origin == "ingestion_header":
            elapsed = (datetime.now(UTC) - receipt).total_seconds()
            if elapsed >= 0:
                self.metrics.latency.labels("receipt_to_db").observe(elapsed)
        self.stats["quarantined" if prepared.summary.should_quarantine else "curated"] += 1
        self.stats["duplicates"] += int(outcome.duplicate)
        if prepared.summary.should_quarantine:
            for code in prepared.summary.failure_codes:
                label = code if code in FAILURE_CODES else "other"
                self.metrics.quarantine.labels(label).inc()
        if outcome.duplicate:
            self.metrics.duplicates.labels(wiki).inc()
        self._retry("publish", lambda: self._publish(prepared, source))
        self._retry("commit", lambda: self._commit(message))
        self.stats["processed"] += 1
        status = "quarantined" if prepared.summary.should_quarantine else "accepted"
        self.metrics.throughput.labels(status).inc()
        self.metrics.latency.labels("total").observe(time.monotonic() - started)
        return outcome

    def _poll_one(self) -> Any:
        message = self._consumer.poll(0.5)
        if message is not None:
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    return None
                raise KafkaException(error)
        return message

    def run(
        self, *, max_messages: int | None = None, max_duration_sec: float | None = None
    ) -> dict[str, int]:
        """Run until stopped/bounded; any exhausted failure stops the whole loop."""
        start = time.monotonic()
        try:
            if (max_messages is not None and max_messages <= 0) or (
                max_duration_sec is not None
                and (not math.isfinite(max_duration_sec) or max_duration_sec <= 0)
            ):
                raise ValueError("Run limits must be finite and positive")
            self._consumer.subscribe([self.settings.kafka_topic_raw])
            while not self._stop.is_set():
                if max_messages is not None and self.stats["processed"] >= max_messages:
                    break
                if max_duration_sec is not None and time.monotonic() - start >= max_duration_sec:
                    break
                message = self._retry("poll", self._poll_one)
                if message is None:
                    continue
                self.process_message(message)
        except InterruptedError:
            if not self._stop.is_set():
                raise
        finally:
            self.close()
        return dict(self.stats)

    def close(self) -> None:
        """Release the group without auto-committing offsets; safe to call twice."""
        if not self._closed:
            self._closed = True
            try:
                self._consumer.close()
            finally:
                try:
                    self._producer.flush(5)
                finally:
                    if self._pool is not None:
                        self._pool.close()


def build_cli_parser() -> argparse.ArgumentParser:
    """Expose bounded runs for local verification, unbounded for service use."""
    parser = argparse.ArgumentParser(description="Wikimedia quality consumer")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--max-duration-sec", type=float)
    parser.add_argument("--workers", type=int, choices=(1, 2, 3), default=1)
    return parser


def main() -> None:
    args = build_cli_parser().parse_args()
    if args.workers > 1 and args.max_messages is not None:
        raise ValueError("Use one worker for an exact max-messages bound")
    settings = get_settings()
    endpoint = (
        start_metrics_endpoint(settings.consumer_metrics_port) if settings.metrics_enabled else None
    )
    try:
        runners: list[QualityConsumer] = []
        try:
            for _ in range(args.workers):
                runners.append(QualityConsumer(reuse_connections=True))
        except BaseException:
            for runner in runners:
                runner.close()
            raise

        def stop_workers(*_: Any) -> None:
            for runner in runners:
                runner.stop()

        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, stop_workers)
        # Each Kafka client, DB pool and engine belongs to one worker. Prometheus
        # counters/histograms share the same thread-safe process registry.
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    runner.run,
                    max_messages=args.max_messages,
                    max_duration_sec=args.max_duration_sec,
                )
                for runner in runners
            ]
            try:
                for future in as_completed(futures):
                    logger.info("Quality worker complete: %s", future.result())
            finally:
                stop_workers()
    finally:
        if endpoint:
            endpoint[0].shutdown()
            endpoint[0].server_close()
            endpoint[1].join(timeout=3)


if __name__ == "__main__":
    main()
