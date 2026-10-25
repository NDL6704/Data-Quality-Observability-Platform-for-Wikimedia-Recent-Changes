"""Resources owned by one fault run; never use the operational consumer group."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import Settings
from src.processing.consumer import QualityConsumer, build_consumer_config
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection


class FaultEnvironment:
    """Allocate UUID-scoped topics, group, payloads and exact DB cleanup keys."""

    def __init__(self, run_id: str, directory: Path) -> None:
        self.run_id = run_id
        self.directory = directory
        self.settings = Settings(
            KAFKA_TOPIC_RAW=f"fault.{run_id}.raw",
            KAFKA_TOPIC_VALIDATED=f"fault.{run_id}.valid",
            KAFKA_TOPIC_DLQ=f"fault.{run_id}.dlq",
            KAFKA_CONSUMER_GROUP_QUALITY=f"fault.{run_id}",
            QUALITY_MAX_RETRIES=0,
        )
        self.topics = [
            self.settings.kafka_topic_raw,
            self.settings.kafka_topic_validated,
            self.settings.kafka_topic_dlq,
        ]
        self.keys: list[str] = []
        self.manifests: list[str] = []
        self.admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap_servers})

    def create(self) -> None:
        """Create fresh topics; partial creation is cleaned by the caller."""
        for future in self.admin.create_topics(
            [NewTopic(topic, 1, 1) for topic in self.topics], request_timeout=10
        ).values():
            future.result(timeout=15)

    def payload(self, **changes: Any) -> bytes:
        """Build a valid synthetic event with an identity owned by this run."""
        return json.dumps(
            {
                "meta": {
                    "id": str(uuid4()),
                    "dt": datetime.now(UTC).isoformat(),
                    "stream": "mediawiki.recentchange",
                },
                "type": "edit",
                "wiki": "fault_harness",
                "title": "Fault verification",
                "server_name": "example.invalid",
                "namespace": 0,
                **changes,
            }
        ).encode()

    def produce(self, values: list[bytes]) -> None:
        """Record original keys before producing; require all delivery confirmations."""
        producer = Producer({"bootstrap.servers": self.settings.kafka_bootstrap_servers})
        engine = QualityEngine()
        engine.register_rules(build_default_rule_set())
        receipt = datetime.now(UTC)
        errors: list[Any] = []
        for raw in values:
            self.keys.append(engine.prepare_payload(raw, received_at=receipt).summary.event_key)
            producer.produce(
                self.settings.kafka_topic_raw,
                value=raw,
                partition=0,
                headers=[("received_at_epoch", str(receipt.timestamp()).encode())],
                on_delivery=lambda error, message: errors.append(error),
            )
        if producer.flush(15) or len(errors) != len(values) or any(errors):
            raise RuntimeError("Harness input delivery was not confirmed")

    def consume(self, count: int) -> dict[str, int]:
        """Use the production consumer with the run's isolated settings."""
        config = build_consumer_config(self.settings)
        config["session.timeout.ms"] = 6000
        return QualityConsumer(
            settings=self.settings, consumer=Consumer(config), auto_create_topics=False
        ).run(max_messages=count, max_duration_sec=60)

    def committed(self) -> int:
        """Read the durable source offset, not a local consumer position."""
        observer = Consumer(build_consumer_config(self.settings))
        try:
            return int(
                observer.committed([TopicPartition(self.settings.kafka_topic_raw, 0)], timeout=10)[
                    0
                ].offset
            )
        finally:
            observer.close()

    def lag(self) -> int:
        """Compare partition high watermark to the durable source acknowledgment."""
        observer = Consumer(build_consumer_config(self.settings))
        try:
            partition = TopicPartition(self.settings.kafka_topic_raw, 0)
            _, high = observer.get_watermark_offsets(partition, timeout=10, cached=False)
            offset = observer.committed([partition], timeout=10)[0].offset
            return int(high - max(0, offset))
        finally:
            observer.close()

    def outputs(self, expected: int) -> list[dict[str, Any]]:
        """Read sink envelopes from offset zero without committing any offsets."""
        config = build_consumer_config(self.settings)
        config["group.id"] += ".observer"
        observer = Consumer(config)
        rows: list[dict[str, Any]] = []
        try:
            observer.assign([TopicPartition(topic, 0, 0) for topic in self.topics[1:]])
            deadline = time.monotonic() + 15
            while len(rows) < expected and time.monotonic() < deadline:
                message = observer.poll(0.2)
                if message is not None and message.error() is None:
                    value = message.value()
                    if value is None:
                        raise ValueError("Unexpected sink tombstone")
                    rows.append({"topic": message.topic(), "body": json.loads(value)})
        finally:
            observer.close()
        return rows

    def curated_count(self, key: str) -> int:
        """Read one identity only, including while the child is paused."""
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM curated_changes WHERE event_key=%s", (key,)
            ).fetchone()
            assert row is not None
            return int(row["n"])

    def cleanup(self) -> None:
        """Delete only resources explicitly owned by this UUID; report cleanup errors."""
        errors: list[str] = []
        try:
            with get_db_connection() as conn:
                runs = conn.execute(
                    "SELECT replay_run_id FROM quarantined_events WHERE event_key=ANY(%s)",
                    (self.keys,),
                ).fetchall()
                conn.execute(
                    "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=ANY(%s)",
                    (self.keys,),
                )
                conn.execute("DELETE FROM curated_changes WHERE event_key=ANY(%s)", (self.keys,))
                conn.execute("DELETE FROM quarantined_events WHERE event_key=ANY(%s)", (self.keys,))
                conn.execute(
                    "DELETE FROM archive_manifests WHERE manifest_id=ANY(%s)", (self.manifests,)
                )
                for row in runs:
                    if row["replay_run_id"]:
                        conn.execute(
                            "DELETE FROM pipeline_runs WHERE run_id=%s", (row["replay_run_id"],)
                        )
        except Exception as exc:
            errors.append(f"DB cleanup: {type(exc).__name__}: {exc}")
        for future in self.admin.delete_topics(self.topics, request_timeout=10).values():
            try:
                future.result(timeout=15)
            except KafkaException as exc:
                if exc.args[0].code() != 3:  # UNKNOWN_TOPIC_OR_PARTITION after partial setup
                    errors.append(f"Topic cleanup: {exc}")
        try:
            group = self.settings.kafka_consumer_group_quality
            self.admin.delete_consumer_groups([group])[group].result(timeout=10)
        except KafkaException as exc:
            if exc.args[0].code() != 69:  # GROUP_ID_NOT_FOUND if never joined
                errors.append(f"Group cleanup: {exc}")
        if errors:
            raise RuntimeError("; ".join(errors))
