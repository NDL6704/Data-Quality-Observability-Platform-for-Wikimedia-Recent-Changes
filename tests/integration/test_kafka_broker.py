"""Integration test to verify Apache Kafka KRaft broker connectivity, produce, and consume."""

import json
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import get_settings


def test_kafka_broker_produce_consume() -> None:
    """Verify producing and consuming a message through the Kafka broker."""
    settings = get_settings()
    test_topic = f"test.topic.{uuid.uuid4().hex[:8]}"

    # 1. Ensure test topic creation via AdminClient
    admin_client = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    fs = admin_client.create_topics([NewTopic(test_topic, num_partitions=1, replication_factor=1)])
    for _topic, f in fs.items():
        try:
            f.result(timeout=10)
        except Exception:
            pass  # Might already exist or auto-created

    # 2. Produce test message
    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "test-kafka-producer",
    })

    event_id = str(uuid.uuid4())
    test_payload = {
        "event_id": event_id,
        "wiki": "enwiki",
        "timestamp": int(time.time()),
        "message": "Kafka KRaft smoke test",
    }
    encoded_payload = json.dumps(test_payload).encode("utf-8")

    delivered = False

    def delivery_report(err: Any, msg: Any) -> None:
        nonlocal delivered
        if err is None:
            delivered = True

    producer.produce(
        topic=test_topic,
        key=event_id.encode("utf-8"),
        value=encoded_payload,
        callback=delivery_report,
    )
    producer.flush(timeout=10)
    assert delivered is True, "Producer must successfully deliver message to Kafka"

    # 3. Consume test message
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": f"test-group-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([test_topic])

    received_msg = None
    start_time = time.time()
    while time.time() - start_time < 15:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        msg_err = msg.error()
        if msg_err is not None:
            if msg_err.code() == KafkaError._PARTITION_EOF:
                continue
            raise Exception(f"Kafka consumer error: {msg_err}")

        msg_val = msg.value()
        if msg_val is None:
            continue
        received_data = json.loads(msg_val.decode("utf-8"))
        if received_data.get("event_id") == event_id:
            received_msg = received_data
            break

    consumer.close()
    assert received_msg is not None, "Consumer must retrieve the exact test message from Kafka"
    assert received_msg["wiki"] == "enwiki"
