"""
Script kiểm thử tích hợp Live: SSE Client -> Kafka Producer -> Kafka Broker (rc.raw.v1).

Quy trình kiểm chứng:
1. Kết nối tới luồng thực tế của Wikimedia EventStreams.
2. Đọc 5 events đầu tiên.
3. Đẩy từng event vào Kafka topic 'rc.raw.v1' qua WikimediaKafkaProducer.
4. Chờ flush và kiểm tra thống kê delivery callback.
5. Sử dụng Kafka Consumer để xác nhận các bản ghi đã thực sự nằm trên broker.
"""

import json
import sys
from pathlib import Path

# Đảm bảo import được module src khi chạy trực tiếp từ thư mục gốc
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, KafkaError  # noqa: E402

from src.common.config import get_settings  # noqa: E402
from src.common.logging import setup_logger  # noqa: E402
from src.ingestion.kafka_producer import (  # noqa: E402
    WikimediaKafkaProducer,
    extract_partition_key,
)
from src.ingestion.sse_client import RawStreamEvent, WikimediaSSEClient  # noqa: E402

logger = setup_logger("test_producer_live")


def main() -> None:
    settings = get_settings()
    topic = settings.kafka_topic_raw
    max_events = 5

    logger.info("=== BẮT ĐẦU KIỂM THỬ TÍCH HỢP SSE -> KAFKA PRODUCER ===")
    logger.info(f"Target Kafka Topic: {topic} | Broker: {settings.kafka_bootstrap_servers}")

    sse_client = WikimediaSSEClient()
    events_collected: list[RawStreamEvent] = []

    # 1. Đọc 5 events thực tế từ Wikimedia
    logger.info(f"Đang thu thập {max_events} events từ Wikimedia...")
    for event in sse_client.stream_events():
        events_collected.append(event)
        logger.info(
            f"Đã nhận Event #{len(events_collected)} | "
            f"Wiki: {event.data.get('wiki')} | "
            f"Key trích xuất: {extract_partition_key(event)}"
        )
        if len(events_collected) >= max_events:
            sse_client.stop()
            break

    # 2. Đẩy vào Kafka qua WikimediaKafkaProducer
    logger.info("Đang đẩy các events vào Kafka topic rc.raw.v1...")
    with WikimediaKafkaProducer() as producer:
        for event in events_collected:
            success = producer.produce(event, topic=topic)
            if not success:
                logger.error("Không thể ghi nhận event vào hàng đợi producer!")

        # Flush và đợi xác nhận từ broker
        remaining = producer.flush(timeout=10.0)
        assert remaining == 0, f"Còn {remaining} message chưa gửi được!"

        stats = producer.stats
        logger.info(f"Thống kê sau khi produce: {stats}")
        assert stats["delivered"] == max_events, (
            f"Kỳ vọng {max_events} message delivered, thực tế: {stats['delivered']}"
        )

    # 3. Sử dụng Consumer để kiểm chứng dữ liệu đã nằm trên Kafka
    logger.info("Đang kết nối Kafka Consumer để kiểm tra dữ liệu trên topic...")
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "test-live-verify-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    verified_count = 0
    poll_attempts = 0
    while verified_count < max_events and poll_attempts < 20:
        poll_attempts += 1
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                continue
            logger.error(f"Consumer lỗi: {err}")
            break

        raw_val = msg.value()
        raw_key = msg.key()
        payload_obj = json.loads(raw_val.decode("utf-8")) if raw_val is not None else {}
        msg_key = raw_key.decode("utf-8") if raw_key is not None else "NoKey"
        headers_dict = dict(msg.headers() or [])
        raw_source = headers_dict.get("source")
        source_header_str = (
            raw_source.decode("utf-8") if isinstance(raw_source, bytes) else str(raw_source)
        )
        verified_count += 1
        logger.info(
            f"Xác minh Event trên Kafka #{verified_count}: "
            f"Partition={msg.partition()} | Offset={msg.offset()} | "
            f"Key={msg_key} | SourceHeader={source_header_str} | "
            f"Wiki={payload_obj.get('wiki')} | Title={payload_obj.get('title')}"
        )

    consumer.close()
    logger.info(
        f"Đã xác minh thành công {verified_count}/{max_events} events trên topic '{topic}'!"
    )
    logger.info("=== KIỂM THỬ HOÀN TẤT THÀNH CÔNG RỰC RỠ! ===")


if __name__ == "__main__":
    main()
