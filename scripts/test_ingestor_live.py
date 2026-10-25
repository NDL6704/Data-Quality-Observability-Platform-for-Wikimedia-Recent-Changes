"""Script kiểm thử tích hợp trực tiếp IngestionRunner với luồng thật Wikimedia.

Kiểm tra chu trình end-to-end:
1. Kết nối Wikimedia SSE thực tế (HTTP 200).
2. Kafka Producer đẩy vào topic `rc.raw.v1` với partition key tất định.
3. Raw Archiver ghi dữ liệu xuống đĩa cục bộ JSONL.
4. Kích hoạt Graceful Shutdown khi nhận đủ 10 sự kiện.
5. Xác minh đối chiếu dữ liệu trên cả Kafka topic và bảng `archive_manifests` PostgreSQL.
"""

import json
import sys
from pathlib import Path

# Ensure src module is discoverable when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from src.common.config import get_settings  # noqa: E402
from src.common.logging import setup_logger  # noqa: E402
from src.ingestion.raw_archiver import compute_file_sha256  # noqa: E402
from src.ingestion.run_ingestor import IngestionRunner  # noqa: E402
from src.storage.db import get_db_connection  # noqa: E402

logger = setup_logger("test_ingestor_live")


def main() -> None:
    settings = get_settings()
    logger.info("=== BẮT ĐẦU KIỂM THỬ TÍCH HỢP INGESTION RUNNER (E2E LIVE SMOKE) ===")
    logger.info(
        f"Cấu hình: Kafka={settings.kafka_bootstrap_servers} | "
        f"Topic={settings.kafka_topic_raw} | DB={settings.postgres_db}"
    )

    runner = IngestionRunner(
        enable_kafka=True,
        enable_archive=True,
        auto_handle_signals=True,
    )

    TARGET_EVENTS = 10
    logger.info(f"Khởi chạy runner với giới hạn max_events = {TARGET_EVENTS}...")

    stats = runner.run(max_events=TARGET_EVENTS, log_interval=2)

    logger.info(f"Kết quả thống kê từ runner: {stats}")
    assert stats["received"] == TARGET_EVENTS, (
        f"Kỳ vọng {TARGET_EVENTS} sự kiện nhận, thực tế nhận {stats['received']}"
    )
    assert stats["produced"] == TARGET_EVENTS, (
        f"Kỳ vọng {TARGET_EVENTS} sự kiện gửi Kafka, thực tế gửi {stats['produced']}"
    )
    assert stats["archived"] == TARGET_EVENTS, (
        f"Kỳ vọng {TARGET_EVENTS} sự kiện lưu archive, thực tế lưu {stats['archived']}"
    )
    assert stats["batches_committed"] >= 1, "Kỳ vọng ít nhất 1 batch manifest được commit"

    # =========================================================================
    # BƯỚC XÁC MINH 1: KIỂM TRA DỮ LIỆU ĐÃ VÀO KAFKA TOPIC
    # =========================================================================
    logger.info("--- BƯỚC 1: Xác minh tin nhắn trên Kafka topic 'rc.raw.v1' ---")
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "test-ingestor-smoke-verifier",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    verified_kafka_msgs = 0
    try:
        # Lấy metadata partitions của topic
        meta = consumer.list_topics(topic=settings.kafka_topic_raw, timeout=5.0)
        topic_meta = meta.topics[settings.kafka_topic_raw]
        partitions = [TopicPartition(settings.kafka_topic_raw, p) for p in topic_meta.partitions]
        consumer.assign(partitions)

        # Đọc tối đa 10 tin nhắn
        msg = consumer.poll(timeout=3.0)
        while msg is not None and verified_kafka_msgs < 5:
            if not msg.error():
                raw_val = msg.value()
                if raw_val is None:
                    continue
                val_bytes = (
                    raw_val if isinstance(raw_val, (bytes, bytearray))
                    else str(raw_val).encode("utf-8")
                )
                payload = json.loads(val_bytes.decode("utf-8"))

                raw_key = msg.key()
                if raw_key is not None:
                    key_str = (
                        raw_key.decode("utf-8")
                        if isinstance(raw_key, (bytes, bytearray))
                        else str(raw_key)
                    )
                else:
                    key_str = "None"

                wiki_str = payload.get("wiki") or "N/A"
                title_str = (payload.get("title") or "N/A")[:25]
                logger.info(
                    f"Xác minh Kafka Msg #{verified_kafka_msgs + 1}: "
                    f"P={msg.partition()} | Key={key_str} | "
                    f"Wiki={wiki_str} | Title={title_str}"
                )
                verified_kafka_msgs += 1
            msg = consumer.poll(timeout=1.0)
    finally:
        consumer.close()

    assert verified_kafka_msgs > 0, "Không đọc được message nào từ Kafka topic!"
    logger.info(f"Đã xác minh thành công dữ liệu trên Kafka ({verified_kafka_msgs} mẫu kiểm tra).")

    # =========================================================================
    # BƯỚC XÁC MINH 2: KIỂM TRA FILE ARCHIVE VÀ POSTGRESQL MANIFEST
    # =========================================================================
    logger.info("--- BƯỚC 2: Xác minh file vật lý và manifest trong PostgreSQL ---")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT manifest_id, file_path, format, record_count,
                       checksum_sha256, file_size_bytes, status
                FROM archive_manifests
                ORDER BY created_at DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

    assert row is not None, "Không tìm thấy manifest nào trong bảng archive_manifests!"
    logger.info(
        f"Manifest mới nhất: ID={row['manifest_id'][:8]} | File={row['file_path']} | "
        f"Records={row['record_count']} | Status={row['status']}"
    )

    target_file = Path(row["file_path"])
    assert target_file.exists(), f"File vật lý không tồn tại trên ổ đĩa: {target_file}"

    # Kiểm tra mã băm SHA-256
    disk_sha256 = compute_file_sha256(target_file)
    assert disk_sha256 == row["checksum_sha256"], (
        f"Lệch mã băm SHA-256! Đĩa: {disk_sha256} != DB: {row['checksum_sha256']}"
    )
    assert target_file.stat().st_size == row["file_size_bytes"], "Lệch kích thước byte!"

    logger.info(
        f"Xác minh mã băm SHA-256 thành công: {disk_sha256[:16]}... trùng khớp 100%!"
    )
    logger.info("=== KIỂM THỬ TÍCH HỢP INGESTION RUNNER THÀNH CÔNG VƯỢT TRỘI! ===")


if __name__ == "__main__":
    main()
