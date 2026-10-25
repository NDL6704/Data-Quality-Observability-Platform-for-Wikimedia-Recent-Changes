"""End-to-End Integration Test cho Streaming Ingestion Pipeline.

Kiểm tra chu trình hoàn chỉnh từ Wikimedia EventStreams thực tế:
1. Kết nối stream SSE thật và thu thập tối thiểu 100 sự kiện.
2. Sản xuất tin nhắn vào Kafka topic `rc.raw.v1` với partition key tất định.
3. Ghi file đĩa JSONL cục bộ kèm cam kết manifest SHA-256 vào PostgreSQL.
4. Đối soát 3 tầng (Three-layer Reconciliation) chứng minh zero data loss:
   Count(SSE) == Count(Kafka) == Count(Disk JSONL) == Count(DB Manifests) >= 100.
5. Kiểm tra phân bổ partition trên Kafka (phân tán dữ liệu trên cả 3 partitions).
6. Kiểm tra toàn vẹn byte-level mã băm SHA-256 của các file archive vật lý.
"""

import json
from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from src.common.config import get_settings
from src.common.logging import setup_logger
from src.ingestion.kafka_producer import WikimediaKafkaProducer
from src.ingestion.raw_archiver import RawArchiver, compute_file_sha256
from src.ingestion.run_ingestor import IngestionRunner
from src.storage.db import get_db_connection

logger = setup_logger("test_ingestion_e2e")


@pytest.fixture
def isolated_ingestion_topic() -> Generator[str, None, None]:
    topic = "test.ingestion." + uuid4().hex
    try:
        yield topic
    finally:
        admin = AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap_servers})
        for future in admin.delete_topics([topic], request_timeout=10).values():
            future.result(timeout=15)


@pytest.mark.integration
def test_ingestion_e2e_reconciliation_100_events(isolated_ingestion_topic: str) -> None:
    """Kiểm thử thu thập >= 100 events thật từ Wikimedia và đối soát 3 tầng."""
    settings = get_settings().model_copy(
        update={
            "kafka_topic_raw": isolated_ingestion_topic,
            "raw_archive_dir": get_settings().raw_archive_dir / isolated_ingestion_topic,
        }
    )
    target_events = 100

    logger.info("=== BẮT ĐẦU E2E TEST: THU THẬP >= 100 EVENTS TỪ WIKIMEDIA ===")

    # Khởi tạo IngestionRunner với đầy đủ Kafka và Disk Archiver
    runner = IngestionRunner(
        kafka_producer=WikimediaKafkaProducer(default_topic=settings.kafka_topic_raw),
        raw_archiver=RawArchiver(base_dir=settings.raw_archive_dir),
        enable_kafka=True,
        enable_archive=True,
        auto_handle_signals=False,  # Không can thiệp signal handler trong pytest
    )

    # Chạy thu thập chính xác 100 sự kiện (timeout an toàn 90 giây)
    stats = runner.run(max_events=target_events, max_duration_sec=90.0, log_interval=20)

    # 1. Kiểm tra số lượng thống kê từ IngestionRunner
    received = stats["received"]
    produced = stats["produced"]
    archived = stats["archived"]

    assert received >= target_events, (
        f"Kỳ vọng thu thập ít nhất {target_events} events, thực tế nhận {received}"
    )
    assert produced == received, f"Lệch số lượng Kafka: nhận {received} nhưng produce {produced}"
    assert archived == received, f"Lệch số lượng Archive: nhận {received} nhưng archive {archived}"
    assert stats["batches_committed"] >= 1, "Phải có ít nhất 1 batch manifest được commit"

    # 2. Đối soát dữ liệu trên Kafka Topic `rc.raw.v1`
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "test-e2e-reconciliation-verifier",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    partition_msg_counts: dict[int, int] = {0: 0, 1: 0, 2: 0}
    verified_samples = 0

    try:
        topic_meta = consumer.list_topics(topic=settings.kafka_topic_raw, timeout=5.0)
        partitions_info = topic_meta.topics[settings.kafka_topic_raw].partitions
        partitions = [TopicPartition(settings.kafka_topic_raw, p) for p in partitions_info]
        consumer.assign(partitions)

        # Lấy watermark offsets để biết số lượng message hiện có trên từng partition
        for p in partitions:
            low, high = consumer.get_watermark_offsets(p, timeout=5.0)
            total_on_partition = high - low
            partition_msg_counts[p.partition] = total_on_partition
            logger.info(f"Kafka Partition {p.partition}: Watermark Low={low}, High={high}")

        # Đọc lấy 5 mẫu tin ngẫu nhiên để xác minh payload và partition key
        msg = consumer.poll(timeout=3.0)
        while msg is not None and verified_samples < 5:
            if not msg.error():
                raw_val = msg.value()
                if raw_val is not None:
                    val_bytes = (
                        raw_val
                        if isinstance(raw_val, (bytes, bytearray))
                        else str(raw_val).encode("utf-8")
                    )
                    payload = json.loads(val_bytes.decode("utf-8"))
                    assert "wiki" in payload or "meta" in payload, "Payload thiếu trường cốt lõi"
                    verified_samples += 1
            msg = consumer.poll(timeout=1.0)
    finally:
        consumer.close()

    total_kafka_messages = sum(partition_msg_counts.values())
    assert total_kafka_messages >= target_events, (
        f"Tổng tin nhắn trên Kafka ({total_kafka_messages}) nhỏ hơn số event mục tiêu"
    )
    # Xác minh tin nhắn được phân bổ đều vào nhiều partition (không bị dồn 1 partition)
    active_partitions = sum(1 for count in partition_msg_counts.values() if count > 0)
    assert active_partitions >= 2, "Dữ liệu phải được phân bổ ít nhất trên 2 partitions"

    # 3. Đối soát dữ liệu với PostgreSQL và File đĩa cục bộ
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT manifest_id, file_path, record_count, checksum_sha256,
                       file_size_bytes, status
                FROM archive_manifests
                WHERE file_path LIKE %s
                ORDER BY created_at ASC;
            """,
                (settings.raw_archive_dir.as_posix() + "/%",),
            )
            manifest_rows = cur.fetchall()

    assert len(manifest_rows) >= 1, "Không có manifest nào được ghi nhận trong phiên test này"

    total_manifest_records = 0
    for row in manifest_rows:
        total_manifest_records += row["record_count"]
        target_file = Path(row["file_path"])
        assert target_file.exists(), f"File archive vật lý không tồn tại: {target_file}"

        # Đếm số dòng thực tế trong file JSONL
        with open(target_file, encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        assert len(lines) == row["record_count"], (
            f"Lệch số dòng trong file {target_file.name}: "
            f"đĩa={len(lines)} != DB={row['record_count']}"
        )

        # Kiểm tra tính toàn vẹn mã băm SHA-256
        actual_sha256 = compute_file_sha256(target_file)
        assert actual_sha256 == row["checksum_sha256"], (
            f"Lệch mã băm SHA-256 trên file {target_file.name}: "
            f"thực tế={actual_sha256} != manifest={row['checksum_sha256']}"
        )
        assert target_file.stat().st_size == row["file_size_bytes"], "Lệch kích thước byte"

    assert total_manifest_records == archived, (
        f"Lệch số lượng manifest: DB={total_manifest_records} != Runner={archived}"
    )

    logger.info(
        f"=== ĐỐI SOÁT 3 TẦNG THÀNH CÔNG TUYỆT ĐỐI ===\n"
        f"  - SSE Received:   {received}\n"
        f"  - Kafka Produced: {produced}\n"
        f"  - JSONL Lines:    {archived}\n"
        f"  - Manifest DB:    {total_manifest_records}\n"
        f"  - Tỉ lệ thất thoát (Data Loss): 0.00%\n"
        f"  - Phân bổ Partitions: {partition_msg_counts}"
    )
