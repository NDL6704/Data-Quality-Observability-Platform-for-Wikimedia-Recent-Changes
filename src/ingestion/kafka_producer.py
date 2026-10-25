"""
Wikimedia Recent Changes Kafka Producer Wrapper.

Đóng gói confluent_kafka.Producer phục vụ streaming dữ liệu thô vào Apache Kafka:
- Khởi tạo producer với cấu hình chuẩn production (idempotence, acks=all, snappy compression).
- Trích xuất deterministic partition key dựa trên wiki và event id nhằm đảm bảo thứ tự sự kiện.
- Delivery report callback bất đồng bộ để ghi nhận trạng thái và cập nhật thống kê metrics.
- Xử lý backpressure (BufferError: Local: Queue full) với cơ chế poll xả đệm và retry.
- Đảm bảo kiểm tra và tự động khởi tạo Kafka topic với 3 partitions và 24h retention nếu chưa có.
"""

import hashlib
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import get_settings
from src.common.logging import setup_logger
from src.ingestion.sse_client import RawStreamEvent

logger = setup_logger("kafka_producer")


def event_id_clean(raw_id: str) -> str:
    """Rút gọn chuỗi SSE event_id nếu là JSON array dài của Wikimedia."""
    if len(raw_id) > 64:
        return hashlib.md5(raw_id.encode("utf-8")).hexdigest()[:16]
    return raw_id


def extract_partition_key(event: RawStreamEvent) -> str:
    """
    Trích xuất partition key mang tính tất định (deterministic) từ sự kiện Wikimedia.

    Chiến lược ưu tiên:
    1. {wiki}:{id} (nếu có id trong payload hoặc meta.id)
    2. {wiki}:{title} (nếu có title trang)
    3. {wiki}:{event_id} (nếu có SSE event_id từ header)
    4. {wiki}:{sha256_hash_prefix} (fallback cuối cùng đảm bảo không bao giờ None/rỗng)
    """
    data = event.data
    wiki = data.get("wiki") or "unknown_wiki"

    # 1. Thử lấy ID bài viết hoặc edit ID
    record_id = data.get("id")
    if record_id is None and isinstance(data.get("meta"), dict):
        record_id = data["meta"].get("id")

    if record_id is not None:
        return f"{wiki}:{record_id}"

    # 2. Thử lấy title trang
    title = data.get("title")
    if title:
        return f"{wiki}:{title}"

    # 3. Thử lấy SSE event_id
    if event.event_id:
        return f"{wiki}:{event_id_clean(event.event_id)}"

    # 4. Fallback: hash nội dung payload (16 ký tự đầu)
    payload_hash = hashlib.sha256(event.raw_payload.encode("utf-8")).hexdigest()[:16]
    return f"{wiki}:{payload_hash}"


class WikimediaKafkaProducer:
    """Producer bao bọc confluent_kafka.Producer với các cơ chế bảo đảm độ tin cậy."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        default_topic: str | None = None,
        client_id: str = "wikimedia-rc-producer",
        auto_create_topic: bool = True,
    ) -> None:
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.default_topic = default_topic or settings.kafka_topic_raw

        # Bộ đếm thống kê vận hành
        self._stats: dict[str, int] = {
            "produced": 0,
            "delivered": 0,
            "failed": 0,
            "buffer_full_retries": 0,
        }

        # Cấu hình Producer chuẩn Production
        self._config: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": client_id,
            "acks": "all",  # Độ bền vững tối đa (chờ toàn bộ ISR xác nhận)
            "enable.idempotence": True,  # Tránh duplicate trên broker khi retry mạng
            "retries": 5,
            "retry.backoff.ms": 200,
            "compression.type": "snappy",  # Nén Snappy giảm tải băng thông I/O
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.kbytes": 1048576,  # 1GB buffer RAM
        }

        logger.info(
            f"Khởi tạo Kafka Producer tới broker {self.bootstrap_servers} "
            f"(client_id={client_id}, default_topic={self.default_topic})"
        )
        self._producer = Producer(self._config)

        if auto_create_topic:
            self._ensure_topic_exists(self.default_topic, num_partitions=3, retention_ms=86400000)

    def _ensure_topic_exists(
        self, topic_name: str, num_partitions: int = 3, retention_ms: int = 86400000
    ) -> None:
        """Đảm bảo topic tồn tại với số lượng partition và retention cấu hình."""
        try:
            admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            metadata = admin_client.list_topics(timeout=5.0)
            if topic_name not in metadata.topics:
                logger.info(
                    f"Topic '{topic_name}' chưa tồn tại. "
                    f"Đang tạo với {num_partitions} partitions..."
                )
                new_topic = NewTopic(
                    topic=topic_name,
                    num_partitions=num_partitions,
                    replication_factor=1,
                    config={"retention.ms": str(retention_ms)},
                )
                fs = admin_client.create_topics([new_topic])
                for t, f in fs.items():
                    f.result(timeout=10.0)
                    logger.info(f"Đã tạo thành công topic '{t}'.")
            else:
                existing_partitions = len(metadata.topics[topic_name].partitions)
                logger.debug(
                    f"Topic '{topic_name}' đã sẵn sàng với {existing_partitions} partitions."
                )
        except Exception as exc:
            logger.warning(
                f"Không thể kiểm tra/tạo topic '{topic_name}' qua AdminClient: {exc}"
            )

    def _delivery_callback(self, err: KafkaError | None, msg: Message) -> None:
        """Callback được librdkafka gọi khi message được gửi hoặc gặp lỗi."""
        if err is not None:
            self._stats["failed"] += 1
            logger.error(
                f"Gửi message vào Kafka thất bại: {err} "
                f"(Topic: {msg.topic()}, Partition: {msg.partition()})"
            )
        else:
            self._stats["delivered"] += 1
            logger.debug(
                f"Đã gửi thành công message tới {msg.topic()} "
                f"[p={msg.partition()}, offset={msg.offset()}]"
            )

    def produce(
        self,
        event: RawStreamEvent,
        topic: str | None = None,
        max_buffer_retries: int = 3,
    ) -> bool:
        """
        Đưa sự kiện thô vào Kafka topic với deterministic key và backpressure handling.

        Args:
            event: Bản tin RawStreamEvent từ SSE client.
            topic: Tên topic đích (mặc định lấy default_topic).
            max_buffer_retries: Số lần thử lại nếu bộ đệm librdkafka bị đầy.

        Returns:
            True nếu message được ghi nhận vào hàng đợi producer thành công, False nếu lỗi.
        """
        target_topic = topic or self.default_topic
        partition_key = extract_partition_key(event)
        key_bytes = partition_key.encode("utf-8")
        value_bytes = event.raw_payload.encode("utf-8")

        # Headers phục vụ tracing & auditing
        headers: list[tuple[str, str | bytes | None]] = [
            ("source", b"wikimedia-sse"),
            ("event_type", event.event_type.encode("utf-8")),
            ("received_at_epoch", str(event.received_at_epoch).encode("utf-8")),
        ]
        if event.event_id:
            headers.append(("sse_event_id", event.event_id.encode("utf-8")))

        retries = 0
        while retries <= max_buffer_retries:
            try:
                self._producer.produce(
                    topic=target_topic,
                    key=key_bytes,
                    value=value_bytes,
                    headers=headers,
                    on_delivery=self._delivery_callback,
                )
                self._stats["produced"] += 1

                # Phục vụ các callback delivery đang chờ mà không block thread
                self._producer.poll(0)
                return True

            except BufferError:
                retries += 1
                self._stats["buffer_full_retries"] += 1
                logger.warning(
                    f"Bộ đệm Kafka Producer đầy (BufferError). "
                    f"Đang xả đệm và thử lại lần {retries}/{max_buffer_retries}..."
                )
                # Chờ tối đa 500ms để librdkafka đẩy message đi bớt
                self._producer.poll(0.5)

            except KafkaException as exc:
                self._stats["failed"] += 1
                logger.error(f"Lỗi KafkaException khi produce message: {exc}")
                return False

            except Exception as exc:
                self._stats["failed"] += 1
                logger.error(f"Ngoại lệ không xác định khi produce: {exc}")
                return False

        logger.error(f"Vượt quá số lần thử lại buffer ({max_buffer_retries}). Message bị từ chối.")
        return False

    def flush(self, timeout: float = 10.0) -> int:
        """
        Chặn đợi cho tới khi toàn bộ message trong buffer được gửi đi hết hoặc hết timeout.

        Returns:
            Số lượng message còn sót lại chưa thể gửi (0 nghĩa là đã gửi sạch).
        """
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"Kafka Producer flush timeout! Còn {remaining} messages trong queue.")
        else:
            logger.debug("Kafka Producer đã flush toàn bộ message trong queue thành công.")
        return remaining

    def close(self, timeout: float = 10.0) -> None:
        """Đóng an toàn producer sau khi flush hết dữ liệu."""
        logger.info("Đang đóng Kafka Producer và xả toàn bộ hàng đợi...")
        self.flush(timeout=timeout)
        logger.info(f"Kafka Producer đã đóng. Thống kê vận hành: {self.stats}")

    @property
    def stats(self) -> dict[str, int]:
        """Trả về bản sao thống kê vận hành của Producer."""
        return dict(self._stats)

    def __enter__(self) -> "WikimediaKafkaProducer":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
