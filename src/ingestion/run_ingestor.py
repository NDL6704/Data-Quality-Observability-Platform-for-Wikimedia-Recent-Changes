"""Ingestion Pipeline Runner with Graceful Teardown.

Orchestrates the continuous Wikimedia Recent Changes streaming pipeline:
1. Ingests raw Server-Sent Events via `WikimediaSSEClient`.
2. Buffers and produces messages to Apache Kafka `rc.raw.v1` via `WikimediaKafkaProducer`.
3. Buffers and seals raw JSONL batches to local filesystem with SHA-256 manifests
   in PostgreSQL `archive_manifests` via `RawArchiver`.
4. Captures OS termination signals (SIGINT / SIGTERM) to execute a lossless,
   ordered graceful shutdown (flush archiver disk pages -> flush Kafka socket buffer).
"""

import argparse
import signal
import sys
import threading
import time
from datetime import UTC, datetime
from typing import Any

from src.common.logging import setup_logger
from src.ingestion.kafka_producer import WikimediaKafkaProducer
from src.ingestion.raw_archiver import RawArchiver
from src.ingestion.sse_client import RawStreamEvent, WikimediaSSEClient
from src.observability.metrics import Metrics, get_metrics, start_metrics_endpoint, wiki_label

logger = setup_logger("wikimedia_dq.ingestion.runner")


class IngestionRunner:
    """Orchestrates streaming ingestion from Wikimedia SSE to Kafka and Raw Archive."""

    def __init__(
        self,
        sse_client: WikimediaSSEClient | None = None,
        kafka_producer: WikimediaKafkaProducer | None = None,
        raw_archiver: RawArchiver | None = None,
        enable_kafka: bool = True,
        enable_archive: bool = True,
        auto_handle_signals: bool = True,
        metrics: Metrics | None = None,
    ) -> None:
        self.enable_kafka = enable_kafka
        self.metrics = metrics or get_metrics()
        self.enable_archive = enable_archive

        self.sse_client = sse_client or WikimediaSSEClient()
        self.kafka_producer = (
            kafka_producer or WikimediaKafkaProducer() if self.enable_kafka else None
        )
        self.raw_archiver = raw_archiver or RawArchiver() if self.enable_archive else None

        self._stop_event = threading.Event()
        self._stats: dict[str, Any] = {
            "received": 0,
            "produced": 0,
            "archived": 0,
            "batches_committed": 0,
            "start_time": None,
            "end_time": None,
            "duration_seconds": 0.0,
            "stop_reason": "RUNNING",
        }

        if auto_handle_signals:
            self._setup_signal_handlers()

        logger.info(
            f"IngestionRunner khởi tạo thành công | KafkaEnabled: {self.enable_kafka} | "
            f"ArchiveEnabled: {self.enable_archive}"
        )

    def _setup_signal_handlers(self) -> None:
        """Đăng ký lắng nghe tín hiệu ngắt OS để kích hoạt graceful shutdown."""
        # Signal handlers chỉ có thể gán từ main thread trong Python
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
                logger.debug("Đã đăng ký signal handlers cho SIGINT và SIGTERM.")
            except (ValueError, AttributeError) as exc:
                logger.warning(f"Không thể gán signal handlers: {exc}")

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Xử lý tín hiệu ngắt từ hệ điều hành (Ctrl+C hoặc lệnh terminate)."""
        sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.info(
            f"Đã nhận tín hiệu ngắt từ hệ điều hành ({sig_name}). "
            f"Bắt đầu quy trình dừng an toàn (Graceful Shutdown)..."
        )
        self._stats["stop_reason"] = f"SIGNAL_{sig_name}"
        self.stop()

    def stop(self) -> None:
        """Kích hoạt cờ dừng hệ thống và thông báo SSE client ngắt kết nối."""
        if not self._stop_event.is_set():
            self._stop_event.set()
            if self.sse_client:
                self.sse_client.stop()

    @property
    def is_running(self) -> bool:
        """Kiểm tra pipeline có đang vận hành hay không."""
        return not self._stop_event.is_set()

    @property
    def stats(self) -> dict[str, Any]:
        """Thống kê chi tiết số liệu sự kiện đã xử lý."""
        return dict(self._stats)

    def run(
        self,
        max_events: int | None = None,
        max_duration_sec: float | None = None,
        log_interval: int = 50,
    ) -> dict[str, Any]:
        """Thực thi vòng lặp thu thập và điều phối dữ liệu chính.

        Args:
            max_events: Số sự kiện tối đa cần thu thập trước khi dừng (dành cho test/batch).
            max_duration_sec: Thời gian chạy tối đa tính bằng giây.
            log_interval: Tần suất in log tiến độ (số sự kiện).

        Returns:
            Dictionary chứa toàn bộ số liệu thống kê sau khi pipeline hoàn tất.
        """
        start_utc = datetime.now(UTC)
        start_mono = time.monotonic()
        self._stats["start_time"] = start_utc.isoformat()
        logger.info(
            f"=== BẮT ĐẦU INGESTION PIPELINE === | MaxEvents: {max_events or 'Unlimited'} | "
            f"MaxDuration: {f'{max_duration_sec}s' if max_duration_sec else 'Unlimited'}"
        )

        try:
            for event in self.sse_client.stream_events():
                # 1. Kiểm tra tín hiệu dừng bên ngoài
                if self._stop_event.is_set():
                    logger.info("Đã phát hiện cờ dừng pipeline. Đang thoát vòng lặp streaming...")
                    break

                self._process_single_event(event)

                # 2. In log tiến độ định kỳ
                received_count = self._stats["received"]
                if received_count % log_interval == 0:
                    elapsed = time.monotonic() - start_mono
                    rate = received_count / elapsed if elapsed > 0 else 0.0
                    logger.info(
                        f"Tiến độ Ingestion: Đã xử lý {received_count} events | "
                        f"Kafka produced: {self._stats['produced']} | "
                        f"Archived: {self._stats['archived']} | "
                        f"Tốc độ: {rate:.1f} events/s"
                    )

                # 3. Kiểm tra điều kiện max_events
                if max_events is not None and received_count >= max_events:
                    logger.info(
                        f"Đã đạt giới hạn cấu hình max_events ({max_events}). "
                        f"Dừng thu thập dữ liệu..."
                    )
                    self._stats["stop_reason"] = "MAX_EVENTS_REACHED"
                    break

                # 4. Kiểm tra điều kiện max_duration_sec
                if max_duration_sec is not None:
                    elapsed = time.monotonic() - start_mono
                    if elapsed >= max_duration_sec:
                        logger.info(
                            f"Đã đạt giới hạn thời gian chạy ({max_duration_sec}s). "
                            f"Dừng thu thập dữ liệu..."
                        )
                        self._stats["stop_reason"] = "MAX_DURATION_REACHED"
                        break

        except Exception as exc:
            logger.error(f"Lỗi ngoại lệ không xử lý trong Ingestion loop: {exc}", exc_info=True)
            self._stats["stop_reason"] = f"ERROR_{type(exc).__name__}"
            raise
        finally:
            # Quy trình Teardown an toàn luôn được kích hoạt kể cả khi có crash
            self._execute_teardown(start_mono)

        return self.stats

    def _process_single_event(self, event: RawStreamEvent) -> None:
        """Đưa sự kiện vào Kafka producer và Raw Archiver."""
        self._stats["received"] += 1
        wiki = wiki_label(event.data.get("wiki"))
        self.metrics.ingest_events.labels(wiki, "received").inc()
        self.metrics.ingest_bytes.labels(wiki).inc(len(event.raw_payload.encode("utf-8")))

        # Bước 1: Đẩy vào Kafka Broker
        if self.enable_kafka and self.kafka_producer:
            try:
                produced = self.kafka_producer.produce(event)
                if produced:
                    self._stats["produced"] += 1
                    self.metrics.ingest_events.labels(wiki, "enqueued").inc()
            except Exception as exc:
                self.metrics.ingest_events.labels(wiki, "produce_error").inc()
                logger.error(f"Lỗi khi produce event vào Kafka: {exc}")

        # Bước 2: Đẩy vào Raw File Archiver
        if self.enable_archive and self.raw_archiver:
            try:
                manifest = self.raw_archiver.append(event)
                self._stats["archived"] += 1
                if manifest is not None:
                    self._stats["batches_committed"] += 1
            except Exception as exc:
                logger.error(f"Lỗi khi append event vào Raw Archiver: {exc}")

    def _execute_teardown(self, start_mono: float) -> None:
        """Quy trình dừng an toàn (Ordered Graceful Shutdown).

        Thứ tự thực hiện:
        1. Xả đệm Raw Archiver -> Cam kết toàn bộ JSONL còn lại xuống đĩa vật lý kèm SHA-256.
        2. Xả đệm Kafka Producer -> Gửi hết message trong queue librdkafka và chờ ACK.
        3. Cập nhật và xuất bản metrics tổng kết phiên chạy.
        """
        logger.info("Đang thực hiện Graceful Shutdown Teardown Sequence...")

        # 1. Xả bộ nhớ đệm Raw Archiver
        if self.enable_archive and self.raw_archiver:
            try:
                pending = self.raw_archiver.pending_count
                if pending > 0:
                    logger.info(
                        f"Đang xả {pending} sự kiện còn lưu trong đệm Raw Archiver xuống đĩa..."
                    )
                    final_manifest = self.raw_archiver.flush()
                    if final_manifest is not None:
                        self._stats["batches_committed"] += 1
                        logger.info(
                            f"Final archive batch committed: ID={final_manifest.manifest_id[:8]} "
                            f"({final_manifest.record_count} records)"
                        )
                else:
                    logger.debug("Raw Archiver buffer rỗng, không cần flush thêm.")
            except Exception as exc:
                logger.error(f"Lỗi khi flush Raw Archiver trong teardown: {exc}")

        # 2. Xả bộ nhớ đệm Kafka Producer
        if self.enable_kafka and self.kafka_producer:
            try:
                logger.info("Đang chờ Kafka Producer flush message trong hàng đợi (10s)...")
                remaining = self.kafka_producer.flush(timeout=10.0)
                if remaining > 0:
                    logger.warning(
                        f"Cảnh báo: Còn {remaining} messages chưa kịp gửi trước khi thoát Kafka!"
                    )
                else:
                    logger.info("Toàn bộ tin nhắn Kafka đã được xác nhận (ACKed) thành công.")
            except Exception as exc:
                logger.error(f"Lỗi khi flush Kafka Producer trong teardown: {exc}")

        # 3. Tính toán tổng kết thống kê
        end_utc = datetime.now(UTC)
        duration_sec = time.monotonic() - start_mono
        self._stats["end_time"] = end_utc.isoformat()
        self._stats["duration_seconds"] = round(duration_sec, 2)
        if self._stats["stop_reason"] == "RUNNING":
            self._stats["stop_reason"] = "COMPLETED"

        logger.info(
            "=== INGESTION PIPELINE ĐÃ DỪNG AN TOÀN ===\n"
            f"  - Thời gian chạy: {self._stats['duration_seconds']}s\n"
            f"  - Tổng sự kiện nhận từ SSE: {self._stats['received']}\n"
            f"  - Đã gửi vào Kafka: {self._stats['produced']}\n"
            f"  - Đã lưu vào đĩa JSONL: {self._stats['archived']}\n"
            f"  - Số file manifest đã commit: {self._stats['batches_committed']}\n"
            f"  - Lý do dừng: {self._stats['stop_reason']}"
        )


def build_cli_parser() -> argparse.ArgumentParser:
    """Tạo parser dòng lệnh cho Ingestion Runner."""
    parser = argparse.ArgumentParser(
        description="Wikimedia Recent Changes Streaming Ingestion Runner"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Số lượng sự kiện tối đa cần thu thập trước khi tự động dừng.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        dest="max_duration_sec",
        help="Thời gian chạy tối đa (tính theo giây) trước khi tự động dừng.",
    )
    parser.add_argument(
        "--log-interval",
        type=int,
        default=50,
        help="Tần suất in log tiến độ (mặc định 50 events).",
    )
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Vô hiệu hóa đẩy sự kiện vào Kafka broker (chỉ ghi file raw archive).",
    )
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Vô hiệu hóa ghi file raw archive (chỉ đẩy vào Kafka broker).",
    )
    return parser


def main() -> None:
    """Điểm khởi chạy trực tiếp từ terminal."""
    parser = build_cli_parser()
    args = parser.parse_args()

    from src.common.config import get_settings

    settings = get_settings()
    endpoint = (
        start_metrics_endpoint(settings.prometheus_port) if settings.metrics_enabled else None
    )
    runner = None
    try:
        runner = IngestionRunner(
            enable_kafka=not args.no_kafka,
            enable_archive=not args.no_archive,
        )
        runner.run(
            max_events=args.max_events,
            max_duration_sec=args.max_duration_sec,
            log_interval=args.log_interval,
        )
    except KeyboardInterrupt:
        logger.info("Nhận KeyboardInterrupt trực tiếp từ terminal console.")
        if runner is not None:
            runner.stop()
    except Exception as exc:
        logger.critical(f"Chương trình dừng do lỗi nghiêm trọng: {exc}")
        sys.exit(1)
    finally:
        if endpoint:
            endpoint[0].shutdown()
            endpoint[0].server_close()
            endpoint[1].join(timeout=3)


if __name__ == "__main__":
    main()
