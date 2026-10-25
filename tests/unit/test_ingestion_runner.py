"""Unit tests for Wikimedia Recent Changes IngestionRunner."""

import time
from collections.abc import Generator
from unittest.mock import MagicMock

import pytest

from src.ingestion.kafka_producer import WikimediaKafkaProducer
from src.ingestion.raw_archiver import BatchManifest, RawArchiver
from src.ingestion.run_ingestor import IngestionRunner, build_cli_parser
from src.ingestion.sse_client import RawStreamEvent, WikimediaSSEClient


def _make_mock_event(event_id: str = "evt-1", wiki: str = "enwiki") -> RawStreamEvent:
    """Tạo event giả lập phục vụ unit test."""
    return RawStreamEvent(
        event_id=event_id,
        event_type="message",
        data={
            "meta": {"id": event_id},
            "id": 1001,
            "wiki": wiki,
            "title": "Artificial Intelligence",
            "type": "edit",
        },
        raw_payload=f'{{"meta": {{"id": "{event_id}"}}, "wiki": "{wiki}"}}',
        received_at_epoch=time.time(),
    )


class MockSSEClient(WikimediaSSEClient):
    """Client giả lập sinh sự kiện kiểm thử có kiểm soát."""

    def __init__(self, events: list[RawStreamEvent] | None = None) -> None:
        super().__init__(stream_url="http://mock-stream")
        self.events_to_yield = events or []
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True
        self._is_running = False

    def stream_events(self) -> Generator[RawStreamEvent, None, None]:
        for evt in self.events_to_yield:
            if not self._is_running:
                break
            yield evt


def test_runner_initialization_defaults_and_options() -> None:
    """Kiểm tra khởi tạo runner với các cờ bật tắt thành phần."""
    mock_client = MockSSEClient([])
    mock_producer = MagicMock(spec=WikimediaKafkaProducer)
    mock_archiver = MagicMock(spec=RawArchiver)

    runner = IngestionRunner(
        sse_client=mock_client,
        kafka_producer=mock_producer,
        raw_archiver=mock_archiver,
        enable_kafka=True,
        enable_archive=True,
        auto_handle_signals=False,
    )

    assert runner.is_running is True
    assert runner.enable_kafka is True
    assert runner.enable_archive is True
    assert runner.stats["received"] == 0
    assert runner.stats["produced"] == 0
    assert runner.stats["archived"] == 0


def test_runner_processes_events_to_kafka_and_archiver() -> None:
    """Xác minh runner điều phối toàn bộ event vào Kafka và Raw Archiver."""
    events = [_make_mock_event(f"evt-{i}", "viwiki") for i in range(5)]
    mock_client = MockSSEClient(events)

    mock_producer = MagicMock(spec=WikimediaKafkaProducer)
    mock_producer.produce.return_value = True
    mock_producer.flush.return_value = 0

    mock_archiver = MagicMock(spec=RawArchiver)
    mock_archiver.append.return_value = None
    mock_archiver.pending_count = 5
    mock_archiver.flush.return_value = MagicMock(
        spec=BatchManifest, manifest_id="man-12345678", record_count=5
    )

    runner = IngestionRunner(
        sse_client=mock_client,
        kafka_producer=mock_producer,
        raw_archiver=mock_archiver,
        enable_kafka=True,
        enable_archive=True,
        auto_handle_signals=False,
    )

    stats = runner.run()

    # Kiểm tra số lượng gọi produce và append
    assert mock_producer.produce.call_count == 5
    assert mock_archiver.append.call_count == 5

    # Kiểm tra teardown flushes
    mock_archiver.flush.assert_called_once()
    mock_producer.flush.assert_called_once_with(timeout=10.0)

    # Kiểm tra thống kê trả về
    assert stats["received"] == 5
    assert stats["produced"] == 5
    assert stats["archived"] == 5
    assert stats["batches_committed"] == 1
    assert stats["stop_reason"] == "COMPLETED"


def test_runner_stops_at_max_events() -> None:
    """Xác minh runner ngắt vòng lặp chính xác khi đạt giới hạn max_events."""
    events = [_make_mock_event(f"evt-{i}") for i in range(10)]
    mock_client = MockSSEClient(events)

    mock_producer = MagicMock(spec=WikimediaKafkaProducer)
    mock_producer.produce.return_value = True

    mock_archiver = MagicMock(spec=RawArchiver)
    mock_archiver.append.return_value = None
    mock_archiver.pending_count = 0

    runner = IngestionRunner(
        sse_client=mock_client,
        kafka_producer=mock_producer,
        raw_archiver=mock_archiver,
        auto_handle_signals=False,
    )

    stats = runner.run(max_events=3)

    assert stats["received"] == 3
    assert stats["produced"] == 3
    assert stats["archived"] == 3
    assert stats["stop_reason"] == "MAX_EVENTS_REACHED"
    assert mock_producer.produce.call_count == 3


def test_runner_stops_at_max_duration() -> None:
    """Xác minh runner tự động dừng khi vượt quá max_duration_sec."""

    def infinite_events() -> Generator[RawStreamEvent, None, None]:
        idx = 0
        while True:
            idx += 1
            yield _make_mock_event(f"evt-{idx}")
            time.sleep(0.02)

    mock_client = MagicMock(spec=WikimediaSSEClient)
    mock_client.stream_events.return_value = infinite_events()

    mock_producer = MagicMock(spec=WikimediaKafkaProducer)
    mock_producer.produce.return_value = True

    mock_archiver = MagicMock(spec=RawArchiver)
    mock_archiver.append.return_value = None
    mock_archiver.pending_count = 0

    runner = IngestionRunner(
        sse_client=mock_client,
        kafka_producer=mock_producer,
        raw_archiver=mock_archiver,
        auto_handle_signals=False,
    )

    stats = runner.run(max_duration_sec=0.08)

    assert stats["stop_reason"] == "MAX_DURATION_REACHED"
    assert stats["received"] > 0
    assert stats["duration_seconds"] >= 0.05


def test_runner_external_stop_signal() -> None:
    """Xác minh gọi runner.stop() ngắt client và dừng vòng lặp an toàn."""
    events = [_make_mock_event(f"evt-{i}") for i in range(10)]
    mock_client = MockSSEClient(events)

    mock_producer = MagicMock(spec=WikimediaKafkaProducer)
    mock_archiver = MagicMock(spec=RawArchiver)
    mock_archiver.pending_count = 0

    runner = IngestionRunner(
        sse_client=mock_client,
        kafka_producer=mock_producer,
        raw_archiver=mock_archiver,
        auto_handle_signals=False,
    )

    # Giả lập sau event thứ 2 thì stop() được gọi
    def side_effect_produce(event: RawStreamEvent) -> bool:
        if runner.stats["received"] >= 2:
            runner.stop()
        return True

    mock_producer.produce.side_effect = side_effect_produce

    stats = runner.run()

    assert runner.is_running is False
    assert mock_client.stopped is True
    assert stats["received"] == 2


def test_runner_teardown_executes_flushes_even_on_exception() -> None:
    """Xác minh quy trình teardown flush disk và broker luôn chạy dù xảy ra Exception."""

    def failing_stream() -> Generator[RawStreamEvent, None, None]:
        yield _make_mock_event("evt-1")
        raise RuntimeError("Mất kết nối bất ngờ!")

    mock_client = MagicMock(spec=WikimediaSSEClient)
    mock_client.stream_events.return_value = failing_stream()

    mock_producer = MagicMock(spec=WikimediaKafkaProducer)
    mock_producer.produce.return_value = True

    mock_archiver = MagicMock(spec=RawArchiver)
    mock_archiver.append.return_value = None
    mock_archiver.pending_count = 1

    runner = IngestionRunner(
        sse_client=mock_client,
        kafka_producer=mock_producer,
        raw_archiver=mock_archiver,
        auto_handle_signals=False,
    )

    with pytest.raises(RuntimeError, match="Mất kết nối bất ngờ!"):
        runner.run()

    # Teardown sequence vẫn phải được gọi đầy đủ
    mock_archiver.flush.assert_called_once()
    mock_producer.flush.assert_called_once_with(timeout=10.0)
    assert runner.stats["stop_reason"] == "ERROR_RuntimeError"


def test_cli_argument_parsing() -> None:
    """Xác minh parser CLI nhận diện chính xác các cờ cấu hình dòng lệnh."""
    parser = build_cli_parser()

    args = parser.parse_args([
        "--max-events", "250",
        "--duration", "15.5",
        "--log-interval", "20",
        "--no-kafka",
        "--no-archive",
    ])

    assert args.max_events == 250
    assert args.max_duration_sec == 15.5
    assert args.log_interval == 20
    assert args.no_kafka is True
    assert args.no_archive is True
