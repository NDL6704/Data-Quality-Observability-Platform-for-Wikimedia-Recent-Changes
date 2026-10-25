"""Unit tests for WikimediaKafkaProducer and partition key extraction."""

from typing import Any
from unittest.mock import MagicMock, patch

from src.ingestion.kafka_producer import (
    WikimediaKafkaProducer,
    extract_partition_key,
)
from src.ingestion.sse_client import RawStreamEvent


def test_extract_partition_key_with_explicit_id() -> None:
    """Verify that {wiki}:{id} is selected when id is present in payload."""
    event = RawStreamEvent(
        event_id="evt_01",
        event_type="message",
        data={"wiki": "enwiki", "id": 123456, "title": "Main_Page"},
        raw_payload='{"wiki": "enwiki", "id": 123456}',
        received_at_epoch=1700000000.0,
    )
    key = extract_partition_key(event)
    assert key == "enwiki:123456"


def test_extract_partition_key_with_meta_id() -> None:
    """Verify fallback to meta.id when top-level id is absent."""
    event = RawStreamEvent(
        event_id="evt_02",
        event_type="message",
        data={"wiki": "viwiki", "meta": {"id": "meta-uuid-999"}, "title": "Trang_Chính"},
        raw_payload='{"wiki": "viwiki", "meta": {"id": "meta-uuid-999"}}',
        received_at_epoch=1700000000.0,
    )
    key = extract_partition_key(event)
    assert key == "viwiki:meta-uuid-999"


def test_extract_partition_key_with_title_fallback() -> None:
    """Verify fallback to title when no id is available."""
    event = RawStreamEvent(
        event_id=None,
        event_type="message",
        data={"wiki": "dewiki", "title": "Berlin"},
        raw_payload='{"wiki": "dewiki", "title": "Berlin"}',
        received_at_epoch=1700000000.0,
    )
    key = extract_partition_key(event)
    assert key == "dewiki:Berlin"


def test_extract_partition_key_hash_fallback() -> None:
    """Verify deterministic hash fallback when both id and title are absent."""
    event = RawStreamEvent(
        event_id=None,
        event_type="message",
        data={},
        raw_payload="empty_payload",
        received_at_epoch=1700000000.0,
    )
    key = extract_partition_key(event)
    assert key.startswith("unknown_wiki:")
    assert len(key.split(":")[1]) == 16


@patch("src.ingestion.kafka_producer.AdminClient")
@patch("src.ingestion.kafka_producer.Producer")
def test_producer_produce_success(
    mock_producer_cls: MagicMock, mock_admin_cls: MagicMock
) -> None:
    """Verify successful event produce with correct serialization and headers."""
    mock_producer_inst = MagicMock()
    mock_producer_cls.return_value = mock_producer_inst

    # Mock admin client so topic creation succeeds without error
    mock_admin_inst = MagicMock()
    mock_metadata = MagicMock()
    mock_metadata.topics = {"rc.raw.v1": MagicMock()}
    mock_admin_inst.list_topics.return_value = mock_metadata
    mock_admin_cls.return_value = mock_admin_inst

    producer_wrapper = WikimediaKafkaProducer(
        bootstrap_servers="localhost:9092",
        default_topic="rc.raw.v1",
        auto_create_topic=False,
    )

    event = RawStreamEvent(
        event_id="test_sse_123",
        event_type="message",
        data={"wiki": "enwiki", "id": 9999},
        raw_payload='{"wiki": "enwiki", "id": 9999}',
        received_at_epoch=1700000000.0,
    )

    success = producer_wrapper.produce(event)

    assert success is True
    assert producer_wrapper.stats["produced"] == 1
    mock_producer_inst.produce.assert_called_once()
    call_kwargs: dict[str, Any] = mock_producer_inst.produce.call_args.kwargs

    assert call_kwargs["topic"] == "rc.raw.v1"
    assert call_kwargs["key"] == b"enwiki:9999"
    assert call_kwargs["value"] == b'{"wiki": "enwiki", "id": 9999}'
    assert any(h[0] == "source" and h[1] == b"wikimedia-sse" for h in call_kwargs["headers"])
    assert any(h[0] == "sse_event_id" and h[1] == b"test_sse_123" for h in call_kwargs["headers"])


@patch("src.ingestion.kafka_producer.Producer")
def test_producer_handles_buffer_error_with_retry(mock_producer_cls: MagicMock) -> None:
    """Verify that BufferError triggers poll and retries up to success."""
    mock_producer_inst = MagicMock()
    # Lần 1 ném BufferError, lần 2 thành công (None)
    mock_producer_inst.produce.side_effect = [BufferError("Local: Queue full"), None]
    mock_producer_cls.return_value = mock_producer_inst

    producer_wrapper = WikimediaKafkaProducer(
        bootstrap_servers="localhost:9092",
        default_topic="rc.raw.v1",
        auto_create_topic=False,
    )

    event = RawStreamEvent(
        event_id="evt_retry",
        event_type="message",
        data={"wiki": "frwiki", "id": 777},
        raw_payload='{"wiki": "frwiki", "id": 777}',
        received_at_epoch=1700000000.0,
    )

    success = producer_wrapper.produce(event)

    assert success is True
    assert producer_wrapper.stats["buffer_full_retries"] == 1
    assert producer_wrapper.stats["produced"] == 1
    assert mock_producer_inst.produce.call_count == 2
    mock_producer_inst.poll.assert_any_call(0.5)


@patch("src.ingestion.kafka_producer.Producer")
def test_producer_delivery_callback_updates_stats(mock_producer_cls: MagicMock) -> None:
    """Verify that delivery callback updates delivered and failed counters."""
    mock_producer_inst = MagicMock()
    mock_producer_cls.return_value = mock_producer_inst

    producer_wrapper = WikimediaKafkaProducer(
        bootstrap_servers="localhost:9092",
        default_topic="rc.raw.v1",
        auto_create_topic=False,
    )

    mock_msg = MagicMock()
    mock_msg.topic.return_value = "rc.raw.v1"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 42

    # Giả lập thành công (err = None)
    producer_wrapper._delivery_callback(None, mock_msg)
    assert producer_wrapper.stats["delivered"] == 1

    # Giả lập lỗi (err != None)
    mock_error = MagicMock()
    producer_wrapper._delivery_callback(mock_error, mock_msg)
    assert producer_wrapper.stats["failed"] == 1
