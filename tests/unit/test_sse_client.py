"""Unit tests for WikimediaSSEClient (mocked HTTP streaming)."""

from collections.abc import Generator
from unittest.mock import MagicMock, patch

from src.ingestion.sse_client import RawStreamEvent, WikimediaSSEClient


class MockStreamResponse:
    """Mock cho httpx streaming response."""

    def __init__(self, lines: list[str], status_code: int = 200) -> None:
        self.lines = lines
        self.status_code = status_code

    def __enter__(self) -> "MockStreamResponse":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass

    def iter_lines(self) -> Generator[str, None, None]:
        yield from self.lines


def test_sse_client_parses_events_correctly() -> None:
    """Xác minh client parse đúng id, event, data và bỏ qua comment."""
    mock_lines = [
        ":ok",
        "",
        "event: message",
        'id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":101}]',
        'data: {"id": 1, "wiki": "enwiki", "title": "Machine Learning"}',
        "",
        "event: message",
        'id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":102}]',
        'data: {"id": 2, "wiki": "viwiki", "title": "Trí tuệ nhân tạo"}',
        "",
    ]

    client = WikimediaSSEClient()

    with patch("httpx.Client") as mock_client_cls:
        mock_http_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_http_client
        mock_http_client.stream.return_value = MockStreamResponse(mock_lines)

        events: list[RawStreamEvent] = []
        for event in client.stream_events():
            events.append(event)
            if len(events) == 2:
                client.stop()

        assert len(events) == 2
        assert events[0].data["wiki"] == "enwiki"
        assert events[0].data["title"] == "Machine Learning"
        assert "offset\":101" in (events[0].event_id or "")

        assert events[1].data["wiki"] == "viwiki"
        assert events[1].data["title"] == "Trí tuệ nhân tạo"
        assert client.last_event_id == events[1].event_id


def test_sse_client_handles_malformed_json_gracefully() -> None:
    """Client không được crash khi upstream gửi JSON bị lỗi cú pháp."""
    mock_lines = [
        "event: message",
        'id: [{"offset": 103}]',
        "data: {malformed_json_without_quotes}",
        "",
        "event: message",
        'id: [{"offset": 104}]',
        'data: {"valid": true}',
        "",
    ]

    client = WikimediaSSEClient()

    with patch("httpx.Client") as mock_client_cls:
        mock_http_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_http_client
        mock_http_client.stream.return_value = MockStreamResponse(mock_lines)

        events: list[RawStreamEvent] = []
        for event in client.stream_events():
            events.append(event)
            client.stop()

        # Event đầu bị bỏ qua vì lỗi json, event thứ hai thành công
        assert len(events) == 1
        assert events[0].data["valid"] is True

def test_sse_client_multiline_data_chunk() -> None:
    """Xác minh SSE client nối chính xác các dòng data: liên tiếp bằng ký tự xuống dòng."""
    mock_lines = [
        "event: message",
        'id: [{"offset": 105}]',
        'data: {"wiki": "viwiki",',
        'data:  "title": "Hồ Chí Minh",',
        'data:  "namespace": 0}',
        "",
    ]

    client = WikimediaSSEClient()

    with patch("httpx.Client") as mock_client_cls:
        mock_http_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_http_client
        mock_http_client.stream.return_value = MockStreamResponse(mock_lines)

        events: list[RawStreamEvent] = []
        for event in client.stream_events():
            events.append(event)
            client.stop()

        assert len(events) == 1
        assert events[0].data["wiki"] == "viwiki"
        assert events[0].data["title"] == "Hồ Chí Minh"
        assert events[0].data["namespace"] == 0
        assert "\n" in events[0].raw_payload


def test_sse_backoff_calculation() -> None:
    """Kiểm tra công thức Exponential Backoff không vượt max_backoff và tăng lũy thừa."""
    initial_backoff = 1.0
    max_backoff = 60.0
    backoff_factor = 2.0

    # Lần 1: 1.0 * (2^0) = 1.0
    # Lần 2: 1.0 * (2^1) = 2.0
    # Lần 3: 1.0 * (2^2) = 4.0
    # Lần 4: 1.0 * (2^3) = 8.0
    # Lần 7: 1.0 * (2^6) = 64.0 -> Capped tại max_backoff = 60.0
    expected_backoffs = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]

    for idx, expected in enumerate(expected_backoffs, start=1):
        calc = min(max_backoff, initial_backoff * (backoff_factor ** (idx - 1)))
        assert calc == expected, f"Lần {idx}: tính ra {calc} != kỳ vọng {expected}"
