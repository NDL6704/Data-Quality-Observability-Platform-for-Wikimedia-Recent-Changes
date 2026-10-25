"""
Wikimedia Recent Changes Server-Sent Events (SSE) Client.

Sử dụng thư viện HTTPX để đọc stream HTTP liên tục từ Wikimedia EventStreams:
- Tuân thủ User-Agent policy của Wikimedia.
- Parse bản tin SSE line-by-line (id, data, event) qua response.iter_lines().
- Quản lý cursor Last-Event-ID phục hồi vị trí khi mạng gián đoạn.
- Tự động kết nối lại (Auto-reconnect) với Exponential Backoff và Jitter.
"""

import json
import random
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import httpx

from src.common.config import get_settings
from src.common.logging import setup_logger
from src.observability.metrics import get_metrics

logger = setup_logger("sse_client")

# User-Agent chuẩn mực tuân thủ RFC và Wikimedia Robot Policy
DEFAULT_WIKIMEDIA_USER_AGENT = (
    "WikimediaDataQualityPlatform/1.0 "
    "(https://github.com/wikimedia-dq/platform; mailto:data-observability@example.com)"
)


@dataclass
class RawStreamEvent:
    """Đại diện cho một sự kiện SSE thô đã nhận được."""

    event_id: str | None
    event_type: str
    data: dict[str, Any]
    raw_payload: str
    received_at_epoch: float


class WikimediaSSEClient:
    """Client đọc Server-Sent Events từ Wikimedia EventStreams bằng HTTPX."""

    def __init__(
        self,
        stream_url: str | None = None,
        user_agent: str | None = None,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
        backoff_factor: float = 2.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
    ) -> None:
        settings = get_settings()
        self.stream_url = stream_url or settings.wikimedia_stream_url
        self.user_agent = user_agent or DEFAULT_WIKIMEDIA_USER_AGENT
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor
        self.timeout = httpx.Timeout(
            connect=connect_timeout, read=read_timeout, write=10.0, pool=10.0
        )

        self.last_event_id: str | None = None
        self.metrics = get_metrics()
        self.metrics.connection.labels("recentchange").set(0)
        self._is_running: bool = True

    def stop(self) -> None:
        """Kích hoạt cờ dừng stream an toàn."""
        logger.info("Yêu cầu dừng SSE client...")
        self._is_running = False
        self.metrics.connection.labels("recentchange").set(0)

    def stream_events(self) -> Generator[RawStreamEvent, None, None]:
        """
        Generator liên tục yield các sự kiện từ Wikimedia.
        Tự động reconnect với Exponential Backoff nếu gặp sự cố mạng.
        """
        reconnect_attempts = 0

        while self._is_running:
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "text/event-stream",
                "Cache-Control": "no-cache",
            }
            if self.last_event_id:
                headers["Last-Event-ID"] = self.last_event_id

            logger.info(
                f"Đang kết nối tới Wikimedia stream: {self.stream_url} "
                f"(Last-Event-ID={self.last_event_id})"
            )

            try:
                with httpx.Client(timeout=self.timeout) as client:
                    with client.stream("GET", self.stream_url, headers=headers) as response:
                        if response.status_code != 200:
                            error_body = response.read().decode("utf-8", errors="ignore")[:200]
                            logger.error(
                                f"Kết nối thất bại với HTTP status: {response.status_code}. "
                                f"Nội dung: {error_body}"
                            )
                            raise ConnectionError(
                                f"HTTP Status {response.status_code}: {error_body}"
                            )

                        logger.info(
                            "Kết nối stream thành công (HTTP 200). Đang lắng nghe sự kiện..."
                        )
                        reconnect_attempts = 0  # Reset bộ đếm khi kết nối thành công
                        self.metrics.connection.labels("recentchange").set(1)

                        # Buffer gom các dòng thuộc về 1 event SSE
                        current_id: str | None = None
                        current_event: str = "message"
                        current_data_lines: list[str] = []

                        # Duyệt qua từng dòng do httpx tự động decode UTF-8
                        for line in response.iter_lines():
                            if not self._is_running:
                                break

                            line_stripped = line.strip()

                            # Dòng trống biểu thị kết thúc một block SSE message
                            if not line_stripped:
                                if current_data_lines:
                                    raw_payload = "\n".join(current_data_lines)
                                    try:
                                        parsed_data = json.loads(raw_payload)
                                        if current_id:
                                            self.last_event_id = current_id

                                        event = RawStreamEvent(
                                            event_id=current_id,
                                            event_type=current_event,
                                            data=parsed_data,
                                            raw_payload=raw_payload,
                                            received_at_epoch=time.time(),
                                        )
                                        yield event
                                    except json.JSONDecodeError as exc:
                                        logger.warning(f"Bỏ qua payload JSON không hợp lệ: {exc}")

                                    # Reset buffer chuẩn bị cho event tiếp theo
                                    current_id = None
                                    current_event = "message"
                                    current_data_lines = []
                                continue

                            # Bỏ qua dòng comment SSE (bắt đầu bằng dấu hai chấm, ví dụ: ':ok')
                            if line.startswith(":"):
                                continue

                            if ":" in line:
                                field, value = line.split(":", 1)
                                field = field.strip()
                                # Quy chuẩn SSE: nếu có space ngay sau dấu ':' thì bỏ đi
                                if value.startswith(" "):
                                    value = value[1:]

                                if field == "id":
                                    current_id = value
                                elif field == "event":
                                    current_event = value
                                elif field == "data":
                                    current_data_lines.append(value)

                if self._is_running:
                    self.metrics.reconnects.labels("stream_eof").inc()
            except Exception as exc:
                self.metrics.connection.labels("recentchange").set(0)
                if not self._is_running:
                    break

                reconnect_attempts += 1
                self.metrics.reconnects.labels("stream_error").inc()
                # Exponential backoff với Full Jitter
                calculated_backoff = min(
                    self.max_backoff,
                    self.initial_backoff * (self.backoff_factor ** (reconnect_attempts - 1)),
                )
                jittered_backoff = random.uniform(0.5, 1.0) * calculated_backoff

                logger.warning(
                    f"Mất kết nối hoặc phát sinh ngoại lệ: {exc}. "
                    f"Thử lại lần {reconnect_attempts} sau {jittered_backoff:.2f}s..."
                )
                time.sleep(jittered_backoff)
            finally:
                self.metrics.connection.labels("recentchange").set(0)

        logger.info("SSE Client đã dừng hoàn toàn.")
