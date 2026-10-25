"""A loopback SSE endpoint that really breaks an HTTP response mid-stream."""

import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from src.faults.contracts import ScenarioReport
from src.ingestion.sse_client import WikimediaSSEClient


def source_disconnect(report: ScenarioReport) -> None:
    """Exercise the production SSE client with real HTTP errors and socket loss."""
    requests: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_GET(self) -> None:
            requests.append({"at": time.monotonic(), "cursor": self.headers.get("Last-Event-ID")})
            number = len(requests)
            if number <= 2:
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            event_id = "cursor-1" if number == 3 else "cursor-2"
            body = f'id: {event_id}\ndata: {{"sequence": {number}}}\n\n'.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            # Deliberately promise more bytes so HTTPX raises on the socket cut.
            self.send_header("Content-Length", str(len(body) + (100 if number == 3 else 0)))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            if number == 3:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = WikimediaSSEClient(
        stream_url=f"http://127.0.0.1:{server.server_port}/events",
        initial_backoff=0.2,
        max_backoff=0.8,
        read_timeout=2,
    )
    events: list[str | None] = []
    errors: list[str] = []

    def read() -> None:
        try:
            stream = client.stream_events()
            try:
                for event in stream:
                    events.append(event.event_id)
                    if len(events) == 2:
                        client.stop()
                        break
            finally:
                stream.close()
        except Exception as exc:
            errors.append(str(exc))

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    try:
        reader.join(timeout=15)
        intervals = [
            requests[i + 1]["at"] - requests[i]["at"] for i in range(min(2, len(requests) - 1))
        ]
        report.evidence.update(
            requests=requests,
            events=events,
            backoff_intervals=intervals,
            fault="HTTP 503 twice, then truncated HTTP body/socket shutdown",
        )
        report.checks.update(
            stream_finished=not reader.is_alive() and not errors,
            reconnected=len(requests) == 4,
            cursor_resumed=len(requests) >= 4 and requests[3]["cursor"] == "cursor-1",
            events_preserved=events == ["cursor-1", "cursor-2"],
            exponential_backoff=len(intervals) == 2
            and intervals[0] >= 0.09
            and intervals[1] >= 0.19,
        )
    finally:
        client.stop()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        reader.join(timeout=4)
