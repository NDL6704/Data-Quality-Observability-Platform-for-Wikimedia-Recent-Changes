"""Host-side metrics collector and authenticated local Alertmanager webhook."""

import hmac
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.common.config import get_settings
from src.common.logging import setup_logger
from src.observability.collector import StateCollector
from src.observability.incidents import IncidentManager, parse_notifications
from src.observability.metrics import Metrics

logger = setup_logger("observability")


def make_server(host: str, port: int, metrics: Metrics, token: str) -> ThreadingHTTPServer:
    """POST /alerts commits before 200; invalid input is 400, DB failure 503."""
    manager = IncidentManager()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def respond(self, status: int, body: bytes, content_type: str = "application/json") -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            if self.path == "/metrics":
                self.respond(200, generate_latest(metrics.registry), CONTENT_TYPE_LATEST)
            elif self.path == "/health":
                self.respond(200, b'{"service":"running"}')
            else:
                self.respond(404, b"{}")

        def do_POST(self) -> None:
            self.connection.settimeout(5)
            if self.path != "/alerts":
                self.respond(404, b"{}")
                return
            if not hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + token):
                self.respond(401, b'{"error":"unauthorized"}')
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 1048576:
                    raise ValueError("Invalid content length")
                notifications = parse_notifications(json.loads(self.rfile.read(length)))
            except (ValueError, TypeError):
                self.respond(400, b'{"error":"invalid webhook"}')
                return
            try:
                ids = manager.apply(notifications)
            except Exception:
                metrics.db_errors.labels("incident").inc()
                self.respond(503, b'{"error":"persistence unavailable"}')
                return
            self.respond(200, json.dumps({"incident_ids": ids}).encode())

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server


def main() -> None:
    """Run metrics/webhook HTTP and periodic state refresh until OS shutdown."""
    settings = get_settings()
    metrics = Metrics()
    collector = StateCollector(settings, metrics)
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    server = make_server(
        "0.0.0.0", settings.observability_port, metrics, settings.observability_webhook_token
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        while not stop.is_set():
            errors = collector.poll()
            if errors:
                logger.warning("Probe failures: %s", errors)
            stop.wait(settings.observability_poll_sec)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        collector.close()


if __name__ == "__main__":
    main()
