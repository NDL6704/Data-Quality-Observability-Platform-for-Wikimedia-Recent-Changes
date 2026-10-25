"""Process-local Prometheus instrumentation with bounded label vocabularies."""

from functools import lru_cache
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

RULE_IDS = frozenset(
    {
        "R-SCH-01",
        "R-SCH-02",
        "R-CMP-01",
        "R-CMP-02",
        "R-VAL-01",
        "R-VAL-02",
        "R-VAL-03",
        "R-DED-01",
        "R-DED-02",
        "R-FRS-01",
        "R-DRF-01",
    }
)
FAILURE_CODES = frozenset(
    {
        "ERR_INVALID_BYTE_LENGTH",
        "ERR_INVALID_JSON",
        "ERR_INVALID_TIMESTAMP",
        "ERR_MISSING_ENVELOPE",
        "ERR_RULE_EXECUTION_EXCEPTION",
        "ERR_SCHEMA_VALIDATION",
        "ERR_MISSING_IDENTITY",
        "ERR_MISSING_TITLE",
        "WARN_WEAK_EVENT_KEY",
        "WARN_SCHEMA_DRIFT",
        "WARN_UNKNOWN_EVENT_TYPE",
        "WARN_FUTURE_EVENT",
        "WARN_STALE_EVENT",
        "ERR_INVALID_NAMESPACE",
        "INFO_DUPLICATE_DELIVERY",
    }
)


def wiki_label(value: Any) -> str:
    """Avoid unbounded wiki labels supplied by arbitrary raw payloads."""
    known = {"enwiki", "viwiki", "dewiki", "frwiki", "commonswiki"}
    return value if isinstance(value, str) and value in known else "other"


class Metrics:
    """Independent registries allow tests and embedded components to coexist.

    Counters count observed deliveries/attempts, not unique business events.
    No event key, URL, exception text, consumer UUID or file path is a label.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        def counter(name: str, help: str, labels: tuple[str, ...] = ()) -> Counter:
            return Counter("wikimedia_" + name, help, labels, registry=self.registry)

        def gauge(name: str, help: str, labels: tuple[str, ...] = ()) -> Gauge:
            return Gauge("wikimedia_" + name, help, labels, registry=self.registry)

        def histogram(name: str, help: str, labels: tuple[str, ...]) -> Histogram:
            return Histogram(
                "wikimedia_" + name,
                help,
                labels,
                registry=self.registry,
                buckets=(0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 1800),
            )

        self.ingest_events = counter(
            "ingest_events_total", "SSE events observed", ("wiki", "status")
        )
        self.ingest_bytes = counter("ingest_bytes_total", "UTF-8 raw bytes observed", ("wiki",))
        self.reconnects = counter("ingest_reconnects_total", "SSE reconnect attempts", ("reason",))
        self.connection = gauge(
            "ingest_connection_state", "SSE HTTP connection state", ("endpoint",)
        )
        self.throughput = counter(
            "consumer_throughput_total", "Source-acknowledged deliveries", ("status",)
        )
        self.lag = gauge(
            "consumer_lag_records", "High watermark minus durable offset", ("topic", "partition")
        )
        self.latency = histogram(
            "processing_latency_seconds", "Observed stage duration", ("stage",)
        )
        self.ingestion_delay = histogram(
            "ingestion_delay_seconds", "Original receipt minus source time", ("wiki",)
        )
        self.rule_failures = counter(
            "quality_rule_failures_total",
            "Failed prechecks per evaluation",
            ("rule_id", "severity"),
        )
        self.quarantine = counter(
            "quarantine_events_total", "DB-committed rejection per failure code", ("failure_code",)
        )
        self.duplicates = counter(
            "duplicates_detected_total", "DB-committed duplicate observations", ("wiki",)
        )
        self.db_errors = counter("database_errors_total", "DB failures per attempt", ("operation",))
        self.incidents = gauge(
            "active_incidents_count", "OPEN or INVESTIGATING episodes", ("severity",)
        )
        self.recovery = histogram(
            "recovery_duration_seconds", "Measured harness recovery duration", ("scenario_id",)
        )
        self.stage_errors = counter(
            "consumer_stage_errors_total", "Failed consumer attempts", ("stage",)
        )
        self.retries = counter("consumer_retries_total", "Scheduled retries", ("stage",))
        self.probe = gauge("observability_probe_success", "Last poll succeeded", ("component",))
        self.archive_corrupt = gauge("archive_corrupted_files", "DB manifests marked CORRUPTED")
        self.archive_errors = counter(
            "archive_verification_errors_total", "Failed archive verifications"
        )
        self.poll_timestamp = gauge(
            "observability_last_poll_timestamp_seconds", "Last completed poll"
        )
        self.receipt_fallback = counter(
            "receipt_fallback_total", "Untrustworthy receipt headers", ("reason",)
        )
        self.clock_skew = counter(
            "source_clock_skew_total", "Source timestamp after receipt", ("wiki",)
        )
        for operation in ("quality_transaction", "observability", "incident"):
            self.db_errors.labels(operation).inc(0)
        for status in ("accepted", "quarantined"):
            self.throughput.labels(status).inc(0)
        for severity in ("CRITICAL", "HIGH", "MEDIUM", "INFO"):
            self.incidents.labels(severity).set(0)


@lru_cache
def get_metrics() -> Metrics:
    """Share one registry per Python process, never bind a port during import."""
    return Metrics()


def start_metrics_endpoint(
    port: int, *, metrics: Metrics | None = None, host: str = "0.0.0.0"
) -> Any:
    """Return (server, thread); caller owns shutdown/server_close/join."""
    return start_http_server(port, addr=host, registry=(metrics or get_metrics()).registry)
