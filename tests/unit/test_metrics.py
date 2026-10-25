"""Metric identity, cardinality and actual HTTP exposition."""

from urllib.request import urlopen

from prometheus_client import generate_latest

from src.observability.metrics import Metrics, start_metrics_endpoint, wiki_label


def test_independent_registries_and_bounded_wikis() -> None:
    first, second = Metrics(), Metrics()
    first.ingest_events.labels(wiki_label("malicious-unique-wiki"), "received").inc()
    assert (
        first.registry.get_sample_value(
            "wikimedia_ingest_events_total", {"wiki": "other", "status": "received"}
        )
        == 1
    )
    assert b"malicious" not in generate_latest(first.registry)
    assert b'wiki="other"' not in generate_latest(second.registry)


def test_all_catalog_families_registered() -> None:
    metrics = Metrics()
    assert len(list(metrics.registry.collect())) >= 14
    metrics.latency.labels("db").observe(0.1)
    assert (
        metrics.registry.get_sample_value(
            "wikimedia_processing_latency_seconds_count", {"stage": "db"}
        )
        == 1
    )


def test_http_metrics_endpoint() -> None:
    server, thread = start_metrics_endpoint(0, metrics=Metrics(), host="127.0.0.1")
    try:
        with urlopen(f"http://127.0.0.1:{server.server_port}/metrics", timeout=5) as response:
            assert b"wikimedia_database_errors_total" in response.read()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
