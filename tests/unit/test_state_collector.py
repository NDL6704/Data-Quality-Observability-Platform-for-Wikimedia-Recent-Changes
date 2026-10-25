"""Retention boundary and stale-probe semantics without changing source offsets."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.common.config import Settings
from src.observability.collector import StateCollector
from src.observability.metrics import Metrics


@pytest.mark.parametrize("offset,expected", [(-1001, 100), (850, 100), (950, 50)])
def test_retained_lag_floor(offset: int, expected: int) -> None:
    with patch("src.observability.collector.Consumer") as cls:
        consumer = cls.return_value
        consumer.list_topics.return_value = SimpleNamespace(
            topics={"rc.raw.v1": SimpleNamespace(error=None, partitions={0: None})}
        )
        consumer.committed.return_value = [SimpleNamespace(partition=0, offset=offset, error=None)]
        consumer.get_watermark_offsets.return_value = (900, 1000)
        collector = StateCollector(Settings(), Metrics())
        collector.poll_kafka()
        assert (
            collector.metrics.registry.get_sample_value(
                "wikimedia_consumer_lag_records", {"topic": "rc.raw.v1", "partition": "0"}
            )
            == expected
        )
        consumer.commit.assert_not_called()


def test_failed_probe_preserves_last_good_gauge() -> None:
    with patch("src.observability.collector.Consumer"):
        metrics = Metrics()
        metrics.lag.labels("raw", "0").set(100)
        collector = StateCollector(Settings(), metrics)
        with (
            patch.object(collector, "poll_kafka", side_effect=RuntimeError("injected")),
            patch.object(collector, "check_archives"),
            patch.object(collector, "poll_database"),
        ):
            assert "kafka" in collector.poll()
        assert (
            metrics.registry.get_sample_value(
                "wikimedia_consumer_lag_records", {"topic": "raw", "partition": "0"}
            )
            == 100
        )
        assert (
            metrics.registry.get_sample_value(
                "wikimedia_observability_probe_success", {"component": "kafka"}
            )
            == 0
        )
