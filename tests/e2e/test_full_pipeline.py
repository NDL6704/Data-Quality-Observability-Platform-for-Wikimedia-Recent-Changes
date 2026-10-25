"""Opt-in public-network smoke through actual Kafka/PostgreSQL and sink receipts."""

import os

import pytest

from scripts.verify_pipeline import verify_pipeline


@pytest.mark.integration
@pytest.mark.skipif(os.getenv("RUN_LIVE_E2E") != "1", reason="set RUN_LIVE_E2E=1 for live SSE")
def test_full_pipeline() -> None:
    """Require all live archive/DB/offset/sink/metric checks, retaining evidence."""
    report = verify_pipeline()
    assert report["passed"], report
