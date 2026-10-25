"""Real SQL cohort selection; historical and current receipt times remain distinct."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dashboards.streamlit import data
from src.quality.engine import RuleCategory, RuleResult, RuleSeverity
from src.storage.db import get_db_connection
from src.storage.repository import QualityRepository


@pytest.mark.integration
def test_original_receipt_and_version_filters() -> None:
    keys = ["cohort:" + uuid4().hex, "cohort:" + uuid4().hex]
    # Future test cohort isolates this SQL test from an ongoing real live workload.
    cutoff = datetime(2099, 1, 1, tzinfo=UTC)
    try:
        with get_db_connection() as conn:
            for key, version, receipt in zip(
                keys, ["1.1.0", "1.2.0"], [cutoff - timedelta(days=1), cutoff], strict=True
            ):
                QualityRepository(conn).record_quality_results(
                    key,
                    [
                        RuleResult(
                            "R-FRS-01",
                            version,
                            RuleCategory.FRESHNESS,
                            RuleSeverity.MEDIUM,
                            passed=True,
                            details={"received_at": receipt.isoformat()},
                        )
                    ],
                )
        result = data.quality(1, "R-FRS-01", "1.2.0", receipt=cutoff)
        assert len(result) == 1 and result[0]["checked"] == 1
        assert result[0]["failed"] == 0 and result[0]["rule_version"] == "1.2.0"
        assert data.quality(1, "R-FRS-01", "1.1.0", receipt=cutoff) == []
        assert data.quality(1, since=cutoff) == []
        assert sum(int(r["checked"]) for r in data.quality_trend(1, receipt=cutoff)) == 1
    finally:
        with get_db_connection() as conn:
            conn.execute(
                "DELETE FROM quality_results WHERE failure_details->0->>'event_key'=ANY(%s)",
                (keys,),
            )
