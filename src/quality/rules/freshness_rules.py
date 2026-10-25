"""Receipt-time freshness and bounded, consumer-local drift observations."""

from datetime import UTC, datetime
from typing import Any

from schemas.models import WikimediaRecentChange
from src.quality.engine import QualityRule, RuleCategory, RuleResult, RuleSeverity


class FreshnessRule(QualityRule):
    """Compare event time with receipt time, injected by the engine input gate."""

    def __init__(
        self, max_staleness_seconds: int = 300, max_future_skew_seconds: int = 2
    ) -> None:
        if type(max_staleness_seconds) is not int or max_staleness_seconds < 0:
            raise ValueError("max_staleness_seconds must be a non-negative integer")
        if type(max_future_skew_seconds) is not int or not 0 <= max_future_skew_seconds <= 5:
            raise ValueError("max_future_skew_seconds must be an integer between 0 and 5")
        super().__init__(
            "R-FRS-01",
            "1.2.0",
            RuleCategory.FRESHNESS,
            RuleSeverity.MEDIUM,
            "Receipt delay within configured SLA",
        )
        self.max_staleness_seconds = max_staleness_seconds
        self.max_future_skew_seconds = max_future_skew_seconds

    def evaluate_at(self, event: WikimediaRecentChange, received_at: datetime) -> RuleResult:
        """Allow small clock skew; never hide large offsets or change original receipt."""
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        age = (received_at - event.meta.dt).total_seconds()
        details = {
            "age_seconds": age,
            "received_at": received_at.isoformat(),
            "max_staleness_seconds": self.max_staleness_seconds,
            "max_future_skew_seconds": self.max_future_skew_seconds,
            "event_timestamp": event.meta.dt.isoformat(),
            "within_future_skew_tolerance": -self.max_future_skew_seconds <= age < 0,
        }
        if age < -self.max_future_skew_seconds:
            return self._fail("WARN_FUTURE_EVENT", details)
        if age > self.max_staleness_seconds:
            return self._fail("WARN_STALE_EVENT", details)
        return self._pass(details)

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        """Direct calls use processing time as a receipt-time approximation."""
        return self.evaluate_at(event, datetime.now(UTC))
