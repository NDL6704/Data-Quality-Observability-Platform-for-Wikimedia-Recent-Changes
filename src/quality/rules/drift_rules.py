"""Consumer-local unknown-event-type drift, not global schema-shape monitoring."""

from collections import deque
from typing import Any

from schemas.models import WikimediaRecentChange
from src.quality.engine import QualityRule, RuleCategory, RuleResult, RuleSeverity
from src.quality.rules.schema_rules import VALID_EVENT_TYPES


class EventTypeDriftRule(QualityRule):
    """Warn when unknown types reach 5% in a full bounded window of 100 events.

    State is local to this rule instance; not a global Kafka-wide incident metric.
    """

    def __init__(self, window_size: int = 100, threshold: float = 0.05) -> None:
        if type(window_size) is not int or window_size <= 0 or not 0 < threshold <= 1:
            raise ValueError("Invalid drift window or threshold")
        super().__init__(
            "R-DRF-01",
            "1.1.0",
            RuleCategory.DRIFT,
            RuleSeverity.MEDIUM,
            "Unknown change-type ratio below threshold",
        )
        self._window: deque[bool] = deque(maxlen=window_size)
        self._threshold = threshold

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        self._window.append(event.type not in VALID_EVENT_TYPES)
        ratio = sum(self._window) / len(self._window)
        ready = len(self._window) == self._window.maxlen
        details = {"unknown_ratio": ratio, "sample_count": len(self._window), "ready": ready}
        if ready and ratio >= self._threshold:
            return self._fail("WARN_SCHEMA_DRIFT", details)
        if event.type not in VALID_EVENT_TYPES:
            return self._fail("WARN_UNKNOWN_EVENT_TYPE", details)
        return self._pass(details)
