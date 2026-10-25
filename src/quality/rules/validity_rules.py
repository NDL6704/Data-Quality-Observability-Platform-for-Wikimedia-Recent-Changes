"""Validity checks; raw values prevent coercion from hiding bad input."""

from datetime import datetime
from typing import Any

from schemas.models import WikimediaRecentChange
from src.quality.engine import QualityRule, RuleCategory, RuleResult, RuleSeverity


class TimestampValidityRule(QualityRule):
    """Validate timezone-aware event time; parsing errors are handled at the input gate."""

    def __init__(self) -> None:
        super().__init__(
            "R-VAL-01",
            "1.1.0",
            RuleCategory.VALIDITY,
            RuleSeverity.CRITICAL,
            "Valid timezone-aware meta.dt",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        dt = event.meta.dt
        if not isinstance(dt, datetime) or dt.tzinfo is None or dt.utcoffset() is None:
            return self._fail("ERR_INVALID_TIMESTAMP")
        return self._pass({"event_time": dt.isoformat()})


class ByteLengthValidityRule(QualityRule):
    """Both byte lengths must be non-negative integers when present."""

    def __init__(self) -> None:
        super().__init__(
            "R-VAL-02",
            "1.1.0",
            RuleCategory.VALIDITY,
            RuleSeverity.HIGH,
            "Old/new byte lengths in PostgreSQL INT range [0, 2147483647]",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        length = raw_payload.get("length")
        if length is None:
            return self._pass()
        if not isinstance(length, dict):
            return self._fail("ERR_INVALID_BYTE_LENGTH")
        invalid = [
            name
            for name in ("old", "new")
            if (value := length.get(name)) is not None
            and (type(value) is not int or not 0 <= value <= 2147483647)
        ]
        return (
            self._fail("ERR_INVALID_BYTE_LENGTH", {"fields": invalid}) if invalid else self._pass()
        )


class NamespaceValidityRule(QualityRule):
    """Allow the planned namespace range, including virtual namespaces -2/-1."""

    def __init__(self) -> None:
        super().__init__(
            "R-VAL-03",
            "1.1.0",
            RuleCategory.VALIDITY,
            RuleSeverity.MEDIUM,
            "Namespace integer in [-2, 10000]",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        value = raw_payload.get("namespace")
        if value is not None and (type(value) is not int or not -2 <= value <= 10000):
            return self._fail("ERR_INVALID_NAMESPACE", {"namespace": value})
        return self._pass()
