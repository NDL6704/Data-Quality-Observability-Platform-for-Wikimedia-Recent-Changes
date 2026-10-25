"""Completeness checks for routing identity and page titles."""

from typing import Any

from schemas.models import WikimediaRecentChange
from src.quality.engine import QualityRule, RuleCategory, RuleResult, RuleSeverity


class IdentityCompletenessRule(QualityRule):
    """Reject unusable wiki/server identifiers before persistence."""

    def __init__(self) -> None:
        super().__init__(
            "R-CMP-01",
            "1.1.0",
            RuleCategory.COMPLETENESS,
            RuleSeverity.HIGH,
            "Non-empty wiki and server_name",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        missing = [
            name
            for name in ("wiki", "server_name")
            if not (value := getattr(event, name)) or not value.strip()
        ]
        return self._fail("ERR_MISSING_IDENTITY", {"fields": missing}) if missing else self._pass()


class TitleCompletenessRule(QualityRule):
    """Warn about missing titles only for page change events."""

    def __init__(self) -> None:
        super().__init__(
            "R-CMP-02",
            "1.1.0",
            RuleCategory.COMPLETENESS,
            RuleSeverity.MEDIUM,
            "Title required for edit/new/categorize",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        if event.type in {"edit", "new", "categorize"} and (
            not event.title or not event.title.strip()
        ):
            return self._fail("ERR_MISSING_TITLE")
        return self._pass()
