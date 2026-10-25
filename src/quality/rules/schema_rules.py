"""R-SCH-01 JSON object and R-SCH-02 structural envelope checks."""

from typing import Any

from schemas.models import WikimediaRecentChange
from src.quality.engine import QualityRule, RuleCategory, RuleResult, RuleSeverity

VALID_EVENT_TYPES = frozenset({"edit", "new", "log", "categorize", "external"})


class JsonObjectRule(QualityRule):
    """JSON decoding is performed by the input gate before a model can exist."""

    def __init__(self) -> None:
        super().__init__(
            "R-SCH-01", "1.1.0", RuleCategory.SCHEMA, RuleSeverity.CRITICAL, "Parseable JSON object"
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        return self._pass()


class SchemaEnvelopeRule(QualityRule):
    """IDs are optional: stable IDs are preferred, hash fallback is allowed."""

    def __init__(self) -> None:
        super().__init__(
            "R-SCH-02",
            "1.1.0",
            RuleCategory.SCHEMA,
            RuleSeverity.CRITICAL,
            "Required typed envelope fields",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        meta = raw_payload.get("meta")
        if not isinstance(meta, dict) or not {"dt", "stream"} <= meta.keys():
            return self._fail("ERR_MISSING_ENVELOPE")
        if not {"type", "wiki"} <= raw_payload.keys() or not event.meta.stream.strip():
            return self._fail("ERR_MISSING_ENVELOPE")
        return self._pass()
