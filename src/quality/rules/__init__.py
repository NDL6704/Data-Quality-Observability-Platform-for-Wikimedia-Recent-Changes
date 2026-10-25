"""Ten default rules; persistent duplicate detection belongs to the repository."""

from src.common.config import get_settings
from src.quality.engine import QualityRule
from src.quality.rules.completeness_rules import IdentityCompletenessRule, TitleCompletenessRule
from src.quality.rules.deduplication_rules import WeakEventKeyRule
from src.quality.rules.drift_rules import EventTypeDriftRule
from src.quality.rules.freshness_rules import FreshnessRule
from src.quality.rules.schema_rules import JsonObjectRule, SchemaEnvelopeRule
from src.quality.rules.validity_rules import (
    ByteLengthValidityRule,
    NamespaceValidityRule,
    TimestampValidityRule,
)


def build_default_rule_set(
    max_staleness_seconds: int = 300, max_future_skew_seconds: int | None = None
) -> list[QualityRule]:
    """Create independent rule instances, including isolated rolling-window state."""
    return [
        SchemaEnvelopeRule(),
        JsonObjectRule(),
        IdentityCompletenessRule(),
        TitleCompletenessRule(),
        TimestampValidityRule(),
        ByteLengthValidityRule(),
        NamespaceValidityRule(),
        WeakEventKeyRule(),
        FreshnessRule(
            max_staleness_seconds,
            get_settings().quality_max_future_skew_seconds
            if max_future_skew_seconds is None
            else max_future_skew_seconds,
        ),
        EventTypeDriftRule(),
    ]
