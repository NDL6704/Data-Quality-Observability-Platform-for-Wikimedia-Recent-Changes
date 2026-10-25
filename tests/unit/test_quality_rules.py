"""Behavioral regression tests for STEP-03.03 and the raw-input boundary."""

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from schemas.models import CuratedRecord, WikimediaRecentChange
from src.quality.engine import QualityEngine, RuleSeverity
from src.quality.rules import build_default_rule_set
from src.quality.rules.drift_rules import EventTypeDriftRule
from src.quality.rules.freshness_rules import FreshnessRule

NOW = datetime(2026, 10, 24, 12, tzinfo=UTC)


@pytest.fixture
def payload() -> dict[str, Any]:
    """Valid event, frozen receipt clock, no network or database."""
    return {
        "meta": {
            "id": "uuid-1",
            "stream": "mediawiki.recentchange",
            "dt": (NOW - timedelta(seconds=60)).isoformat(),
        },
        "id": 12,
        "wiki": "viwiki",
        "server_name": "vi.wikipedia.org",
        "type": "edit",
        "title": "Example",
        "namespace": 0,
        "length": {"old": 10, "new": 20},
    }


def _build_engine() -> QualityEngine:
    """Create a consumer-local rules registry."""
    instance = QualityEngine()
    instance.register_rules(build_default_rule_set())
    return instance


def test_default_contract(payload: dict[str, Any]) -> None:
    rules = build_default_rule_set()
    assert len(rules) == len({r.rule_id for r in rules}) == 10
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert summary.pass_count == 10 and summary.fail_count == 0
    assert summary.is_valid and not summary.should_quarantine


@pytest.mark.parametrize("raw", ['{"meta":', "[]", "null", "42", b"\xff", '{"x":NaN}'])
def test_invalid_json_quarantined(raw: str | bytes) -> None:
    summary = _build_engine().evaluate_payload(raw, received_at=NOW)
    assert summary.should_quarantine
    assert summary.failure_codes == ["ERR_INVALID_JSON"]
    assert summary.highest_severity == RuleSeverity.CRITICAL


@pytest.mark.parametrize("field", ["meta", "type", "wiki"])
def test_missing_envelope(payload: dict[str, Any], field: str) -> None:
    del payload[field]
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert summary.should_quarantine
    assert summary.failure_codes == ["ERR_MISSING_ENVELOPE"]


@pytest.mark.parametrize("field", ["stream", "dt"])
def test_missing_meta_field(payload: dict[str, Any], field: str) -> None:
    del payload["meta"][field]
    assert _build_engine().evaluate_payload(payload, received_at=NOW).should_quarantine


@pytest.mark.parametrize("value", ["", "   "])
def test_empty_stream(payload: dict[str, Any], value: str) -> None:
    payload["meta"]["stream"] = value
    assert (
        "ERR_MISSING_ENVELOPE"
        in _build_engine().evaluate_payload(payload, received_at=NOW).failure_codes
    )


@pytest.mark.parametrize("value", ["bad-date", None, 42])
def test_invalid_event_time(payload: dict[str, Any], value: Any) -> None:
    payload["meta"]["dt"] = value
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert summary.should_quarantine and "ERR_INVALID_TIMESTAMP" in summary.failure_codes


@pytest.mark.parametrize("field", ["wiki", "server_name"])
@pytest.mark.parametrize("value", ["", " ", None])
def test_missing_identity(payload: dict[str, Any], field: str, value: Any) -> None:
    payload[field] = value
    assert _build_engine().evaluate_payload(payload, received_at=NOW).should_quarantine


@pytest.mark.parametrize("kind", ["edit", "new", "categorize", "log", "external"])
@pytest.mark.parametrize("title", [None, "", "  "])
def test_conditional_title(payload: dict[str, Any], kind: str, title: Any) -> None:
    payload.update(type=kind, title=title)
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert not summary.should_quarantine
    assert ("ERR_MISSING_TITLE" in summary.failure_codes) == (kind in {"edit", "new", "categorize"})


@pytest.mark.parametrize(
    "namespace,valid",
    [
        (-3, False),
        (-2, True),
        (-1, True),
        (0, True),
        (10000, True),
        (10001, False),
        (None, True),
        ("abc", False),
        (True, False),
        ("1", False),
    ],
)
def test_namespace_bounds(payload: dict[str, Any], namespace: Any, valid: bool) -> None:
    payload["namespace"] = namespace
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert not summary.should_quarantine
    assert ("ERR_INVALID_NAMESPACE" not in summary.failure_codes) == valid


@pytest.mark.parametrize("field", ["old", "new"])
@pytest.mark.parametrize("value", [-1, True, "4"])
def test_invalid_byte_lengths(payload: dict[str, Any], field: str, value: Any) -> None:
    payload["length"][field] = value
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert summary.should_quarantine and "ERR_INVALID_BYTE_LENGTH" in summary.failure_codes


@pytest.mark.parametrize("length", [None, {}, {"old": 0, "new": 0}, {"old": None}])
def test_optional_lengths(payload: dict[str, Any], length: Any) -> None:
    payload["length"] = length
    assert _build_engine().evaluate_payload(payload, received_at=NOW).fail_count == 0


@pytest.mark.parametrize(
    "age,code",
    [
        (0, None),
        (300, None),
        (300.000001, "WARN_STALE_EVENT"),
        (600, "WARN_STALE_EVENT"),
        (-1, None),
        (-2, None),
        (-2.000001, "WARN_FUTURE_EVENT"),
        (-25.58, "WARN_FUTURE_EVENT"),
    ],
)
def test_freshness_exact_boundaries(payload: dict[str, Any], age: float, code: str | None) -> None:
    payload["meta"]["dt"] = (NOW - timedelta(seconds=age)).isoformat()
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert not summary.should_quarantine
    assert summary.failure_codes == ([] if code is None else [code])


def test_freshness_recorded_receipt(payload: dict[str, Any]) -> None:
    event = WikimediaRecentChange.model_validate(payload)
    rule = FreshnessRule(60)
    assert rule.evaluate_at(event, NOW).passed
    assert not rule.evaluate_at(event, NOW + timedelta(microseconds=1)).passed
    with pytest.raises(ValueError):
        rule.evaluate_at(event, NOW.replace(tzinfo=None))


@pytest.mark.parametrize("threshold", [-1, True])
def test_bad_freshness_config(threshold: int) -> None:
    with pytest.raises(ValueError):
        FreshnessRule(threshold)


@pytest.mark.parametrize("threshold", [-1, True, 1.5, 6])
def test_future_skew_rejects_unsafe_configuration(threshold: Any) -> None:
    with pytest.raises(ValueError):
        FreshnessRule(max_future_skew_seconds=threshold)


def test_future_skew_strict_mode_and_diagnostics(payload: dict[str, Any]) -> None:
    payload["meta"]["dt"] = (NOW + timedelta(seconds=1)).isoformat()
    event = WikimediaRecentChange.model_validate(payload)
    result = FreshnessRule().evaluate_at(event, NOW)
    assert result.passed and result.rule_version == "1.2.0"
    assert result.details["age_seconds"] == -1
    assert result.details["within_future_skew_tolerance"] is True
    assert result.details["max_future_skew_seconds"] == 2
    assert (
        FreshnessRule(max_future_skew_seconds=0).evaluate_at(event, NOW).failure_code
        == "WARN_FUTURE_EVENT"
    )


def test_default_builder_honors_skew_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.common.config import get_settings

    monkeypatch.setenv("QUALITY_MAX_FUTURE_SKEW_SECONDS", "0")
    get_settings.cache_clear()
    try:
        rule = next(r for r in build_default_rule_set() if isinstance(r, FreshnessRule))
        assert rule.max_future_skew_seconds == 0
    finally:
        get_settings.cache_clear()


def test_unknown_type_warns(payload: dict[str, Any]) -> None:
    payload["type"] = "new_upstream_type"
    payload["future_field"] = {"score": 1}
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert not summary.should_quarantine and summary.has_warnings
    assert "WARN_UNKNOWN_EVENT_TYPE" in summary.failure_codes


def test_drift_window_threshold_and_eviction(payload: dict[str, Any]) -> None:
    rule = EventTypeDriftRule()
    event = WikimediaRecentChange.model_validate(payload)
    unknown = event.model_copy(update={"type": "future_type"})
    for _ in range(5):
        assert not rule.evaluate(unknown, payload).details["ready"]  # Warm-up; type warning only.
    for _ in range(94):
        assert rule.evaluate(event, payload).passed
    result = rule.evaluate(event, payload)
    assert not result.passed and result.details["unknown_ratio"] == 0.05
    assert rule.evaluate(event, payload).passed  # Oldest unknown evicted: 4%.
    other = EventTypeDriftRule()
    assert other.evaluate(event, payload).details["sample_count"] == 1


@pytest.mark.parametrize("size,threshold", [(0, 0.05), (100, 0), (100, 1.1)])
def test_bad_drift_config(size: int, threshold: float) -> None:
    with pytest.raises(ValueError):
        EventTypeDriftRule(size, threshold)


@pytest.mark.parametrize(
    "numeric_id,meta_id,tier", [(12, "uuid", 1), (None, "uuid", 2), (None, None, 3), (0, None, 3)]
)
def test_key_tiers_and_consistency(
    payload: dict[str, Any],
    numeric_id: int | None,
    meta_id: str | None,
    tier: int,
) -> None:
    payload["id"] = numeric_id
    payload["meta"]["id"] = meta_id
    event = WikimediaRecentChange.model_validate(payload)
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    curated = CuratedRecord.from_validated_event(event, payload, NOW)
    result = next(r for r in summary.results if r.rule_id == "R-DED-01")
    assert summary.event_key == curated.event_key == result.details["event_key"]
    assert result.details["key_tier"] == tier
    assert result.passed == (tier != 3)
    assert not summary.should_quarantine
    reordered = dict(reversed(list(payload.items())))
    assert (
        _build_engine().evaluate_payload(reordered, received_at=NOW).event_key == summary.event_key
    )


def test_json_string_input(payload: dict[str, Any]) -> None:
    instance = _build_engine()
    assert instance.evaluate_payload(json.dumps(payload), received_at=NOW).pass_count == 10


def test_naive_receipt_rejected(payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _build_engine().evaluate_payload(payload, received_at=NOW.replace(tzinfo=None))


def test_fallback_key_changes_with_payload(payload: dict[str, Any]) -> None:
    """Different content must not collide in the fallback tier."""
    payload["id"] = None
    payload["meta"]["id"] = None
    first = _build_engine().evaluate_payload(payload, received_at=NOW).event_key
    payload["comment"] = "different payload"
    assert _build_engine().evaluate_payload(payload, received_at=NOW).event_key != first


def test_hash_like_source_id_is_not_fallback(payload: dict[str, Any]) -> None:
    """Determine key tier from source IDs, not a substring in their value."""
    payload["id"] = None
    payload["meta"]["id"] = "hash_source_uuid"
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    result = next(r for r in summary.results if r.rule_id == "R-DED-01")
    assert result.passed and result.details["key_tier"] == 2


@pytest.mark.parametrize("namespace", ["abc", "1", True, -3, 10001])
def test_prepared_normalization_preserves_raw(payload: dict[str, Any], namespace: Any) -> None:
    payload["namespace"] = namespace
    prepared = _build_engine().prepare_payload(payload, received_at=NOW)
    assert prepared.event is not None and prepared.event.namespace is None
    assert prepared.raw_payload == payload
    record = prepared.to_curated_record()
    assert record.namespace is None and record.raw_payload["namespace"] == namespace
    assert record.event_key == prepared.summary.event_key
    assert record.received_timestamp == NOW
    payload["title"] = "mutated by caller"
    assert prepared.raw_payload["title"] == "Example"


@pytest.mark.parametrize("raw", ['{"meta":', b"\xff", {"meta": None}])
def test_prepared_rejected_has_no_curated(raw: Any) -> None:
    prepared = _build_engine().prepare_payload(raw, received_at=NOW)
    assert prepared.event is None and prepared.summary.should_quarantine
    with pytest.raises(ValueError, match="Rejected"):
        prepared.to_curated_record()


def test_restored_schema_id_mapping(payload: dict[str, Any]) -> None:
    instance = _build_engine()
    assert instance.prepare_payload("bad", received_at=NOW).summary.results[0].rule_id == "R-SCH-01"
    del payload["meta"]
    result = instance.prepare_payload(payload, received_at=NOW).summary.results[0]
    assert result.rule_id == "R-SCH-02" and result.severity == RuleSeverity.CRITICAL
    assert result.rule_version == "1.1.0"


def test_mixed_model_errors_remain_critical(payload: dict[str, Any]) -> None:
    payload["length"]["old"] = -1
    payload["wiki"] = None
    result = _build_engine().prepare_payload(payload, received_at=NOW).summary.results[0]
    assert result.rule_id == "R-SCH-02" and result.severity == RuleSeverity.CRITICAL


@pytest.mark.parametrize("timestamp", ["2026-10-24T19:00:00+07:00", "2026-10-24T12:00:00Z"])
def test_event_time_normalized_to_utc(payload: dict[str, Any], timestamp: str) -> None:
    payload["meta"]["dt"] = timestamp
    prepared = _build_engine().prepare_payload(payload, received_at=NOW)
    assert prepared.event is not None and prepared.event.meta.dt == NOW
    assert prepared.event.meta.dt.tzinfo == UTC
    assert prepared.to_curated_record().raw_payload["meta"]["dt"] == timestamp


def test_naive_event_time_quarantined(payload: dict[str, Any]) -> None:
    payload["meta"]["dt"] = "2026-10-24T12:00:00"
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert summary.should_quarantine and summary.failure_codes == ["ERR_INVALID_TIMESTAMP"]


def test_invalid_dict_key_is_order_independent() -> None:
    first = _build_engine().evaluate_payload({"z": 1, "a": 2}, received_at=NOW)
    second = _build_engine().evaluate_payload({"a": 2, "z": 1}, received_at=NOW)
    assert first.event_key == second.event_key


@pytest.mark.parametrize("comment,accepted", [("literal \\u0000", True), ("actual \x00", False)])
def test_nul_detection_checks_values_not_escapes(
    payload: dict[str, Any],
    comment: str,
    accepted: bool,
) -> None:
    payload["comment"] = comment
    summary = _build_engine().prepare_payload(payload, received_at=NOW).summary
    assert summary.is_valid == accepted


@pytest.mark.parametrize("field", ["old", "new"])
@pytest.mark.parametrize("value,valid", [(2147483647, True), (2147483648, False)])
def test_byte_length_database_range(
    payload: dict[str, Any], field: str, value: int, valid: bool
) -> None:
    payload["length"][field] = value
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert summary.is_valid == valid
    assert ("ERR_INVALID_BYTE_LENGTH" not in summary.failure_codes) == valid


@pytest.mark.parametrize("source_id", [True, 1.5, "12"])
def test_source_id_cannot_be_coerced_into_another_identity(
    payload: dict[str, Any], source_id: Any
) -> None:
    payload["id"] = source_id
    summary = _build_engine().evaluate_payload(payload, received_at=NOW)
    assert summary.should_quarantine and summary.results[0].rule_id == "R-SCH-02"


def test_blank_metadata_id_uses_hash_fallback(payload: dict[str, Any]) -> None:
    payload["id"] = None
    payload["meta"]["id"] = "   "
    prepared = _build_engine().prepare_payload(payload, received_at=NOW)
    assert not prepared.summary.should_quarantine
    assert ":hash_" in prepared.summary.event_key
    assert prepared.to_curated_record().source_event_id is None
