"""Unit test cho Core Quality Rules Engine (STEP-03.02)."""

from datetime import UTC, datetime
from typing import Any

import pytest

from schemas.models import MediaWikiMeta, WikimediaRecentChange
from src.quality.engine import (
    QualityEngine,
    QualityRule,
    RuleCategory,
    RuleResult,
    RuleSeverity,
)


@pytest.fixture
def sample_event() -> WikimediaRecentChange:
    """Fixture tạo một đối tượng WikimediaRecentChange hợp lệ."""
    return WikimediaRecentChange(
        meta=MediaWikiMeta(
            id="test-meta-uuid",
            dt=datetime(2026, 10, 24, 12, 0, 0, tzinfo=UTC),
            stream="mediawiki.recentchange",
        ),
        type="edit",
        wiki="viwiki",
        id=123456,
        title="Trang Chủ",
    )


class MockPassRule(QualityRule):
    """Quy tắc giả lập luôn trả về kết quả đạt."""

    def __init__(self, rule_id: str = "R-MOCK-PASS") -> None:
        super().__init__(
            rule_id=rule_id,
            rule_version="1.0",
            category=RuleCategory.SCHEMA,
            severity=RuleSeverity.CRITICAL,
            description="Mock rule luôn đạt chuẩn",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        return self._pass(details={"check": "ok"})


class MockFailRule(QualityRule):
    """Quy tắc giả lập luôn trả về kết quả vi phạm."""

    def __init__(
        self,
        rule_id: str = "R-MOCK-FAIL",
        severity: RuleSeverity = RuleSeverity.CRITICAL,
        failure_code: str = "ERR_MOCK_FAIL",
    ) -> None:
        super().__init__(
            rule_id=rule_id,
            rule_version="1.0",
            category=RuleCategory.VALIDITY,
            severity=severity,
            description="Mock rule luôn vi phạm",
        )
        self.code = failure_code

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        return self._fail(failure_code=self.code, details={"reason": "simulated_error"})


class MockCrashingRule(QualityRule):
    """Quy tắc giả lập ném Exception ngoài dự kiến."""

    def __init__(self, rule_id: str = "R-MOCK-CRASH") -> None:
        super().__init__(
            rule_id=rule_id,
            rule_version="1.0",
            category=RuleCategory.DRIFT,
            severity=RuleSeverity.MEDIUM,
            description="Mock rule phát sinh ngoại lệ",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        raise RuntimeError("Cố ý ném lỗi kiểm thử phòng vệ ngoại lệ!")


def test_rule_registration_and_retrieval() -> None:
    """Xác minh đăng ký, lấy và xóa quy tắc trong QualityEngine."""
    engine = QualityEngine()
    rule1 = MockPassRule("R-1")
    rule2 = MockFailRule("R-2")

    engine.register_rules([rule1, rule2])

    assert len(engine.list_rules()) == 2
    assert engine.get_rule("R-1") is rule1
    assert engine.get_rule("R-2") is rule2
    assert engine.get_rule("R-NONEXISTENT") is None

    engine.clear_rules()
    assert len(engine.list_rules()) == 0


def test_rule_override_updates_registry() -> None:
    """Xác minh đăng ký đè quy tắc cùng ID sẽ cập nhật bản ghi mới."""
    engine = QualityEngine()
    rule_v1 = MockPassRule("R-VERSIONED")
    rule_v2 = MockFailRule("R-VERSIONED")

    engine.register_rule(rule_v1)
    assert engine.get_rule("R-VERSIONED") is rule_v1

    engine.register_rule(rule_v2)
    assert engine.get_rule("R-VERSIONED") is rule_v2


def test_evaluation_all_rules_passed(sample_event: WikimediaRecentChange) -> None:
    """Xác minh sự kiện đạt toàn bộ rule sẽ có is_valid=True và không bị cách ly."""
    engine = QualityEngine()
    engine.register_rules([MockPassRule("R-1"), MockPassRule("R-2")])

    summary = engine.evaluate(sample_event, {})

    assert summary.is_valid is True
    assert summary.should_quarantine is False
    assert summary.pass_count == 2
    assert summary.fail_count == 0
    assert len(summary.failure_codes) == 0
    assert summary.highest_severity is None
    assert summary.has_warnings is False
    assert summary.event_key == "viwiki:123456"


def test_evaluation_critical_failure_quarantined(sample_event: WikimediaRecentChange) -> None:
    """Xác minh vi phạm CRITICAL sẽ kích hoạt cách ly (should_quarantine=True)."""
    engine = QualityEngine()
    engine.register_rules([
        MockPassRule("R-1"),
        MockFailRule("R-CRIT", severity=RuleSeverity.CRITICAL, failure_code="ERR_CRIT_01"),
    ])

    summary = engine.evaluate(sample_event, {})

    assert summary.is_valid is False
    assert summary.should_quarantine is True
    assert summary.pass_count == 1
    assert summary.fail_count == 1
    assert "ERR_CRIT_01" in summary.failure_codes
    assert summary.highest_severity == RuleSeverity.CRITICAL


def test_evaluation_high_failure_quarantined(sample_event: WikimediaRecentChange) -> None:
    """Xác minh vi phạm HIGH cũng kích hoạt cách ly."""
    engine = QualityEngine()
    engine.register_rule(
        MockFailRule("R-HIGH", severity=RuleSeverity.HIGH, failure_code="ERR_HIGH_01")
    )

    summary = engine.evaluate(sample_event, {})

    assert summary.is_valid is False
    assert summary.should_quarantine is True
    assert summary.highest_severity == RuleSeverity.HIGH


def test_evaluation_medium_failure_allows_curated_with_warning(
    sample_event: WikimediaRecentChange,
) -> None:
    """Xác minh vi phạm MEDIUM chỉ cảnh báo, không cách ly (cho phép lưu curated)."""
    engine = QualityEngine()
    engine.register_rules([
        MockPassRule("R-1"),
        MockFailRule("R-WARN", severity=RuleSeverity.MEDIUM, failure_code="WARN_TITLE_EMPTY"),
    ])

    summary = engine.evaluate(sample_event, {})

    # Bất biến: MEDIUM fail vẫn xem là hợp lệ để ghi curated, nhưng mang cảnh báo
    assert summary.is_valid is True
    assert summary.should_quarantine is False
    assert summary.has_warnings is True
    assert summary.highest_severity == RuleSeverity.MEDIUM
    assert "WARN_TITLE_EMPTY" in summary.failure_codes


def test_evaluation_disabled_rule_skipped(sample_event: WikimediaRecentChange) -> None:
    """Xác minh rule có enabled=False sẽ bị bỏ qua không chạy."""
    engine = QualityEngine()
    disabled_fail_rule = MockFailRule("R-DISABLED")
    disabled_fail_rule.enabled = False

    engine.register_rule(disabled_fail_rule)
    summary = engine.evaluate(sample_event, {})

    assert summary.is_valid is True
    assert summary.should_quarantine is False
    assert len(summary.results) == 0


def test_evaluation_catches_unhandled_exception_as_critical(
    sample_event: WikimediaRecentChange,
) -> None:
    """Xác minh cơ chế phòng vệ: bắt gọn exception của rule, biến thành CRITICAL lỗi."""
    engine = QualityEngine()
    engine.register_rule(MockCrashingRule("R-CRASH"))

    # Pipeline không bị crash bởi unhandled exception
    summary = engine.evaluate(sample_event, {})

    assert summary.is_valid is False
    assert summary.should_quarantine is True
    assert summary.highest_severity == RuleSeverity.CRITICAL
    assert "ERR_RULE_EXECUTION_EXCEPTION" in summary.failure_codes
    assert summary.results[0].details["exception_type"] == "RuntimeError"


def test_rule_result_to_dict_format() -> None:
    """Xác minh chuyển đổi RuleResult sang dictionary chuẩn hóa."""
    rule = MockFailRule("R-FORMAT", severity=RuleSeverity.HIGH, failure_code="ERR_FMT")
    res = rule._fail(failure_code="ERR_FMT", details={"k": "v"})
    data = res.to_dict()

    assert data["rule_id"] == "R-FORMAT"
    assert data["rule_version"] == "1.0"
    assert data["category"] == "VALIDITY"
    assert data["severity"] == "HIGH"
    assert data["passed"] is False
    assert data["failure_code"] == "ERR_FMT"
    assert data["details"] == {"k": "v"}
    assert "evaluated_at" in data
