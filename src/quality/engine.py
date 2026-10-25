"""Core Quality Rules Engine & Specification Interfaces.

Cung cấp:
- Phân cấp mức độ nghiêm trọng (RuleSeverity) và phân nhóm danh mục (RuleCategory).
- Lớp cơ sở trừu tượng QualityRule hỗ trợ versioning và dynamic evaluation.
- Kết quả đánh giá RuleResult và bản tổng hợp QualityEvaluationSummary.
- QualityEngine: Registry điều phối đánh giá tập luật với cơ chế phòng vệ lỗi ngoại lệ.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from schemas.models import CuratedRecord, WikimediaRecentChange, generate_payload_event_key
from src.common.logging import setup_logger

logger = setup_logger("quality.engine")


def _contains_nul(value: Any) -> bool:
    """Inspect decoded values, not JSON escapes, to avoid false positives."""
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_contains_nul(k) or _contains_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_contains_nul(item) for item in value)
    return False


class RuleSeverity(StrEnum):
    """Mức độ nghiêm trọng của vi phạm chất lượng dữ liệu."""

    CRITICAL = "CRITICAL"  # Vi phạm nghiêm trọng -> Cách ly bắt buộc (Quarantine / DLQ)
    HIGH = "HIGH"  # Vi phạm cao -> Cách ly bắt buộc (Quarantine)
    MEDIUM = "MEDIUM"  # Cảnh báo chất lượng -> Ghi nhận cờ, vẫn cho phép lưu curated
    INFO = "INFO"  # Thông tin kiểm toán / duplicate delivery hợp lệ


class RuleCategory(StrEnum):
    """Phân nhóm danh mục quy tắc chất lượng dữ liệu."""

    SCHEMA = "SCHEMA"
    COMPLETENESS = "COMPLETENESS"
    VALIDITY = "VALIDITY"
    DEDUPLICATION = "DEDUPLICATION"
    FRESHNESS = "FRESHNESS"
    DRIFT = "DRIFT"


@dataclass
class RuleResult:
    """Kết quả đánh giá của một quy tắc chất lượng dữ liệu."""

    rule_id: str
    rule_version: str
    category: RuleCategory
    severity: RuleSeverity
    passed: bool
    failure_code: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Chuyển đổi sang dict phục vụ ghi vào bảng quality_results / quarantined_events."""
        return {
            "rule_id": self.rule_id,
            "rule_version": self.rule_version,
            "category": self.category.value,
            "severity": self.severity.value,
            "passed": self.passed,
            "failure_code": self.failure_code,
            "details": self.details,
            "evaluated_at": self.evaluated_at.isoformat(),
        }


class QualityRule(ABC):
    """Lớp cơ sở trừu tượng cho mọi quy tắc kiểm tra chất lượng dữ liệu."""

    def __init__(
        self,
        rule_id: str,
        rule_version: str,
        category: RuleCategory,
        severity: RuleSeverity,
        description: str,
        enabled: bool = True,
    ) -> None:
        self.rule_id = rule_id
        self.rule_version = rule_version
        self.category = category
        self.severity = severity
        self.description = description
        self.enabled = enabled

    @abstractmethod
    def evaluate(
        self,
        event: WikimediaRecentChange,
        raw_payload: dict[str, Any],
    ) -> RuleResult:
        """Thực thi logic kiểm tra quy tắc trên một sự kiện.

        Args:
            event: Đối tượng WikimediaRecentChange đã được Pydantic parse sơ bộ.
            raw_payload: Từ điển JSON gốc chưa qua xử lý.

        Returns:
            RuleResult chứa trạng thái pass/fail và chi tiết lỗi nếu có.
        """
        pass

    def _pass(self, details: dict[str, Any] | None = None) -> RuleResult:
        """Helper tạo RuleResult đạt chuẩn (passed=True)."""
        return RuleResult(
            rule_id=self.rule_id,
            rule_version=self.rule_version,
            category=self.category,
            severity=self.severity,
            passed=True,
            details=details or {},
        )

    def _fail(
        self,
        failure_code: str,
        details: dict[str, Any] | None = None,
    ) -> RuleResult:
        """Helper tạo RuleResult vi phạm (passed=False)."""
        return RuleResult(
            rule_id=self.rule_id,
            rule_version=self.rule_version,
            category=self.category,
            severity=self.severity,
            passed=False,
            failure_code=failure_code,
            details=details or {},
        )


@dataclass
class QualityEvaluationSummary:
    """Bản tổng hợp kết quả đánh giá toàn bộ tập luật trên một sự kiện."""

    event_key: str
    is_valid: bool
    should_quarantine: bool
    results: list[RuleResult] = field(default_factory=list)
    failure_codes: list[str] = field(default_factory=list)
    highest_severity: RuleSeverity | None = None
    pass_count: int = 0
    fail_count: int = 0

    @property
    def has_warnings(self) -> bool:
        """Kiểm tra xem sự kiện có bị cảnh báo chất lượng (MEDIUM/INFO fail) hay không."""
        return any(
            not r.passed and r.severity in (RuleSeverity.MEDIUM, RuleSeverity.INFO)
            for r in self.results
        )


@dataclass(frozen=True)
class PreparedEvaluation:
    """One validated handoff: original input, normalized model, receipt and summary.

    Treat nested payload/model/summary as read-only after preparation.
    """

    source_payload: str | bytes | dict[str, Any]
    raw_payload: dict[str, Any] | None
    event: WikimediaRecentChange | None
    received_at: datetime
    summary: QualityEvaluationSummary

    def to_curated_record(self) -> CuratedRecord:
        """Map the already-normalized model; never reparse raw input."""
        if self.summary.should_quarantine or self.event is None or self.raw_payload is None:
            raise ValueError("Rejected input cannot become curated")
        record = CuratedRecord.from_validated_event(self.event, self.raw_payload, self.received_at)
        if record.event_key != self.summary.event_key:
            raise ValueError("Prepared payload/model changed after evaluation")
        return record


class QualityEngine:
    """Lõi điều phối đăng ký động và thực thi tập luật chất lượng dữ liệu."""

    # Thứ tự ưu tiên xác định mức độ nghiêm trọng cao nhất
    _SEVERITY_ORDER: dict[RuleSeverity, int] = {
        RuleSeverity.CRITICAL: 4,
        RuleSeverity.HIGH: 3,
        RuleSeverity.MEDIUM: 2,
        RuleSeverity.INFO: 1,
    }

    def __init__(self) -> None:
        self._rules: dict[str, QualityRule] = {}

    def register_rule(self, rule: QualityRule) -> None:
        """Đăng ký một quy tắc chất lượng vào engine."""
        if rule.rule_id in self._rules:
            logger.warning(
                f"Ghi đè quy tắc đã đăng ký trước đó: {rule.rule_id} "
                f"(v{self._rules[rule.rule_id].rule_version} -> v{rule.rule_version})"
            )
        self._rules[rule.rule_id] = rule
        logger.debug(
            f"Đã đăng ký rule: {rule.rule_id} [v{rule.rule_version}] "
            f"Category={rule.category.value} Severity={rule.severity.value}"
        )

    def register_rules(self, rules: list[QualityRule]) -> None:
        """Đăng ký danh sách nhiều quy tắc cùng lúc."""
        for rule in rules:
            self.register_rule(rule)

    def get_rule(self, rule_id: str) -> QualityRule | None:
        """Lấy một quy tắc theo mã ID."""
        return self._rules.get(rule_id)

    def list_rules(self) -> list[QualityRule]:
        """Liệt kê toàn bộ các quy tắc hiện có trong engine."""
        return list(self._rules.values())

    def clear_rules(self) -> None:
        """Xóa toàn bộ quy tắc đã đăng ký."""
        self._rules.clear()

    def evaluate(
        self,
        event: WikimediaRecentChange,
        raw_payload: dict[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> QualityEvaluationSummary:
        """Thực thi đánh giá toàn bộ quy tắc đã kích hoạt trên sự kiện.

        Quy tắc phân loại:
        - Nếu có bất kỳ rule nào thuộc CRITICAL hoặc HIGH bị thất bại:
          -> should_quarantine = True, is_valid = False
        - Nếu chỉ có MEDIUM hoặc INFO bị thất bại:
          -> should_quarantine = False, is_valid = True (hợp lệ nhưng có cờ cảnh báo)
        - Nếu tất cả các rule đều passed:
          -> should_quarantine = False, is_valid = True

        Cơ chế phòng vệ:
        Nếu một rule phát sinh lỗi ngoại lệ chưa bắt (Unhandled Exception),
        engine sẽ ghi nhận lỗi nội bộ và gắn cờ CRITICAL để ngăn dữ liệu hỏng lọt vào kho.
        """
        event_key = generate_payload_event_key(event, raw_payload)
        results: list[RuleResult] = []
        failure_codes: list[str] = []
        should_quarantine = False
        highest_sev: RuleSeverity | None = None
        pass_count = 0
        fail_count = 0

        for rule in self._rules.values():
            if not rule.enabled:
                logger.debug(f"Bỏ qua rule đang bị vô hiệu hóa: {rule.rule_id}")
                continue

            try:
                from src.quality.rules.freshness_rules import FreshnessRule

                if isinstance(rule, FreshnessRule) and received_at is not None:
                    res = rule.evaluate_at(event, received_at)
                else:
                    res = rule.evaluate(event, raw_payload)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    f"Ngoại lệ ngoài dự kiến khi đánh giá rule {rule.rule_id}: {exc}",
                    exc_info=True,
                )
                res = RuleResult(
                    rule_id=rule.rule_id,
                    rule_version=rule.rule_version,
                    category=rule.category,
                    severity=RuleSeverity.CRITICAL,  # Biến lỗi engine thành Critical vi phạm
                    passed=False,
                    failure_code="ERR_RULE_EXECUTION_EXCEPTION",
                    details={"exception_type": type(exc).__name__, "error_message": str(exc)},
                )

            results.append(res)

            if res.passed:
                pass_count += 1
            else:
                fail_count += 1
                if res.failure_code:
                    failure_codes.append(res.failure_code)

                # Cập nhật mức độ nghiêm trọng cao nhất
                if highest_sev is None or (
                    self._SEVERITY_ORDER[res.severity] > self._SEVERITY_ORDER[highest_sev]
                ):
                    highest_sev = res.severity

                # Kiểm tra ngưỡng cách ly (Quarantine Threshold)
                if res.severity in (RuleSeverity.CRITICAL, RuleSeverity.HIGH):
                    should_quarantine = True

        is_valid = not should_quarantine

        return QualityEvaluationSummary(
            event_key=event_key,
            is_valid=is_valid,
            should_quarantine=should_quarantine,
            results=results,
            failure_codes=failure_codes,
            highest_severity=highest_sev,
            pass_count=pass_count,
            fail_count=fail_count,
        )

    def evaluate_payload(
        self,
        payload: str | bytes | dict[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> QualityEvaluationSummary:
        """Compatibility summary API; production persistence uses prepare_payload."""
        return self.prepare_payload(payload, received_at=received_at).summary

    def prepare_payload(
        self,
        payload: str | bytes | dict[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> PreparedEvaluation:
        """Decode once, normalize once, evaluate once; preserve unmodified source.

        Gate failure stops dependent checks; missing results are not counted as
        passes. All parse/gate results use the same catalog IDs/version as rules.
        """
        receipt = received_at or datetime.now(UTC)
        if receipt.tzinfo is None or receipt.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        receipt = receipt.astimezone(UTC)
        source = deepcopy(payload)
        raw: Any = source
        rule_id, category, severity, code = (
            "R-SCH-01",
            RuleCategory.SCHEMA,
            RuleSeverity.CRITICAL,
            "ERR_INVALID_JSON",
        )
        errors: list[dict[str, Any]] = []
        try:
            if isinstance(source, (str, bytes)):
                raw = json.loads(source)
            if not isinstance(raw, dict):
                raise ValueError("JSON envelope must be an object")
            json.dumps(raw, allow_nan=False, ensure_ascii=True)
            json.dumps(raw, ensure_ascii=False).encode("utf-8")
            if _contains_nul(raw):
                raise ValueError("NUL cannot be represented in PostgreSQL JSONB")
            rule_id, code = "R-SCH-02", "ERR_MISSING_ENVELOPE"
            meta = raw.get("meta")
            if (
                not isinstance(meta, dict)
                or not {"dt", "stream"} <= meta.keys()
                or not {"type", "wiki"} <= raw.keys()
                or not isinstance(meta.get("stream"), str)
                or not meta["stream"].strip()
                or not isinstance(raw.get("type"), str)
                or not raw["type"].strip()
            ):
                raise ValueError("Required envelope fields are missing or malformed")
            code = "ERR_SCHEMA_VALIDATION"
            model_input = deepcopy(raw)
            ns = raw.get("namespace")
            if ns is not None and (type(ns) is not int or not -2 <= ns <= 10000):
                model_input["namespace"] = None
            event = WikimediaRecentChange.model_validate(model_input)
        except ValidationError as exc:
            validation_errors = exc.errors()
            errors = [{"loc": list(e["loc"]), "type": e["type"]} for e in validation_errors]
            # Only attribute a gate failure to a specific rule if all errors
            # belong to that field; mixed failures remain CRITICAL schema errors.
            if all(e["loc"][:2] == ("meta", "dt") for e in validation_errors):
                rule_id, category, code = "R-VAL-01", RuleCategory.VALIDITY, "ERR_INVALID_TIMESTAMP"
            elif all(e["loc"][:1] == ("length",) for e in validation_errors):
                rule_id, category, severity, code = (
                    "R-VAL-02",
                    RuleCategory.VALIDITY,
                    RuleSeverity.HIGH,
                    "ERR_INVALID_BYTE_LENGTH",
                )
        except (ValueError, TypeError, UnicodeError):
            pass
        else:
            summary = self.evaluate(event, raw, received_at=receipt)
            return PreparedEvaluation(source, raw, event, receipt, summary)
        if isinstance(source, bytes):
            encoded = source
        elif isinstance(source, str):
            encoded = source.encode("utf-8", errors="surrogatepass")
        else:
            encoded = json.dumps(source, sort_keys=True, ensure_ascii=True).encode("utf-8")
        key = "invalid:hash_" + hashlib.sha256(encoded).hexdigest()[:32]
        result = RuleResult(rule_id, "1.1.0", category, severity, False, code, {"errors": errors})
        summary = QualityEvaluationSummary(
            event_key=key,
            is_valid=False,
            should_quarantine=True,
            results=[result],
            failure_codes=[code],
            highest_severity=severity,
            fail_count=1,
        )
        return PreparedEvaluation(
            source, raw if isinstance(raw, dict) else None, None, receipt, summary
        )
