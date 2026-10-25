"""Nhóm quy tắc DEDUPLICATION: Phát hiện event_key yếu (không có định danh mạnh).

Rules:
- R-DED-01 (INFO): Cảnh báo nếu event_key phải fallback dùng hash SHA-256.
"""

from __future__ import annotations

from typing import Any

from schemas.models import WikimediaRecentChange, generate_payload_event_key
from src.quality.engine import (
    QualityRule,
    RuleCategory,
    RuleResult,
    RuleSeverity,
)


class WeakEventKeyRule(QualityRule):
    """R-DED-01: Cảnh báo nếu event_key phải fallback dùng hash SHA-256.

    Hệ thống deduplication dựa vào event_key để loại bỏ bản ghi trùng lặp:
    - Key dạng '{wiki}:{id}' (tier-1): Mạnh nhất, đảm bảo idempotent upsert.
    - Key dạng '{wiki}:{meta.id}' (tier-2): UUID Wikimedia, vẫn đủ mạnh.
    - Key dạng '{wiki}:hash_...' (tier-3): Yếu — phụ thuộc nội dung payload.
      Canonical JSON giữ key ổn định khi thứ tự trường thay đổi.

    Vi phạm INFO (severity thấp nhất):
    - Không block event (severity INFO không trigger quarantine).
    - Chỉ ghi nhận cờ cảnh báo để đội vận hành theo dõi xu hướng.
    - Tỉ lệ weak key cao = dấu hiệu cần cải thiện upstream key generation.
    """

    def __init__(self) -> None:
        super().__init__(
            rule_id="R-DED-01",
            rule_version="1.1.0",
            category=RuleCategory.DEDUPLICATION,
            severity=RuleSeverity.INFO,
            description=(
                "Warn when event_key falls back to hash-based key "
                "(no numeric id or meta.id UUID available)"
            ),
        )

    def evaluate(
        self,
        event: WikimediaRecentChange,
        raw_payload: dict[str, Any],
    ) -> RuleResult:
        """Kiểm tra event_key có cần fallback hash SHA-256 hay không."""
        event_key = generate_payload_event_key(event, raw_payload)

        # Tier-3 key luôn chứa ':hash_' theo thuật toán trong models.py
        if not (event.id is not None and event.id > 0) and not event.meta.id:
            return self._fail(
                failure_code="WARN_WEAK_EVENT_KEY",
                details={
                    "event_key": event_key,
                    "numeric_id": event.id,
                    "meta_id": event.meta.id,
                    "wiki": event.wiki,
                    "key_tier": 3,
                },
            )

        # Xác định tier của key để ghi vào details
        key_tier = 1 if (event.id is not None and event.id > 0) else 2
        return self._pass(details={"event_key": event_key, "key_tier": key_tier})
