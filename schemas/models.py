"""Pydantic v2 Models cho Wikimedia RecentChange Stream & Curated Records.

Cung cấp:
- Type-safe schema validation với Pydantic v2 (Rust-backed core).
- Thuật toán sinh deterministic event_key (R-DED-01).
- Chuyển đổi an toàn từ Raw Payload sang Curated Record chuẩn hóa.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MediaWikiMeta(BaseModel):
    """Thông tin envelope metadata của sự kiện Wikimedia EventStreams."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str | None = Field(default=None, description="UUID sự kiện do Wikimedia sinh ra")
    dt: datetime = Field(description="Thời điểm sinh sự kiện (ISO-8601 UTC)")
    stream: str = Field(description="Tên luồng EventStream (vd: mediawiki.recentchange)")
    uri: str | None = Field(default=None, description="URI canonical của trang wiki")
    request_id: str | None = Field(default=None, description="Request ID phục vụ tracing")
    domain: str | None = Field(default=None, description="Tên miền (vd: vi.wikipedia.org)")
    topic: str | None = Field(default=None, description="Kafka topic nguồn của Wikimedia")
    partition: int | None = None
    offset: int | None = None

    @field_validator("id")
    @classmethod
    def normalize_optional_id(cls, value: str | None) -> str | None:
        """Blank metadata IDs are absent, never a shared source identity."""
        return value if value and value.strip() else None

    @field_validator("dt", mode="before")
    @classmethod
    def parse_datetime_utc(cls, v: Any) -> datetime:
        """Đảm bảo trường dt luôn được phân tích thành UTC datetime nhận biết timezone."""
        dt: datetime
        if isinstance(v, datetime):
            dt = v
        elif isinstance(v, str):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        else:
            raise ValueError("meta.dt must be an ISO-8601 timestamp")
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("meta.dt must include a timezone")
        return dt.astimezone(UTC)


class ByteLengthInfo(BaseModel):
    """Thông tin độ dài byte trang trước và sau sửa đổi."""

    model_config = ConfigDict(extra="allow")

    old: int | None = Field(default=None, ge=0, description="Độ dài byte trước sửa đổi")
    new: int | None = Field(default=None, ge=0, description="Độ dài byte sau sửa đổi")


class RevisionInfo(BaseModel):
    """Thông tin ID phiên bản trang (revid)."""

    model_config = ConfigDict(extra="allow")

    old: int | None = None
    new: int | None = None


class WikimediaRecentChange(BaseModel):
    """Model đại diện cho một bản tin Recent Change thô hợp lệ từ Wikimedia."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    meta: MediaWikiMeta
    type: str = Field(description="Loại thay đổi: edit, new, log, categorize, external")
    wiki: str = Field(description="Database name của wiki (vd: viwiki, enwiki)")
    id: int | None = Field(default=None, strict=True, description="ID số của recent change")
    title: str | None = Field(default=None, description="Tiêu đề trang")
    namespace: int | None = Field(default=None, description="Mã namespace")
    comment: str | None = Field(default=None, description="Tóm lược sửa đổi")
    timestamp: int | float | None = Field(default=None, description="Unix timestamp (giây)")
    user: str | None = Field(default=None, description="Tên người dùng thực hiện")
    bot: bool = Field(default=False, description="Cờ xác định bot")
    minor: bool = Field(default=False, description="Cờ sửa đổi nhỏ")
    patrolled: bool | None = None
    length: ByteLengthInfo | None = None
    revision: RevisionInfo | None = None
    server_url: str | None = None
    server_name: str | None = None
    server_script_path: str | None = None
    parsedcomment: str | None = None

    def generate_event_key(self, raw_payload_bytes: bytes | None = None) -> str:
        """Sinh event_key tất định (Deterministic Event Key) theo Rule R-DED-01.

        Thứ tự ưu tiên:
        1. '{wiki}:{id}' nếu id số hợp lệ > 0.
        2. '{wiki}:{meta.id}' nếu có UUID meta.id.
        3. '{wiki}:hash_{sha256(payload)[:32]}' nếu không có cả hai ID trên.
        """
        if self.id is not None and self.id > 0:
            return f"{self.wiki}:{self.id}"

        if self.meta.id:
            return f"{self.wiki}:{self.meta.id}"

        if raw_payload_bytes is not None:
            digest = hashlib.sha256(raw_payload_bytes).hexdigest()[:32]
            return f"{self.wiki}:hash_{digest}"

        fallback_str = f"{self.wiki}:{self.title}:{self.meta.dt.isoformat()}"
        digest = hashlib.sha256(fallback_str.encode("utf-8")).hexdigest()[:32]
        return f"{self.wiki}:hash_{digest}"


class CuratedRecord(BaseModel):
    """Bản ghi tinh chế sạch sẵn sàng để Upsert vào bảng curated_changes."""

    model_config = ConfigDict(extra="ignore")

    event_key: str
    source_event_id: str | None = None
    event_timestamp: datetime
    received_timestamp: datetime
    wiki: str
    server_name: str | None = None
    title: str | None = None
    user_id: int | None = None
    user_text: str | None = None
    change_type: str
    namespace: int | None = None
    is_bot: bool = False
    is_minor: bool = False
    old_length: int | None = None
    new_length: int | None = None
    comment: str | None = None
    raw_payload: dict[str, Any]

    @classmethod
    def from_validated_event(
        cls,
        event: WikimediaRecentChange,
        raw_dict: dict[str, Any],
        received_at: datetime | None = None,
    ) -> CuratedRecord:
        """Chuyển đổi từ WikimediaRecentChange sang CuratedRecord."""
        recv_dt = received_at or datetime.now(UTC)
        event_key = generate_payload_event_key(event, raw_dict)

        old_len = event.length.old if event.length else None
        new_len = event.length.new if event.length else None

        return cls(
            event_key=event_key,
            source_event_id=(
                str(event.id) if event.id is not None and event.id > 0 else event.meta.id
            ),
            event_timestamp=event.meta.dt,
            received_timestamp=recv_dt,
            wiki=event.wiki,
            server_name=event.server_name,
            title=event.title,
            user_id=None,
            user_text=event.user,
            change_type=event.type,
            namespace=event.namespace,
            is_bot=event.bot,
            is_minor=event.minor,
            old_length=old_len,
            new_length=new_len,
            comment=event.comment,
            raw_payload=raw_dict,
        )


def generate_payload_event_key(event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> str:
    """Use canonical JSON bytes consistently across quality and curated persistence."""
    raw_bytes = json.dumps(raw_payload, sort_keys=True, allow_nan=False).encode("utf-8")
    return event.generate_event_key(raw_bytes)
