"""Unit test cho JSON Schema Contract và Pydantic v2 Models (STEP-03.01)."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from schemas.models import CuratedRecord, WikimediaRecentChange


@pytest.fixture
def sample_valid_event_dict() -> dict[str, object]:
    """Sample payload hợp lệ trích xuất từ Wikimedia stream thực tế."""
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": "https://vi.wikipedia.org/wiki/H%C3%A0_N%E1%BB%99i",
            "request_id": "req-12345",
            "id": "e8966ec7-1b32-4753-9d41-e945c92c4623",
            "dt": "2026-10-24T10:15:30Z",
            "domain": "vi.wikipedia.org",
            "stream": "mediawiki.recentchange",
            "topic": "eqiad.mediawiki.recentchange",
        },
        "id": 987654321,
        "type": "edit",
        "namespace": 0,
        "title": "Hà Nội",
        "comment": "Cập nhật dân số 2026",
        "timestamp": 1792836930,
        "user": "EditorPro",
        "bot": False,
        "minor": True,
        "patrolled": True,
        "length": {"old": 54000, "new": 54120},
        "revision": {"old": 11111, "new": 11112},
        "server_url": "https://vi.wikipedia.org",
        "server_name": "vi.wikipedia.org",
        "wiki": "viwiki",
    }


def test_json_schema_file_exists_and_valid() -> None:
    """Xác minh file JSON Schema tồn tại và có cú pháp JSON hợp lệ."""
    schema_path = Path("schemas/wikimedia_recentchange_v1.json")
    assert schema_path.exists(), f"Không tìm thấy file schema tại: {schema_path}"

    with open(schema_path, encoding="utf-8") as f:
        schema_json = json.load(f)

    assert schema_json["$schema"] == "http://json-schema.org/draft-07/schema#"
    assert "meta" in schema_json["required"]
    assert "wiki" in schema_json["required"]


def test_pydantic_valid_event_parsing(sample_valid_event_dict: dict[str, object]) -> None:
    """Xác minh parse thành công event hợp lệ và chuẩn hóa datetime UTC."""
    event = WikimediaRecentChange.model_validate(sample_valid_event_dict)

    assert event.wiki == "viwiki"
    assert event.id == 987654321
    assert event.type == "edit"
    assert event.bot is False
    assert event.minor is True
    assert event.length is not None and event.length.new == 54120
    assert event.meta.dt == datetime(2026, 10, 24, 10, 15, 30, tzinfo=UTC)


def test_deterministic_event_key_generation(sample_valid_event_dict: dict[str, object]) -> None:
    """Xác minh thuật toán sinh event_key tất định theo các tầng ưu tiên (R-DED-01)."""
    # 1. Ưu tiên có id số
    event = WikimediaRecentChange.model_validate(sample_valid_event_dict)
    assert event.generate_event_key() == "viwiki:987654321"

    # 2. Không có id số -> dùng meta.id
    sample_valid_event_dict["id"] = None
    event_no_id = WikimediaRecentChange.model_validate(sample_valid_event_dict)
    assert event_no_id.generate_event_key() == "viwiki:e8966ec7-1b32-4753-9d41-e945c92c4623"

    # 3. Không có cả id và meta.id -> hash payload
    sample_valid_event_dict["meta"] = {
        "dt": "2026-10-24T10:15:30Z",
        "stream": "mediawiki.recentchange",
    }
    event_no_meta_id = WikimediaRecentChange.model_validate(sample_valid_event_dict)
    key_hash = event_no_meta_id.generate_event_key(b"dummy_payload")
    assert key_hash.startswith("viwiki:hash_")


def test_pydantic_missing_required_fields_raises_error() -> None:
    """Xác minh Pydantic ném ValidationError khi thiếu trường bắt buộc."""
    invalid_payload = {"type": "edit"}  # Thiếu meta và wiki
    with pytest.raises(ValidationError) as exc_info:
        WikimediaRecentChange.model_validate(invalid_payload)

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert "meta" in missing_fields
    assert "wiki" in missing_fields


def test_curated_record_mapping(sample_valid_event_dict: dict[str, object]) -> None:
    """Xác minh chuyển đổi sang CuratedRecord chuẩn hóa."""
    event = WikimediaRecentChange.model_validate(sample_valid_event_dict)
    now_utc = datetime.now(UTC)

    curated = CuratedRecord.from_validated_event(
        event=event,
        raw_dict=sample_valid_event_dict,
        received_at=now_utc,
    )

    assert curated.event_key == "viwiki:987654321"
    assert curated.wiki == "viwiki"
    assert curated.title == "Hà Nội"
    assert curated.change_type == "edit"
    assert curated.old_length == 54000
    assert curated.new_length == 54120
    assert curated.is_bot is False
    assert curated.is_minor is True
    assert curated.received_timestamp == now_utc
