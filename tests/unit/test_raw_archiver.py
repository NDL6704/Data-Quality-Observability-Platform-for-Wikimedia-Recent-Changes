"""Unit tests for RawArchiver, SHA-256 calculation, and batch file writing."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ingestion.raw_archiver import (
    BatchManifest,
    RawArchiver,
    compute_file_sha256,
)
from src.ingestion.sse_client import RawStreamEvent


def test_compute_file_sha256(tmp_path: Path) -> None:
    """Verifies that compute_file_sha256 produces exact cryptographic hashes."""
    test_file = tmp_path / "sample.txt"
    content = b"Wikimedia Data Quality Platform - SHA256 Test"
    test_file.write_bytes(content)

    import hashlib

    expected_hash = hashlib.sha256(content).hexdigest()
    actual_hash = compute_file_sha256(test_file)

    assert actual_hash == expected_hash
    assert len(actual_hash) == 64


def test_raw_archiver_buffering_and_pending_count(tmp_path: Path) -> None:
    """Verifies that events are buffered in memory until flush criteria are met."""
    archiver = RawArchiver(
        base_dir=tmp_path,
        batch_size=10,
        flush_interval_sec=60,
        enable_db_manifest=False,
    )

    assert archiver.pending_count == 0

    res1 = archiver.append({"id": 1, "wiki": "enwiki", "title": "Test 1"})
    res2 = archiver.append({"id": 2, "wiki": "enwiki", "title": "Test 2"})

    assert res1 is None
    assert res2 is None
    assert archiver.pending_count == 2
    assert archiver.stats["appended"] == 2
    assert archiver.stats["batches_committed"] == 0


def test_raw_archiver_flushes_on_batch_size_threshold(tmp_path: Path) -> None:
    """Verifies that reaching batch_size immediately triggers an atomic batch flush."""
    archiver = RawArchiver(
        base_dir=tmp_path,
        batch_size=3,
        flush_interval_sec=60,
        enable_db_manifest=False,
    )

    archiver.append({"id": 101, "wiki": "viwiki", "title": "Hanoi"})
    archiver.append({"id": 102, "wiki": "viwiki", "title": "Saigon"})
    manifest = archiver.append({"id": 103, "wiki": "viwiki", "title": "Danang"})

    assert isinstance(manifest, BatchManifest)
    assert archiver.pending_count == 0
    assert manifest.record_count == 3
    assert manifest.format == "jsonl"
    assert manifest.status == "COMMITTED"
    assert archiver.stats["batches_committed"] == 1
    assert archiver.stats["records_committed"] == 3

    # Verify physical file existence and contents
    target_file = Path(manifest.file_path)
    assert target_file.is_file()
    assert target_file.stat().st_size == manifest.file_size_bytes

    # Re-calculate hash independently
    actual_sha256 = compute_file_sha256(target_file)
    assert actual_sha256 == manifest.checksum_sha256

    # Verify lines in file
    lines = target_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 3
    first_record = json.loads(lines[0])
    assert first_record["id"] == 101
    assert first_record["title"] == "Hanoi"


def test_raw_archiver_supports_raw_stream_event_dataclass(tmp_path: Path) -> None:
    """Verifies that RawArchiver accepts RawStreamEvent dataclass objects."""
    archiver = RawArchiver(
        base_dir=tmp_path,
        batch_size=5,
        enable_db_manifest=False,
    )

    stream_event = RawStreamEvent(
        event_id="123456",
        event_type="message",
        data={"meta": {"id": "ev-1"}, "wiki": "jawiki", "title": "Tokyo"},
        raw_payload='data: {"meta": {"id": "ev-1"}}',
        received_at_epoch=1728500000.0,
    )

    archiver.append(stream_event)
    manifest = archiver.flush()

    assert manifest is not None
    assert manifest.record_count == 1

    target_file = Path(manifest.file_path)
    lines = target_file.read_text(encoding="utf-8").strip().split("\n")
    record = json.loads(lines[0])
    assert record["wiki"] == "jawiki"
    assert record["meta"]["id"] == "ev-1"


def test_raw_archiver_context_manager_flushes_on_exit(tmp_path: Path) -> None:
    """Verifies that the context manager automatically flushes remaining events on close."""
    with RawArchiver(
        base_dir=tmp_path,
        batch_size=10,
        enable_db_manifest=False,
    ) as archiver:
        archiver.append({"id": 999, "wiki": "dewiki", "title": "Berlin"})
        assert archiver.pending_count == 1

    # After exiting block, file should have been committed
    assert archiver.pending_count == 0
    assert archiver.stats["batches_committed"] == 1
    assert archiver.stats["records_committed"] == 1


def test_raw_archiver_verify_file_checksum_integrity_and_mutation(tmp_path: Path) -> None:
    """Verifies checksum validation and detection of tampered bytes (Scenario 10)."""
    archiver = RawArchiver(
        base_dir=tmp_path,
        batch_size=2,
        enable_db_manifest=False,
    )

    archiver.append({"id": 1, "wiki": "frwiki", "title": "Paris"})
    manifest = archiver.append({"id": 2, "wiki": "frwiki", "title": "Lyon"})
    assert manifest is not None

    file_path = Path(manifest.file_path)
    expected_hash = manifest.checksum_sha256

    # 1. Verification of untampered file must pass
    assert RawArchiver.verify_file_checksum(file_path, expected_hash) is True

    # 2. Simulate bit rot / tampering by mutating one byte
    file_bytes = bytearray(file_path.read_bytes())
    file_bytes[0] = ord(b"X") if file_bytes[0] != ord(b"X") else ord(b"Y")
    file_path.write_bytes(bytes(file_bytes))

    # 3. Verification of tampered file must fail
    assert RawArchiver.verify_file_checksum(file_path, expected_hash) is False


def test_raw_archiver_calls_database_insert_when_enabled(tmp_path: Path) -> None:
    """Verifies that _record_manifest_in_db executes the expected SQL insert query."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.ingestion.raw_archiver.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        archiver = RawArchiver(
            base_dir=tmp_path,
            batch_size=1,
            enable_db_manifest=True,
        )

        manifest = archiver.append({"id": 500, "wiki": "eswiki", "title": "Madrid"})
        assert manifest is not None

        mock_cursor.execute.assert_called_once()
        call_args, call_kwargs = mock_cursor.execute.call_args
        sql = call_args[0]
        params = call_args[1]

        assert "INSERT INTO archive_manifests" in sql
        assert params["manifest_id"] == manifest.manifest_id
        assert params["record_count"] == 1
        assert params["status"] == "COMMITTED"
        assert params["checksum_sha256"] == manifest.checksum_sha256
