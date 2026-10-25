"""Raw File Archiver with cryptographic SHA-256 checksums and database manifests.

Maintains an immutable byte-for-byte local JSONL archive of raw Wikimedia
recentchange events, partitioned by date (YYYY/MM/DD), and registers atomic
audit records in PostgreSQL `archive_manifests`.
"""

import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.config import get_settings
from src.common.logging import setup_logger
from src.ingestion.sse_client import RawStreamEvent
from src.storage.db import get_db_connection

logger = setup_logger("wikimedia_dq.ingestion.raw_archiver")


@dataclass(frozen=True)
class BatchManifest:
    """Immutable audit manifest for a flushed raw archive batch."""

    manifest_id: str
    file_path: str
    format: str
    record_count: int
    start_time: datetime
    end_time: datetime
    checksum_sha256: str
    file_size_bytes: int
    status: str = "COMMITTED"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Converts manifest data to dictionary representation."""
        return {
            "manifest_id": self.manifest_id,
            "file_path": self.file_path,
            "format": self.format,
            "record_count": self.record_count,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "checksum_sha256": self.checksum_sha256,
            "file_size_bytes": self.file_size_bytes,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
        }


def compute_file_sha256(path: Path | str, chunk_size: int = 65536) -> str:
    """Computes SHA-256 cryptographic checksum over the binary contents of a file."""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


class RawArchiver:
    """Buffers raw Wikimedia events into sealed JSONL files and writes audit manifests.

    Guarantees the Atomic Commit Invariant:
    1. Write batch to temporary file (.tmp).
    2. Flush and fsync physical disk pages.
    3. Calculate byte size and SHA-256 cryptographic checksum.
    4. Atomic rename (.tmp -> .jsonl).
    5. Insert audit record into PostgreSQL `archive_manifests`.
    """

    def __init__(
        self,
        base_dir: Path | str | None = None,
        batch_size: int | None = None,
        flush_interval_sec: int | None = None,
        enable_db_manifest: bool = True,
    ) -> None:
        settings = get_settings()
        self.base_dir = Path(base_dir or settings.raw_archive_dir)
        self.batch_size = batch_size if batch_size is not None else settings.raw_archive_batch_size
        self.flush_interval_sec = (
            flush_interval_sec
            if flush_interval_sec is not None
            else settings.raw_archive_flush_interval_sec
        )
        self.enable_db_manifest = enable_db_manifest

        self._buffer: list[dict[str, Any]] = []
        self._first_event_at: datetime | None = None
        self._last_flush_mono = time.monotonic()
        self._stats = {
            "appended": 0,
            "batches_committed": 0,
            "records_committed": 0,
            "bytes_committed": 0,
        }

        # Ensure base archive directory exists
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"RawArchiver initialized | BaseDir: {self.base_dir.as_posix()} | "
            f"BatchSize: {self.batch_size} | FlushInterval: {self.flush_interval_sec}s | "
            f"DBManifest: {self.enable_db_manifest}"
        )

    @property
    def pending_count(self) -> int:
        """Number of records currently waiting in memory buffer."""
        return len(self._buffer)

    @property
    def stats(self) -> dict[str, int]:
        """Cumulative performance metrics for this archiver instance."""
        return dict(self._stats)

    def append(self, event: RawStreamEvent | dict[str, Any] | str) -> BatchManifest | None:
        """Appends a raw event to the in-memory buffer.

        Automatically triggers flush if buffer reaches batch_size or flush_interval_sec elapsed.
        """
        payload: dict[str, Any]
        if isinstance(event, RawStreamEvent):
            payload = event.data
        elif isinstance(event, dict):
            payload = event
        elif isinstance(event, str):
            payload = json.loads(event)
        else:
            raise TypeError(f"Unsupported event type: {type(event)}")

        now_utc = datetime.now(UTC)
        if not self._buffer:
            self._first_event_at = now_utc

        self._buffer.append(payload)
        self._stats["appended"] += 1

        # Check flush conditions
        elapsed = time.monotonic() - self._last_flush_mono
        if len(self._buffer) >= self.batch_size or elapsed >= self.flush_interval_sec:
            return self.flush()

        return None

    def flush(self) -> BatchManifest | None:
        """Flushes the current in-memory buffer to an immutable JSONL file.

        Executes the atomic write, fsync, checksum, and database manifest sequence.
        Returns None if buffer is empty.
        """
        if not self._buffer:
            return None

        record_count = len(self._buffer)
        now_utc = datetime.now(UTC)
        start_time = self._first_event_at or now_utc
        end_time = now_utc

        manifest_id = str(uuid.uuid4())
        short_id = manifest_id[:8]

        # Directory partitioned by year/month/day
        target_dir = (
            self.base_dir
            / f"{start_time:%Y}"
            / f"{start_time:%m}"
            / f"{start_time:%d}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        target_filename = f"rc_raw_{start_time:%Y%m%d_%H%M%S}_{short_id}.jsonl"
        target_path = target_dir / target_filename
        temp_path = target_dir / f".tmp_{manifest_id}.tmp"

        # Step 1: Write to temp file with fsync
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                for item in self._buffer:
                    line = json.dumps(item, ensure_ascii=False)
                    f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())

            # Step 2: Calculate checksum and exact byte size
            checksum_sha256 = compute_file_sha256(temp_path)
            file_size_bytes = temp_path.stat().st_size

            # Step 3: Atomic rename temp -> target
            temp_path.replace(target_path)
        except Exception as exc:
            logger.error(f"Failed during file write/fsync for batch {manifest_id}: {exc}")
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            raise

        # Step 4: Atomic database manifest insert
        normalized_file_path = target_path.as_posix()
        if self.enable_db_manifest:
            try:
                self._record_manifest_in_db(
                    manifest_id=manifest_id,
                    file_path=normalized_file_path,
                    format_name="jsonl",
                    record_count=record_count,
                    start_time=start_time,
                    end_time=end_time,
                    checksum_sha256=checksum_sha256,
                    file_size_bytes=file_size_bytes,
                    status="COMMITTED",
                )
            except Exception as exc:
                logger.error(
                    f"Failed to record archive manifest {manifest_id} in database: {exc}. "
                    f"Physical file preserved at {normalized_file_path}"
                )
                raise

        manifest = BatchManifest(
            manifest_id=manifest_id,
            file_path=normalized_file_path,
            format="jsonl",
            record_count=record_count,
            start_time=start_time,
            end_time=end_time,
            checksum_sha256=checksum_sha256,
            file_size_bytes=file_size_bytes,
            status="COMMITTED",
        )

        # Update statistics and clear buffer
        self._stats["batches_committed"] += 1
        self._stats["records_committed"] += record_count
        self._stats["bytes_committed"] += file_size_bytes
        self._buffer.clear()
        self._first_event_at = None
        self._last_flush_mono = time.monotonic()

        logger.info(
            f"Batch committed | ManifestID: {manifest_id} | Records: {record_count} | "
            f"Size: {file_size_bytes}B | SHA: {checksum_sha256[:12]} | "
            f"Path: {normalized_file_path}"
        )
        return manifest

    def _record_manifest_in_db(
        self,
        manifest_id: str,
        file_path: str,
        format_name: str,
        record_count: int,
        start_time: datetime,
        end_time: datetime,
        checksum_sha256: str,
        file_size_bytes: int,
        status: str,
    ) -> None:
        """Executes INSERT into `archive_manifests` table."""
        sql = """
            INSERT INTO archive_manifests (
                manifest_id,
                file_path,
                format,
                record_count,
                start_time,
                end_time,
                checksum_sha256,
                file_size_bytes,
                status,
                created_at
            ) VALUES (
                %(manifest_id)s,
                %(file_path)s,
                %(format)s,
                %(record_count)s,
                %(start_time)s,
                %(end_time)s,
                %(checksum_sha256)s,
                %(file_size_bytes)s,
                %(status)s,
                NOW()
            );
        """
        params = {
            "manifest_id": manifest_id,
            "file_path": file_path,
            "format": format_name,
            "record_count": record_count,
            "start_time": start_time,
            "end_time": end_time,
            "checksum_sha256": checksum_sha256,
            "file_size_bytes": file_size_bytes,
            "status": status,
        }
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    @staticmethod
    def verify_file_checksum(file_path: Path | str, expected_sha256: str) -> bool:
        """Verifies if the file's current SHA-256 matches expected checksum."""
        p = Path(file_path)
        if not p.is_file():
            return False
        actual = compute_file_sha256(p)
        return hmac.compare_digest(actual.lower(), expected_sha256.lower())

    @staticmethod
    def verify_manifest_in_db(manifest_id: str) -> dict[str, Any]:
        """Queries manifest from database and verifies file integrity on disk."""
        sql = """
            SELECT manifest_id, file_path, format, record_count,
                   start_time, end_time, checksum_sha256, file_size_bytes,
                   status, created_at
            FROM archive_manifests
            WHERE manifest_id = %(manifest_id)s;
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"manifest_id": manifest_id})
                row = cur.fetchone()

        if not row:
            return {"valid": False, "error": f"Manifest '{manifest_id}' not found in database"}

        file_path = Path(str(row["file_path"]))
        if not file_path.exists():
            return {
                "valid": False,
                "error": f"Physical archive file missing: {file_path}",
                "manifest": row,
            }

        actual_sha256 = compute_file_sha256(file_path)
        actual_size = file_path.stat().st_size
        expected_sha256 = str(row["checksum_sha256"])
        expected_size = int(row["file_size_bytes"])

        is_checksum_match = hmac.compare_digest(actual_sha256.lower(), expected_sha256.lower())
        is_size_match = actual_size == expected_size

        return {
            "valid": is_checksum_match and is_size_match,
            "checksum_match": is_checksum_match,
            "size_match": is_size_match,
            "expected_sha256": expected_sha256,
            "actual_sha256": actual_sha256,
            "expected_size": expected_size,
            "actual_size": actual_size,
            "manifest": row,
        }

    def close(self) -> BatchManifest | None:
        """Flushes any remaining buffered events."""
        return self.flush()

    def __enter__(self) -> "RawArchiver":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
