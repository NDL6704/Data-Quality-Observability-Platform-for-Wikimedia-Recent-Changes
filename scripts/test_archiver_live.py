"""Live Integration Test: SSE Stream -> RawArchiver -> JSONL -> PostgreSQL manifests.

Verification steps:
1. Connects to real Wikimedia EventStreams over SSE.
2. Collects 5 live events.
3. Appends events to RawArchiver with batch_size=5 and enable_db_manifest=True.
4. Asserts batch flush triggers on reaching batch threshold.
5. Verifies local JSONL file exists with exactly 5 lines.
6. Queries PostgreSQL `archive_manifests` table to verify database row.
7. Asserts cryptographic SHA-256 matches byte-for-byte with 0 discrepancy.
"""

import json
import sys
from pathlib import Path

# Ensure src module is discoverable when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.common.config import get_settings  # noqa: E402
from src.common.logging import setup_logger  # noqa: E402
from src.ingestion.raw_archiver import RawArchiver, compute_file_sha256  # noqa: E402
from src.ingestion.sse_client import RawStreamEvent, WikimediaSSEClient  # noqa: E402

logger = setup_logger("test_archiver_live")


def main() -> None:
    settings = get_settings()
    max_events = 5

    logger.info("=== BẮT ĐẦU KIỂM THỬ TÍCH HỢP SSE -> RAW ARCHIVER -> POSTGRES MANIFEST ===")
    logger.info(f"Archive Base Dir: {settings.raw_archive_dir} | Target DB: {settings.postgres_db}")

    sse_client = WikimediaSSEClient()
    events_collected: list[RawStreamEvent] = []

    # 1. Thu thập 5 events thật từ Wikimedia
    logger.info(f"Đang thu thập {max_events} events từ Wikimedia SSE...")
    for event in sse_client.stream_events():
        events_collected.append(event)
        wiki = event.data.get("wiki", "unknown")
        title = event.data.get("title", "no_title")
        logger.info(f"Đã nhận Event #{len(events_collected)} | Wiki: {wiki} | Title: {title[:40]}")
        if len(events_collected) >= max_events:
            sse_client.stop()
            break

    # 2. Đưa vào RawArchiver với batch_size=5 (ép flush ngay khi đủ 5 events)
    logger.info("Đang nạp events vào RawArchiver...")
    archiver = RawArchiver(
        batch_size=max_events,
        enable_db_manifest=True,
    )

    manifest = None
    for idx, event in enumerate(events_collected, start=1):
        result = archiver.append(event)
        if result is not None:
            manifest = result
            logger.info(
                f"Event #{idx} đã kích hoạt batch flush! ID: {manifest.manifest_id[:8]}"
            )

    assert manifest is not None, "Batch flush không được kích hoạt sau khi nạp đủ 5 events!"

    # 3. Kiểm tra file vật lý trên đĩa
    file_path = Path(manifest.file_path)
    logger.info(f"Đang kiểm tra file vật lý: {file_path.as_posix()}")
    assert file_path.is_file(), f"File vật lý không tồn tại tại {file_path}"

    with open(file_path, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    assert len(lines) == max_events, f"Kỳ vọng {max_events} dòng, thực tế {len(lines)}"
    logger.info(f"File vật lý chứa đúng {len(lines)} dòng JSONL hợp lệ.")

    # Kiểm tra JSON parse từng dòng
    for idx, line in enumerate(lines, start=1):
        parsed = json.loads(line)
        logger.info(f"Dòng #{idx}: wiki={parsed.get('wiki')} | id={parsed.get('id')}")

    # 4. Kiểm tra mã băm SHA-256 thực tế của file
    disk_sha256 = compute_file_sha256(file_path)
    logger.info(f"Mã băm SHA-256 tính từ file đĩa: {disk_sha256}")
    assert disk_sha256 == manifest.checksum_sha256, (
        f"Lệch checksum: manifest={manifest.checksum_sha256} vs disk={disk_sha256}"
    )

    # 5. Xác minh bản ghi trong database PostgreSQL qua hàm verify_manifest_in_db
    logger.info("Đang đối chiếu dữ liệu với bảng archive_manifests trong PostgreSQL...")
    verification = RawArchiver.verify_manifest_in_db(manifest.manifest_id)

    assert verification["valid"] is True, f"Xác minh manifest trong DB thất bại: {verification}"
    logger.info(
        f"Xác minh toàn vẹn thành công: "
        f"ManifestID={manifest.manifest_id} | "
        f"Status={verification['manifest']['status']} | "
        f"Size={verification['actual_size']} bytes | "
        f"ChecksumMatch={verification['checksum_match']}"
    )

    logger.info("=== KIỂM THỬ HOÀN TẤT: RAW ARCHIVER ĐẠT 100% YÊU CẦU PRODUCTION ===")


if __name__ == "__main__":
    main()
