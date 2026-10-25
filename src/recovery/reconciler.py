"""Read-only archive integrity and unique-event reconciliation.

Archive rows count deliveries; DB quarantine rows count attempts. Compare sets of
event keys, never subtract attempt counts and call the difference data loss.
"""

import argparse
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ingestion.raw_archiver import RawArchiver
from src.quality.engine import QualityEngine
from src.storage.db import get_db_connection


@dataclass(frozen=True)
class ReconciliationReport:
    """Mutually exclusive outcomes for unique source keys in a bounded input."""

    raw_deliveries: int
    unique_raw: int
    duplicate_deliveries: int
    curated: int
    quarantine_only: int
    missing: tuple[str, ...]
    overlap: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Export aggregate counts plus actionable missing/overlap identities."""
        return asdict(self)


def reconcile_keys(raw_keys: Iterable[str]) -> ReconciliationReport:
    """Read both destination sets in one consistent DB snapshot, scoped to keys."""
    deliveries = list(raw_keys)
    keys = set(deliveries)
    with get_db_connection() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        curated = {
            r["event_key"]
            for r in conn.execute(
                "SELECT event_key FROM curated_changes WHERE event_key=ANY(%s)", (list(keys),)
            )
        }
        quarantine = {
            r["event_key"]
            for r in conn.execute(
                "SELECT DISTINCT event_key FROM quarantined_events WHERE event_key=ANY(%s)",
                (list(keys),),
            )
        }
    return ReconciliationReport(
        len(deliveries),
        len(keys),
        len(deliveries) - len(keys),
        len(curated),
        len(quarantine - curated),
        tuple(sorted(keys - curated - quarantine)),
        tuple(sorted(curated & quarantine)),
    )


def reconcile_manifest(manifest_id: str) -> ReconciliationReport:
    """Verify DB manifest before interpreting its JSONL records as raw evidence.

    RawArchiver writes JSON objects, not unparseable Kafka byte messages. Those
    require reconciliation by source keys/delivery identity instead.
    """
    verification = RawArchiver.verify_manifest_in_db(manifest_id)
    if not verification["valid"]:
        raise ValueError("Archive integrity verification failed")
    manifest = verification["manifest"]
    if manifest["format"] != "jsonl":
        raise ValueError("Only JSONL archives are supported")
    path = Path(manifest["file_path"])
    keys: list[str] = []
    engine = QualityEngine()
    with path.open(encoding="utf-8") as source:
        for line in source:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("Archive row must be a JSON object")
            prepared = engine.prepare_payload(payload, received_at=datetime.now(UTC))
            if prepared.event is None:
                raise ValueError("Gate-invalid archive row requires exact source bytes/event keys")
            keys.append(prepared.summary.event_key)
    if len(keys) != manifest["record_count"]:
        raise ValueError("Archive record count differs from manifest")
    # Detect modifications between the initial checksum and parsing pass.
    if not RawArchiver.verify_file_checksum(path, manifest["checksum_sha256"]):
        raise ValueError("Archive changed during reconciliation")
    return reconcile_keys(keys)


def main() -> int:
    """Reconcile an explicit archive manifest or a bounded list of event keys."""
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--manifest-id")
    source.add_argument("--event-key", action="append")
    args = parser.parse_args()
    try:
        report = (
            reconcile_manifest(args.manifest_id)
            if args.manifest_id
            else reconcile_keys(args.event_key)
        )
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(json.dumps(report.to_dict(), indent=2))
    return 1 if report.missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
