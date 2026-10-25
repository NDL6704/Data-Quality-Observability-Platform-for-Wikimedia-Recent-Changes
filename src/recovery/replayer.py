"""Transactional quarantine promotion under an explicitly supplied rule engine.

Replay writes PostgreSQL only; it does not publish validated Kafka output or move
source offsets. Original bytes and receipt are preserved, never silently repaired.
"""

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from uuid import uuid4

from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.db import get_db_connection
from src.storage.repository import QualityRepository, decode_quarantine_payload


@dataclass(frozen=True)
class ReplayOutcome:
    """Distinguish promotion, still-rejected input, and an already processed row."""

    quarantine_id: int
    status: str
    event_key: str | None = None
    duplicate: bool = False


def replay_quarantine(quarantine_id: int, *, engine: QualityEngine | None = None) -> ReplayOutcome:
    """Lock one pending row and atomically commit curated, audit and replay status.

    No SKIP LOCKED: concurrent callers wait and observe REPLAYED after the winner
    commits. Missing original receipt is rejected rather than faking freshness.
    Rejected re-evaluations keep PENDING and append quality evidence only.
    """
    if quarantine_id <= 0:
        raise ValueError("quarantine_id must be positive")
    if engine is None:
        engine = QualityEngine()
        engine.register_rules(build_default_rule_set())
    with get_db_connection() as conn:
        conn.execute("SET LOCAL lock_timeout = '5s'")
        conn.execute("SET LOCAL statement_timeout = '10s'")
        row = conn.execute(
            "SELECT * FROM quarantined_events WHERE id=%s FOR UPDATE", (quarantine_id,)
        ).fetchone()
        if row is None:
            raise ValueError("Unknown quarantine_id")
        if row["replay_status"] != "PENDING":
            return ReplayOutcome(quarantine_id, row["replay_status"], row["event_key"])
        stored = row["raw_payload"]
        if stored.get("format") != "wikimedia-dq.quarantine" or not stored.get("received_at"):
            raise ValueError("Replay requires a versioned envelope with original received_at")
        receipt = datetime.fromisoformat(stored["received_at"])
        if receipt.tzinfo is None or receipt.utcoffset() is None:
            raise ValueError("Original received_at must be timezone-aware")
        prepared = engine.prepare_payload(
            decode_quarantine_payload(stored), received_at=receipt.astimezone(UTC)
        )
        repo = QualityRepository(conn)
        if prepared.summary.should_quarantine:
            repo.record_quality_results(prepared.summary.event_key, prepared.summary.results)
            return ReplayOutcome(quarantine_id, "PENDING", prepared.summary.event_key)
        run_id = f"replay-{uuid4().hex}"
        conn.execute(
            """INSERT INTO pipeline_runs
               (run_id, pipeline_name, start_time, status, records_read)
               VALUES (%s, 'quarantine-replay', clock_timestamp(), 'RUNNING', 1)""",
            (run_id,),
        )
        outcome = repo.persist_prepared(prepared, source_metadata=stored.get("source"))
        conn.execute(
            """UPDATE quarantined_events SET replay_status='REPLAYED',
               replay_run_id=%s, replayed_at=clock_timestamp() WHERE id=%s""",
            (run_id, quarantine_id),
        )
        conn.execute(
            """UPDATE pipeline_runs SET status='COMPLETED', end_time=clock_timestamp(),
               records_written=%s WHERE run_id=%s""",
            (int(not outcome.duplicate), run_id),
        )
        return ReplayOutcome(quarantine_id, "REPLAYED", outcome.event_key, outcome.duplicate)


def main() -> int:
    """Replay one explicitly selected row using current default production rules."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarantine-id", type=int, required=True)
    args = parser.parse_args()
    try:
        outcome = replay_quarantine(args.quarantine_id)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(asdict(outcome)))
    return 0 if outcome.status == "REPLAYED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
