"""Child process used only by the isolated fault harness.

Subclass/context instrumentation creates deterministic barriers; faults themselves
are OS process termination or server-side termination of an owned DB connection.
The normal production consumer has no fault switches.
"""

import argparse
import json
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
from confluent_kafka import Consumer

from src.common.config import Settings
from src.processing.consumer import QualityConsumer, build_consumer_config
from src.quality.engine import PreparedEvaluation
from src.storage.db import get_db_connection
from src.storage.repository import PersistenceOutcome


def barrier(path: Path, evidence: dict[str, Any]) -> None:
    """Publish evidence atomically and wait for the parent to release or kill us."""
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(evidence), encoding="utf-8")
    temporary.replace(path)
    deadline = time.monotonic() + 60
    while not path.with_suffix(".release").exists():
        if time.monotonic() > deadline:
            raise TimeoutError("Fault barrier was not released")
        time.sleep(0.02)


def main() -> None:
    """Run one isolated message with a selected synchronization boundary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", required=True)
    parser.add_argument("--barrier", type=Path, required=True)
    parser.add_argument("--mode", choices=["before-db", "after-db", "db-drop"], required=True)
    args = parser.parse_args()
    settings = Settings(**json.loads(args.settings))

    @contextmanager
    def connection() -> Generator[psycopg.Connection[Any], None, None]:
        with get_db_connection() as conn:
            conn.execute(
                "SELECT set_config('application_name', %s, true)",
                (settings.kafka_consumer_group_quality,),
            )
            if args.mode == "db-drop":
                barrier(
                    args.barrier,
                    {
                        "pid": conn.info.backend_pid,
                        "application": settings.kafka_consumer_group_quality,
                    },
                )
            yield conn
            if args.mode == "before-db":
                barrier(args.barrier, {"boundary": "before-outer-db-commit"})

    class BarrierConsumer(QualityConsumer):
        def _persist(
            self, prepared: PreparedEvaluation, source: dict[str, Any]
        ) -> PersistenceOutcome:
            outcome = super()._persist(prepared, source)
            if args.mode == "after-db":
                barrier(args.barrier, {"boundary": "after-db-before-sink-and-offset"})
            return outcome

    config = build_consumer_config(settings)
    config["session.timeout.ms"] = 6000
    runner = BarrierConsumer(
        settings=settings,
        consumer=Consumer(config),
        connection_factory=connection,
        auto_create_topics=False,
    )
    try:
        stats = runner.run(max_messages=1, max_duration_sec=45)
    except Exception:
        print("FAULT_STATS=" + json.dumps(runner.stats), flush=True)
        raise
    print("FAULT_STATS=" + json.dumps(stats), flush=True)


if __name__ == "__main__":
    main()
