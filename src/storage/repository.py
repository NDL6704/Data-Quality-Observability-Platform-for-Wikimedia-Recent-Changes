"""Quality persistence inside a caller-owned PostgreSQL transaction.

Curated rows are idempotent by event_key. Quarantine and evaluation records are
append-only processing attempts, not unique source-event counters.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from schemas.models import CuratedRecord
from src.quality.engine import (
    PreparedEvaluation,
    QualityEvaluationSummary,
    RuleCategory,
    RuleResult,
    RuleSeverity,
)


@dataclass(frozen=True)
class PersistenceOutcome:
    """Outcome available before commit; only the caller can confirm durability."""

    event_key: str
    duplicate: bool = False
    quarantine_id: int | None = None


def encode_quarantine_payload(
    payload: dict[str, Any] | str | bytes,
    *,
    received_at: datetime | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Use an explicit versioned envelope for every rejected input.

    This avoids confusing a real source dict with a base64 wrapper. Dict input
    retains JSON values (including diagnostic NaN); bytes retain exact octets.
    """
    if received_at is not None and (received_at.tzinfo is None or received_at.utcoffset() is None):
        raise ValueError("received_at must be timezone-aware")
    if isinstance(payload, dict):
        encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        kind = "serialized_json"
    elif isinstance(payload, str):
        encoded = payload.encode("utf-8", errors="surrogatepass")
        kind = "utf8_text"
    else:
        encoded, kind = payload, "bytes"
    return {
        "source": source_metadata,
        "format": "wikimedia-dq.quarantine",
        "version": 1,
        "encoding": "base64",
        "received_at": received_at.astimezone(UTC).isoformat() if received_at else None,
        "source_kind": kind,
        "data": base64.b64encode(encoded).decode("ascii"),
    }


def decode_quarantine_payload(stored: dict[str, Any]) -> dict[str, Any] | str | bytes:
    """Decode the versioned format and legacy rows written before reconciliation.

    Legacy raw dicts are retained. Old unversioned wrappers are recognized only
    by their exact three-key shape; that old format has an unavoidable ambiguity.
    """
    versioned = stored.get("format") == "wikimedia-dq.quarantine"
    legacy = set(stored) == {"encoding", "source_kind", "data"} and stored["encoding"] == "base64"
    if not versioned and not legacy:
        return stored
    if versioned and (stored.get("version") != 1 or stored.get("encoding") != "base64"):
        raise ValueError("Unsupported quarantine format version/encoding")
    data = base64.b64decode(stored["data"], validate=True)
    kind = stored.get("source_kind")
    if kind == "bytes":
        return data
    text = data.decode("utf-8", errors="surrogatepass")
    if kind == "utf8_text":
        return text
    if kind == "serialized_json":
        decoded = json.loads(text)
        if isinstance(decoded, dict):
            return decoded
    raise ValueError("Invalid quarantine source_kind or serialized object")


class QualityRepository:
    """Execute parameterized SQL; never commit or acknowledge Kafka offsets.

    Requires dict-row connections such as get_db_connection(), with an active
    READ COMMITTED transaction. Do not share one connection across consumers.
    """

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def _require_transaction(self) -> None:
        if self._connection.autocommit or (
            self._connection.info.transaction_status != TransactionStatus.INTRANS
        ):
            raise ValueError("Repository requires an active caller-owned transaction")

    def upsert_curated(self, record: CuratedRecord) -> bool:
        """Insert once, or refresh only processed_timestamp; return True on insert.

        Unique-key conflict resolution is done by PostgreSQL, not SELECT-before-
        INSERT. The follow-up UPDATE observes the winner at READ COMMITTED.
        Deadlocks/serialization errors propagate for a whole-transaction retry.
        """
        self._require_transaction()
        values = record.model_dump()
        values["raw_payload"] = Jsonb(record.raw_payload)
        row = self._connection.execute(
            """
            INSERT INTO curated_changes (
                event_key, source_event_id, event_timestamp, received_timestamp,
                wiki, server_name, title, user_id, user_text, change_type,
                namespace, is_bot, is_minor, old_length, new_length, comment, raw_payload
            ) VALUES (
                %(event_key)s, %(source_event_id)s, %(event_timestamp)s, %(received_timestamp)s,
                %(wiki)s, %(server_name)s, %(title)s, %(user_id)s, %(user_text)s, %(change_type)s,
                %(namespace)s, %(is_bot)s, %(is_minor)s, %(old_length)s, %(new_length)s,
                %(comment)s, %(raw_payload)s
            ) ON CONFLICT (event_key) DO NOTHING
            RETURNING event_key
            """,
            values,
        ).fetchone()
        if row is not None:
            return True
        updated = self._connection.execute(
            """UPDATE curated_changes SET processed_timestamp = clock_timestamp()
               WHERE event_key = %s RETURNING event_key""",
            (record.event_key,),
        ).fetchone()
        if updated is None:
            raise RuntimeError("Conflicting curated row disappeared; retry transaction")
        return False

    def insert_quarantine(
        self,
        summary: QualityEvaluationSummary,
        payload: dict[str, Any] | str | bytes,
        *,
        received_at: datetime | None = None,
        source_metadata: dict[str, Any] | None = None,
    ) -> int:
        """Append one rejected processing attempt, preserving original input."""
        self._require_transaction()
        if not summary.should_quarantine or not summary.failure_codes:
            raise ValueError("Quarantine requires a rejected summary with failure codes")
        row = self._connection.execute(
            """INSERT INTO quarantined_events
               (event_key, raw_payload, failure_codes, failure_details)
               VALUES (%s, %s, %s, %s) RETURNING id""",
            (
                summary.event_key,
                Jsonb(
                    encode_quarantine_payload(
                        payload, received_at=received_at, source_metadata=source_metadata
                    )
                ),
                summary.failure_codes,
                Jsonb([result.to_dict() for result in summary.results if not result.passed]),
            ),
        ).fetchone()
        if row is None:
            raise RuntimeError("Quarantine INSERT returned no id")
        return int(row["id"])

    def record_quality_results(self, event_key: str, results: list[RuleResult]) -> None:
        """Store one single-event window per rule, counting processing attempts.

        Keep event_key/result metadata even for passes, because migration 001 has
        no event_key column. Aggregation/dedup of deliveries is a later concern.
        """
        self._require_transaction()
        with self._connection.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO quality_results (
                       rule_id, rule_version, time_window_start, time_window_end,
                       rows_checked, rows_failed, failure_rate, failure_details, evaluated_at
                   ) VALUES (%s, %s, %s, %s, 1, %s, %s, %s, %s)""",
                [
                    (
                        r.rule_id,
                        r.rule_version,
                        r.evaluated_at,
                        r.evaluated_at,
                        int(not r.passed),
                        float(not r.passed),
                        Jsonb([{"event_key": event_key, **r.to_dict()}]),
                        r.evaluated_at,
                    )
                    for r in results
                ],
            )

    def persist_prepared(
        self,
        prepared: PreparedEvaluation,
        *,
        source_metadata: dict[str, Any] | None = None,
    ) -> PersistenceOutcome:
        """Persist the engine handoff without reparsing or losing original bytes."""
        curated = None if prepared.summary.should_quarantine else prepared.to_curated_record()
        payload = prepared.source_payload if curated is None else prepared.raw_payload
        if payload is None:
            raise ValueError("Accepted handoff requires a raw object")
        return self.persist_evaluation(
            prepared.summary,
            payload,
            curated=curated,
            received_at=prepared.received_at,
            source_metadata=source_metadata,
        )

    def persist_evaluation(
        self,
        summary: QualityEvaluationSummary,
        payload: dict[str, Any] | str | bytes,
        *,
        curated: CuratedRecord | None = None,
        received_at: datetime | None = None,
        source_metadata: dict[str, Any] | None = None,
    ) -> PersistenceOutcome:
        """Atomically write destination plus quality ledger using a savepoint.

        A later failure rolls back this operation even if the caller catches it.
        The outer transaction still owns commit/rollback. Duplicate delivery is
        observed as R-DED-02 INFO without mutating the engine summary.
        """
        self._require_transaction()
        if summary.is_valid == summary.should_quarantine:
            raise ValueError("Inconsistent routing flags")
        if summary.should_quarantine:
            if curated is not None:
                raise ValueError("Rejected events cannot carry a curated record")
        elif curated is None or curated.event_key != summary.event_key:
            raise ValueError("Accepted events require a matching curated record")
        elif not isinstance(payload, dict) or curated.raw_payload != payload:
            raise ValueError("Curated raw payload must match the evaluated input dictionary")

        with self._connection.transaction():
            results = list(summary.results)
            if summary.should_quarantine:
                outcome = PersistenceOutcome(
                    summary.event_key,
                    quarantine_id=self.insert_quarantine(
                        summary, payload, received_at=received_at, source_metadata=source_metadata
                    ),
                )
            else:
                assert curated is not None
                inserted = self.upsert_curated(curated)
                outcome = PersistenceOutcome(summary.event_key, duplicate=not inserted)
                results.append(
                    RuleResult(
                        "R-DED-02",
                        "1.1.0",
                        RuleCategory.DEDUPLICATION,
                        RuleSeverity.INFO,
                        passed=inserted,
                        failure_code=None if inserted else "INFO_DUPLICATE_DELIVERY",
                        details={"duplicate": not inserted},
                    )
                )
            self.record_quality_results(summary.event_key, results)
            return outcome
