"""Bounded read queries through a cached, read-only PostgreSQL pool."""

import atexit
import json
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import streamlit as st
from psycopg import Connection
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from src.common.config import get_settings
from src.quality.engine import QualityEngine
from src.quality.rules import build_default_rule_set
from src.storage.repository import decode_quarantine_payload


@st.cache_resource
def get_pool() -> ConnectionPool[Connection[Any]]:
    """Share pool, never a transaction/connection between Streamlit sessions."""
    cfg = get_settings()
    pool = ConnectionPool[Connection[Any]](
        make_conninfo(
            host=cfg.postgres_host,
            port=cfg.postgres_port,
            dbname=cfg.postgres_db,
            user=cfg.postgres_user,
            password=cfg.postgres_password,
            connect_timeout=5,
        ),
        min_size=0,
        max_size=4,
        timeout=5,
        kwargs={
            "row_factory": dict_row,
            "options": "-c default_transaction_read_only=on -c statement_timeout=10000",
        },
        open=True,
    )
    atexit.register(pool.close)
    return pool


def read(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """SQL text comes only from application constants; values are bound parameters."""
    with get_pool().connection() as conn:
        return list(conn.execute(sql, params).fetchall())


def overview() -> dict[str, Any]:
    """One snapshot for independent destination/incident counts, not a loss equation."""
    return read("""SELECT
        (SELECT count(*) FROM curated_changes) AS curated,
        (SELECT count(*) FROM quarantined_events WHERE replay_status='PENDING') AS pending,
        (SELECT count(*) FROM incidents WHERE status IN ('OPEN','INVESTIGATING')) AS incidents,
        (SELECT count(*) FROM archive_manifests WHERE status='CORRUPTED') AS corrupted""")[0]


QUALITY_WHERE = """evaluated_at >= clock_timestamp()-(%s * interval '1 hour')
    AND (%s='' OR rule_id=%s) AND (%s='' OR rule_version=%s)
    AND (%s::timestamptz IS NULL OR evaluated_at >= %s)
    AND (%s::timestamptz IS NULL OR (rule_id='R-FRS-01'
         AND (failure_details->0->'details'->>'received_at')::timestamptz >= %s))"""


def quality_params(
    hours: int, rule_id: str, version: str, since: datetime | None, receipt: datetime | None
) -> tuple[Any, ...]:
    return (hours, rule_id, rule_id, version, version, since, since, receipt, receipt)


def quality(
    hours: int,
    rule_id: str = "",
    version: str = "",
    since: datetime | None = None,
    receipt: datetime | None = None,
) -> list[dict[str, Any]]:
    """Attempt-weighted rule ratios; do not average pre-aggregated percentages."""
    return read(
        """SELECT rule_id,rule_version,sum(rows_checked) AS checked,
        sum(rows_failed) AS failed,
        sum(rows_failed)::float/NULLIF(sum(rows_checked),0) AS failure_rate
        FROM quality_results WHERE """
        + QUALITY_WHERE
        + """
        GROUP BY rule_id,rule_version ORDER BY failed DESC,rule_id LIMIT 100""",
        quality_params(hours, rule_id, version, since, receipt),
    )


def quality_trend(
    hours: int,
    rule_id: str = "",
    version: str = "",
    since: datetime | None = None,
    receipt: datetime | None = None,
) -> list[dict[str, Any]]:
    bucket = max(60, hours * 3600 // 240)
    return read(
        """SELECT to_timestamp(floor(extract(epoch FROM evaluated_at)/%s)*%s) AS time,
        sum(rows_checked) AS checked,sum(rows_failed) AS failed,
        sum(rows_failed)::float/NULLIF(sum(rows_checked),0) AS failure_rate
        FROM quality_results WHERE """
        + QUALITY_WHERE
        + """
        GROUP BY 1 ORDER BY 1 DESC LIMIT 250""",
        (bucket, bucket, *quality_params(hours, rule_id, version, since, receipt)),
    )


def wiki_distribution() -> list[dict[str, Any]]:
    return read("""SELECT wiki,count(*) AS events FROM curated_changes
        GROUP BY wiki ORDER BY events DESC LIMIT 12""")


def event_quality(key: str) -> list[dict[str, Any]]:
    return read(
        """SELECT rule_id,rule_version,rows_failed,failure_details,evaluated_at
        FROM quality_results WHERE failure_details->0->>'event_key'=%s
        ORDER BY evaluated_at DESC LIMIT 100""",
        (key,),
    )


def quarantine_summary() -> list[dict[str, Any]]:
    return read("""SELECT replay_status,count(*) AS events FROM quarantined_events
        GROUP BY replay_status""")


def curated(wiki: str, title: str) -> list[dict[str, Any]]:
    return read(
        """SELECT event_key,wiki,title,change_type,event_timestamp,processed_timestamp
        FROM curated_changes WHERE (%s='' OR wiki=%s) AND (%s='' OR title ILIKE %s)
        ORDER BY processed_timestamp DESC,event_key LIMIT 100""",
        (wiki, wiki, title, "%" + title + "%"),
    )


def event_detail(key: str) -> list[dict[str, Any]]:
    return read("SELECT * FROM curated_changes WHERE event_key=%s", (key,))


def quarantine(status: str, code: str, key: str) -> list[dict[str, Any]]:
    return read(
        """SELECT id,event_key,failure_codes,replay_status,quarantined_at
        FROM quarantined_events WHERE (%s='ALL' OR replay_status=%s)
        AND (%s='' OR %s=ANY(failure_codes)) AND (%s='' OR event_key=%s)
        ORDER BY quarantined_at DESC,id DESC LIMIT 100""",
        (status, status, code, code, key, key),
    )


def quarantine_detail(row_id: int) -> dict[str, Any] | None:
    rows = read("SELECT * FROM quarantined_events WHERE id=%s", (row_id,))
    return rows[0] if rows else None


def preview_replay(row: dict[str, Any]) -> dict[str, Any]:
    """Pure DB-read preview; no promotion/audit write until explicit replay action."""
    from datetime import datetime

    stored = row["raw_payload"]
    if stored.get("format") != "wikimedia-dq.quarantine" or not stored.get("received_at"):
        raise ValueError("Original receipt is missing; explicit migration is required")
    engine = QualityEngine()
    engine.register_rules(build_default_rule_set())
    prepared = engine.prepare_payload(
        decode_quarantine_payload(stored), received_at=datetime.fromisoformat(stored["received_at"])
    )
    return {
        "event_key": prepared.summary.event_key,
        "would_promote": not prepared.summary.should_quarantine,
        "original_received_at": prepared.received_at.isoformat(),
        "current_results": [result.to_dict() for result in prepared.summary.results],
    }


def incidents(status: str) -> list[dict[str, Any]]:
    return read(
        """SELECT incident_id,incident_type,severity,status,detected_at,resolved_at,
        occurrence_count,root_cause,alert_labels FROM incidents WHERE (%s='ALL' OR status=%s)
        ORDER BY detected_at DESC,incident_id LIMIT 100""",
        (status, status),
    )


def prom_query(expression: str, *, history: bool = False) -> list[dict[str, Any]]:
    """Timeout/missing data propagates to UI; it is never coerced to zero."""
    import time

    args: dict[str, Any] = {"query": expression}
    if history:
        args.update(start=time.time() - 1800, end=time.time(), step=30)
    base = get_settings().prometheus_url.rstrip("/")
    route = "/api/v1/query_range" if history else "/api/v1/query"
    with urlopen(base + route + "?" + urlencode(args), timeout=5) as response:
        body = json.loads(response.read())
    if body.get("status") != "success":
        raise ValueError("Prometheus query failed")
    return list(body["data"]["result"])
