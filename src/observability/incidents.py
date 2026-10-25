"""Persist Alertmanager alert episodes with concurrency-safe lifecycle transitions."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from psycopg.types.json import Jsonb

from src.storage.db import get_db_connection


@dataclass(frozen=True)
class AlertNotification:
    """Identity includes startsAt so delayed old resolves cannot close a new episode."""

    incident_id: str
    status: str
    labels: dict[str, str]
    starts_at: datetime
    ends_at: datetime | None
    summary: str


def parse_notifications(body: Any) -> list[AlertNotification]:
    """Validate a bounded Alertmanager webhook before opening a DB transaction."""
    if not isinstance(body, dict) or body.get("version") != "4":
        raise ValueError("Expected Alertmanager webhook version 4")
    alerts = body.get("alerts")
    if not isinstance(alerts, list) or not 1 <= len(alerts) <= 100:
        raise ValueError("Expected 1..100 alerts")
    parsed: list[AlertNotification] = []
    for alert in alerts:
        if not isinstance(alert, dict) or alert.get("status") not in {"firing", "resolved"}:
            raise ValueError("Invalid alert status")
        labels = alert.get("labels")
        if (
            not isinstance(labels, dict)
            or not 1 <= len(labels) <= 32
            or any(
                not isinstance(k, str) or not isinstance(v, str) or len(k) > 128 or len(v) > 256
                for k, v in labels.items()
            )
        ):
            raise ValueError("Invalid alert labels")
        if not labels.get("alertname") or len(labels["alertname"]) > 64:
            raise ValueError("alertname required (max 64 chars)")
        severity = labels.get("severity", "MEDIUM").upper()
        if severity not in {"CRITICAL", "HIGH", "MEDIUM", "INFO"}:
            raise ValueError("Unknown severity")
        labels = {**labels, "severity": severity}
        if len(labels.get("component", "pipeline")) > 64:
            raise ValueError("component too long")

        def timestamp(value: Any) -> datetime:
            if not isinstance(value, str):
                raise ValueError("Timestamp must be RFC3339")
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None or dt.utcoffset() is None or dt.year < 1970:
                raise ValueError("Timestamp must be timezone-aware and >=1970")
            return dt.astimezone(UTC)

        start = timestamp(alert.get("startsAt"))
        end = timestamp(alert.get("endsAt")) if alert["status"] == "resolved" else None
        if end is not None and end < start:
            raise ValueError("endsAt precedes startsAt")
        annotations = alert.get("annotations", {})
        if not isinstance(annotations, dict) or not isinstance(annotations.get("summary", ""), str):
            raise ValueError("Invalid annotations")
        identity = json.dumps([labels, start.isoformat()], sort_keys=True).encode()
        parsed.append(
            AlertNotification(
                hashlib.sha256(identity).hexdigest(),
                alert["status"],
                labels,
                start,
                end,
                annotations.get("summary", "")[:2000],
            )
        )
    return parsed


class IncidentManager:
    """An episode is OPEN -> INVESTIGATING -> RESOLVED; RESOLVED never reopens.

    occurrence_count counts received webhook observations (including retries),
    not distinct source events. A new startsAt creates a new incident episode.
    """

    def apply(self, notifications: list[AlertNotification]) -> list[str]:
        """Apply the entire notification batch atomically; SQL serializes conflicts."""
        with get_db_connection() as conn:
            conn.execute("SET LOCAL lock_timeout='5s'")
            conn.execute("SET LOCAL statement_timeout='10s'")
            for item in notifications:
                resolved = item.status == "resolved"
                conn.execute(
                    """INSERT INTO incidents
                       (incident_id,incident_type,severity,component,status,detected_at,resolved_at,
                        root_cause,resolution_note,alert_labels,alert_starts_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (incident_id) DO UPDATE SET
                         occurrence_count=incidents.occurrence_count+1,
                         updated_at=clock_timestamp(),
                         status=CASE WHEN incidents.status='RESOLVED' THEN 'RESOLVED'
                                     WHEN EXCLUDED.status='RESOLVED' THEN 'RESOLVED'
                                     ELSE incidents.status END,
                         resolved_at=COALESCE(incidents.resolved_at,EXCLUDED.resolved_at),
                         resolution_note=COALESCE(incidents.resolution_note,EXCLUDED.resolution_note)
                    """,
                    (
                        item.incident_id,
                        item.labels["alertname"],
                        item.labels["severity"],
                        item.labels.get("component", "pipeline"),
                        "RESOLVED" if resolved else "OPEN",
                        item.starts_at,
                        item.ends_at,
                        item.summary,
                        "Alertmanager condition resolved" if resolved else None,
                        Jsonb(item.labels),
                        item.starts_at,
                    ),
                )
        return [item.incident_id for item in notifications]

    def investigate(self, incident_id: str, note: str) -> bool:
        """Mark an open episode as investigated without reopening resolved records."""
        with get_db_connection() as conn:
            row = conn.execute(
                """UPDATE incidents SET status='INVESTIGATING',root_cause=%s,
                   updated_at=clock_timestamp() WHERE incident_id=%s AND status='OPEN'
                   RETURNING incident_id""",
                (note, incident_id),
            ).fetchone()
            return row is not None
