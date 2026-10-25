"""Reject malformed webhooks before persistence and isolate distinct episodes."""

from copy import deepcopy
from typing import Any

import pytest

from src.observability.incidents import parse_notifications


def notification(status: str = "firing", **labels: str) -> dict[str, Any]:
    return {
        "version": "4",
        "alerts": [
            {
                "status": status,
                "labels": {"alertname": "TestAlert", "severity": "HIGH", **labels},
                "startsAt": "2026-10-25T00:00:00Z",
                "endsAt": "2026-10-25T00:01:00Z",
                "annotations": {"summary": "test"},
            }
        ],
    }


@pytest.mark.parametrize(
    "change", ["version", "status", "severity", "naive", "ends-before", "label-type"]
)
def test_invalid_webhook(change: str) -> None:
    body = notification("resolved")
    alert = body["alerts"][0]
    if change == "version":
        body["version"] = "3"
    elif change == "status":
        alert["status"] = "OPEN"
    elif change == "severity":
        alert["labels"]["severity"] = "unknown"
    elif change == "naive":
        alert["startsAt"] = "2026-10-25T00:00:00"
    elif change == "ends-before":
        alert["endsAt"] = "2026-10-24T00:00:00Z"
    else:
        alert["labels"]["key"] = []
    with pytest.raises(ValueError):
        parse_notifications(body)


def test_episode_identity_and_timezone_equivalence() -> None:
    body = notification()
    same = deepcopy(body)
    same["alerts"][0]["startsAt"] = "2026-10-25T07:00:00+07:00"
    # Equivalent source times should identify the same episode.
    assert parse_notifications(body)[0].starts_at == parse_notifications(same)[0].starts_at
    assert parse_notifications(body)[0].incident_id == parse_notifications(same)[0].incident_id
    later = deepcopy(body)
    later["alerts"][0]["startsAt"] = "2026-10-25T01:00:00Z"
    assert parse_notifications(body)[0].incident_id != parse_notifications(later)[0].incident_id
