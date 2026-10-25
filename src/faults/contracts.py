"""Serializable evidence contracts shared by the CLI and recovery tests."""

import math
from dataclasses import asdict, dataclass, field
from typing import Any

SCENARIOS: dict[str, str] = {
    "source-disconnect": "SSE socket cut, reconnect backoff and Last-Event-ID resume",
    "consumer-crash-before-commit": "OS kill after DB commit, before source offset commit",
    "database-outage": "Terminate the harness DB connection, then restart and recover",
    "invalid-json-payload": "Malformed bytes to quarantine and DLQ",
    "missing-required-field": "Missing identity to quarantine with exact failure code",
    "schema-drift": "Unknown types, rolling drift and invalid field format",
    "duplicate-delivery": "Repeated input with one unique curated row",
    "consumer-lag-surge": "Bounded burst, measured Kafka lag and catch-up",
    "replay-after-rule-fix": "Re-evaluate original quarantine under corrected rule policy",
    "archive-checksum-mismatch": "Detect a one-byte mutation against a DB manifest",
}

REQUIRED_CHECKS: dict[str, set[str]] = {
    "source-disconnect": {
        "stream_finished",
        "reconnected",
        "cursor_resumed",
        "events_preserved",
        "exponential_backoff",
    },
    "consumer-crash-before-commit": {
        f"{mode}_{check}"
        for mode in ("before-db", "after-db")
        for check in ("process_killed", "no_ack", "db_boundary", "recovered", "sink_recovered")
    },
    "database-outage": {
        "db_failure_exits",
        "db_error_observed",
        "db-drop_no_ack",
        "db-drop_db_boundary",
        "db-drop_recovered",
    },
    "invalid-json-payload": {
        "quarantined",
        "exact_error_code",
        "original_bytes_preserved",
        "dlq_routed",
    },
    "missing-required-field": {
        "quarantined",
        "exact_error_code",
        "original_bytes_preserved",
        "dlq_routed",
    },
    "schema-drift": {
        "unknown_type_observed",
        "full_window_drift",
        "unknown_fields_preserved",
        "invalid_format_quarantined",
    },
    "duplicate-delivery": {"curated_unique", "duplicates_counted"},
    "consumer-lag-surge": {"lag_surge_measured", "caught_up"},
    "replay-after-rule-fix": {
        "obsolete_policy_rejected",
        "rejected_replay_stays_pending",
        "corrected_policy_promoted",
        "second_replay_noop",
        "original_bytes_preserved",
        "original_receipt_preserved",
        "replay_audit_linked",
        "reconciliation_complete",
    },
    "archive-checksum-mismatch": {
        "baseline_integrity",
        "mismatch_detected",
        "size_unchanged",
        "corrupt_archive_refused",
        "reconciliation_complete",
    },
}
for _name in SCENARIOS:
    if _name != "source-disconnect":
        REQUIRED_CHECKS[_name].add("owned_resources_cleaned")
    if _name in {
        "invalid-json-payload",
        "missing-required-field",
        "schema-drift",
        "duplicate-delivery",
        "consumer-lag-surge",
    }:
        REQUIRED_CHECKS[_name].update(
            {"all_processed", "source_acknowledged", "sink_confirmed", "reconciliation_complete"}
        )


@dataclass
class ScenarioReport:
    """A run passes only if it has explicit checks and all are true."""

    scenario_id: str
    run_id: str
    checks: dict[str, bool] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0
    error: str | None = None
    version: int = 1

    @property
    def passed(self) -> bool:
        """Reject empty, malformed, failed or interrupted reports."""
        return (
            type(self.version) is int
            and self.version == 1
            and isinstance(self.scenario_id, str)
            and self.scenario_id in SCENARIOS
            and isinstance(self.run_id, str)
            and bool(self.run_id)
            and isinstance(self.checks, dict)
            and REQUIRED_CHECKS[self.scenario_id] <= self.checks.keys()
            and all(value is True for value in self.checks.values())
            and isinstance(self.evidence, dict)
            and self.error is None
            and type(self.duration_seconds) in (float, int)
            and math.isfinite(self.duration_seconds)
            and self.duration_seconds >= 0
        )

    def to_dict(self) -> dict[str, Any]:
        """Export JSON-compatible evidence including a derived outcome."""
        return {**asdict(self), "passed": self.passed}
