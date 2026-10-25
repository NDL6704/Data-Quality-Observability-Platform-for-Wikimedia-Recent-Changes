"""Data, lag, replay and archive scenarios built on production components."""

import json
import time
from typing import Any

from schemas.models import WikimediaRecentChange
from src.faults.contracts import ScenarioReport
from src.faults.environment import FaultEnvironment
from src.ingestion.raw_archiver import RawArchiver
from src.processing.consumer import QualityConsumer
from src.quality.engine import QualityEngine, QualityRule, RuleCategory, RuleResult, RuleSeverity
from src.quality.rules import build_default_rule_set
from src.recovery.reconciler import reconcile_keys, reconcile_manifest
from src.recovery.replayer import replay_quarantine
from src.storage.db import get_db_connection
from src.storage.repository import decode_quarantine_payload


class StrictTitleRule(QualityRule):
    """Harness-only obsolete policy; never change the production rule catalog."""

    def __init__(self) -> None:
        super().__init__(
            "FAULT-TITLE",
            "test.1",
            RuleCategory.COMPLETENESS,
            RuleSeverity.HIGH,
            "Obsolete synthetic title policy",
        )

    def evaluate(self, event: WikimediaRecentChange, raw_payload: dict[str, Any]) -> RuleResult:
        return self._fail("FAULT_OBSOLETE_TITLE_POLICY")


def replay_scenario(env: FaultEnvironment, report: ScenarioReport) -> None:
    """Quarantine under obsolete policy, then promote original input under defaults."""
    raw = env.payload()
    env.produce([raw])
    obsolete = QualityEngine()
    obsolete.register_rules([*build_default_rule_set(), StrictTitleRule()])
    stats = QualityConsumer(settings=env.settings, engine=obsolete, auto_create_topics=False).run(
        max_messages=1, max_duration_sec=30
    )
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT * FROM quarantined_events WHERE event_key=%s", (env.keys[0],)
        ).fetchone()
    if row is None:
        raise RuntimeError("Obsolete rule did not quarantine input")
    rejected = replay_quarantine(row["id"], engine=obsolete)
    promoted = replay_quarantine(row["id"])
    again = replay_quarantine(row["id"])
    with get_db_connection() as conn:
        stored = conn.execute(
            "SELECT * FROM quarantined_events WHERE id=%s", (row["id"],)
        ).fetchone()
        curated = conn.execute(
            "SELECT * FROM curated_changes WHERE event_key=%s", (env.keys[0],)
        ).fetchone()
    reconciliation = reconcile_keys(env.keys)
    report.evidence.update(
        quarantine_id=row["id"],
        original_receipt=row["raw_payload"]["received_at"],
        replay_status=promoted.status,
        reconciliation=reconciliation.to_dict(),
    )
    report.checks.update(
        obsolete_policy_rejected=stats["quarantined"] == 1,
        rejected_replay_stays_pending=rejected.status == "PENDING",
        corrected_policy_promoted=promoted.status == "REPLAYED",
        second_replay_noop=again.status == "REPLAYED" and env.curated_count(env.keys[0]) == 1,
        original_bytes_preserved=stored is not None
        and decode_quarantine_payload(stored["raw_payload"]) == raw,
        original_receipt_preserved=curated is not None
        and curated["received_timestamp"].isoformat() == row["raw_payload"]["received_at"],
        replay_audit_linked=stored is not None
        and stored["replay_run_id"] is not None
        and stored["replayed_at"] is not None,
        reconciliation_complete=not reconciliation.missing
        and reconciliation.curated == 1
        and len(reconciliation.overlap) == 1,
    )


def checksum_scenario(env: FaultEnvironment, report: ScenarioReport) -> None:
    """Archive owned data, reconcile it, then corrupt only that run's physical file."""
    raw = env.payload()
    env.produce([raw, raw])
    stats = env.consume(2)
    archiver = RawArchiver(base_dir=env.directory / "archive", batch_size=3)
    archiver.append(json.loads(raw))
    archiver.append(json.loads(raw))
    manifest = archiver.flush()
    if manifest is None:
        raise RuntimeError("No archive manifest")
    env.manifests.append(manifest.manifest_id)
    before = RawArchiver.verify_manifest_in_db(manifest.manifest_id)
    reconciled = reconcile_manifest(manifest.manifest_id)
    with open(manifest.file_path, "r+b") as file:
        first = file.read(1)
        file.seek(0)
        file.write(bytes([first[0] ^ 1]))
        file.flush()
    after = RawArchiver.verify_manifest_in_db(manifest.manifest_id)
    refused = False
    try:
        reconcile_manifest(manifest.manifest_id)
    except ValueError:
        refused = True
    report.evidence.update(
        manifest_id=manifest.manifest_id,
        archive_path=manifest.file_path,
        expected_sha256=after["expected_sha256"],
        actual_sha256=after["actual_sha256"],
        reconciliation=reconciled.to_dict(),
        integrity_signal="ARCHIVE_CHECKSUM_MISMATCH",
    )
    report.checks.update(
        baseline_integrity=before["valid"],
        mismatch_detected=not after["checksum_match"],
        size_unchanged=after["size_match"],
        corrupt_archive_refused=refused,
        reconciliation_complete=reconciled.unique_raw == 1
        and reconciled.raw_deliveries == 2
        and reconciled.duplicate_deliveries == 1
        and reconciled.curated == 1
        and not reconciled.missing
        and stats["duplicates"] == 1,
    )


def run_data_scenario(env: FaultEnvironment, report: ScenarioReport) -> None:
    """Assert destinations, failure codes, sink output and source acknowledgments."""
    name = report.scenario_id
    if name == "replay-after-rule-fix":
        replay_scenario(env, report)
        return
    if name == "archive-checksum-mismatch":
        checksum_scenario(env, report)
        return
    code: str | None = None
    if name == "invalid-json-payload":
        values = [b'{"fault_run":"' + env.run_id.encode() + b'",']
        code = "ERR_INVALID_JSON"
    elif name == "missing-required-field":
        values = [env.payload(server_name=None)]
        code = "ERR_MISSING_IDENTITY"
    elif name == "schema-drift":
        values = [env.payload(type="future-change", new_field={"version": 2}) for _ in range(100)]
        values.append(env.payload(length={"new": "unexpected-format"}))
    elif name == "duplicate-delivery":
        raw = env.payload()
        values = [raw] * 3
    elif name == "consumer-lag-surge":
        values = [env.payload() for _ in range(200)]
    else:
        raise ValueError(f"No implementation for {name}")
    env.produce(values)
    lag_before = env.lag()
    recovery_start = time.monotonic()
    stats = env.consume(len(values))
    recovery_duration = time.monotonic() - recovery_start
    outputs = env.outputs(len(values))
    reconciliation = reconcile_keys(env.keys)
    report.evidence.update(
        stats=stats,
        lag_before=lag_before,
        lag_after=env.lag(),
        recovery_duration_seconds=recovery_duration,
        reconciliation=reconciliation.to_dict(),
    )
    report.checks.update(
        all_processed=stats["processed"] == len(values),
        source_acknowledged=env.committed() == len(values),
        sink_confirmed=len(outputs) == len(values),
        reconciliation_complete=not reconciliation.missing,
    )
    if code:
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT * FROM quarantined_events WHERE event_key=%s", (env.keys[0],)
            ).fetchone()
        report.checks.update(
            quarantined=stats["quarantined"] == 1 and env.curated_count(env.keys[0]) == 0,
            exact_error_code=row is not None and code in row["failure_codes"],
            original_bytes_preserved=row is not None
            and decode_quarantine_payload(row["raw_payload"]) == values[0],
            dlq_routed=len(outputs) == 1 and outputs[0]["topic"] == env.settings.kafka_topic_dlq,
        )
    elif name == "duplicate-delivery":
        report.checks.update(
            curated_unique=env.curated_count(env.keys[0]) == 1,
            duplicates_counted=stats["duplicates"] == 2,
        )
    elif name == "schema-drift":
        results = [r for out in outputs for r in out["body"]["quality_results"]]
        report.checks.update(
            unknown_type_observed=any(
                r["failure_code"] == "WARN_UNKNOWN_EVENT_TYPE" for r in results
            ),
            full_window_drift=any(r["failure_code"] == "WARN_SCHEMA_DRIFT" for r in results),
            unknown_fields_preserved=all(
                out["body"]["payload"]["raw_payload"].get("new_field") == {"version": 2}
                for out in outputs
                if out["topic"] == env.settings.kafka_topic_validated
            ),
            invalid_format_quarantined=stats["quarantined"] == 1 and stats["curated"] == 100,
        )
    elif name == "consumer-lag-surge":
        report.checks.update(
            lag_surge_measured=lag_before >= 200,
            caught_up=env.lag() == 0 and reconciliation.curated == 200,
        )
        report.evidence["scope"] = "Kafka lag evidence; Prometheus/Grafana alerts are Phase5"
