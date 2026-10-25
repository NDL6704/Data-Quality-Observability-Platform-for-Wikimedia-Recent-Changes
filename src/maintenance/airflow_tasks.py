"""Airflow adapters imported without I/O; job libraries load inside task execution."""

from datetime import UTC
from typing import Any


def run_batch(kind: str, **context: Any) -> list[dict[str, Any]]:
    from airflow.models import Variable

    from src.maintenance.jobs import maintenance_batch

    conf = context["dag_run"].conf or {}
    manifest_id = conf.get("manifest_id") or Variable.get(
        "wikimedia_maintenance_manifest", default_var=None
    )
    if manifest_id is not None and (not isinstance(manifest_id, str) or len(manifest_id) > 64):
        raise ValueError("Invalid manifest ID")
    return maintenance_batch(kind, limit=10, manifest_id=manifest_id)


def run_daily(**context: Any) -> dict[str, Any]:
    from src.maintenance.jobs import daily_quality_report

    day = context["data_interval_start"].astimezone(UTC).date()
    return daily_quality_report(day)


def run_retention(**context: Any) -> dict[str, Any]:
    from src.maintenance.jobs import retention_report

    conf = context["dag_run"].conf or {}
    return retention_report(days=30, apply=conf.get("apply_retention") is True)
