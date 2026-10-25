"""Hourly bounded source-manifest reconciliation."""

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.maintenance.airflow_tasks import run_batch

with DAG(
    "dq_reconciliation",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["wikimedia", "maintenance"],
) as dag:
    PythonOperator(
        task_id="reconcile_archives",
        python_callable=run_batch,
        op_kwargs={"kind": "reconciliation"},
    )
