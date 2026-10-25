"""Daily Snappy compaction; original JSONL is retained."""

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.maintenance.airflow_tasks import run_batch

with DAG(
    "archive_compaction",
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["wikimedia", "maintenance"],
) as dag:
    PythonOperator(
        task_id="compact_archives", python_callable=run_batch, op_kwargs={"kind": "compaction"}
    )
