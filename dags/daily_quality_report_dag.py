"""Daily attempt-weighted UTC quality summary."""

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.maintenance.airflow_tasks import run_daily

with DAG(
    "daily_quality_report",
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["wikimedia", "maintenance"],
) as dag:
    PythonOperator(task_id="summarize_quality", python_callable=run_daily)
