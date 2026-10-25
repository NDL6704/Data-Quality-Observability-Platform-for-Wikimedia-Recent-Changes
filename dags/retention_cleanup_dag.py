"""Weekly inventory; manual conf apply_retention=true explicitly opts into deletion."""

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.maintenance.airflow_tasks import run_retention

with DAG(
    "retention_cleanup",
    schedule="@weekly",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["wikimedia", "dry-run"],
) as dag:
    PythonOperator(task_id="retention_inventory", python_callable=run_retention)
