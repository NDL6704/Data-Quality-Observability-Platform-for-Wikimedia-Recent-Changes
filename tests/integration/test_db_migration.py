"""Integration test to verify PostgreSQL connectivity and schema migrations."""

import uuid
from datetime import UTC, datetime

from src.storage.db import check_db_health, get_db_connection
from src.storage.migration_runner import run_migrations


def test_postgres_connectivity() -> None:
    """Verify that PostgreSQL is alive and accepting queries."""
    assert check_db_health() is True, "PostgreSQL database should be healthy and responding"


def test_schema_migrations_and_tables() -> None:
    """Verify migrations run idempotently and all expected tables exist."""
    # Run migrations
    run_migrations()

    expected_tables = {
        "schema_migrations",
        "pipeline_runs",
        "raw_events",
        "curated_changes",
        "quality_rules",
        "quality_results",
        "incidents",
        "quarantined_events",
        "archive_manifests",
        "consumer_checkpoints",
    }

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';
            """)
            existing_tables = {row["table_name"] for row in cur.fetchall()}

    missing_tables = expected_tables - existing_tables
    assert not missing_tables, f"Missing expected tables in public schema: {missing_tables}"


def test_pipeline_runs_crud() -> None:
    """Verify basic CRUD on pipeline_runs table within transaction."""
    run_id = f"test-run-{uuid.uuid4()}"
    now = datetime.now(UTC)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, pipeline_name, start_time, status,
                    records_read, records_written, records_rejected
                ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (run_id, "smoke-test-pipeline", now, "RUNNING", 100, 95, 5),
            )

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pipeline_runs WHERE run_id = %s;", (run_id,))
            row = cur.fetchone()
            assert row is not None
            assert row["pipeline_name"] == "smoke-test-pipeline"
            assert row["records_read"] == 100
            assert row["records_written"] == 95
            assert row["records_rejected"] == 5

            # Cleanup test run
            cur.execute("DELETE FROM pipeline_runs WHERE run_id = %s;", (run_id,))
