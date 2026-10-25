"""Idempotent SQL migration runner for PostgreSQL schema management."""

import argparse
import sys
from pathlib import Path

from src.common.logging import setup_logger
from src.storage.db import get_db_connection

logger = setup_logger("wikimedia_dq.migration_runner")


def ensure_migration_tracker() -> None:
    """Ensures the schema_migrations ledger table exists."""
    ddl = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version VARCHAR(64) PRIMARY KEY,
        description TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def get_applied_migrations() -> set[str]:
    """Retrieves all previously applied migration versions."""
    ensure_migration_tracker()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_migrations;")
            rows = cur.fetchall()
            return {row["version"] for row in rows}


def run_migrations(migrations_dir: Path = Path("sql/migrations")) -> list[str]:
    """Applies all pending SQL migrations idempotently in alphabetical order."""
    if not migrations_dir.exists():
        logger.warning(f"Migrations directory does not exist: {migrations_dir}")
        return []

    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        logger.info(f"No migration files found in {migrations_dir}")
        return []

    applied = get_applied_migrations()
    applied_in_this_run: list[str] = []

    for file_path in migration_files:
        version = file_path.stem
        if version in applied:
            logger.info(f"Migration already applied: {version} (skipping)")
            continue

        logger.info(f"Applying migration: {version} from {file_path.name}...")
        sql_content = file_path.read_text(encoding="utf-8")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Execute migration statements
                cur.execute(sql_content)
                # Record migration version in ledger
                cur.execute(
                    """
                    INSERT INTO schema_migrations (version, description)
                    VALUES (%s, %s);
                    """,
                    (version, f"Applied from {file_path.name}"),
                )

        applied_in_this_run.append(version)
        logger.info(f"Successfully applied migration: {version}")

    logger.info(f"Migration run finished. Newly applied migrations: {len(applied_in_this_run)}")
    return applied_in_this_run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run database schema migrations")
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path("sql/migrations"),
        help="Path to directory containing SQL migrations",
    )
    args = parser.parse_args()

    try:
        run_migrations(args.dir)
    except Exception as exc:
        logger.error(f"Migration failed with error: {exc}")
        sys.exit(1)
