"""Database connection management and query execution helpers using psycopg 3."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.common.config import get_settings
from src.common.logging import setup_logger

logger = setup_logger("wikimedia_dq.storage.db")


@contextmanager
def get_db_connection() -> Generator[psycopg.Connection[Any], None, None]:
    """Provides a transactional database connection context.

    Automatically rolls back on exception, and commits on clean exit.
    """
    settings = get_settings()
    conn = psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        row_factory=dict_row,
        connect_timeout=10,
    )
    try:
        with conn.transaction():
            yield conn
    except Exception as exc:
        logger.error(f"Database transaction failed: {exc}")
        raise
    finally:
        conn.close()


def check_db_health() -> bool:
    """Verifies that the PostgreSQL database is reachable and accepting queries."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS is_alive;")
                row = cur.fetchone()
                return bool(row and row.get("is_alive") == 1)
    except Exception as exc:
        logger.warning(f"Database health check failed: {exc}")
        return False
