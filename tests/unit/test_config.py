"""Unit tests for configuration management."""

import os
from unittest import mock

from src.common.config import Settings


def test_default_settings() -> None:
    """Verify default configuration values."""
    settings = Settings()
    assert settings.environment == "local"
    assert settings.kafka_topic_raw == "rc.raw.v1"
    assert settings.kafka_topic_validated == "rc.validated.v1"
    assert settings.kafka_topic_dlq == "rc.dlq.v1"
    assert settings.postgres_db == "wikimedia_dq"
    assert "postgresql://" in settings.postgres_dsn


def test_settings_env_override() -> None:
    """Verify environment variables override default settings."""
    with mock.patch.dict(os.environ, {
        "ENVIRONMENT": "production",
        "POSTGRES_DB": "wikimedia_prod",
        "POSTGRES_PORT": "5433",
        "KAFKA_TOPIC_RAW": "custom.raw.v1",
    }):
        settings = Settings()
        assert settings.environment == "production"
        assert settings.postgres_db == "wikimedia_prod"
        assert settings.postgres_port == 5433
        assert settings.kafka_topic_raw == "custom.raw.v1"
        assert "5433/wikimedia_prod" in settings.postgres_dsn
