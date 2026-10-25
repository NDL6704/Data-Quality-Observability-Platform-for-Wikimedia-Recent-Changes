"""Centralized configuration management for the Wikimedia Data Quality Platform."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("quality_max_future_skew_seconds", mode="before")
    @classmethod
    def parse_future_skew_seconds(cls, value: object) -> object:
        """Parse environment integer text while rejecting bools and fractional input."""
        if isinstance(value, str) and value.isascii() and value.isdecimal():
            return int(value)
        return value

    # General Environment
    environment: str = Field(default="local", alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Wikimedia SSE Stream
    wikimedia_stream_url: str = Field(
        default="https://stream.wikimedia.org/v2/stream/recentchange",
        alias="WIKIMEDIA_STREAM_URL",
    )
    wikimedia_user_agent: str = Field(
        default="WikimediaDataQualityPlatform/0.1.0 (local-dev; data-platform-portfolio)",
        alias="WIKIMEDIA_USER_AGENT",
    )
    sse_connect_timeout_sec: int = Field(default=15, alias="SSE_CONNECT_TIMEOUT_SEC")
    sse_read_timeout_sec: int = Field(default=60, alias="SSE_READ_TIMEOUT_SEC")
    sse_max_backoff_sec: int = Field(default=60, alias="SSE_MAX_BACKOFF_SEC")

    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_topic_raw: str = Field(default="rc.raw.v1", alias="KAFKA_TOPIC_RAW")
    kafka_topic_validated: str = Field(default="rc.validated.v1", alias="KAFKA_TOPIC_VALIDATED")
    kafka_topic_dlq: str = Field(default="rc.dlq.v1", alias="KAFKA_TOPIC_DLQ")
    kafka_consumer_group_quality: str = Field(
        default="rc-quality-consumer-v1",
        alias="KAFKA_CONSUMER_GROUP_QUALITY",
    )
    kafka_auto_offset_reset: str = Field(default="earliest", alias="KAFKA_AUTO_OFFSET_RESET")

    quality_max_retries: int = Field(default=3, ge=0, le=10, alias="QUALITY_MAX_RETRIES")
    quality_max_future_skew_seconds: int = Field(
        default=2, ge=0, le=5, strict=True, alias="QUALITY_MAX_FUTURE_SKEW_SECONDS"
    )
    quality_retry_backoff_sec: float = Field(
        default=1.0, ge=0, le=5, alias="QUALITY_RETRY_BACKOFF_SEC"
    )
    quality_delivery_timeout_sec: int = Field(
        default=10, ge=1, le=30, alias="QUALITY_DELIVERY_TIMEOUT_SEC"
    )

    # PostgreSQL
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="wikimedia_dq", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")
    postgres_pool_min: int = Field(default=2, alias="POSTGRES_POOL_MIN")
    postgres_pool_max: int = Field(default=10, alias="POSTGRES_POOL_MAX")

    # Raw Archive
    raw_archive_dir: Path = Field(default=Path("./data/raw_archive"), alias="RAW_ARCHIVE_DIR")
    raw_archive_batch_size: int = Field(default=500, alias="RAW_ARCHIVE_BATCH_SIZE")
    raw_archive_flush_interval_sec: int = Field(default=30, alias="RAW_ARCHIVE_FLUSH_INTERVAL_SEC")

    # Observability
    prometheus_port: int = Field(default=8000, ge=1, le=65535, alias="PROMETHEUS_PORT")
    metrics_enabled: bool = Field(default=True, alias="METRICS_ENABLED")
    prometheus_url: str = Field(default="http://127.0.0.1:9090", alias="PROMETHEUS_URL")
    consumer_metrics_port: int = Field(default=8001, ge=1, le=65535, alias="CONSUMER_METRICS_PORT")
    observability_port: int = Field(default=8002, ge=1, le=65535, alias="OBSERVABILITY_PORT")
    observability_poll_sec: float = Field(default=5, ge=1, le=300, alias="OBSERVABILITY_POLL_SEC")
    observability_webhook_token: str = Field(
        default="local-observability", min_length=16, alias="OBSERVABILITY_WEBHOOK_TOKEN"
    )

    @property
    def postgres_dsn(self) -> str:
        """Constructs PostgreSQL connection string."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Provides a cached singleton instance of Settings."""
    return Settings()
