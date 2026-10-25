"""Read durable lag/incident state and periodically verify registered archives."""

import os
import time
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, TopicPartition

from src.common.config import Settings
from src.ingestion.raw_archiver import RawArchiver
from src.observability.metrics import Metrics
from src.processing.consumer import build_consumer_config
from src.storage.db import get_db_connection


class StateCollector:
    """Background I/O never occurs inside a Prometheus scrape request."""

    def __init__(self, settings: Settings, metrics: Metrics) -> None:
        self.settings, self.metrics = settings, metrics
        config = build_consumer_config(settings)
        config["allow.auto.create.topics"] = False
        self.consumer = Consumer(config)  # Never subscribe, assign or commit.

    def poll_kafka(self) -> None:
        """Measure all configured raw partitions relative to the operational group."""
        topic = self.settings.kafka_topic_raw
        metadata = self.consumer.list_topics(topic, timeout=5).topics[topic]
        if metadata.error:
            raise RuntimeError(str(metadata.error))
        partitions = [TopicPartition(topic, p) for p in metadata.partitions]
        committed = self.consumer.committed(partitions, timeout=5)
        samples: list[tuple[int, int]] = []
        for part in committed:
            if part.error:
                raise RuntimeError(str(part.error))
            low, high = self.consumer.get_watermark_offsets(part, timeout=5, cached=False)
            if part.offset > high:
                raise RuntimeError("Committed offset exceeds high watermark")
            # New groups start at retained earliest, not offsets already removed by retention.
            samples.append((part.partition, high - max(low, part.offset)))
        self.metrics.lag.clear()
        for partition, lag in samples:
            self.metrics.lag.labels(topic, str(partition)).set(lag)

    def poll_database(self) -> None:
        """Refresh authoritative incident and persisted corruption gauges."""
        with get_db_connection() as conn:
            counts = conn.execute(
                """SELECT severity,count(*) AS n FROM incidents
                   WHERE status IN ('OPEN','INVESTIGATING') GROUP BY severity"""
            ).fetchall()
            corrupt = conn.execute(
                "SELECT count(*) AS n FROM archive_manifests WHERE status='CORRUPTED'"
            ).fetchone()
        by_severity = {row["severity"]: row["n"] for row in counts}
        for severity in ("CRITICAL", "HIGH", "MEDIUM", "INFO"):
            self.metrics.incidents.labels(severity).set(by_severity.get(severity, 0))
        assert corrupt is not None
        self.metrics.archive_corrupt.set(corrupt["n"])

    def check_archives(self, limit: int = 100) -> None:
        """Rotate oldest checks; inspect only paths within configured archive root.

        Synthetic Phase4 archives outside that root are not operational inputs.
        Persist CORRUPTED status so a process restart cannot hide the detector state.
        """
        root = self.settings.raw_archive_dir.resolve()
        if not 1 <= limit <= 1000:
            raise ValueError("Archive scan limit must be between 1 and 1000")
        try:
            relative = Path(os.path.relpath(root, Path.cwd())).as_posix()
        except ValueError:
            relative = root.as_posix()

        def prefix(value: str) -> str:
            return value.replace("!", "!!").replace("%", "!%").replace("_", "!_") + "/%"

        with get_db_connection() as conn:
            rows = conn.execute(
                """SELECT * FROM archive_manifests
                   WHERE file_path LIKE %s ESCAPE '!' OR file_path LIKE %s ESCAPE '!'
                   ORDER BY integrity_checked_at NULLS FIRST,manifest_id LIMIT %s""",
                (prefix(root.as_posix()), prefix(relative), limit),
            ).fetchall()
        for row in rows:
            path = Path(row["file_path"])
            in_scope = path.resolve().is_relative_to(root)
            if not in_scope:
                continue
            valid = True
            if in_scope:
                try:
                    valid = RawArchiver.verify_file_checksum(path, row["checksum_sha256"])
                    valid = valid and path.stat().st_size == row["file_size_bytes"]
                except OSError:
                    valid = False
            with get_db_connection() as conn:
                if in_scope:
                    compacted = conn.execute(
                        "SELECT 1 FROM archive_compactions WHERE source_manifest_id=%s",
                        (row["manifest_id"],),
                    ).fetchone() is not None
                    status = (
                        ("COMPACTED" if row["format"] == "parquet" or compacted else "COMMITTED")
                        if valid
                        else "CORRUPTED"
                    )
                    conn.execute(
                        """UPDATE archive_manifests SET status=%s,
                                   integrity_checked_at=clock_timestamp() WHERE manifest_id=%s
                                   AND checksum_sha256=%s AND file_path=%s""",
                        (status, row["manifest_id"], row["checksum_sha256"], row["file_path"]),
                    )
                    if not valid and row["status"] != "CORRUPTED":
                        self.metrics.archive_errors.inc()
                else:
                    conn.execute(
                        """UPDATE archive_manifests SET integrity_checked_at=clock_timestamp()
                           WHERE manifest_id=%s""",
                        (row["manifest_id"],),
                    )

    def poll(self) -> dict[str, str]:
        """Expose probe health explicitly; stale last-good gauges are not zeros."""
        errors: dict[str, str] = {}
        for name, operation in (
            ("kafka", self.poll_kafka),
            ("archive", self.check_archives),
            ("database", self.poll_database),
        ):
            try:
                operation()
            except Exception as exc:
                errors[name] = f"{type(exc).__name__}: {exc}"
                self.metrics.probe.labels(name).set(0)
                if isinstance(exc, psycopg.Error):
                    self.metrics.db_errors.labels("observability").inc()
            else:
                self.metrics.probe.labels(name).set(1)
        self.metrics.poll_timestamp.set(time.time())
        return errors

    def close(self) -> None:
        """Release observer resources; no offsets were changed."""
        self.consumer.close()
