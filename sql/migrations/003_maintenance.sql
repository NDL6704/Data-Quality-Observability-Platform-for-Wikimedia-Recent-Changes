-- Source JSONL manifests remain immutable evidence; derived Parquet has its own manifest.
CREATE TABLE IF NOT EXISTS archive_compactions (
    source_manifest_id VARCHAR(64) PRIMARY KEY REFERENCES archive_manifests(manifest_id),
    parquet_manifest_id VARCHAR(64) NOT NULL UNIQUE REFERENCES archive_manifests(manifest_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS reconciliation_reports (
    report_id VARCHAR(64) PRIMARY KEY,
    manifest_id VARCHAR(64) NOT NULL REFERENCES archive_manifests(manifest_id),
    report JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS daily_quality_summary (
    summary_day DATE NOT NULL,
    wiki TEXT NOT NULL,
    rule_id VARCHAR(64) NOT NULL,
    rule_version VARCHAR(32) NOT NULL,
    rows_checked BIGINT NOT NULL,
    rows_failed BIGINT NOT NULL,
    PRIMARY KEY(summary_day,wiki,rule_id,rule_version)
);
