-- Migration 001: Initial Schema
-- Description: Core tables for Wikimedia Data Quality & Observability Platform

-- 0. Schema Migration Tracker (ensures idempotent migration execution)
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(64) PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 1. Pipeline Runs (Operational Metadata)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    pipeline_name VARCHAR(64) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status VARCHAR(32) NOT NULL, -- RUNNING, COMPLETED, FAILED
    records_read BIGINT NOT NULL DEFAULT 0,
    records_written BIGINT NOT NULL DEFAULT 0,
    records_rejected BIGINT NOT NULL DEFAULT 0,
    error_information TEXT,
    code_version VARCHAR(64),
    config_version VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_start
    ON pipeline_runs (pipeline_name, start_time DESC);

-- 2. Raw Events (Operational Raw Ingestion Ledger)
CREATE TABLE IF NOT EXISTS raw_events (
    id BIGSERIAL PRIMARY KEY,
    event_key TEXT NOT NULL,
    source_event_id TEXT,
    received_at TIMESTAMPTZ NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    schema_version VARCHAR(32) NOT NULL,
    ingest_run_id VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_events_key_hash UNIQUE (event_key, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_received_at ON raw_events (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_key ON raw_events (event_key);

-- 3. Curated Changes (Analytical Source of Truth - Deduplicated, Cleaned)
CREATE TABLE IF NOT EXISTS curated_changes (
    event_key TEXT PRIMARY KEY,
    source_event_id TEXT,
    event_timestamp TIMESTAMPTZ,
    received_timestamp TIMESTAMPTZ NOT NULL,
    wiki TEXT NOT NULL,
    server_name TEXT,
    title TEXT,
    user_id BIGINT,
    user_text TEXT,
    change_type TEXT,
    namespace INT,
    is_bot BOOLEAN NOT NULL DEFAULT FALSE,
    is_minor BOOLEAN NOT NULL DEFAULT FALSE,
    old_length INT,
    new_length INT,
    comment TEXT,
    raw_payload JSONB,
    processed_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_wiki_time ON curated_changes (wiki, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_curated_change_type ON curated_changes (change_type);
CREATE INDEX IF NOT EXISTS idx_curated_processed_time ON curated_changes (processed_timestamp DESC);

-- 4. Quality Rules (Configurable & Versioned Rule Definitions)
CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id VARCHAR(64) NOT NULL,
    version VARCHAR(32) NOT NULL,
    name VARCHAR(128) NOT NULL,
    dataset VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL, -- Critical, High, Medium, Info
    description TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    rule_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rule_id, version)
);

-- 5. Quality Results (Time-windowed Quality Rule Execution Results)
CREATE TABLE IF NOT EXISTS quality_results (
    id BIGSERIAL PRIMARY KEY,
    rule_id VARCHAR(64) NOT NULL,
    rule_version VARCHAR(32) NOT NULL,
    time_window_start TIMESTAMPTZ NOT NULL,
    time_window_end TIMESTAMPTZ NOT NULL,
    rows_checked BIGINT NOT NULL DEFAULT 0,
    rows_failed BIGINT NOT NULL DEFAULT 0,
    failure_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    failure_details JSONB NOT NULL DEFAULT '[]'::jsonb,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quality_results_rule_eval
    ON quality_results (rule_id, evaluated_at DESC);

-- 6. Incidents (Data Observability & Incident Lifecycle Tracking)
CREATE TABLE IF NOT EXISTS incidents (
    incident_id VARCHAR(64) PRIMARY KEY,
    incident_type VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL, -- Critical, High, Medium, Low
    component VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL, -- OPEN, INVESTIGATING, RESOLVED
    detected_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    occurrence_count INT NOT NULL DEFAULT 1,
    root_cause TEXT,
    resolution_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_incidents_status_detected
    ON incidents (status, detected_at DESC);

-- 7. Quarantined Events (Dead-Letter & Data Quality Failures Store)
CREATE TABLE IF NOT EXISTS quarantined_events (
    id BIGSERIAL PRIMARY KEY,
    event_key TEXT,
    raw_payload JSONB NOT NULL,
    failure_codes TEXT[] NOT NULL,
    failure_details JSONB NOT NULL,
    replay_status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, REPLAYED, DISCARDED
    replay_run_id VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_quarantined_status ON quarantined_events (replay_status);
CREATE INDEX IF NOT EXISTS idx_quarantined_time ON quarantined_events (quarantined_at DESC);

-- 8. Archive Manifests (Audit Trail for On-Disk Raw Archive Batches)
CREATE TABLE IF NOT EXISTS archive_manifests (
    manifest_id VARCHAR(64) PRIMARY KEY,
    file_path TEXT NOT NULL,
    format VARCHAR(16) NOT NULL, -- jsonl, parquet
    record_count BIGINT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    checksum_sha256 VARCHAR(64) NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'COMMITTED', -- COMMITTED, COMPACTED, CORRUPTED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 9. Consumer Checkpoints (Low-level offset audit / reconciliation support)
CREATE TABLE IF NOT EXISTS consumer_checkpoints (
    consumer_group VARCHAR(64) NOT NULL,
    topic VARCHAR(64) NOT NULL,
    partition INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition)
);
