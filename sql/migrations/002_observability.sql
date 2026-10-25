-- Alert episodes retain labels and source start time; resolved episodes are terminal.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS alert_labels JSONB NOT NULL DEFAULT '{}';
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS alert_starts_at TIMESTAMPTZ;
ALTER TABLE archive_manifests ADD COLUMN IF NOT EXISTS integrity_checked_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_archive_integrity_checked ON archive_manifests (integrity_checked_at);
