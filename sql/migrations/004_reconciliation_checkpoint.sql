-- Distinguish immutable report content from when that state was last observed.
ALTER TABLE reconciliation_reports ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ
    NOT NULL DEFAULT clock_timestamp();
