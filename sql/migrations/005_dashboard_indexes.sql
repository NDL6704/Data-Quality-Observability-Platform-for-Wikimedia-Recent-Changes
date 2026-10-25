-- Bound dashboard time-window and exact-event inspection queries as audit history grows.
CREATE INDEX IF NOT EXISTS idx_quality_results_evaluated_at ON quality_results(evaluated_at);
CREATE INDEX IF NOT EXISTS idx_quality_results_event_eval
ON quality_results ((failure_details->0->>'event_key'), evaluated_at DESC);
