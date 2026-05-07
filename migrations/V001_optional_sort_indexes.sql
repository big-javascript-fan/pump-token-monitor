-- Optional performance indexes for token listing sorts.
-- Safe on every run: additive only (IF NOT EXISTS). Older deployments keep working without this file.

CREATE INDEX IF NOT EXISTS pump_tokens_first_seen_desc_idx ON pump_tokens (first_seen DESC);
CREATE INDEX IF NOT EXISTS pump_tokens_last_seen_desc_idx ON pump_tokens (last_seen DESC);
