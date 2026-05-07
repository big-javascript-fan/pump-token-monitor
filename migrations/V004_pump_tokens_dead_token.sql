-- Dead-token detection: excluded from GET /tokens pagination, price cron, and live quote updates.
-- Aligns with runtime `init_db` ALTERs on pump_tokens.

ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS dead_token boolean NOT NULL DEFAULT false;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS dead_marked_at timestamptz NULL;

-- Speeds up list_mints / scans that skip dead rows (partial index).
CREATE INDEX IF NOT EXISTS pump_tokens_alive_last_seen_idx
    ON pump_tokens (last_seen DESC)
    WHERE COALESCE(dead_token, false) = false;
