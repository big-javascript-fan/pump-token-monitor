-- USD quote columns on pump_tokens (cron-updated prices + first-seen baseline).
-- Safe for legacy DBs that only had mint/name/slots; matches runtime `init_db` ALTERs.

ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS first_price_usd double precision NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS first_price_at timestamptz NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS price_usd double precision NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS price_updated_at timestamptz NULL;
