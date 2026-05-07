-- Jupiter Tokens API v2 fields merged during price cron when `jupiter_api_key` is set.
-- Aligns with runtime `init_db` ALTERs in `db.rs`.
-- Ref: https://developers.jup.ag/docs/guides/how-to-get-token-information

ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS token_symbol text NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS token_icon_url text NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS token_decimals integer NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS jupiter_is_verified boolean NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS jupiter_mcap_usd double precision NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS jupiter_organic_score double precision NULL;
ALTER TABLE pump_tokens ADD COLUMN IF NOT EXISTS stats_24h_price_change_pct double precision NULL;

CREATE INDEX IF NOT EXISTS pump_tokens_jupiter_mcap_desc_idx
    ON pump_tokens (jupiter_mcap_usd DESC NULLS LAST);
