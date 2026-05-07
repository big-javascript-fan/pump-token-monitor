-- Time-series USD prices per mint (e.g. 30-minute cron inserts via insert_price_point).
-- Idempotent: safe if init_db already created this table.

CREATE TABLE IF NOT EXISTS token_prices (
    mint text NOT NULL,
    ts timestamptz NOT NULL,
    price_usd double precision NOT NULL,
    PRIMARY KEY (mint, ts)
);

CREATE INDEX IF NOT EXISTS token_prices_mint_ts_idx
    ON token_prices (mint, ts DESC);
