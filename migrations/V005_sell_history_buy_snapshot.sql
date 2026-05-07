-- Buy price & decimals snapshot at sell time (sell history P/L vs prices).
-- Aligns with runtime `init_trade_tables` ALTERs on sell_history.

ALTER TABLE sell_history ADD COLUMN IF NOT EXISTS buy_price_usd double precision NOT NULL DEFAULT 0;
ALTER TABLE sell_history ADD COLUMN IF NOT EXISTS token_decimals smallint NOT NULL DEFAULT 9;
