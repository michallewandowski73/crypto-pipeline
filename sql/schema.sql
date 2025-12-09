CREATE TABLE IF NOT EXISTS crypto_prices_raw (
    id             BIGSERIAL PRIMARY KEY,
    symbol         TEXT NOT NULL,
    currency       TEXT NOT NULL,
    price          NUMERIC(18, 8) NOT NULL,
    market_cap     NUMERIC(20, 2),
    volume_24h     NUMERIC(20, 2),
    fetched_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE VIEW v_crypto_prices_latest AS
SELECT DISTINCT ON (symbol)
    symbol,
    currency,
    price,
    market_cap,
    volume_24h,
    fetched_at_utc
FROM crypto_prices_raw
ORDER BY symbol, fetched_at_utc DESC;
