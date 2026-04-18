-- ============================================================
-- 010_address_connections.sql
-- Create materialized connections table for fast address graph queries
-- This denormalized table aggregates the full transaction history per
-- address pair, eliminating the need for recursive CTEs and pagination
-- ============================================================

CREATE TABLE IF NOT EXISTS address_connections (
    token_symbol TEXT NOT NULL,
    address TEXT NOT NULL,
    counterparty TEXT NOT NULL,
    total_volume NUMERIC NOT NULL DEFAULT 0,
    transaction_count INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (token_symbol, address, counterparty)
);

-- Index for efficient lookups by address
CREATE INDEX IF NOT EXISTS idx_address_connections_token_address
    ON address_connections(token_symbol, address);

-- Index for reverse lookups (who connects to this address)
CREATE INDEX IF NOT EXISTS idx_address_connections_token_counterparty
    ON address_connections(token_symbol, counterparty);

-- Populate from existing edges table (one-time backfill).
-- Aggregate after UNION ALL so self-transfers do not produce duplicate keys.
WITH directional_rows AS (
    SELECT
        e.token_symbol,
        e.from_address AS address,
        e.to_address AS counterparty,
        e.amount_normalized AS total_volume,
        1 AS transaction_count
    FROM edges e
    UNION ALL
    SELECT
        e.token_symbol,
        e.to_address AS address,
        e.from_address AS counterparty,
        e.amount_normalized AS total_volume,
        1 AS transaction_count
    FROM edges e
),
aggregated_rows AS (
    SELECT
        token_symbol,
        address,
        counterparty,
        SUM(total_volume) AS total_volume,
        SUM(transaction_count) AS transaction_count
    FROM directional_rows
    GROUP BY token_symbol, address, counterparty
)
INSERT INTO address_connections (
    token_symbol,
    address,
    counterparty,
    total_volume,
    transaction_count
)
SELECT
    token_symbol,
    address,
    counterparty,
    total_volume,
    transaction_count
FROM aggregated_rows
ON CONFLICT (token_symbol, address, counterparty) DO UPDATE
SET total_volume = EXCLUDED.total_volume,
    transaction_count = EXCLUDED.transaction_count,
    last_updated = CURRENT_TIMESTAMP;
