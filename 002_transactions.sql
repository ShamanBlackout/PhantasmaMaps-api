-- ============================================================
-- 002_transactions.sql
-- Full transaction history per token + address
-- ============================================================

CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,

    -- Blockchain identifiers
    tx_hash TEXT NOT NULL,
    event_index INTEGER NOT NULL DEFAULT 0,
    token_symbol TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,

    -- Participants
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,

    -- Value transferred
    amount NUMERIC,

    -- Raw RPC payload or extra fields
    metadata JSONB
);

ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS event_index INTEGER NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_hash_event
    ON transactions(tx_hash, event_index);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_tx_token
    ON transactions(token_symbol);

CREATE INDEX IF NOT EXISTS idx_tx_block
    ON transactions(block_height);

CREATE INDEX IF NOT EXISTS idx_tx_from
    ON transactions(from_address);

CREATE INDEX IF NOT EXISTS idx_tx_to
    ON transactions(to_address);

CREATE INDEX IF NOT EXISTS idx_tx_timestamp
    ON transactions(timestamp);
