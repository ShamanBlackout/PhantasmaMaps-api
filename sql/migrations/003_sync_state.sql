CREATE TABLE IF NOT EXISTS sync_state (
    token_symbol TEXT PRIMARY KEY,
    last_block_height BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB
);
