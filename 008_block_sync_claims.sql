CREATE TABLE IF NOT EXISTS block_sync_claims (
    block_height BIGINT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    claimed_by TEXT,
    claimed_at TIMESTAMP,
    completed_at TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT block_sync_claims_status_check
        CHECK (status IN ('pending', 'claimed', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_block_sync_claims_status_height
    ON block_sync_claims(status, block_height);

CREATE INDEX IF NOT EXISTS idx_block_sync_claims_claimed_at
    ON block_sync_claims(claimed_at);