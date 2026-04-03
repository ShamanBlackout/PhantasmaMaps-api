CREATE TABLE IF NOT EXISTS token_metadata (
    token_symbol TEXT PRIMARY KEY,
    name TEXT,
    decimals INTEGER NOT NULL DEFAULT 0,
    current_supply_raw TEXT NOT NULL,
    current_supply_normalized NUMERIC,
    max_supply_raw TEXT,
    max_supply_normalized NUMERIC,
    flags JSONB,
    metadata JSONB,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_token_metadata_updated_at
    ON token_metadata(updated_at DESC);
