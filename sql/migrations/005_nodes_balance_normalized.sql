ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS balance_normalized NUMERIC;

CREATE INDEX IF NOT EXISTS idx_nodes_balance_normalized
    ON nodes(token_symbol, balance_normalized DESC);
