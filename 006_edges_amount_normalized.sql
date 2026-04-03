ALTER TABLE edges
    ADD COLUMN IF NOT EXISTS amount_normalized NUMERIC;

CREATE INDEX IF NOT EXISTS idx_edges_token_amount_normalized
    ON edges(token_symbol, amount_normalized DESC);
