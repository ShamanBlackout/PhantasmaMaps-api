ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS amount_normalized NUMERIC;

CREATE INDEX IF NOT EXISTS idx_tx_token_amount_normalized
    ON transactions(token_symbol, amount_normalized DESC);
