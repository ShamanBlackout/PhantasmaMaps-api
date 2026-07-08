-- ============================================================
-- 014_transaction_event_kind.sql
-- Add explicit ledger metadata for token burns and mints
-- ============================================================

ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS event_kind TEXT NOT NULL DEFAULT 'transfer';

ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS related_address TEXT;

UPDATE transactions
   SET event_kind = COALESCE(event_kind, 'transfer');

CREATE INDEX IF NOT EXISTS idx_transactions_event_kind
    ON transactions(event_kind);

CREATE INDEX IF NOT EXISTS idx_transactions_related_address
    ON transactions(related_address);
