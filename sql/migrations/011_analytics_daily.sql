-- ============================================================
-- 011_analytics_daily.sql
-- Daily analytics rollups for tokens and wallets
-- ============================================================

CREATE TABLE IF NOT EXISTS token_daily_metrics (
    token_symbol TEXT NOT NULL,
    bucket_date DATE NOT NULL,
    holder_count INTEGER NOT NULL DEFAULT 0,
    new_holder_count INTEGER NOT NULL DEFAULT 0,
    lost_holder_count INTEGER NOT NULL DEFAULT 0,
    active_wallet_count INTEGER NOT NULL DEFAULT 0,
    transaction_count INTEGER NOT NULL DEFAULT 0,
    transfer_volume NUMERIC NOT NULL DEFAULT 0,
    current_supply NUMERIC NOT NULL DEFAULT 0,
    top10_share NUMERIC NOT NULL DEFAULT 0,
    top50_share NUMERIC NOT NULL DEFAULT 0,
    top_wallet_share NUMERIC NOT NULL DEFAULT 0,
    gini_coefficient NUMERIC NOT NULL DEFAULT 0,
    median_transfer_amount NUMERIC NOT NULL DEFAULT 0,
    avg_transfer_amount NUMERIC NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_symbol, bucket_date)
);

CREATE INDEX IF NOT EXISTS idx_token_daily_metrics_bucket_date
    ON token_daily_metrics(bucket_date);

CREATE TABLE IF NOT EXISTS wallet_daily_balances (
    token_symbol TEXT NOT NULL,
    address TEXT NOT NULL,
    bucket_date DATE NOT NULL,
    balance NUMERIC NOT NULL DEFAULT 0,
    balance_normalized NUMERIC NOT NULL DEFAULT 0,
    share_of_supply NUMERIC NOT NULL DEFAULT 0,
    wallet_type TEXT,
    first_seen_at TIMESTAMP,
    last_seen_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_symbol, address, bucket_date)
);

CREATE INDEX IF NOT EXISTS idx_wallet_daily_balances_token_date
    ON wallet_daily_balances(token_symbol, bucket_date);

CREATE INDEX IF NOT EXISTS idx_wallet_daily_balances_token_address
    ON wallet_daily_balances(token_symbol, address);

CREATE TABLE IF NOT EXISTS wallet_daily_activity (
    token_symbol TEXT NOT NULL,
    address TEXT NOT NULL,
    bucket_date DATE NOT NULL,
    incoming_tx_count INTEGER NOT NULL DEFAULT 0,
    outgoing_tx_count INTEGER NOT NULL DEFAULT 0,
    incoming_volume NUMERIC NOT NULL DEFAULT 0,
    outgoing_volume NUMERIC NOT NULL DEFAULT 0,
    net_flow NUMERIC NOT NULL DEFAULT 0,
    counterparty_count INTEGER NOT NULL DEFAULT 0,
    last_tx_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_symbol, address, bucket_date)
);

CREATE INDEX IF NOT EXISTS idx_wallet_daily_activity_token_date
    ON wallet_daily_activity(token_symbol, bucket_date);

CREATE INDEX IF NOT EXISTS idx_wallet_daily_activity_token_address
    ON wallet_daily_activity(token_symbol, address);
