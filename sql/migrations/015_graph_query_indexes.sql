-- ============================================================
-- 015_graph_query_indexes.sql
-- Additional composite indexes for graph/read hot paths
-- ============================================================

-- Address subgraph depth-1 and recursive expansion helpers
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_edges_token_from_amount_id
    ON edges(token_symbol, from_address, amount_normalized DESC, id ASC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_edges_token_to_amount_id
    ON edges(token_symbol, to_address, amount_normalized DESC, id ASC);

-- Token graph scans with stable ordering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_edges_token_id
    ON edges(token_symbol, id ASC);

-- Address connection lookups in descending volume order
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_connections_token_address_volume
    ON address_connections(token_symbol, address, total_volume DESC, counterparty);

-- Transactions list endpoint and activity filters
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_token_block_hash
    ON transactions(token_symbol, block_height DESC, tx_hash ASC, event_index ASC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_token_from_time
    ON transactions(token_symbol, from_address, timestamp DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_token_to_time
    ON transactions(token_symbol, to_address, timestamp DESC);

-- Analytics reads and precompute support
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wallet_daily_balances_token_date_balance
    ON wallet_daily_balances(token_symbol, bucket_date DESC, balance_normalized DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wallet_daily_activity_token_date_address
    ON wallet_daily_activity(token_symbol, bucket_date DESC, address);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_token_daily_metrics_token_date
    ON token_daily_metrics(token_symbol, bucket_date DESC);
