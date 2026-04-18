-- ============================================================
-- 009_performance_indexes.sql
-- Optimization: Add composite indexes for common query patterns
-- Expected improvement: 20-30% faster for getTransactionsPage,
--                       getAddressSubgraph, and activity queries
-- ============================================================

-- Composite indexes for transaction filtering patterns
CREATE INDEX IF NOT EXISTS idx_tx_token_from_to
    ON transactions(token_symbol, from_address, to_address);

CREATE INDEX IF NOT EXISTS idx_tx_token_to_from
    ON transactions(token_symbol, to_address, from_address);

-- Composite index for token + timestamp range queries (activity)
CREATE INDEX IF NOT EXISTS idx_tx_token_timestamp
    ON transactions(token_symbol, timestamp DESC);

-- Partial index for fungible token transactions (more selective)
CREATE INDEX IF NOT EXISTS idx_tx_token_fungible_timestamp
    ON transactions(token_symbol, timestamp DESC)
    WHERE amount_normalized > 0;

-- Composite index for edge lookups by token and addresses
CREATE INDEX IF NOT EXISTS idx_edges_token_from_to
    ON edges(token_symbol, from_address, to_address);

CREATE INDEX IF NOT EXISTS idx_edges_token_to_from
    ON edges(token_symbol, to_address, from_address);

-- Composite index for node lookups by token (improve graph assembly)
CREATE INDEX IF NOT EXISTS idx_nodes_token_address
    ON nodes(token_symbol, address)
    INCLUDE (balance, balance_normalized, label, metadata);

-- Index to support address searches in different directions
CREATE INDEX IF NOT EXISTS idx_tx_from_token_timestamp
    ON transactions(from_address, token_symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_tx_to_token_timestamp
    ON transactions(to_address, token_symbol, timestamp DESC);

-- ============================================================
-- ANALYSIS: These indexes target the most expensive operations:
--
-- 1. getTransactionsPage filtering
--    - NOW: Sequential scan on transactions table with WHERE clause
--    - WITH: BTrees on (token_symbol, timestamp) for quick range scans
--
-- 2. getAddressSubgraph recursive CTE
--    - NOW: Sequential join on edges for each depth level
--    - WITH: Composite indexes on (token_symbol, from_address, to_address)
--
-- 3. getAddressActivity (activity sparkline)
--    - NOW: Scans all transactions with filters
--    - WITH: (token_symbol, timestamp DESC) index + partial filter
--
-- 4. Graph assembly (getFullTokenGraph)
--    - NOW: Separate queries, node lookup is sequential
--    - WITH: (token_symbol, address) composite with INCLUDE reduces I/O
--
-- ============================================================
-- END OF MIGRATION
-- ============================================================
