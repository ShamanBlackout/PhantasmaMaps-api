-- ============================================================
--  PhantasmaMaps Multi‑Token Graph Database Schema
--  Supports unlimited tokens (SOUL, KCAL, NFTs, etc.)
--  Address + token_symbol = unique key
-- ============================================================

-- -----------------------------
-- 1. Graph Versions (optional)
-- -----------------------------
-- Allows storing multiple snapshots of the same token graph
-- Example: SOUL_v1, SOUL_v2, KCAL_2024, NFT_Collection_X

CREATE TABLE IF NOT EXISTS graph_versions (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- -----------------------------
-- 2. Nodes Table (multi‑token)
-- -----------------------------
-- Each address can appear once per token.
-- Example:
--   P2abc... + SOUL
--   P2abc... + KCAL
--   P2abc... + TEL

CREATE TABLE IF NOT EXISTS nodes (
    address TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    balance NUMERIC,
    label TEXT,
    metadata JSONB,
    version_id INT REFERENCES graph_versions(id),

    PRIMARY KEY (address, token_symbol)
);

-- Index for fast token‑specific queries
CREATE INDEX IF NOT EXISTS idx_nodes_token
    ON nodes(token_symbol);

-- Index for fast version filtering
CREATE INDEX IF NOT EXISTS idx_nodes_version
    ON nodes(version_id);

-- -----------------------------
-- 3. Edges Table (multi‑token)
-- -----------------------------
-- Represents directional relationships between addresses
-- within a specific token graph.

CREATE TABLE IF NOT EXISTS edges (
    id BIGSERIAL PRIMARY KEY,
    token_symbol TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    amount NUMERIC,
    tx_hash TEXT NOT NULL,
    event_index INTEGER NOT NULL DEFAULT 0,
    metadata JSONB,
    version_id INT REFERENCES graph_versions(id),

    FOREIGN KEY (from_address, token_symbol)
        REFERENCES nodes(address, token_symbol),

    FOREIGN KEY (to_address, token_symbol)
        REFERENCES nodes(address, token_symbol)
);

ALTER TABLE edges
    ADD COLUMN IF NOT EXISTS tx_hash TEXT;

ALTER TABLE edges
    ADD COLUMN IF NOT EXISTS event_index INTEGER NOT NULL DEFAULT 0;

-- High‑performance indexes for graph traversal
CREATE INDEX IF NOT EXISTS idx_edges_token
    ON edges(token_symbol);

CREATE INDEX IF NOT EXISTS idx_edges_from
    ON edges(from_address);

CREATE INDEX IF NOT EXISTS idx_edges_to
    ON edges(to_address);

CREATE INDEX IF NOT EXISTS idx_edges_version
    ON edges(version_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_tx_event
    ON edges(tx_hash, event_index);

-- ============================================================
-- END OF MIGRATION
-- ============================================================
