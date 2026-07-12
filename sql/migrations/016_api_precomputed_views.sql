-- ============================================================
-- 016_api_precomputed_views.sql
-- Persistent precomputed payloads for common API views
-- ============================================================

CREATE TABLE IF NOT EXISTS api_precomputed_views (
    view_key TEXT PRIMARY KEY,
    token_symbol TEXT,
    payload JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_precomputed_views_token_symbol
    ON api_precomputed_views(token_symbol, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_precomputed_views_expires_at
    ON api_precomputed_views(expires_at);
