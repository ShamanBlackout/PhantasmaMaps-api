-- ============================================================
-- 013_query_cache.sql
-- Persistent query-result cache with TTL expiry
-- ============================================================

CREATE TABLE IF NOT EXISTS api_query_cache (
    cache_key TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_query_cache_expires_at
    ON api_query_cache(expires_at);

CREATE OR REPLACE FUNCTION touch_api_query_cache_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_api_query_cache_set_updated_at ON api_query_cache;

CREATE TRIGGER trg_api_query_cache_set_updated_at
BEFORE UPDATE ON api_query_cache
FOR EACH ROW
EXECUTE FUNCTION touch_api_query_cache_updated_at();
