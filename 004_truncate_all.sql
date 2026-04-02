-- ============================================================
-- 004_truncate_all.sql
-- Clears all runtime data while keeping schema, indexes, and constraints.
-- ============================================================

BEGIN;

TRUNCATE TABLE
  edges,
  nodes,
  transactions,
  sync_state,
  graph_versions
RESTART IDENTITY CASCADE;

COMMIT;
