-- Recovery script: Requeue the commit-gap block (last_committed + 1) 
-- if it is stuck in failed or stale claimed state.
-- 
-- Use when committedThrough appears frozen. Run this, then restart workers.
-- Example: psql "$DATABASE_URL" -f sql/maintenance/005_recover_commit_gap.sql

WITH current_state AS (
  SELECT COALESCE(
    (SELECT last_block_height FROM sync_state WHERE token_symbol = '__chain__'),
    0::bigint
  ) AS last_height
),
gap_claim AS (
  SELECT block_height, status, claimed_at
  FROM block_sync_claims, current_state
  WHERE block_height = current_state.last_height + 1
  LIMIT 1
),
updated AS (
  UPDATE block_sync_claims claims
  SET status = 'pending',
      claimed_by = NULL,
      claimed_at = NULL,
      updated_at = NOW(),
      attempt_count = CASE WHEN claims.status = 'failed' THEN 0 ELSE claims.attempt_count END,
      error = COALESCE(claims.error, 'commit gap claim recovered')
  FROM gap_claim
  WHERE claims.block_height = gap_claim.block_height
    AND (
      gap_claim.status = 'failed'
      OR (gap_claim.status = 'claimed' AND gap_claim.claimed_at < NOW() - INTERVAL '60 seconds')
    )
  RETURNING claims.block_height
)
SELECT 'Recovered block: ' || block_height AS result FROM updated
UNION ALL
SELECT 'No gap block found or already pending' WHERE NOT EXISTS (SELECT 1 FROM updated);
