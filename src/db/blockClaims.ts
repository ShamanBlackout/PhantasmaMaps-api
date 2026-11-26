import { CHAIN_SYNC_TOKEN } from "../phantasma.types";
import { syncConfig } from "../phantasma.config";
import { databasePool } from "./connection";
import { updateChainSyncHeight } from "./ingestion";

export async function getChainSyncHeight(): Promise<number | null> {
  const result = await databasePool.query(
    `SELECT last_block_height FROM sync_state WHERE token_symbol = $1`,
    [CHAIN_SYNC_TOKEN],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return Number(result.rows[0].last_block_height);
}

export async function seedBlockSyncClaims(
  startHeight: number,
  endHeight: number,
): Promise<number> {
  if (startHeight > endHeight) {
    return 0;
  }

  const result = await databasePool.query<{ count: string }>(
    `WITH inserted AS (
       INSERT INTO block_sync_claims (block_height)
       SELECT generate_series($1::bigint, $2::bigint)
       ON CONFLICT (block_height) DO NOTHING
       RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM inserted`,
    [startHeight, endHeight],
  );

  return Number(result.rows[0]?.count ?? 0);
}

export async function resetStaleBlockSyncClaims(
  staleAfterSeconds: number,
): Promise<number> {
  if (staleAfterSeconds <= 0) {
    return 0;
  }

  const result = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE block_sync_claims
          SET status = 'pending',
              claimed_by = NULL,
              claimed_at = NULL,
              updated_at = NOW(),
              error = COALESCE(error, 'stale claim reset')
        WHERE status = 'claimed'
          AND claimed_at < NOW() - make_interval(secs => $1)
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
    [staleAfterSeconds],
  );

  return Number(result.rows[0]?.count ?? 0);
}

export async function claimNextBlockHeight(
  workerId: string,
  maxAttempts: number,
  retryBaseDelaySeconds: number,
  retryMaxDelaySeconds: number,
  staleAfterSeconds: number,
): Promise<number | null> {
  const result = await databasePool.query<{ block_height: string }>(
    `WITH candidate AS (
       SELECT block_height
         FROM block_sync_claims
        WHERE (
             status = 'claimed'
             AND claimed_at < NOW() - make_interval(secs => $5)
           )
           OR status = 'pending'
           OR (
             status = 'failed'
             AND attempt_count < $2
             AND updated_at <= NOW() - make_interval(
               secs => LEAST(
                 $4::double precision,
                 $3::double precision * POWER(2::double precision, GREATEST(attempt_count - 1, 0))
               )::int
             )
           )
        ORDER BY block_height ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
     )
     UPDATE block_sync_claims claims
        SET status = 'claimed',
            claimed_by = $1,
            claimed_at = NOW(),
            updated_at = NOW(),
            error = NULL,
            attempt_count = claims.attempt_count + 1
       FROM candidate
      WHERE claims.block_height = candidate.block_height
      RETURNING claims.block_height::text AS block_height`,
    [
      workerId,
      Math.max(1, Math.floor(maxAttempts)),
      Math.max(1, Math.floor(retryBaseDelaySeconds)),
      Math.max(1, Math.floor(retryMaxDelaySeconds)),
      Math.max(1, Math.floor(staleAfterSeconds)),
    ],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return Number(result.rows[0].block_height);
}

export async function getBlockSyncClaimWaitState(
  startHeight: number,
  endHeight: number,
  maxAttempts: number,
  retryBaseDelaySeconds: number,
  retryMaxDelaySeconds: number,
): Promise<{
  pendingCount: number;
  claimedCount: number;
  retryBlockedCount: number;
  nextRetryAt: Date | null;
}> {
  const result = await databasePool.query<{
    pending_count: string;
    claimed_count: string;
    retry_blocked_count: string;
    next_retry_at: Date | null;
  }>(
    `WITH scoped_claims AS (
       SELECT status,
              attempt_count,
              updated_at,
              CASE
                WHEN status = 'failed' AND attempt_count < $3 THEN
                  updated_at + make_interval(
                    secs => LEAST(
                      $5::double precision,
                      $4::double precision * POWER(2::double precision, GREATEST(attempt_count - 1, 0))
                    )::int
                  )
                ELSE NULL
              END AS next_retry_at
         FROM block_sync_claims
        WHERE block_height BETWEEN $1 AND $2
     )
     SELECT COUNT(*) FILTER (WHERE status = 'pending')::text AS pending_count,
            COUNT(*) FILTER (WHERE status = 'claimed')::text AS claimed_count,
            COUNT(*) FILTER (
              WHERE status = 'failed'
                AND next_retry_at IS NOT NULL
                AND next_retry_at > NOW()
            )::text AS retry_blocked_count,
            MIN(next_retry_at) FILTER (
              WHERE status = 'failed'
                AND next_retry_at IS NOT NULL
                AND next_retry_at > NOW()
            ) AS next_retry_at
       FROM scoped_claims`,
    [
      startHeight,
      endHeight,
      Math.max(1, Math.floor(maxAttempts)),
      Math.max(1, Math.floor(retryBaseDelaySeconds)),
      Math.max(1, Math.floor(retryMaxDelaySeconds)),
    ],
  );

  return {
    pendingCount: Number(result.rows[0]?.pending_count ?? 0),
    claimedCount: Number(result.rows[0]?.claimed_count ?? 0),
    retryBlockedCount: Number(result.rows[0]?.retry_blocked_count ?? 0),
    nextRetryAt: result.rows[0]?.next_retry_at ?? null,
  };
}

export async function getExhaustedBlockSyncClaims(
  startHeight: number,
  endHeight: number,
  maxAttempts: number,
  limit: number,
): Promise<
  Array<{ blockHeight: number; attemptCount: number; error: string | null }>
> {
  const result = await databasePool.query<{
    block_height: string;
    attempt_count: number;
    error: string | null;
  }>(
    `SELECT block_height::text AS block_height,
            attempt_count,
            error
       FROM block_sync_claims
      WHERE block_height BETWEEN $1 AND $2
        AND status = 'failed'
        AND attempt_count >= $3
      ORDER BY block_height ASC
      LIMIT $4`,
    [
      startHeight,
      endHeight,
      Math.max(1, Math.floor(maxAttempts)),
      Math.max(1, Math.floor(limit)),
    ],
  );

  return result.rows.map((row) => ({
    blockHeight: Number(row.block_height),
    attemptCount: Number(row.attempt_count),
    error: row.error,
  }));
}

export async function requeueExhaustedBlockSyncClaims(
  startHeight: number,
  endHeight: number,
  maxAttempts: number,
  limit: number,
): Promise<number> {
  const result = await databasePool.query<{ count: string }>(
    `WITH exhausted AS (
       SELECT block_height
         FROM block_sync_claims
        WHERE block_height BETWEEN $1 AND $2
          AND status = 'failed'
          AND attempt_count >= $3
        ORDER BY block_height ASC
        LIMIT $4
     ),
     updated AS (
       UPDATE block_sync_claims claims
          SET status = 'pending',
              claimed_by = NULL,
              claimed_at = NULL,
              updated_at = NOW(),
              attempt_count = 0,
              error = COALESCE(claims.error, 'exhausted claim requeued')
         FROM exhausted
        WHERE claims.block_height = exhausted.block_height
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
    [
      startHeight,
      endHeight,
      Math.max(1, Math.floor(maxAttempts)),
      Math.max(1, Math.floor(limit)),
    ],
  );

  return Number(result.rows[0]?.count ?? 0);
}

export async function recoverCommitGapBlockClaim(
  maxClaimAgeSeconds: number,
): Promise<number | null> {
  const result = await databasePool.query<{ block_height: string }>(
    `WITH current_state AS (
       SELECT COALESCE(
                (
                  SELECT last_block_height
                    FROM sync_state
                   WHERE token_symbol = $1
                ),
                0::bigint
              ) AS last_height
     ),
     gap_claim AS (
       SELECT claims.block_height,
              claims.status,
              claims.claimed_at
         FROM block_sync_claims claims, current_state
        WHERE claims.block_height = current_state.last_height + 1
        LIMIT 1
     ),
     updated AS (
       UPDATE block_sync_claims claims
          SET status = 'pending',
              claimed_by = NULL,
              claimed_at = NULL,
              updated_at = NOW(),
              attempt_count = CASE
                WHEN claims.status = 'failed' THEN 0
                ELSE claims.attempt_count
              END,
              error = COALESCE(claims.error, 'commit gap claim requeued')
         FROM gap_claim
        WHERE claims.block_height = gap_claim.block_height
          AND (
            gap_claim.status = 'failed'
            OR (
              gap_claim.status = 'claimed'
              AND gap_claim.claimed_at IS NOT NULL
              AND gap_claim.claimed_at < NOW() - make_interval(secs => $2)
            )
          )
      RETURNING claims.block_height::text AS block_height
     )
     SELECT block_height FROM updated`,
    [CHAIN_SYNC_TOKEN, Math.max(30, Math.floor(maxClaimAgeSeconds))],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return Number(result.rows[0].block_height);
}

export async function getBlockSyncClaimsView(options?: {
  statuses?: string[];
  fromBlock?: number;
  toBlock?: number;
  limit?: number;
}): Promise<{
  summary: {
    pending: number;
    claimed: number;
    completed: number;
    failed: number;
    exhausted: number;
    retryBlocked: number;
    nextRetryAt: Date | null;
  };
  items: Array<{
    blockHeight: number;
    status: string;
    claimedBy: string | null;
    claimedAt: Date | null;
    completedAt: Date | null;
    attemptCount: number;
    error: string | null;
    createdAt: Date | null;
    updatedAt: Date | null;
    nextRetryAt: Date | null;
    retryBlocked: boolean;
    exhausted: boolean;
  }>;
}> {
  const filters: string[] = [];
  const values: Array<string | number | string[]> = [];
  const statuses = options?.statuses?.filter(Boolean) ?? [];
  const limit = Math.min(Math.max(options?.limit ?? 100, 1), 500);

  if (statuses.length > 0) {
    values.push(statuses);
    filters.push(`status = ANY($${values.length}::text[])`);
  }

  if (options?.fromBlock !== undefined) {
    values.push(options.fromBlock);
    filters.push(`block_height >= $${values.length}`);
  }

  if (options?.toBlock !== undefined) {
    values.push(options.toBlock);
    filters.push(`block_height <= $${values.length}`);
  }

  const whereClause =
    filters.length > 0 ? `WHERE ${filters.join(" AND ")}` : "";
  const retryBaseDelaySeconds = Math.max(
    1,
    Math.floor(syncConfig.claimRetryBaseDelaySeconds),
  );
  const retryMaxDelaySeconds = Math.max(
    1,
    Math.floor(syncConfig.claimRetryMaxDelaySeconds),
  );
  const claimMaxAttempts = Math.max(1, Math.floor(syncConfig.claimMaxAttempts));

  const summaryResult = await databasePool.query<{
    pending: string;
    claimed: string;
    completed: string;
    failed: string;
    exhausted: string;
    retry_blocked: string;
    next_retry_at: Date | null;
  }>(
    `WITH scoped_claims AS (
       SELECT *,
              CASE
                WHEN status = 'failed' AND attempt_count < ${claimMaxAttempts} THEN
                  updated_at + make_interval(
                    secs => LEAST(
                      ${retryMaxDelaySeconds}::double precision,
                      ${retryBaseDelaySeconds}::double precision * POWER(2::double precision, GREATEST(attempt_count - 1, 0))
                    )::int
                  )
                ELSE NULL
              END AS next_retry_at
         FROM block_sync_claims
         ${whereClause}
     )
     SELECT COUNT(*) FILTER (WHERE status = 'pending')::text AS pending,
            COUNT(*) FILTER (WHERE status = 'claimed')::text AS claimed,
            COUNT(*) FILTER (WHERE status = 'completed')::text AS completed,
            COUNT(*) FILTER (WHERE status = 'failed')::text AS failed,
            COUNT(*) FILTER (WHERE status = 'failed' AND attempt_count >= ${claimMaxAttempts})::text AS exhausted,
            COUNT(*) FILTER (
              WHERE status = 'failed'
                AND next_retry_at IS NOT NULL
                AND next_retry_at > NOW()
            )::text AS retry_blocked,
            MIN(next_retry_at) FILTER (
              WHERE status = 'failed'
                AND next_retry_at IS NOT NULL
                AND next_retry_at > NOW()
            ) AS next_retry_at
       FROM scoped_claims`,
    values,
  );

  const itemValues = [...values, limit];
  const itemsResult = await databasePool.query<{
    block_height: string;
    status: string;
    claimed_by: string | null;
    claimed_at: Date | null;
    completed_at: Date | null;
    attempt_count: number;
    error: string | null;
    created_at: Date | null;
    updated_at: Date | null;
    next_retry_at: Date | null;
    retry_blocked: boolean;
    exhausted: boolean;
  }>(
    `WITH scoped_claims AS (
       SELECT *,
              CASE
                WHEN status = 'failed' AND attempt_count < ${claimMaxAttempts} THEN
                  updated_at + make_interval(
                    secs => LEAST(
                      ${retryMaxDelaySeconds}::double precision,
                      ${retryBaseDelaySeconds}::double precision * POWER(2::double precision, GREATEST(attempt_count - 1, 0))
                    )::int
                  )
                ELSE NULL
              END AS next_retry_at
         FROM block_sync_claims
         ${whereClause}
     )
     SELECT block_height::text AS block_height,
            status,
            claimed_by,
            claimed_at,
            completed_at,
            attempt_count,
            error,
            created_at,
            updated_at,
            next_retry_at,
            (
              status = 'failed'
              AND next_retry_at IS NOT NULL
              AND next_retry_at > NOW()
            ) AS retry_blocked,
            (status = 'failed' AND attempt_count >= ${claimMaxAttempts}) AS exhausted
       FROM scoped_claims
      ORDER BY block_height ASC
      LIMIT $${itemValues.length}`,
    itemValues,
  );

  return {
    summary: {
      pending: Number(summaryResult.rows[0]?.pending ?? 0),
      claimed: Number(summaryResult.rows[0]?.claimed ?? 0),
      completed: Number(summaryResult.rows[0]?.completed ?? 0),
      failed: Number(summaryResult.rows[0]?.failed ?? 0),
      exhausted: Number(summaryResult.rows[0]?.exhausted ?? 0),
      retryBlocked: Number(summaryResult.rows[0]?.retry_blocked ?? 0),
      nextRetryAt: summaryResult.rows[0]?.next_retry_at ?? null,
    },
    items: itemsResult.rows.map((row) => ({
      blockHeight: Number(row.block_height),
      status: row.status,
      claimedBy: row.claimed_by,
      claimedAt: row.claimed_at,
      completedAt: row.completed_at,
      attemptCount: Number(row.attempt_count),
      error: row.error,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      nextRetryAt: row.next_retry_at,
      retryBlocked: Boolean(row.retry_blocked),
      exhausted: Boolean(row.exhausted),
    })),
  };
}

export async function renewBlockSyncClaim(
  workerId: string,
  blockHeight: number,
): Promise<boolean> {
  const result = await databasePool.query(
    `UPDATE block_sync_claims
        SET claimed_at = NOW(),
            updated_at = NOW()
      WHERE block_height = $1
        AND status = 'claimed'
        AND claimed_by = $2`,
    [blockHeight, workerId],
  );

  return (result.rowCount ?? 0) > 0;
}

export async function completeBlockSyncClaim(
  workerId: string,
  blockHeight: number,
): Promise<boolean> {
  const result = await databasePool.query(
    `UPDATE block_sync_claims
        SET status = 'completed',
            completed_at = NOW(),
            updated_at = NOW(),
            error = NULL
      WHERE block_height = $1
        AND status = 'claimed'
        AND claimed_by = $2`,
    [blockHeight, workerId],
  );

  return (result.rowCount ?? 0) > 0;
}

export async function failBlockSyncClaim(
  workerId: string,
  blockHeight: number,
  errorMessage: string,
): Promise<boolean> {
  const result = await databasePool.query(
    `UPDATE block_sync_claims
        SET status = 'failed',
            claimed_by = NULL,
            claimed_at = NULL,
            updated_at = NOW(),
            error = $3
      WHERE block_height = $1
        AND status = 'claimed'
        AND claimed_by = $2`,
    [blockHeight, workerId, errorMessage],
  );

  return (result.rowCount ?? 0) > 0;
}

export async function advanceChainSyncHeightFromClaims(
  defaultPreviousHeight: number,
): Promise<number | null> {
  const result = await databasePool.query<{ commit_height: string | null }>(
    `WITH current_state AS (
       SELECT COALESCE(
                (
                  SELECT last_block_height
                    FROM sync_state
                   WHERE token_symbol = $1
                ),
                $2::bigint
              ) AS last_height
     ),
     next_gap AS (
       SELECT MIN(block_height) AS block_height
         FROM block_sync_claims, current_state
        WHERE block_sync_claims.block_height > current_state.last_height
          AND block_sync_claims.status <> 'completed'
     ),
     next_completed AS (
       SELECT MAX(block_height) AS block_height
         FROM block_sync_claims, current_state
        WHERE block_sync_claims.block_height > current_state.last_height
          AND block_sync_claims.status = 'completed'
     )
     SELECT CASE
              WHEN next_completed.block_height IS NULL THEN NULL
              WHEN next_gap.block_height IS NULL THEN next_completed.block_height::text
              ELSE LEAST(
                next_completed.block_height,
                next_gap.block_height - 1
              )::text
            END AS commit_height
       FROM next_gap, next_completed`,
    [CHAIN_SYNC_TOKEN, defaultPreviousHeight],
  );

  const commitHeight =
    result.rowCount === 0 || result.rows[0].commit_height === null
      ? null
      : Number(result.rows[0].commit_height);

  if (commitHeight === null || commitHeight <= defaultPreviousHeight) {
    return null;
  }

  await updateChainSyncHeight(commitHeight);
  return commitHeight;
}
