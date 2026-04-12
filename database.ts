import {
  Pool,
  type PoolClient,
  type PoolConfig,
  type QueryResultRow,
} from "pg";
import { PhantasmaTS } from "phantasma-sdk-ts";
import { apiConfig, databaseConfig, syncConfig } from "./phantasma.config";
import {
  CHAIN_SYNC_TOKEN,
  type AddressSubgraphResult,
  type GraphEdgeRecord,
  type GraphNodeRecord,
  type PaginatedTransactionsResult,
  type ParsedTransfer,
  type SyncStateRecord,
  type TokenMetadataRecord,
  type TokenMetadataUpsertInput,
  type TopHoldersResult,
} from "./phantasma.types";

function buildPoolConfig(): PoolConfig {
  if (databaseConfig.connectionString) {
    return {
      connectionString: databaseConfig.connectionString,
      ssl: databaseConfig.ssl ? { rejectUnauthorized: false } : undefined,
    };
  }

  return {
    host: databaseConfig.host,
    port: databaseConfig.port,
    user: databaseConfig.user,
    password: databaseConfig.password,
    database: databaseConfig.database,
    ssl: databaseConfig.ssl ? { rejectUnauthorized: false } : undefined,
  };
}

export const databasePool = new Pool(buildPoolConfig());

const RESTORE_BATCH_SIZE = 500;

function readRawEventAmountFromMetadata(
  metadata: Record<string, unknown> | null | undefined,
): string | null {
  const rawEvents = metadata?.rawEvents;

  if (!Array.isArray(rawEvents)) {
    return null;
  }

  for (const rawEvent of rawEvents) {
    if (!rawEvent || typeof rawEvent !== "object") {
      continue;
    }

    const eventRecord = rawEvent as { data?: unknown; kind?: unknown };
    const eventData = eventRecord.data;

    if (typeof eventData !== "string" || eventData.length === 0) {
      continue;
    }

    try {
      const decoded = PhantasmaTS.getTokenEventData(eventData);
      return String(decoded.value);
    } catch {
      continue;
    }
  }

  return null;
}

async function restoreFungibleTransactionAmountsFromMetadata(): Promise<number> {
  const result = await databasePool.query<{
    id: string;
    amount: string | null;
    amount_normalized: string | null;
    decimals: number;
    metadata: Record<string, unknown> | null;
  }>(
    `SELECT t.id::text AS id,
            t.amount::text AS amount,
            t.amount_normalized::text AS amount_normalized,
            tm.decimals,
            t.metadata
       FROM transactions t
       JOIN token_metadata tm ON tm.token_symbol = t.token_symbol
      WHERE COALESCE((tm.flags->>'isFungible')::boolean, false) = true
        AND (
          t.amount IN (0::numeric, 1::numeric)
          OR t.amount_normalized IN (0::numeric, 1::numeric)
        )`,
  );

  let updatedCount = 0;
  const pendingUpdates: Array<{
    id: string;
    amount: string;
    amountNormalized: string;
  }> = [];

  for (const row of result.rows) {
    const rawAmount = readRawEventAmountFromMetadata(row.metadata);

    if (!rawAmount) {
      continue;
    }

    const amountNormalized = normalizeRawAmount(
      rawAmount,
      Number(row.decimals),
    );

    if (
      row.amount === rawAmount &&
      row.amount_normalized === amountNormalized
    ) {
      continue;
    }

    pendingUpdates.push({
      id: row.id,
      amount: rawAmount,
      amountNormalized,
    });

    if (pendingUpdates.length < RESTORE_BATCH_SIZE) {
      continue;
    }

    const valuesClause = pendingUpdates
      .map(
        (_, index) =>
          `($${index * 3 + 1}::bigint, $${index * 3 + 2}::numeric, $${index * 3 + 3}::numeric)`,
      )
      .join(", ");
    const queryValues = pendingUpdates.flatMap((item) => [
      item.id,
      item.amount,
      item.amountNormalized,
    ]);
    const updateResult = await databasePool.query(
      `UPDATE transactions AS t
          SET amount = updates.amount,
              amount_normalized = updates.amount_normalized
         FROM (VALUES ${valuesClause}) AS updates(id, amount, amount_normalized)
        WHERE t.id = updates.id`,
      queryValues,
    );

    updatedCount += updateResult.rowCount ?? 0;
    pendingUpdates.length = 0;
  }

  if (pendingUpdates.length > 0) {
    const valuesClause = pendingUpdates
      .map(
        (_, index) =>
          `($${index * 3 + 1}::bigint, $${index * 3 + 2}::numeric, $${index * 3 + 3}::numeric)`,
      )
      .join(", ");
    const queryValues = pendingUpdates.flatMap((item) => [
      item.id,
      item.amount,
      item.amountNormalized,
    ]);
    const updateResult = await databasePool.query(
      `UPDATE transactions AS t
          SET amount = updates.amount,
              amount_normalized = updates.amount_normalized
         FROM (VALUES ${valuesClause}) AS updates(id, amount, amount_normalized)
        WHERE t.id = updates.id`,
      queryValues,
    );

    updatedCount += updateResult.rowCount ?? 0;
  }

  return updatedCount;
}

async function restoreFungibleEdgeAmountsFromMetadata(): Promise<number> {
  const result = await databasePool.query<{
    id: string;
    amount: string | null;
    amount_normalized: string | null;
    decimals: number;
    metadata: Record<string, unknown> | null;
  }>(
    `SELECT e.id::text AS id,
            e.amount::text AS amount,
            e.amount_normalized::text AS amount_normalized,
            tm.decimals,
            e.metadata
       FROM edges e
       JOIN token_metadata tm ON tm.token_symbol = e.token_symbol
      WHERE COALESCE((tm.flags->>'isFungible')::boolean, false) = true
        AND (
          e.amount IN (0::numeric, 1::numeric)
          OR e.amount_normalized IN (0::numeric, 1::numeric)
        )`,
  );

  let updatedCount = 0;
  const pendingUpdates: Array<{
    id: string;
    amount: string;
    amountNormalized: string;
  }> = [];

  for (const row of result.rows) {
    const rawAmount = readRawEventAmountFromMetadata(row.metadata);

    if (!rawAmount) {
      continue;
    }

    const amountNormalized = normalizeRawAmount(
      rawAmount,
      Number(row.decimals),
    );

    if (
      row.amount === rawAmount &&
      row.amount_normalized === amountNormalized
    ) {
      continue;
    }

    pendingUpdates.push({
      id: row.id,
      amount: rawAmount,
      amountNormalized,
    });

    if (pendingUpdates.length < RESTORE_BATCH_SIZE) {
      continue;
    }

    const valuesClause = pendingUpdates
      .map(
        (_, index) =>
          `($${index * 3 + 1}::bigint, $${index * 3 + 2}::numeric, $${index * 3 + 3}::numeric)`,
      )
      .join(", ");
    const queryValues = pendingUpdates.flatMap((item) => [
      item.id,
      item.amount,
      item.amountNormalized,
    ]);
    const updateResult = await databasePool.query(
      `UPDATE edges AS e
          SET amount = updates.amount,
              amount_normalized = updates.amount_normalized
         FROM (VALUES ${valuesClause}) AS updates(id, amount, amount_normalized)
        WHERE e.id = updates.id`,
      queryValues,
    );

    updatedCount += updateResult.rowCount ?? 0;
    pendingUpdates.length = 0;
  }

  if (pendingUpdates.length > 0) {
    const valuesClause = pendingUpdates
      .map(
        (_, index) =>
          `($${index * 3 + 1}::bigint, $${index * 3 + 2}::numeric, $${index * 3 + 3}::numeric)`,
      )
      .join(", ");
    const queryValues = pendingUpdates.flatMap((item) => [
      item.id,
      item.amount,
      item.amountNormalized,
    ]);
    const updateResult = await databasePool.query(
      `UPDATE edges AS e
          SET amount = updates.amount,
              amount_normalized = updates.amount_normalized
         FROM (VALUES ${valuesClause}) AS updates(id, amount, amount_normalized)
        WHERE e.id = updates.id`,
      queryValues,
    );

    updatedCount += updateResult.rowCount ?? 0;
  }

  return updatedCount;
}

function mapSyncStateRow(row: QueryResultRow): SyncStateRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    lastBlockHeight: Number(row.last_block_height),
    updatedAt: row.updated_at ? new Date(row.updated_at) : null,
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  };
}

function mapGraphNodeRow(row: QueryResultRow): GraphNodeRecord {
  return {
    address: String(row.address),
    tokenSymbol: String(row.token_symbol),
    balance: row.balance === null ? null : String(row.balance),
    balanceNormalized:
      row.balance_normalized === null ? null : String(row.balance_normalized),
    label: row.label === null ? null : String(row.label),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  };
}

function normalizeRawAmount(rawAmount: string, decimals: number): string {
  const cleanRaw = rawAmount.trim();
  const negative = cleanRaw.startsWith("-");
  const digitsOnly = (negative ? cleanRaw.slice(1) : cleanRaw).replace(
    /\D/g,
    "",
  );
  const safeDigits = digitsOnly.length > 0 ? digitsOnly : "0";
  const safeDecimals = Math.max(0, Math.floor(decimals));

  if (safeDecimals === 0) {
    return `${negative ? "-" : ""}${safeDigits}`;
  }

  const padded = safeDigits.padStart(safeDecimals + 1, "0");
  const splitAt = padded.length - safeDecimals;
  const integerPart = padded.slice(0, splitAt);
  const fractionalPart = padded.slice(splitAt).replace(/0+$/, "");

  if (!fractionalPart) {
    return `${negative ? "-" : ""}${integerPart}`;
  }

  return `${negative ? "-" : ""}${integerPart}.${fractionalPart}`;
}

function isTokenFungible(
  flags: Record<string, unknown> | null | undefined,
): boolean {
  if (!flags) {
    return false;
  }

  const directFlag = flags.isFungible;

  if (typeof directFlag !== "boolean") {
    return false;
  }

  return directFlag;
}

function resolveStoredTransferAmounts(
  rawAmount: string,
  tokenMetadata:
    | Pick<TokenMetadataUpsertInput, "decimals" | "flags">
    | undefined,
): { amount: string; amountNormalized: string } {
  if (!tokenMetadata) {
    return {
      amount: "1",
      amountNormalized: "1",
    };
  }

  if (!isTokenFungible(tokenMetadata.flags)) {
    return {
      amount: "1",
      amountNormalized: "1",
    };
  }

  return {
    amount: rawAmount,
    amountNormalized: normalizeRawAmount(rawAmount, tokenMetadata.decimals),
  };
}

function mapGraphEdgeRow(row: QueryResultRow): GraphEdgeRecord {
  return {
    id: String(row.id),
    tokenSymbol: String(row.token_symbol),
    fromAddress: String(row.from_address),
    toAddress: String(row.to_address),
    amount: row.amount === null ? null : String(row.amount),
    amountNormalized:
      row.amount_normalized === null ? null : String(row.amount_normalized),
    txHash: String(row.tx_hash),
    eventIndex: Number(row.event_index),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  };
}

function mapTokenMetadataRow(row: QueryResultRow): TokenMetadataRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    name: row.name === null ? null : String(row.name),
    decimals: Number(row.decimals),
    currentSupplyRaw: String(row.current_supply_raw),
    currentSupplyNormalized: String(row.current_supply_normalized),
    maxSupplyRaw:
      row.max_supply_raw === null ? null : String(row.max_supply_raw),
    maxSupplyNormalized:
      row.max_supply_normalized === null
        ? null
        : String(row.max_supply_normalized),
    flags: (row.flags as Record<string, unknown> | null) ?? null,
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
    updatedAt: row.updated_at ? new Date(row.updated_at) : null,
  };
}

function mapTransactionRow(row: QueryResultRow): Record<string, unknown> {
  return {
    id: String(row.id),
    txHash: String(row.tx_hash),
    eventIndex:
      row.event_index === null || row.event_index === undefined
        ? null
        : Number(row.event_index),
    eventIndexes: Array.isArray(row.event_indexes)
      ? row.event_indexes.map((value: unknown) => Number(value))
      : [],
    transferCount: Number(row.transfer_count ?? 1),
    tokenSymbol: String(row.token_symbol),
    blockHeight: Number(row.block_height),
    timestamp: row.timestamp,
    fromAddress: String(row.from_address),
    toAddress: String(row.to_address),
    amount: row.amount === null ? null : String(row.amount),
    amountNormalized:
      row.amount_normalized === null ? null : String(row.amount_normalized),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
    tx_hash: String(row.tx_hash),
    event_index:
      row.event_index === null || row.event_index === undefined
        ? null
        : Number(row.event_index),
    event_indexes: Array.isArray(row.event_indexes)
      ? row.event_indexes.map((value: unknown) => Number(value))
      : [],
    transfer_count: Number(row.transfer_count ?? 1),
    token_symbol: String(row.token_symbol),
    block_height: Number(row.block_height),
    from_address: String(row.from_address),
    to_address: String(row.to_address),
    amount_normalized:
      row.amount_normalized === null ? null : String(row.amount_normalized),
  };
}

export async function closeDatabasePool(): Promise<void> {
  await databasePool.end();
}

export async function testDatabaseConnection(): Promise<void> {
  await databasePool.query("SELECT 1");
}

export async function withDatabaseTransaction<T>(
  callback: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await databasePool.connect();

  try {
    await client.query("BEGIN");
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

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

export async function getSyncStates(): Promise<SyncStateRecord[]> {
  const result = await databasePool.query(
    `SELECT token_symbol, last_block_height, updated_at, metadata
       FROM sync_state
      ORDER BY token_symbol ASC`,
  );

  return result.rows.map(mapSyncStateRow);
}

export async function getTrackedTokenSymbolsFromSyncState(): Promise<string[]> {
  const result = await databasePool.query<{ token_symbol: string }>(
    `SELECT DISTINCT token_symbol
       FROM sync_state
      WHERE token_symbol <> $1
      ORDER BY token_symbol ASC`,
    [CHAIN_SYNC_TOKEN],
  );

  return result.rows.map((row) => row.token_symbol);
}

export async function getTrackedNodeAddressTokens(): Promise<
  Array<{ address: string; tokenSymbol: string }>
> {
  const result = await databasePool.query<{
    address: string;
    token_symbol: string;
  }>(
    `SELECT address, token_symbol
       FROM nodes
      ORDER BY address ASC, token_symbol ASC`,
  );

  return result.rows.map((row) => ({
    address: row.address,
    tokenSymbol: row.token_symbol,
  }));
}

export async function updateTrackedNodeBalances(
  client: PoolClient,
  items: Array<{ address: string; tokenSymbol: string; balance: string }>,
): Promise<number> {
  let updatedCount = 0;

  for (const item of items) {
    const result = await client.query(
      `UPDATE nodes
          SET balance = $3
        WHERE address = $1
          AND token_symbol = $2
          AND balance IS DISTINCT FROM $3`,
      [item.address, item.tokenSymbol, item.balance],
    );

    updatedCount += result.rowCount ?? 0;
  }

  return updatedCount;
}

export async function upsertTransfers(
  client: PoolClient,
  transfers: ParsedTransfer[],
  tokenMetadataBySymbol: Map<
    string,
    Pick<TokenMetadataUpsertInput, "decimals" | "flags">
  >,
): Promise<void> {
  for (const transfer of transfers) {
    const storedAmounts = resolveStoredTransferAmounts(
      transfer.amount,
      tokenMetadataBySymbol.get(transfer.tokenSymbol),
    );

    await client.query(
      `INSERT INTO transactions (
         tx_hash,
         event_index,
         token_symbol,
         block_height,
         timestamp,
         from_address,
         to_address,
         amount,
         amount_normalized,
         metadata
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (tx_hash, event_index) DO UPDATE
         SET token_symbol = EXCLUDED.token_symbol,
             block_height = EXCLUDED.block_height,
             timestamp = EXCLUDED.timestamp,
             from_address = EXCLUDED.from_address,
             to_address = EXCLUDED.to_address,
             amount = EXCLUDED.amount,
             amount_normalized = EXCLUDED.amount_normalized,
             metadata = EXCLUDED.metadata`,
      [
        transfer.txHash,
        transfer.eventIndex,
        transfer.tokenSymbol,
        transfer.blockHeight,
        transfer.timestamp,
        transfer.fromAddress,
        transfer.toAddress,
        storedAmounts.amount,
        storedAmounts.amountNormalized,
        transfer.metadata,
      ],
    );
  }
}

export async function syncTransactionAmountsNormalized(): Promise<{
  updatedUsingMetadata: number;
  updatedFallback: number;
  totalUpdated: number;
}> {
  const restoredFromMetadata =
    await restoreFungibleTransactionAmountsFromMetadata();

  const usingMetadataResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE transactions t
          SET amount = CASE
                WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
                ELSE t.amount
              END,
              amount_normalized = CASE
                WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
                WHEN tm.decimals <= 0 THEN t.amount
                ELSE t.amount / POWER(10::numeric, tm.decimals)
              END
         FROM token_metadata tm
        WHERE tm.token_symbol = t.token_symbol
          AND t.amount IS NOT NULL
          AND (
            t.amount IS DISTINCT FROM CASE
              WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
              ELSE t.amount
            END
            OR t.amount_normalized IS DISTINCT FROM CASE
              WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
              WHEN tm.decimals <= 0 THEN t.amount
              ELSE t.amount / POWER(10::numeric, tm.decimals)
            END
          )
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
  );

  const fallbackResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE transactions t
          SET amount = 1::numeric,
              amount_normalized = 1::numeric
        WHERE t.amount IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
              FROM token_metadata tm
             WHERE tm.token_symbol = t.token_symbol
          )
          AND (
            t.amount IS DISTINCT FROM 1::numeric
            OR t.amount_normalized IS DISTINCT FROM 1::numeric
          )
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
  );

  const updatedUsingMetadata = Number(usingMetadataResult.rows[0]?.count ?? 0);
  const updatedFallback = Number(fallbackResult.rows[0]?.count ?? 0);

  return {
    updatedUsingMetadata: updatedUsingMetadata + restoredFromMetadata,
    updatedFallback,
    totalUpdated: updatedUsingMetadata + restoredFromMetadata + updatedFallback,
  };
}

export async function upsertNodes(
  client: PoolClient,
  transfers: ParsedTransfer[],
  balancesByNodeKey: Map<string, string>,
  tokenDecimalsBySymbol: Map<string, number>,
): Promise<void> {
  const nodeMap = new Map<
    string,
    { address: string; tokenSymbol: string; metadata: Record<string, unknown> }
  >();

  for (const transfer of transfers) {
    for (const address of [transfer.fromAddress, transfer.toAddress]) {
      const key = `${transfer.tokenSymbol}:${address}`;

      if (!nodeMap.has(key)) {
        nodeMap.set(key, {
          address,
          tokenSymbol: transfer.tokenSymbol,
          metadata: {
            discoveredFromTx: transfer.txHash,
            lastSeenBlockHeight: transfer.blockHeight,
          },
        });
      }
    }
  }

  for (const node of nodeMap.values()) {
    const nodeKey = `${node.tokenSymbol}:${node.address}`;
    const balanceRaw = balancesByNodeKey.has(nodeKey)
      ? (balancesByNodeKey.get(nodeKey) ?? "0")
      : null;
    const tokenDecimals = tokenDecimalsBySymbol.get(node.tokenSymbol) ?? 0;
    const balanceNormalized =
      balanceRaw === null
        ? null
        : normalizeRawAmount(balanceRaw, tokenDecimals);

    await client.query(
      `INSERT INTO nodes (address, token_symbol, balance, balance_normalized, metadata)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (address, token_symbol) DO UPDATE
         SET balance = COALESCE(EXCLUDED.balance, nodes.balance),
             balance_normalized = COALESCE(EXCLUDED.balance_normalized, nodes.balance_normalized),
             metadata = COALESCE(nodes.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)`,
      [
        node.address,
        node.tokenSymbol,
        balanceRaw,
        balanceNormalized,
        node.metadata,
      ],
    );
  }
}

export async function syncNodeBalancesNormalized(): Promise<{
  updatedUsingMetadata: number;
  updatedFallback: number;
  totalUpdated: number;
}> {
  const usingMetadataResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE nodes n
          SET balance_normalized = CASE
            WHEN tm.decimals <= 0 THEN n.balance
            ELSE n.balance / POWER(10::numeric, tm.decimals)
          END
         FROM token_metadata tm
        WHERE tm.token_symbol = n.token_symbol
          AND n.balance IS NOT NULL
          AND n.balance_normalized IS DISTINCT FROM CASE
            WHEN tm.decimals <= 0 THEN n.balance
            ELSE n.balance / POWER(10::numeric, tm.decimals)
          END
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
  );

  const fallbackResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE nodes n
          SET balance_normalized = n.balance
        WHERE n.balance IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
              FROM token_metadata tm
             WHERE tm.token_symbol = n.token_symbol
          )
          AND n.balance_normalized IS DISTINCT FROM n.balance
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
  );

  const updatedUsingMetadata = Number(usingMetadataResult.rows[0]?.count ?? 0);
  const updatedFallback = Number(fallbackResult.rows[0]?.count ?? 0);

  return {
    updatedUsingMetadata,
    updatedFallback,
    totalUpdated: updatedUsingMetadata + updatedFallback,
  };
}

export async function upsertEdges(
  client: PoolClient,
  transfers: ParsedTransfer[],
  tokenMetadataBySymbol: Map<
    string,
    Pick<TokenMetadataUpsertInput, "decimals" | "flags">
  >,
): Promise<void> {
  for (const transfer of transfers) {
    const storedAmounts = resolveStoredTransferAmounts(
      transfer.amount,
      tokenMetadataBySymbol.get(transfer.tokenSymbol),
    );

    await client.query(
      `INSERT INTO edges (
         token_symbol,
         from_address,
         to_address,
         amount,
         amount_normalized,
         tx_hash,
         event_index,
         metadata
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (tx_hash, event_index) DO NOTHING`,
      [
        transfer.tokenSymbol,
        transfer.fromAddress,
        transfer.toAddress,
        storedAmounts.amount,
        storedAmounts.amountNormalized,
        transfer.txHash,
        transfer.eventIndex,
        transfer.metadata,
      ],
    );
  }
}

export async function syncEdgeAmountsNormalized(): Promise<{
  updatedUsingMetadata: number;
  updatedFallback: number;
  totalUpdated: number;
}> {
  const restoredFromMetadata = await restoreFungibleEdgeAmountsFromMetadata();

  const usingMetadataResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE edges e
          SET amount = CASE
                WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
                ELSE e.amount
              END,
              amount_normalized = CASE
                WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
                WHEN tm.decimals <= 0 THEN e.amount
                ELSE e.amount / POWER(10::numeric, tm.decimals)
              END
         FROM token_metadata tm
        WHERE tm.token_symbol = e.token_symbol
          AND e.amount IS NOT NULL
          AND (
            e.amount IS DISTINCT FROM CASE
              WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
              ELSE e.amount
            END
            OR e.amount_normalized IS DISTINCT FROM CASE
              WHEN COALESCE((tm.flags->>'isFungible')::boolean, false) = false THEN 1::numeric
              WHEN tm.decimals <= 0 THEN e.amount
              ELSE e.amount / POWER(10::numeric, tm.decimals)
            END
          )
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
  );

  const fallbackResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE edges e
          SET amount = 1::numeric,
              amount_normalized = 1::numeric
        WHERE e.amount IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
              FROM token_metadata tm
             WHERE tm.token_symbol = e.token_symbol
          )
          AND (
            e.amount IS DISTINCT FROM 1::numeric
            OR e.amount_normalized IS DISTINCT FROM 1::numeric
          )
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
  );

  const updatedUsingMetadata = Number(usingMetadataResult.rows[0]?.count ?? 0);
  const updatedFallback = Number(fallbackResult.rows[0]?.count ?? 0);

  return {
    updatedUsingMetadata: updatedUsingMetadata + restoredFromMetadata,
    updatedFallback,
    totalUpdated: updatedUsingMetadata + restoredFromMetadata + updatedFallback,
  };
}

export async function updateTokenSyncStateForBlock(
  client: PoolClient,
  blockHeight: number,
  tokenSymbols: string[],
): Promise<void> {
  const checkpointTokens = new Set<string>(tokenSymbols);

  for (const tokenSymbol of checkpointTokens) {
    await client.query(
      `INSERT INTO sync_state (token_symbol, last_block_height, updated_at, metadata)
       VALUES ($1, $2, NOW(), $3)
       ON CONFLICT (token_symbol) DO UPDATE
         SET last_block_height = GREATEST(sync_state.last_block_height, EXCLUDED.last_block_height),
             updated_at = NOW(),
             metadata = COALESCE(sync_state.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)`,
      [
        tokenSymbol,
        blockHeight,
        {
          checkpointType: "token",
          lastCommittedBlockHeight: blockHeight,
        },
      ],
    );
  }
}

export async function updateChainSyncHeight(
  blockHeight: number,
): Promise<void> {
  await databasePool.query(
    `INSERT INTO sync_state (token_symbol, last_block_height, updated_at, metadata)
     VALUES ($1, $2, NOW(), $3)
     ON CONFLICT (token_symbol) DO UPDATE
       SET last_block_height = GREATEST(sync_state.last_block_height, EXCLUDED.last_block_height),
           updated_at = NOW(),
           metadata = COALESCE(sync_state.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)`,
    [
      CHAIN_SYNC_TOKEN,
      blockHeight,
      {
        checkpointType: "chain",
        lastCommittedBlockHeight: blockHeight,
      },
    ],
  );
}

export async function upsertTokenMetadata(
  client: PoolClient,
  items: TokenMetadataUpsertInput[],
): Promise<void> {
  for (const item of items) {
    await client.query(
      `INSERT INTO token_metadata (
         token_symbol,
         name,
         decimals,
         current_supply_raw,
         current_supply_normalized,
         max_supply_raw,
         max_supply_normalized,
         flags,
         metadata,
         updated_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
       ON CONFLICT (token_symbol) DO UPDATE
         SET name = EXCLUDED.name,
             decimals = EXCLUDED.decimals,
             current_supply_raw = EXCLUDED.current_supply_raw,
             current_supply_normalized = EXCLUDED.current_supply_normalized,
             max_supply_raw = EXCLUDED.max_supply_raw,
             max_supply_normalized = EXCLUDED.max_supply_normalized,
             flags = EXCLUDED.flags,
             metadata = EXCLUDED.metadata,
             updated_at = NOW()`,
      [
        item.tokenSymbol,
        item.name,
        item.decimals,
        item.currentSupplyRaw,
        item.currentSupplyNormalized,
        item.maxSupplyRaw,
        item.maxSupplyNormalized,
        item.flags,
        item.metadata,
      ],
    );
  }
}

export async function getTopHolders(
  tokenSymbol: string,
  limit: number,
): Promise<TopHoldersResult> {
  const result = await databasePool.query<{
    address: string;
    net_balance: string;
  }>(
    `SELECT address,
            SUM(received) - SUM(sent) AS net_balance
       FROM (
         SELECT to_address   AS address,
                COALESCE(SUM(amount), 0) AS received,
                0                        AS sent
           FROM transactions
          WHERE token_symbol = $1
          GROUP BY to_address
         UNION ALL
         SELECT from_address AS address,
                0            AS received,
                COALESCE(SUM(amount), 0) AS sent
           FROM transactions
          WHERE token_symbol = $1
          GROUP BY from_address
       ) t
      GROUP BY address
      ORDER BY net_balance DESC
      LIMIT $2`,
    [tokenSymbol, limit],
  );

  return {
    tokenSymbol,
    limit,
    items: result.rows.map((row) => ({
      address: row.address,
      tokenSymbol,
      netBalance: String(row.net_balance),
    })),
  };
}

export async function getAvailableTokens(): Promise<string[]> {
  const result = await databasePool.query<{ token_symbol: string }>(
    `SELECT DISTINCT token_symbol
       FROM transactions
      WHERE token_symbol <> $1
      ORDER BY token_symbol ASC`,
    [CHAIN_SYNC_TOKEN],
  );

  return result.rows.map((row) => row.token_symbol);
}

export async function getTokenMetadata(
  tokenSymbol: string,
): Promise<TokenMetadataRecord | null> {
  const result = await databasePool.query(
    `SELECT token_symbol,
            name,
            decimals,
            current_supply_raw,
            current_supply_normalized,
            max_supply_raw,
            max_supply_normalized,
            flags,
            metadata,
            updated_at
       FROM token_metadata
      WHERE token_symbol = $1`,
    [tokenSymbol],
  );

  if (result.rowCount === 0) {
    return null;
  }

  return mapTokenMetadataRow(result.rows[0]);
}

export async function getFullTokenGraph(
  tokenSymbol: string,
): Promise<AddressSubgraphResult> {
  const edgesResult = await databasePool.query(
    `SELECT id, token_symbol, from_address, to_address, amount, amount_normalized, tx_hash, event_index, metadata
       FROM edges
      WHERE token_symbol = $1
      ORDER BY id ASC
      LIMIT $2`,
    [tokenSymbol, apiConfig.tokenGraphMaxEdges],
  );

  const edges = edgesResult.rows.map(mapGraphEdgeRow);
  const addressSet = new Set<string>();

  for (const edge of edges) {
    addressSet.add(edge.fromAddress);
    addressSet.add(edge.toAddress);
  }

  const nodesResult = addressSet.size
    ? await databasePool.query(
        `SELECT address, token_symbol, balance, balance_normalized, label, metadata
           FROM nodes
          WHERE token_symbol = $1
            AND address = ANY($2::text[])
          ORDER BY address ASC`,
        [tokenSymbol, [...addressSet]],
      )
    : { rows: [] as QueryResultRow[] };

  return {
    tokenSymbol,
    rootAddress: "",
    depth: 0,
    nodes: nodesResult.rows.map(mapGraphNodeRow),
    edges,
  };
}

export async function getAddressSubgraph(
  tokenSymbol: string,
  rootAddress: string,
  requestedDepth: number,
  requestedEdgeLimit: number,
): Promise<AddressSubgraphResult> {
  const depth = Math.min(
    Math.max(requestedDepth, 1),
    apiConfig.graphHardMaxDepth,
  );
  const edgeLimit = Math.min(
    Math.max(requestedEdgeLimit, 1),
    apiConfig.graphMaxEdgesPerRequest,
  );

  const edgesResult = await databasePool.query(
    `WITH RECURSIVE walk AS (
       SELECT $2::text AS address, 0 AS depth
       UNION ALL
       SELECT CASE
                WHEN e.from_address = walk.address THEN e.to_address
                ELSE e.from_address
              END AS address,
              walk.depth + 1 AS depth
         FROM walk
         JOIN edges e
           ON e.token_symbol = $1
          AND (e.from_address = walk.address OR e.to_address = walk.address)
        WHERE walk.depth < $3
     ),
     address_depths AS (
       SELECT address, MIN(depth) AS depth
         FROM walk
        GROUP BY address
     ),
     ranked_edges AS (
       SELECT DISTINCT ON (e.tx_hash, e.event_index)
              e.id,
              e.token_symbol,
              e.from_address,
              e.to_address,
              e.amount,
              e.amount_normalized,
              e.tx_hash,
              e.event_index,
              e.metadata,
              LEAST(from_depth.depth, to_depth.depth) AS edge_depth
         FROM edges e
         JOIN address_depths from_depth
           ON from_depth.address = e.from_address
         JOIN address_depths to_depth
           ON to_depth.address = e.to_address
        WHERE e.token_symbol = $1
        ORDER BY e.tx_hash,
                 e.event_index,
                 LEAST(from_depth.depth, to_depth.depth),
                 e.id
     ),
     limited_edges AS (
       SELECT id,
              token_symbol,
              from_address,
              to_address,
              amount,
              amount_normalized,
              tx_hash,
              event_index,
              metadata
         FROM ranked_edges
        ORDER BY edge_depth ASC, id ASC
        LIMIT $4
     )
     SELECT * FROM limited_edges
     ORDER BY id ASC`,
    [tokenSymbol, rootAddress, depth, edgeLimit],
  );

  const edges = edgesResult.rows.map(mapGraphEdgeRow);
  const addressSet = new Set<string>([rootAddress]);

  for (const edge of edges) {
    addressSet.add(edge.fromAddress);
    addressSet.add(edge.toAddress);
  }

  const nodesResult = await databasePool.query(
    `SELECT address, token_symbol, balance, balance_normalized, label, metadata
       FROM nodes
      WHERE token_symbol = $1
        AND address = ANY($2::text[])
      ORDER BY address ASC`,
    [tokenSymbol, [...addressSet]],
  );

  return {
    tokenSymbol,
    rootAddress,
    depth,
    nodes: nodesResult.rows.map(mapGraphNodeRow),
    edges,
  };
}

export async function getTransactionsPage(options: {
  tokenSymbol?: string;
  address?: string;
  fromBlock?: number;
  toBlock?: number;
  direction?: "from" | "to";
  counterparty?: string;
  startTime?: Date;
  endTime?: Date;
  minAmount?: number;
  maxAmount?: number;
  minUsd?: number;
  maxUsd?: number;
  usdRateNow?: number;
  sortBy?: "amount" | "usd" | "time";
  sortDir?: "asc" | "desc";
  page: number;
  pageSize: number;
}): Promise<PaginatedTransactionsResult> {
  const filters: string[] = [];
  const values: Array<string | number> = [];
  let addressParamIndex: number | null = null;

  if (options.tokenSymbol) {
    values.push(options.tokenSymbol);
    filters.push(`token_symbol = $${values.length}`);
  }

  if (options.address) {
    values.push(options.address);
    addressParamIndex = values.length;
    filters.push(
      `(from_address = $${values.length} OR to_address = $${values.length})`,
    );
  }

  if (options.direction === "from" && addressParamIndex !== null) {
    filters.push(`to_address = $${addressParamIndex}`);
  }

  if (options.direction === "to" && addressParamIndex !== null) {
    filters.push(`from_address = $${addressParamIndex}`);
  }

  if (options.counterparty) {
    values.push(`%${options.counterparty}%`);
    if (addressParamIndex !== null) {
      filters.push(
        `(
          (from_address = $${addressParamIndex} AND to_address ILIKE $${values.length})
          OR
          (to_address = $${addressParamIndex} AND from_address ILIKE $${values.length})
        )`,
      );
    } else {
      filters.push(
        `(from_address ILIKE $${values.length} OR to_address ILIKE $${values.length})`,
      );
    }
  }

  if (options.startTime) {
    values.push(options.startTime.toISOString());
    filters.push(`timestamp >= $${values.length}::timestamptz`);
  }

  if (options.endTime) {
    values.push(options.endTime.toISOString());
    filters.push(`timestamp <= $${values.length}::timestamptz`);
  }

  if (options.minAmount !== undefined) {
    values.push(options.minAmount);
    filters.push(`amount_normalized >= $${values.length}::numeric`);
  }

  if (options.maxAmount !== undefined) {
    values.push(options.maxAmount);
    filters.push(`amount_normalized <= $${values.length}::numeric`);
  }

  if (options.usdRateNow !== undefined && options.minUsd !== undefined) {
    values.push(options.usdRateNow, options.minUsd);
    filters.push(
      `(amount_normalized * $${values.length - 1}::numeric) >= $${values.length}::numeric`,
    );
  }

  if (options.usdRateNow !== undefined && options.maxUsd !== undefined) {
    values.push(options.usdRateNow, options.maxUsd);
    filters.push(
      `(amount_normalized * $${values.length - 1}::numeric) <= $${values.length}::numeric`,
    );
  }

  if (options.fromBlock !== undefined) {
    values.push(options.fromBlock);
    filters.push(`block_height >= $${values.length}`);
  }

  if (options.toBlock !== undefined) {
    values.push(options.toBlock);
    filters.push(`block_height <= $${values.length}`);
  }

  const whereClause = filters.length ? `WHERE ${filters.join(" AND ")}` : "";
  const pageSize = Math.min(
    Math.max(options.pageSize, 1),
    apiConfig.transactionPageSizeMax,
  );
  const page = Math.max(options.page, 1);
  const offset = (page - 1) * pageSize;
  const appliedFilters: Record<string, unknown> = {};
  const normalizedSortDir = options.sortDir === "asc" ? "ASC" : "DESC";
  const orderByClause =
    options.sortBy === "amount"
      ? `SUM(amount_normalized) ${normalizedSortDir}, block_height DESC, tx_hash ASC`
      : options.sortBy === "usd"
        ? `(SUM(amount_normalized) * ${
            options.usdRateNow !== undefined
              ? `${Number(options.usdRateNow)}`
              : "1"
          }::numeric) ${normalizedSortDir}, block_height DESC, tx_hash ASC`
        : options.sortBy === "time"
          ? `block_height ${normalizedSortDir}, tx_hash ASC`
          : "block_height DESC, tx_hash ASC";

  if (options.tokenSymbol) {
    appliedFilters.token = options.tokenSymbol;
  }

  if (options.address) {
    appliedFilters.address = options.address;
  }

  if (
    options.address &&
    (options.direction === "from" || options.direction === "to")
  ) {
    appliedFilters.dir = options.direction;
  }

  if (options.counterparty) {
    appliedFilters.counterparty = options.counterparty;
  }

  if (options.startTime) {
    appliedFilters.startTime = options.startTime.toISOString();
  }

  if (options.endTime) {
    appliedFilters.endTime = options.endTime.toISOString();
  }

  if (options.minAmount !== undefined) {
    appliedFilters.minAmount = options.minAmount;
  }

  if (options.maxAmount !== undefined) {
    appliedFilters.maxAmount = options.maxAmount;
  }

  if (options.fromBlock !== undefined) {
    appliedFilters.fromBlock = options.fromBlock;
  }

  if (options.toBlock !== undefined) {
    appliedFilters.toBlock = options.toBlock;
  }

  if (
    options.usdRateNow !== undefined &&
    (options.minUsd !== undefined || options.maxUsd !== undefined)
  ) {
    appliedFilters.usdRateNow = options.usdRateNow;
    if (options.minUsd !== undefined) {
      appliedFilters.minUsd = options.minUsd;
    }
    if (options.maxUsd !== undefined) {
      appliedFilters.maxUsd = options.maxUsd;
    }
  }

  if (options.sortBy === "amount" || options.sortBy === "usd") {
    appliedFilters.sortBy = options.sortBy;
    appliedFilters.sortDir = options.sortDir === "asc" ? "asc" : "desc";
  }

  const countResult = await databasePool.query(
    `SELECT COUNT(*)::bigint AS total
       FROM (
         SELECT tx_hash,
                token_symbol,
                block_height,
                timestamp,
                from_address,
                to_address
           FROM transactions
           ${whereClause}
          GROUP BY tx_hash,
                   token_symbol,
                   block_height,
                   timestamp,
                   from_address,
                   to_address
       ) grouped_transactions`,
    values,
  );

  values.push(pageSize, offset);
  const result = await databasePool.query(
    `SELECT MIN(id) AS id,
            tx_hash,
            NULL::integer AS event_index,
            ARRAY_AGG(event_index ORDER BY event_index) AS event_indexes,
            COUNT(*)::integer AS transfer_count,
            token_symbol,
            block_height,
            timestamp,
            from_address,
            to_address,
            SUM(amount) AS amount,
            SUM(amount_normalized) AS amount_normalized,
            CASE
              WHEN COUNT(*) = 1 THEN (JSONB_AGG(metadata ORDER BY event_index))->0
              ELSE JSONB_AGG(metadata ORDER BY event_index)
            END AS metadata
       FROM transactions
       ${whereClause}
      GROUP BY tx_hash,
               token_symbol,
               block_height,
               timestamp,
               from_address,
               to_address
      ORDER BY ${orderByClause}
      LIMIT $${values.length - 1} OFFSET $${values.length}`,
    values,
  );

  return {
    page,
    pageSize,
    total: Number(countResult.rows[0]?.total ?? 0),
    appliedFilters,
    items: result.rows.map(mapTransactionRow),
  };
}
