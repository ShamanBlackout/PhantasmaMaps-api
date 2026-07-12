import {
  Pool,
  type PoolClient,
  type PoolConfig,
  type QueryResult,
  type QueryResultRow,
} from "pg";
import { PhantasmaTS } from "phantasma-sdk-ts";
import {
  apiConfig,
  cacheDatabaseConfig,
  connectionPoolConfig,
  databaseConfig,
  syncConfig,
} from "./phantasma.config";
import {
  CHAIN_SYNC_TOKEN,
  type AddressSubgraphResult,
  type GraphEdgeRecord,
  type GraphNodeRecord,
  type PaginatedTransactionsResult,
  type ParsedTransfer,
  type SyncStateRecord,
  type TokenDailyMetricsRecord,
  type TokenMetadataRecord,
  type TokenMetadataUpsertInput,
  type TokenTopMoverRecord,
  type TopHoldersResult,
} from "./phantasma.types";

type DbConnectionConfig = {
  connectionString?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  ssl: boolean;
};

const isApiProcess = process.argv.some((arg) =>
  /(^|[\\/])startApiServer(\.ts|\.js)?$/i.test(String(arg || "")),
);

const isDirectDatabaseProcess = process.argv.some((arg) =>
  /(^|[\\/])(backfill|backfillDryRun|syncNodeBalancesNormalized|cleanupBlockClaims|testDatabaseInserts|labelingDryRun|labelingReviewSample|_temp_restore_fungible_amounts)(\.ts|\.js)?$/i.test(
    String(arg || ""),
  ),
);

function buildPoolConfig(
  config: DbConnectionConfig,
  options: { useExternalPooler?: boolean } = {},
): PoolConfig {
  const baseConfig: PoolConfig = {
    // Keep a very small local queue when the upstream DATABASE_URL is already a pooler.
    min: options.useExternalPooler ? 0 : 2,
    max: options.useExternalPooler ? 4 : 20,
    idleTimeoutMillis: 30000, // Close idle connections after 30 seconds
    connectionTimeoutMillis: 5000, // Wait up to 5 seconds to acquire a connection
    query_timeout: 30000, // Query timeout of 30 seconds
  };

  if (config.connectionString) {
    return {
      connectionString: config.connectionString,
      ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
      ...baseConfig,
    };
  }

  return {
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
    ...baseConfig,
  };
}

if (
  isDirectDatabaseProcess &&
  !databaseConfig.connectionString &&
  !(
    databaseConfig.host &&
    databaseConfig.port &&
    databaseConfig.user &&
    databaseConfig.database
  )
) {
  throw new Error(
    "Direct database connection is required for worker/sync jobs. Set DATABASE_URL (or PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE). CONNECTION_POOL is API-only.",
  );
}

if (isDirectDatabaseProcess && connectionPoolConfig.connectionString) {
  console.info(
    JSON.stringify({
      level: "info",
      event: "direct_database_mode",
      timestamp: new Date().toISOString(),
      message:
        "Worker/sync process detected; using DATABASE_URL/direct PG settings and ignoring CONNECTION_POOL.",
    }),
  );
}

const mainDatabaseConfig = isApiProcess ? connectionPoolConfig : databaseConfig;
const mainUsesExternalPooler =
  isApiProcess &&
  Boolean(connectionPoolConfig.connectionString) &&
  connectionPoolConfig.connectionString !== databaseConfig.connectionString;

export const databasePool = new Pool(
  buildPoolConfig(mainDatabaseConfig, {
    useExternalPooler: mainUsesExternalPooler,
  }),
);

const hasDedicatedCacheDatabase = Boolean(
  cacheDatabaseConfig.connectionString ||
  cacheDatabaseConfig.host ||
  cacheDatabaseConfig.database,
);

const cachePoolConfig = hasDedicatedCacheDatabase
  ? buildPoolConfig(cacheDatabaseConfig)
  : buildPoolConfig(mainDatabaseConfig, {
      useExternalPooler: mainUsesExternalPooler,
    });

// Use a smaller pool for cache because entries are short-lived and low-cost.
if (hasDedicatedCacheDatabase) {
  cachePoolConfig.min = 1;
  cachePoolConfig.max = 5;
}

const cacheQueryPool = new Pool(cachePoolConfig);

// Prevent process crashes when an idle pooled client disconnects unexpectedly.
databasePool.on("error", (error: Error) => {
  const maybeError = error as Error & {
    code?: string;
    errno?: string | number;
    syscall?: string;
    address?: string;
    port?: number;
  };
  console.error(
    JSON.stringify({
      level: "error",
      event: "database_pool_error",
      timestamp: new Date().toISOString(),
      message: maybeError.message,
      code: maybeError.code ?? null,
      errno: maybeError.errno ?? null,
      syscall: maybeError.syscall ?? null,
      address: maybeError.address ?? null,
      port: maybeError.port ?? null,
      pool: {
        totalCount: databasePool.totalCount,
        idleCount: databasePool.idleCount,
        waitingCount: databasePool.waitingCount,
      },
    }),
  );
});

cacheQueryPool.on("error", (error: Error) => {
  const maybeError = error as Error & {
    code?: string;
    errno?: string | number;
    syscall?: string;
    address?: string;
    port?: number;
  };
  console.error(
    JSON.stringify({
      level: "error",
      event: "cache_database_pool_error",
      timestamp: new Date().toISOString(),
      message: maybeError.message,
      code: maybeError.code ?? null,
      errno: maybeError.errno ?? null,
      syscall: maybeError.syscall ?? null,
      address: maybeError.address ?? null,
      port: maybeError.port ?? null,
      pool: {
        totalCount: cacheQueryPool.totalCount,
        idleCount: cacheQueryPool.idleCount,
        waitingCount: cacheQueryPool.waitingCount,
      },
    }),
  );
});

const RESTORE_BATCH_SIZE = 500;
const READ_RETRY_ATTEMPTS = 3;
const READ_RETRY_BASE_DELAY_MS = 150;

type RetryableDbError = Error & {
  code?: string;
  errno?: string | number;
  syscall?: string;
};

type DatabaseError = Error & {
  code?: string;
};

type CacheLookupStatus = "hit" | "miss" | "stale";

type CacheLookupResult = {
  status: CacheLookupStatus;
  payload: string | null;
};

let queryCacheTableMissingLogged = false;

function isUndefinedTableError(error: unknown): boolean {
  return (error as DatabaseError | undefined)?.code === "42P01";
}

function logQueryCacheTableMissingOnce(): void {
  if (queryCacheTableMissingLogged) {
    return;
  }

  queryCacheTableMissingLogged = true;
  console.warn(
    JSON.stringify({
      level: "warn",
      event: "query_cache_table_missing",
      timestamp: new Date().toISOString(),
      message:
        "api_query_cache table not found. Apply migration 013_query_cache.sql to enable persistent API caching.",
    }),
  );
}

function parseTokenSymbolFromCacheKey(cacheKey: string): string | null {
  const tokenScopedPrefixes = [
    "token-metadata:",
    "top-holders:",
    "token-graph-max:",
    "token-graph:",
    "address-subgraph:",
    "address-connections:",
    "trace-paths:",
  ];

  for (const prefix of tokenScopedPrefixes) {
    if (!cacheKey.startsWith(prefix)) {
      continue;
    }

    const remainder = cacheKey.slice(prefix.length);
    const tokenSymbol = remainder.split(":")[0]?.trim().toUpperCase();
    return tokenSymbol || null;
  }

  return null;
}

function isRetryableReadError(error: unknown): boolean {
  const candidate = error as RetryableDbError;
  const code = String(candidate?.code ?? "").toUpperCase();
  const errno = String(candidate?.errno ?? "").toUpperCase();
  const message = String(candidate?.message ?? "").toLowerCase();

  if (
    [
      "ECONNRESET",
      "ETIMEDOUT",
      "ECONNREFUSED",
      "EPIPE",
      "EHOSTUNREACH",
      "57P01",
      "57P02",
      "57P03",
      "53300",
      "08000",
      "08001",
      "08003",
      "08006",
    ].includes(code)
  ) {
    return true;
  }

  if (["ECONNRESET", "ETIMEDOUT", "ECONNREFUSED", "EPIPE"].includes(errno)) {
    return true;
  }

  return (
    message.includes("connection terminated unexpectedly") ||
    message.includes("server closed the connection unexpectedly") ||
    message.includes("connection reset") ||
    message.includes("timeout")
  );
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function queryReadWithRetry<T extends QueryResultRow = QueryResultRow>(
  text: string,
  values?: unknown[],
  context = "query",
): Promise<QueryResult<T>> {
  let attempt = 0;

  while (attempt < READ_RETRY_ATTEMPTS) {
    attempt += 1;

    try {
      return await databasePool.query<T>(text, values);
    } catch (error) {
      const retryable = isRetryableReadError(error);
      const isLastAttempt = attempt >= READ_RETRY_ATTEMPTS;

      if (!retryable || isLastAttempt) {
        throw error;
      }

      const backoffMs = READ_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1);
      const candidate = error as RetryableDbError;
      console.warn(
        JSON.stringify({
          level: "warn",
          event: "database_read_retry",
          timestamp: new Date().toISOString(),
          context,
          attempt,
          maxAttempts: READ_RETRY_ATTEMPTS,
          backoffMs,
          code: candidate?.code ?? null,
          message: candidate?.message ?? String(error),
        }),
      );
      await delay(backoffMs);
    }
  }

  throw new Error("unreachable retry state");
}

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
    holderCount: Number(row.holder_count ?? 0),
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
    eventKind: row.event_kind === null ? "transfer" : String(row.event_kind),
    tokenSymbol: String(row.token_symbol),
    blockHeight: Number(row.block_height),
    timestamp: row.timestamp,
    fromAddress: String(row.from_address),
    toAddress: String(row.to_address),
    relatedAddress:
      row.related_address === null ? null : String(row.related_address),
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
    event_kind: row.event_kind === null ? "transfer" : String(row.event_kind),
    token_symbol: String(row.token_symbol),
    block_height: Number(row.block_height),
    from_address: String(row.from_address),
    to_address: String(row.to_address),
    related_address:
      row.related_address === null ? null : String(row.related_address),
    amount_normalized:
      row.amount_normalized === null ? null : String(row.amount_normalized),
  };
}

export async function closeDatabasePool(): Promise<void> {
  await Promise.all([databasePool.end(), cacheQueryPool.end()]);
}

export async function testDatabaseConnection(): Promise<void> {
  await queryReadWithRetry("SELECT 1", [], "health_check");
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

export async function getTrackedPositiveNodeBalances(
  tokenSymbol: string,
): Promise<Array<{ address: string; tokenSymbol: string; balance: string }>> {
  const result = await databasePool.query<{
    address: string;
    token_symbol: string;
    balance: string;
  }>(
    `SELECT address,
            token_symbol,
            balance::text AS balance
       FROM nodes
      WHERE token_symbol = $1
        AND balance IS NOT NULL
        AND balance > 0
      ORDER BY address ASC`,
    [tokenSymbol],
  );

  return result.rows.map((row) => ({
    address: row.address,
    tokenSymbol: row.token_symbol,
    balance: String(row.balance),
  }));
}

export async function updateTrackedNodeBalances(
  client: PoolClient,
  items: Array<{ address: string; tokenSymbol: string; balance: string }>,
): Promise<number> {
  if (items.length === 0) return 0;

  // Batch updates into groups of 100 for better performance
  const BATCH_SIZE = 100;
  let updatedCount = 0;

  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    const batch = items.slice(i, i + BATCH_SIZE);
    const values: Array<unknown> = [];
    const valuesList: string[] = [];

    for (let j = 0; j < batch.length; j++) {
      const item = batch[j];
      const baseIndex = j * 3 + 1;
      valuesList.push(
        `($${baseIndex}::text, $${baseIndex + 1}::text, $${baseIndex + 2}::numeric)`,
      );
      values.push(item.address, item.tokenSymbol, item.balance);
    }

    const result = await client.query(
      `UPDATE nodes n
        SET balance = updates.balance
       FROM (VALUES ${valuesList.join(", ")}) AS updates(address, token_symbol, balance)
       WHERE n.address = updates.address
         AND n.token_symbol = updates.token_symbol
         AND n.balance IS DISTINCT FROM updates.balance`,
      values,
    );

    updatedCount += result.rowCount ?? 0;
  }

  return updatedCount;
}

export async function syncNodeBalancesNormalizedForToken(
  tokenSymbol: string,
): Promise<number> {
  const usingMetadataResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE nodes n
          SET balance_normalized = CASE
            WHEN tm.decimals <= 0 THEN n.balance
            ELSE n.balance / POWER(10::numeric, tm.decimals)
          END
         FROM token_metadata tm
        WHERE tm.token_symbol = n.token_symbol
          AND n.token_symbol = $1
          AND n.balance IS NOT NULL
          AND n.balance_normalized IS DISTINCT FROM CASE
            WHEN tm.decimals <= 0 THEN n.balance
            ELSE n.balance / POWER(10::numeric, tm.decimals)
          END
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
    [tokenSymbol],
  );

  const fallbackResult = await databasePool.query<{ count: string }>(
    `WITH updated AS (
       UPDATE nodes n
          SET balance_normalized = n.balance
        WHERE n.token_symbol = $1
          AND n.balance IS NOT NULL
          AND NOT EXISTS (
            SELECT 1
              FROM token_metadata tm
             WHERE tm.token_symbol = n.token_symbol
          )
          AND n.balance_normalized IS DISTINCT FROM n.balance
      RETURNING 1
     )
     SELECT COUNT(*)::text AS count FROM updated`,
    [tokenSymbol],
  );

  return (
    Number(usingMetadataResult.rows[0]?.count ?? 0) +
    Number(fallbackResult.rows[0]?.count ?? 0)
  );
}

export async function upsertTransfers(
  client: PoolClient,
  transfers: ParsedTransfer[],
  tokenMetadataBySymbol: Map<
    string,
    Pick<TokenMetadataUpsertInput, "decimals" | "flags">
  >,
): Promise<void> {
  if (transfers.length === 0) return;

  // Batch inserts into groups of 100 for better performance
  const BATCH_SIZE = 100;
  for (let i = 0; i < transfers.length; i += BATCH_SIZE) {
    const batch = transfers.slice(i, i + BATCH_SIZE);
    const values: Array<unknown> = [];
    const placeholders: string[] = [];

    for (let j = 0; j < batch.length; j++) {
      const transfer = batch[j];
      const storedAmounts = resolveStoredTransferAmounts(
        transfer.amount,
        tokenMetadataBySymbol.get(transfer.tokenSymbol),
      );

      const baseIndex = j * 10 + 1;
      placeholders.push(
        `($${baseIndex}, $${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, $${baseIndex + 9})`,
      );

      values.push(
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
      );
    }

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
       ) VALUES ${placeholders.join(", ")}
       ON CONFLICT (tx_hash, event_index) DO UPDATE
         SET token_symbol = EXCLUDED.token_symbol,
             block_height = EXCLUDED.block_height,
             timestamp = EXCLUDED.timestamp,
             from_address = EXCLUDED.from_address,
             to_address = EXCLUDED.to_address,
             amount = EXCLUDED.amount,
             amount_normalized = EXCLUDED.amount_normalized,
             metadata = EXCLUDED.metadata`,
      values,
    );
  }
}

export async function upsertTokenLedgerEvents(
  client: PoolClient,
  events: Array<{
    eventIndex: number;
    txHash: string;
    blockHeight: number;
    timestamp: Date;
    tokenSymbol: string;
    address: string;
    relatedAddress: string | null;
    eventKind: "burn" | "mint";
    amount: string;
    metadata: Record<string, unknown>;
  }>,
  tokenMetadataBySymbol: Map<
    string,
    Pick<TokenMetadataUpsertInput, "decimals" | "flags">
  >,
): Promise<void> {
  if (events.length === 0) {
    return;
  }

  const BATCH_SIZE = 100;

  for (let i = 0; i < events.length; i += BATCH_SIZE) {
    const batch = events.slice(i, i + BATCH_SIZE);
    const values: Array<unknown> = [];
    const placeholders: string[] = [];

    for (let j = 0; j < batch.length; j++) {
      const event = batch[j];
      const storedAmounts = resolveStoredTransferAmounts(
        event.amount,
        tokenMetadataBySymbol.get(event.tokenSymbol),
      );
      const counterparty = event.relatedAddress ?? event.address;
      const baseIndex = j * 12 + 1;
      placeholders.push(
        `($${baseIndex}, $${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, $${baseIndex + 9}, $${baseIndex + 10}, $${baseIndex + 11})`,
      );

      values.push(
        event.txHash,
        event.eventIndex,
        event.eventKind,
        event.tokenSymbol,
        event.blockHeight,
        event.timestamp,
        event.eventKind === "burn" ? event.address : counterparty,
        event.eventKind === "burn" ? counterparty : event.address,
        event.relatedAddress,
        storedAmounts.amount,
        storedAmounts.amountNormalized,
        event.metadata,
      );
    }

    await client.query(
      `INSERT INTO transactions (
         tx_hash,
         event_index,
         event_kind,
         token_symbol,
         block_height,
         timestamp,
         from_address,
         to_address,
         related_address,
         amount,
         amount_normalized,
         metadata
       ) VALUES ${placeholders.join(", ")}
       ON CONFLICT (tx_hash, event_index) DO UPDATE
         SET event_kind = EXCLUDED.event_kind,
             token_symbol = EXCLUDED.token_symbol,
             block_height = EXCLUDED.block_height,
             timestamp = EXCLUDED.timestamp,
             from_address = EXCLUDED.from_address,
             to_address = EXCLUDED.to_address,
             related_address = EXCLUDED.related_address,
             amount = EXCLUDED.amount,
             amount_normalized = EXCLUDED.amount_normalized,
             metadata = EXCLUDED.metadata`,
      values,
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
  transfers: Array<{
    txHash: string;
    blockHeight: number;
    tokenSymbol: string;
    fromAddress: string;
    toAddress: string;
  }>,
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
): Promise<
  Array<{
    tokenSymbol: string;
    fromAddress: string;
    toAddress: string;
    amountNormalized: string;
  }>
> {
  const insertedEdges: Array<{
    tokenSymbol: string;
    fromAddress: string;
    toAddress: string;
    amountNormalized: string;
  }> = [];

  for (const transfer of transfers) {
    const storedAmounts = resolveStoredTransferAmounts(
      transfer.amount,
      tokenMetadataBySymbol.get(transfer.tokenSymbol),
    );

    const result = await client.query<{
      token_symbol: string;
      from_address: string;
      to_address: string;
      amount_normalized: string;
    }>(
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
       ON CONFLICT (tx_hash, event_index) DO NOTHING
       RETURNING token_symbol, from_address, to_address, amount_normalized`,
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

    if (result.rowCount && result.rowCount > 0) {
      const row = result.rows[0];
      insertedEdges.push({
        tokenSymbol: String(row.token_symbol),
        fromAddress: String(row.from_address),
        toAddress: String(row.to_address),
        amountNormalized: String(row.amount_normalized ?? "0"),
      });
    }
  }

  return insertedEdges;
}

export async function upsertAddressConnections(
  client: PoolClient,
  edges: Array<{
    tokenSymbol: string;
    fromAddress: string;
    toAddress: string;
    amountNormalized: string;
  }>,
): Promise<void> {
  if (edges.length === 0) {
    return;
  }

  const BATCH_SIZE = 300;

  for (let i = 0; i < edges.length; i += BATCH_SIZE) {
    const batch = edges.slice(i, i + BATCH_SIZE);
    const values: Array<unknown> = [];
    const placeholders: string[] = [];
    let rowIndex = 0;

    for (const edge of batch) {
      const baseIndexA = rowIndex * 5 + 1;
      placeholders.push(
        `($${baseIndexA}, $${baseIndexA + 1}, $${baseIndexA + 2}, $${baseIndexA + 3}, $${baseIndexA + 4})`,
      );
      values.push(
        edge.tokenSymbol,
        edge.fromAddress,
        edge.toAddress,
        edge.amountNormalized,
        1,
      );
      rowIndex += 1;

      const baseIndexB = rowIndex * 5 + 1;
      placeholders.push(
        `($${baseIndexB}, $${baseIndexB + 1}, $${baseIndexB + 2}, $${baseIndexB + 3}, $${baseIndexB + 4})`,
      );
      values.push(
        edge.tokenSymbol,
        edge.toAddress,
        edge.fromAddress,
        edge.amountNormalized,
        1,
      );
      rowIndex += 1;
    }

    await client.query(
      `WITH input_rows (token_symbol, address, counterparty, total_volume, transaction_count) AS (
         VALUES ${placeholders.join(", ")}
       ),
       aggregated AS (
         SELECT token_symbol,
                address,
                counterparty,
                SUM(total_volume::numeric) AS total_volume,
                SUM(transaction_count::integer) AS transaction_count
           FROM input_rows
          GROUP BY token_symbol, address, counterparty
       )
       INSERT INTO address_connections (
         token_symbol,
         address,
         counterparty,
         total_volume,
         transaction_count,
         last_updated
       )
       SELECT token_symbol,
              address,
              counterparty,
              total_volume,
              transaction_count,
              NOW()
         FROM aggregated
       ON CONFLICT (token_symbol, address, counterparty) DO UPDATE
         SET total_volume = address_connections.total_volume + EXCLUDED.total_volume,
             transaction_count = address_connections.transaction_count + EXCLUDED.transaction_count,
             last_updated = NOW()`,
      values,
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
    `WITH node_balances AS (
       SELECT address,
              balance AS net_balance
         FROM public.nodes
        WHERE token_symbol = $1
          AND balance IS NOT NULL
          AND balance > 0
     ),
     tx_net_balances AS (
       SELECT address,
              SUM(received) - SUM(sent) AS net_balance
         FROM (
           SELECT to_address AS address,
                  COALESCE(SUM(amount), 0) AS received,
                  0::numeric               AS sent
             FROM public.transactions
            WHERE token_symbol = $1
            GROUP BY to_address
           UNION ALL
           SELECT from_address AS address,
                  0::numeric             AS received,
                  COALESCE(SUM(amount), 0) AS sent
             FROM public.transactions
            WHERE token_symbol = $1
            GROUP BY from_address
         ) t
        GROUP BY address
       HAVING SUM(received) - SUM(sent) > 0
     )
     SELECT address,
            net_balance
       FROM (
         SELECT *
           FROM node_balances
         UNION ALL
         SELECT *
           FROM tx_net_balances
          WHERE NOT EXISTS (SELECT 1 FROM node_balances)
       ) holders
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

async function getTopHolderGraphNodes(
  tokenSymbol: string,
  limit: number,
): Promise<GraphNodeRecord[]> {
  const result = await databasePool.query<{
    address: string;
    balance: string;
    balance_normalized: string | null;
    label: string | null;
    metadata: Record<string, unknown> | null;
    decimals: number | null;
  }>(
    `WITH node_balances AS (
       SELECT address,
              balance AS net_balance,
              balance_normalized,
              label,
              metadata
         FROM public.nodes
        WHERE token_symbol = $1
          AND balance IS NOT NULL
          AND balance > 0
     ),
     tx_net_balances AS (
       SELECT address,
              SUM(received) - SUM(sent) AS net_balance
         FROM (
           SELECT to_address AS address,
                  COALESCE(SUM(amount), 0) AS received,
                  0::numeric AS sent
             FROM public.transactions
            WHERE token_symbol = $1
            GROUP BY to_address
           UNION ALL
           SELECT from_address AS address,
                  0::numeric AS received,
                  COALESCE(SUM(amount), 0) AS sent
             FROM public.transactions
            WHERE token_symbol = $1
            GROUP BY from_address
         ) t
        GROUP BY address
       HAVING SUM(received) - SUM(sent) > 0
     ),
     holders AS (
       SELECT address,
              net_balance,
              balance_normalized,
              label,
              metadata
         FROM node_balances
       UNION ALL
       SELECT address,
              net_balance,
              NULL::numeric AS balance_normalized,
              NULL::text AS label,
              NULL::jsonb AS metadata
         FROM tx_net_balances
        WHERE NOT EXISTS (SELECT 1 FROM node_balances)
     )
     SELECT holders.address,
            holders.net_balance::text AS balance,
            holders.balance_normalized::text AS balance_normalized,
            holders.label,
            holders.metadata,
            tm.decimals
       FROM holders
      LEFT JOIN public.token_metadata tm
         ON tm.token_symbol = $1
      ORDER BY holders.net_balance DESC
      LIMIT $2`,
    [tokenSymbol, limit],
  );

  return result.rows.map((row) => ({
    address: String(row.address),
    tokenSymbol,
    balance: String(row.balance),
    balanceNormalized:
      row.balance_normalized !== null
        ? String(row.balance_normalized)
        : normalizeRawAmount(String(row.balance), Number(row.decimals ?? 0)),
    label: row.label === null ? null : String(row.label),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  }));
}

export async function getAvailableTokens(): Promise<string[]> {
  const result = await queryReadWithRetry<{ token_symbol: string }>(
    `SELECT DISTINCT token_symbol
      FROM public.transactions
      WHERE token_symbol <> $1
      ORDER BY token_symbol ASC`,
    [CHAIN_SYNC_TOKEN],
    "get_available_tokens",
  );

  return result.rows.map((row) => row.token_symbol);
}

export async function getTokenMetadata(
  tokenSymbol: string,
): Promise<TokenMetadataRecord | null> {
  const result = await queryReadWithRetry(
    `SELECT token_symbol,
            name,
            decimals,
            (
              SELECT COUNT(1)
                FROM public.nodes
               WHERE token_symbol = tm.token_symbol
                 AND COALESCE(balance, 0) > 0
            )::bigint AS holder_count,
            current_supply_raw,
            current_supply_normalized,
            max_supply_raw,
            max_supply_normalized,
            flags,
            metadata,
            updated_at
      FROM public.token_metadata tm
      WHERE token_symbol = $1`,
    [tokenSymbol],
    "get_token_metadata",
  );

  if (result.rowCount === 0) {
    return null;
  }

  return mapTokenMetadataRow(result.rows[0]);
}

export async function getFullTokenGraph(
  tokenSymbol: string,
  options: { includeTopHoldersLimit?: number; edgeLimit?: number } = {},
): Promise<AddressSubgraphResult> {
  const includeTopHoldersLimit = Math.max(
    0,
    Math.floor(Number(options.includeTopHoldersLimit ?? 0) || 0),
  );
  const edgeLimit = Number.isFinite(Number(options.edgeLimit))
    ? Math.max(0, Math.floor(Number(options.edgeLimit) || 0))
    : 0;
  const edgesQuery =
    edgeLimit > 0
      ? {
          text: `SELECT id, token_symbol, from_address, to_address, amount, amount_normalized, tx_hash, event_index
                   FROM public.edges
                  WHERE token_symbol = $1
                  ORDER BY id ASC
                  LIMIT $2`,
          values: [tokenSymbol, edgeLimit],
        }
      : {
          text: `SELECT id, token_symbol, from_address, to_address, amount, amount_normalized, tx_hash, event_index
                   FROM public.edges
                  WHERE token_symbol = $1
                  ORDER BY id ASC`,
          values: [tokenSymbol],
        };
  const edgesResult = await queryReadWithRetry(
    edgesQuery.text,
    edgesQuery.values,
    "get_full_token_graph_edges",
  );

  const edges = edgesResult.rows.map(mapGraphEdgeRow);
  const addressSet = new Set<string>();

  for (const edge of edges) {
    addressSet.add(edge.fromAddress);
    addressSet.add(edge.toAddress);
  }

  const nodesResult = addressSet.size
    ? await queryReadWithRetry(
        `SELECT address, token_symbol, balance, balance_normalized, label, metadata
           FROM public.nodes
          WHERE token_symbol = $1
            AND address = ANY($2::text[])
          ORDER BY address ASC`,
        [tokenSymbol, [...addressSet]],
        "get_full_token_graph_nodes",
      )
    : { rows: [] as QueryResultRow[] };

  const nodes = nodesResult.rows.map(mapGraphNodeRow);

  if (includeTopHoldersLimit > 0) {
    const topHolderNodes = await getTopHolderGraphNodes(
      tokenSymbol,
      includeTopHoldersLimit,
    );
    const nodeMap = new Map(nodes.map((node) => [node.address, node]));

    for (const node of topHolderNodes) {
      if (!nodeMap.has(node.address)) {
        nodeMap.set(node.address, node);
      }
    }

    return {
      tokenSymbol,
      rootAddress: "",
      depth: 0,
      nodes: [...nodeMap.values()],
      edges,
    };
  }

  return {
    tokenSymbol,
    rootAddress: "",
    depth: 0,
    nodes,
    edges,
  };
}

// LRU-style fixed-size cache for address subgraph traversal results.
// The recursive CTE is the most CPU-intensive query; memoizing it for
// 60 seconds eliminates re-computation for repeated or concurrent requests
// for the same address (e.g., multiple users viewing the same whale wallet).
const SUBGRAPH_CACHE_MAX = 200;
const SUBGRAPH_CACHE_TTL_MS = 60_000;
const subgraphCache = new Map<
  string,
  { result: AddressSubgraphResult; expiresAt: number }
>();

function subgraphCacheGet(key: string): AddressSubgraphResult | null {
  const entry = subgraphCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    subgraphCache.delete(key);
    return null;
  }
  return entry.result;
}

function subgraphCacheSet(key: string, result: AddressSubgraphResult): void {
  // Evict oldest entry when at capacity
  if (subgraphCache.size >= SUBGRAPH_CACHE_MAX) {
    subgraphCache.delete(subgraphCache.keys().next().value!);
  }
  subgraphCache.set(key, {
    result,
    expiresAt: Date.now() + SUBGRAPH_CACHE_TTL_MS,
  });
}

export function clearSubgraphCache(): void {
  subgraphCache.clear();
}

export async function getCachedApiResponse(
  cacheKey: string,
): Promise<CacheLookupResult> {
  try {
    const result = await cacheQueryPool.query<{
      payload: string;
      expires_at: Date;
      updated_at: Date;
    }>(
      `SELECT payload::text AS payload,
              expires_at,
              updated_at
         FROM public.api_query_cache
        WHERE cache_key = $1
        LIMIT 1`,
      [cacheKey],
    );

    if (result.rowCount === 0) {
      return {
        status: "miss",
        payload: null,
      };
    }

    const cacheRow = result.rows[0];
    const expiresAt = new Date(cacheRow.expires_at).getTime();

    const now = Date.now();
    const staleAgeMs = Number.isFinite(expiresAt) ? now - expiresAt : Infinity;
    const canServeStale =
      apiConfig.cacheServeStale &&
      Number.isFinite(staleAgeMs) &&
      staleAgeMs >= 0 &&
      staleAgeMs <= apiConfig.cacheStaleMaxMs;

    if (!Number.isFinite(expiresAt)) {
      return {
        status: "stale",
        payload: null,
      };
    }

    if (now > expiresAt) {
      return {
        status: "stale",
        payload: canServeStale ? cacheRow.payload : null,
      };
    }

    const tokenSymbol = parseTokenSymbolFromCacheKey(cacheKey);
    if (tokenSymbol) {
      const tokenSyncStateResult = await queryReadWithRetry<{
        updated_at: Date;
      }>(
        `SELECT updated_at
           FROM public.sync_state
          WHERE token_symbol = $1
          LIMIT 1`,
        [tokenSymbol],
        "cache_freshness_token_sync_state",
      );

      if (tokenSyncStateResult.rows.length > 0) {
        const mainUpdatedAt = new Date(
          tokenSyncStateResult.rows[0].updated_at,
        ).getTime();
        const cacheUpdatedAt = new Date(cacheRow.updated_at).getTime();

        if (
          Number.isFinite(mainUpdatedAt) &&
          Number.isFinite(cacheUpdatedAt) &&
          cacheUpdatedAt < mainUpdatedAt
        ) {
          return {
            status: "stale",
            payload: apiConfig.cacheServeStale ? cacheRow.payload : null,
          };
        }
      }
    }

    return {
      status: "hit",
      payload: cacheRow.payload,
    };
  } catch (error) {
    if (isUndefinedTableError(error)) {
      logQueryCacheTableMissingOnce();
      return {
        status: "miss",
        payload: null,
      };
    }

    throw error;
  }
}

export async function setCachedApiResponse(
  cacheKey: string,
  payloadJson: string,
  ttlMs: number,
): Promise<void> {
  if (ttlMs <= 0) {
    return;
  }

  try {
    await cacheQueryPool.query(
      `INSERT INTO public.api_query_cache (cache_key, payload, expires_at)
       VALUES (
         $1,
         $2::jsonb,
         NOW() + (($3::bigint) * INTERVAL '1 millisecond')
       )
       ON CONFLICT (cache_key)
       DO UPDATE SET
         payload = EXCLUDED.payload,
         expires_at = EXCLUDED.expires_at,
         updated_at = NOW()`,
      [cacheKey, payloadJson, ttlMs],
    );
  } catch (error) {
    if (isUndefinedTableError(error)) {
      logQueryCacheTableMissingOnce();
      return;
    }

    throw error;
  }
}

export async function clearApiQueryCache(): Promise<void> {
  try {
    await cacheQueryPool.query("DELETE FROM public.api_query_cache");
  } catch (error) {
    if (isUndefinedTableError(error)) {
      logQueryCacheTableMissingOnce();
      return;
    }

    throw error;
  }
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

  const cacheKey = `${tokenSymbol}:${rootAddress}:${depth}:${edgeLimit}`;
  const cached = subgraphCacheGet(cacheKey);
  if (cached) return cached;

  const edgesResult =
    depth === 1
      ? await queryReadWithRetry(
          `SELECT id,
                  token_symbol,
                  from_address,
                  to_address,
                  amount,
                  amount_normalized,
                  tx_hash,
                  event_index
             FROM public.edges
            WHERE token_symbol = $1
              AND (from_address = $2 OR to_address = $2)
            ORDER BY amount_normalized DESC, id ASC
            LIMIT $3`,
          [tokenSymbol, rootAddress, edgeLimit],
          "get_address_subgraph_edges_depth1",
        )
      : await queryReadWithRetry(
          `WITH RECURSIVE walk AS (
             SELECT $2::text AS address, 0 AS depth
             UNION ALL
             SELECT CASE
                      WHEN e.from_address = walk.address THEN e.to_address
                      ELSE e.from_address
                    END AS address,
                    walk.depth + 1 AS depth
               FROM walk
               JOIN public.edges e
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
                    LEAST(from_depth.depth, to_depth.depth) AS edge_depth
               FROM public.edges e
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
                    event_index
               FROM ranked_edges
              ORDER BY edge_depth ASC, id ASC
              LIMIT $4
           )
           SELECT * FROM limited_edges
           ORDER BY id ASC`,
          [tokenSymbol, rootAddress, depth, edgeLimit],
          "get_address_subgraph_edges",
        );

  const edges = edgesResult.rows.map(mapGraphEdgeRow);
  const addressSet = new Set<string>([rootAddress]);

  for (const edge of edges) {
    addressSet.add(edge.fromAddress);
    addressSet.add(edge.toAddress);
  }

  const nodesResult = await queryReadWithRetry(
    `SELECT address, token_symbol, balance, balance_normalized, label, metadata
      FROM public.nodes
      WHERE token_symbol = $1
        AND address = ANY($2::text[])
      ORDER BY address ASC`,
    [tokenSymbol, [...addressSet]],
    "get_address_subgraph_nodes",
  );

  const subgraphResult = {
    tokenSymbol,
    rootAddress,
    depth,
    nodes: nodesResult.rows.map(mapGraphNodeRow),
    edges,
  };

  subgraphCacheSet(cacheKey, subgraphResult);
  return subgraphResult;
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

  // Combine count and data queries into a single query using window function
  // This reduces database round trips by 50% for this endpoint
  values.push(pageSize, offset);
  const result = await queryReadWithRetry<{
    id: string;
    tx_hash: string;
    event_index: number;
    event_indexes: number[];
    transfer_count: number;
    event_kind: string | null;
    token_symbol: string;
    block_height: string;
    timestamp: string;
    from_address: string;
    to_address: string;
    related_address: string | null;
    amount: string;
    amount_normalized: string;
    metadata: unknown;
    total: string;
  }>(
    `SELECT MIN(id) AS id,
            tx_hash,
            NULL::integer AS event_index,
            ARRAY_AGG(event_index ORDER BY event_index) AS event_indexes,
            COUNT(*)::integer AS transfer_count,
            MIN(event_kind) AS event_kind,
            token_symbol,
            block_height,
            timestamp,
            from_address,
            to_address,
            MIN(related_address) AS related_address,
            SUM(amount) AS amount,
            SUM(amount_normalized) AS amount_normalized,
            CASE
              WHEN COUNT(*) = 1 THEN (JSONB_AGG(metadata ORDER BY event_index))->0
              ELSE JSONB_AGG(metadata ORDER BY event_index)
            END AS metadata,
            COUNT(*) OVER () AS total
      FROM public.transactions
       ${whereClause}
      GROUP BY tx_hash,
               event_kind,
               token_symbol,
               block_height,
               timestamp,
               from_address,
               to_address,
               related_address
      ORDER BY ${orderByClause}
      LIMIT $${values.length - 1} OFFSET $${values.length}`,
    values,
    "get_transactions_page",
  );

  // Extract total from first row (COUNT(*) OVER() returns same value for all rows)
  const total = result.rows.length > 0 ? Number(result.rows[0].total) : 0;

  return {
    page,
    pageSize,
    total,
    appliedFilters,
    items: result.rows.map(mapTransactionRow),
  };
}

export async function getAddressActivity(
  tokenSymbol: string,
  address: string,
  days: number,
): Promise<{ date: string; txCount: number; volume: number }[]> {
  const result = await queryReadWithRetry(
    `SELECT
       date_trunc('day', timestamp)::date::text AS date,
       COUNT(*) AS tx_count,
       COALESCE(SUM(amount_normalized), 0) AS volume
     FROM transactions
     WHERE token_symbol = $1
       AND (from_address = $2 OR to_address = $2)
       AND timestamp >= NOW() - ($3 || ' days')::interval
     GROUP BY date_trunc('day', timestamp)::date
     ORDER BY date_trunc('day', timestamp)::date ASC`,
    [tokenSymbol, address, days],
    "get_address_activity",
  );

  return result.rows.map((row) => ({
    date: String(row.date),
    txCount: Number(row.tx_count),
    volume: Number(row.volume),
  }));
}

export async function getAddressConnections(
  tokenSymbol: string,
  address: string,
): Promise<
  Array<{
    counterparty: string;
    totalVolume: number;
    transactionCount: number;
  }>
> {
  const result = await queryReadWithRetry(
    `SELECT
       counterparty,
       total_volume,
       transaction_count
    FROM public.address_connections
     WHERE token_symbol = $1
       AND address = $2
     ORDER BY total_volume DESC`,
    [tokenSymbol, address],
    "get_address_connections",
  );

  return result.rows.map((row) => ({
    counterparty: String(row.counterparty),
    totalVolume: Number(row.total_volume),
    transactionCount: Number(row.transaction_count),
  }));
}

export async function findAddressPaths(options: {
  tokenSymbol: string;
  fromAddress: string;
  toAddress: string;
  maxHops: number;
  pathLimit: number;
  stopAtTerminals?: boolean;
}): Promise<
  Array<{
    nodePath: string[];
    hopCount: number;
    totalVolume: number;
  }>
> {
  const maxHops = Math.max(1, Math.min(Math.floor(options.maxHops), 8));
  const pathLimit = Math.max(1, Math.min(Math.floor(options.pathLimit), 500));
  const stopAtTerminals = options.stopAtTerminals !== false;
  const fromAddress = String(options.fromAddress || "").trim();
  const toAddress = String(options.toAddress || "").trim();

  if (!fromAddress || !toAddress || fromAddress === toAddress) {
    return [];
  }

  const adjacency = new Map<
    string,
    Array<{ nextAddress: string; totalVolume: number }>
  >();

  const visited = new Set<string>([fromAddress]);
  let frontier = new Set<string>([fromAddress]);
  const MAX_DISCOVERED_ADDRESSES = 25000;

  for (let depth = 0; depth < maxHops && frontier.size > 0; depth += 1) {
    const frontierAddresses = [...frontier];
    const frontierResult = await queryReadWithRetry<{
      address: string;
      counterparty: string;
      total_volume: string | number;
    }>(
      `SELECT address, counterparty, total_volume
         FROM public.address_connections
        WHERE token_symbol = $1
          AND address = ANY($2::text[])`,
      [options.tokenSymbol, frontierAddresses],
      "find_address_paths_frontier",
    );

    const nextFrontier = new Set<string>();

    for (const row of frontierResult.rows) {
      const address = String(row.address || "").trim();
      const counterparty = String(row.counterparty || "").trim();
      if (!address || !counterparty) continue;

      if (!adjacency.has(address)) {
        adjacency.set(address, []);
      }
      adjacency.get(address)?.push({
        nextAddress: counterparty,
        totalVolume: Number(row.total_volume) || 0,
      });

      if (!visited.has(counterparty)) {
        visited.add(counterparty);
        nextFrontier.add(counterparty);
      }
    }

    frontier = nextFrontier;
    if (visited.size >= MAX_DISCOVERED_ADDRESSES) {
      break;
    }
  }

  const terminalAddressSet = new Set<string>();
  if (stopAtTerminals) {
    const terminalLabelRows = await queryReadWithRetry<{
      address: string;
      label_type: string | null;
      label: string | null;
    }>(
      `SELECT address, label_type, label
         FROM public.nodes
        WHERE token_symbol = $1
          AND address = ANY($2::text[])`,
      [options.tokenSymbol, [...visited]],
      "find_address_paths_terminal_labels",
    );

    terminalLabelRows.rows.forEach((row) => {
      const address = String(row.address || "").trim();
      if (!address) return;

      const labelType = String(row.label_type || "")
        .trim()
        .toLowerCase();
      const label = String(row.label || "")
        .trim()
        .toLowerCase();

      const isHub = labelType.includes("hub") || label.includes("hub");
      const isHighInbound =
        labelType.includes("high_inbound") ||
        labelType.includes("high inbound") ||
        label.includes("high_inbound") ||
        label.includes("high inbound");
      const isHighOutbound =
        labelType.includes("high_outbound") ||
        labelType.includes("high outbound") ||
        label.includes("high_outbound") ||
        label.includes("high outbound");

      if (isHub || isHighInbound || isHighOutbound) {
        terminalAddressSet.add(address);
      }
    });
  }

  const results: Array<{
    nodePath: string[];
    hopCount: number;
    totalVolume: number;
  }> = [];

  function dfs(
    currentAddress: string,
    path: string[],
    totalVolume: number,
  ): void {
    if (results.length >= pathLimit) {
      return;
    }

    const hopCount = Math.max(0, path.length - 1);
    if (hopCount > maxHops) {
      return;
    }

    if (currentAddress === toAddress && hopCount > 0) {
      results.push({
        nodePath: [...path],
        hopCount,
        totalVolume,
      });
      return;
    }

    if (
      stopAtTerminals &&
      currentAddress !== fromAddress &&
      terminalAddressSet.has(currentAddress)
    ) {
      if (hopCount > 0) {
        results.push({
          nodePath: [...path],
          hopCount,
          totalVolume,
        });
      }
      return;
    }

    if (hopCount === maxHops) {
      return;
    }

    const neighbors = adjacency.get(currentAddress) || [];
    for (const neighbor of neighbors) {
      if (path.includes(neighbor.nextAddress)) {
        continue;
      }

      dfs(
        neighbor.nextAddress,
        [...path, neighbor.nextAddress],
        totalVolume + (Number(neighbor.totalVolume) || 0),
      );

      if (results.length >= pathLimit) {
        return;
      }
    }
  }

  dfs(fromAddress, [fromAddress], 0);

  results.sort(
    (left, right) =>
      left.hopCount - right.hopCount || right.totalVolume - left.totalVolume,
  );

  return results;
}

export async function getLabeledNodes(options: {
  tokenSymbol?: string;
  label?: string;
  labelType?: string;
  labelSource?: string;
  minConfidence?: number;
  maxConfidence?: number;
  updatedSince?: Date;
  windowDays?: number;
  page: number;
  pageSize: number;
}): Promise<{
  page: number;
  pageSize: number;
  total: number;
  appliedFilters: Record<string, unknown>;
  items: Array<Record<string, unknown>>;
}> {
  const filters: string[] = ["n.label IS NOT NULL"];
  const values: Array<string | number> = [];
  const appliedFilters: Record<string, unknown> = {};

  if (options.tokenSymbol) {
    values.push(options.tokenSymbol);
    filters.push(`n.token_symbol = $${values.length}`);
    appliedFilters.tokenSymbol = options.tokenSymbol;
  }

  if (options.label) {
    values.push(options.label);
    filters.push(`n.label = $${values.length}`);
    appliedFilters.label = options.label;
  }

  if (options.labelType) {
    values.push(options.labelType);
    filters.push(`n.label_type = $${values.length}`);
    appliedFilters.labelType = options.labelType;
  }

  if (options.labelSource) {
    values.push(options.labelSource);
    filters.push(`n.label_source = $${values.length}`);
    appliedFilters.labelSource = options.labelSource;
  }

  if (options.minConfidence !== undefined) {
    values.push(options.minConfidence);
    filters.push(
      `COALESCE(n.label_confidence, 0::numeric) >= $${values.length}::numeric`,
    );
    appliedFilters.minConfidence = options.minConfidence;
  }

  if (options.maxConfidence !== undefined) {
    values.push(options.maxConfidence);
    filters.push(
      `COALESCE(n.label_confidence, 0::numeric) <= $${values.length}::numeric`,
    );
    appliedFilters.maxConfidence = options.maxConfidence;
  }

  if (options.updatedSince) {
    values.push(options.updatedSince.toISOString());
    filters.push(`n.label_updated_at >= $${values.length}::timestamptz`);
    appliedFilters.updatedSince = options.updatedSince.toISOString();
  }

  const page = Math.max(options.page, 1);
  const pageSize = Math.max(options.pageSize, 1);
  const offset = (page - 1) * pageSize;
  const windowDays = Math.max(0, Math.floor(Number(options.windowDays ?? 30)));

  values.push(windowDays, pageSize, offset);

  const result = await databasePool.query<{
    token_symbol: string;
    address: string;
    label: string | null;
    label_type: string | null;
    label_source: string | null;
    label_version: string | null;
    label_confidence: string | null;
    label_updated_at: Date | null;
    label_evidence: Record<string, unknown> | null;
    score_window_days: number | null;
    in_tx_count: string | null;
    out_tx_count: string | null;
    in_unique_counterparties: string | null;
    out_unique_counterparties: string | null;
    in_volume: string | null;
    out_volume: string | null;
    in_percent_rank: string | null;
    out_percent_rank: string | null;
    in_z_score_log: string | null;
    out_z_score_log: string | null;
    in_mad_score_log: string | null;
    out_mad_score_log: string | null;
    computed_at: Date | null;
    total: string;
  }>(
    `WITH filtered AS (
       SELECT n.token_symbol,
              n.address,
              n.label,
              n.label_type,
              n.label_source,
              n.label_version,
              n.label_confidence,
              n.label_updated_at,
              n.label_evidence,
              s.window_days AS score_window_days,
              s.in_tx_count,
              s.out_tx_count,
              s.in_unique_counterparties,
              s.out_unique_counterparties,
              s.in_volume,
              s.out_volume,
              s.in_percent_rank,
              s.out_percent_rank,
              s.in_z_score_log,
              s.out_z_score_log,
              s.in_mad_score_log,
              s.out_mad_score_log,
              s.computed_at
         FROM nodes n
         LEFT JOIN node_label_scores s
           ON s.token_symbol = n.token_symbol
          AND s.address = n.address
          AND s.window_days = $${values.length - 2}::int
        WHERE ${filters.join(" AND ")}
     )
     SELECT token_symbol,
            address,
            label,
            label_type,
            label_source,
            label_version,
            label_confidence::text,
            label_updated_at,
            label_evidence,
            score_window_days,
            in_tx_count::text,
            out_tx_count::text,
            in_unique_counterparties::text,
            out_unique_counterparties::text,
            in_volume::text,
            out_volume::text,
            in_percent_rank::text,
            out_percent_rank::text,
            in_z_score_log::text,
            out_z_score_log::text,
            in_mad_score_log::text,
            out_mad_score_log::text,
            computed_at,
            COUNT(*) OVER ()::text AS total
       FROM filtered
      ORDER BY COALESCE(label_confidence, 0::numeric) DESC,
               COALESCE(label_updated_at, computed_at) DESC NULLS LAST,
               token_symbol ASC,
               address ASC
      LIMIT $${values.length - 1} OFFSET $${values.length}`,
    values,
  );

  const total = result.rows.length > 0 ? Number(result.rows[0].total) : 0;

  return {
    page,
    pageSize,
    total,
    appliedFilters: {
      ...appliedFilters,
      windowDays,
    },
    items: result.rows.map((row) => ({
      tokenSymbol: String(row.token_symbol),
      address: String(row.address),
      label: row.label === null ? null : String(row.label),
      labelType: row.label_type === null ? null : String(row.label_type),
      labelSource: row.label_source === null ? null : String(row.label_source),
      labelVersion:
        row.label_version === null ? null : String(row.label_version),
      labelConfidence:
        row.label_confidence === null ? null : Number(row.label_confidence),
      labelUpdatedAt: row.label_updated_at,
      labelEvidence:
        (row.label_evidence as Record<string, unknown> | null) ?? null,
      scoreSnapshot:
        row.score_window_days === null
          ? null
          : {
              windowDays: Number(row.score_window_days),
              inTxCount:
                row.in_tx_count === null ? null : Number(row.in_tx_count),
              outTxCount:
                row.out_tx_count === null ? null : Number(row.out_tx_count),
              inUniqueCounterparties:
                row.in_unique_counterparties === null
                  ? null
                  : Number(row.in_unique_counterparties),
              outUniqueCounterparties:
                row.out_unique_counterparties === null
                  ? null
                  : Number(row.out_unique_counterparties),
              inVolume: row.in_volume === null ? null : Number(row.in_volume),
              outVolume:
                row.out_volume === null ? null : Number(row.out_volume),
              inPercentRank:
                row.in_percent_rank === null
                  ? null
                  : Number(row.in_percent_rank),
              outPercentRank:
                row.out_percent_rank === null
                  ? null
                  : Number(row.out_percent_rank),
              inZScoreLog:
                row.in_z_score_log === null ? null : Number(row.in_z_score_log),
              outZScoreLog:
                row.out_z_score_log === null
                  ? null
                  : Number(row.out_z_score_log),
              inMadScoreLog:
                row.in_mad_score_log === null
                  ? null
                  : Number(row.in_mad_score_log),
              outMadScoreLog:
                row.out_mad_score_log === null
                  ? null
                  : Number(row.out_mad_score_log),
              computedAt: row.computed_at,
            },
    })),
  };
}

function normalizeBucketDate(bucketDate: Date): string {
  const year = bucketDate.getUTCFullYear();
  const month = String(bucketDate.getUTCMonth() + 1).padStart(2, "0");
  const day = String(bucketDate.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function mapTokenDailyMetricsRow(row: QueryResultRow): TokenDailyMetricsRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    bucketDate: String(row.bucket_date),
    holderCount: Number(row.holder_count ?? 0),
    newHolderCount: Number(row.new_holder_count ?? 0),
    lostHolderCount: Number(row.lost_holder_count ?? 0),
    activeWalletCount: Number(row.active_wallet_count ?? 0),
    transactionCount: Number(row.transaction_count ?? 0),
    transferVolume: Number(row.transfer_volume ?? 0),
    currentSupply: Number(row.current_supply ?? 0),
    top10Share: Number(row.top10_share ?? 0),
    top50Share: Number(row.top50_share ?? 0),
    topWalletShare: Number(row.top_wallet_share ?? 0),
    giniCoefficient: Number(row.gini_coefficient ?? 0),
    medianTransferAmount: Number(row.median_transfer_amount ?? 0),
    avgTransferAmount: Number(row.avg_transfer_amount ?? 0),
    updatedAt: row.updated_at ? new Date(row.updated_at) : null,
  };
}

function mapTokenTopMoverRow(row: QueryResultRow): TokenTopMoverRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    address: String(row.address),
    latestDate: String(row.latest_date),
    baselineDate: String(row.baseline_date),
    latestBalance: Number(row.latest_balance ?? 0),
    baselineBalance: Number(row.baseline_balance ?? 0),
    deltaBalance: Number(row.delta_balance ?? 0),
    deltaPct: Number(row.delta_pct ?? 0),
  };
}

export async function refreshTokenAnalyticsForDate(
  tokenSymbol: string,
  bucketDate = new Date(),
): Promise<void> {
  const normalizedBucketDate = normalizeBucketDate(bucketDate);

  await withDatabaseTransaction(async (client) => {
    await client.query(
      `DELETE FROM wallet_daily_balances
        WHERE token_symbol = $1
          AND bucket_date = $2::date`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `INSERT INTO wallet_daily_balances (
         token_symbol,
         address,
         bucket_date,
         balance,
         balance_normalized,
         share_of_supply,
         wallet_type,
         first_seen_at,
         last_seen_at,
         created_at,
         updated_at
       )
       WITH balance_ledger AS (
         SELECT
           address,
           SUM(delta) AS balance_normalized
         FROM (
           SELECT
             to_address AS address,
             COALESCE(amount_normalized, 0::numeric) AS delta
           FROM transactions
           WHERE token_symbol = $1
             AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
             AND COALESCE(to_address, '') <> ''
           UNION ALL
           SELECT
             from_address AS address,
             -COALESCE(amount_normalized, 0::numeric) AS delta
           FROM transactions
           WHERE token_symbol = $1
             AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
             AND COALESCE(from_address, '') <> ''
         ) deltas
         GROUP BY address
       )
       SELECT
         $1 AS token_symbol,
         bl.address,
         $2::date AS bucket_date,
         COALESCE(bl.balance_normalized, 0::numeric) AS balance,
         COALESCE(bl.balance_normalized, 0::numeric) AS balance_normalized,
         CASE
           WHEN COALESCE(tm.current_supply_normalized::numeric, 0::numeric) > 0
             THEN (COALESCE(bl.balance_normalized, 0::numeric) / tm.current_supply_normalized::numeric) * 100::numeric
           ELSE 0::numeric
         END AS share_of_supply,
         NULL::text AS wallet_type,
         NULL::timestamp AS first_seen_at,
         NULL::timestamp AS last_seen_at,
         NOW(),
         NOW()
       FROM balance_ledger bl
       LEFT JOIN token_metadata tm
         ON tm.token_symbol = $1
       WHERE COALESCE(bl.balance_normalized, 0::numeric) > 0::numeric
       ON CONFLICT (token_symbol, address, bucket_date) DO UPDATE
         SET balance = EXCLUDED.balance,
             balance_normalized = EXCLUDED.balance_normalized,
             share_of_supply = EXCLUDED.share_of_supply,
             wallet_type = EXCLUDED.wallet_type,
             first_seen_at = EXCLUDED.first_seen_at,
             last_seen_at = EXCLUDED.last_seen_at,
             updated_at = NOW()`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `DELETE FROM wallet_daily_activity
        WHERE token_symbol = $1
          AND bucket_date = $2::date`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `WITH scoped_transactions AS (
         SELECT
           token_symbol,
           from_address,
           to_address,
           COALESCE(amount_normalized, 0::numeric) AS amount_normalized,
           timestamp
         FROM transactions
         WHERE token_symbol = $1
           AND timestamp >= ($2::date)::timestamp
           AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
       ),
       expanded AS (
         SELECT
           token_symbol,
           to_address AS address,
           from_address AS counterparty,
           1 AS incoming_tx_count,
           0 AS outgoing_tx_count,
           amount_normalized AS incoming_volume,
           0::numeric AS outgoing_volume,
           timestamp
         FROM scoped_transactions
         UNION ALL
         SELECT
           token_symbol,
           from_address AS address,
           to_address AS counterparty,
           0 AS incoming_tx_count,
           1 AS outgoing_tx_count,
           0::numeric AS incoming_volume,
           amount_normalized AS outgoing_volume,
           timestamp
         FROM scoped_transactions
       )
       INSERT INTO wallet_daily_activity (
         token_symbol,
         address,
         bucket_date,
         incoming_tx_count,
         outgoing_tx_count,
         incoming_volume,
         outgoing_volume,
         net_flow,
         counterparty_count,
         last_tx_at,
         created_at,
         updated_at
       )
       SELECT
         token_symbol,
         address,
         $2::date AS bucket_date,
         SUM(incoming_tx_count)::integer,
         SUM(outgoing_tx_count)::integer,
         SUM(incoming_volume),
         SUM(outgoing_volume),
         SUM(incoming_volume) - SUM(outgoing_volume) AS net_flow,
         COUNT(DISTINCT counterparty)::integer AS counterparty_count,
         MAX(timestamp) AS last_tx_at,
         NOW(),
         NOW()
       FROM expanded
       GROUP BY token_symbol, address
       ON CONFLICT (token_symbol, address, bucket_date) DO UPDATE
         SET incoming_tx_count = EXCLUDED.incoming_tx_count,
             outgoing_tx_count = EXCLUDED.outgoing_tx_count,
             incoming_volume = EXCLUDED.incoming_volume,
             outgoing_volume = EXCLUDED.outgoing_volume,
             net_flow = EXCLUDED.net_flow,
             counterparty_count = EXCLUDED.counterparty_count,
             last_tx_at = EXCLUDED.last_tx_at,
             updated_at = NOW()`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `INSERT INTO token_daily_metrics (
         token_symbol,
         bucket_date,
         holder_count,
         new_holder_count,
         lost_holder_count,
         active_wallet_count,
         transaction_count,
         transfer_volume,
         current_supply,
         top10_share,
         top50_share,
         top_wallet_share,
         gini_coefficient,
         median_transfer_amount,
         avg_transfer_amount,
         created_at,
         updated_at
       )
       WITH today_balances AS (
         SELECT address, balance_normalized
         FROM wallet_daily_balances
         WHERE token_symbol = $1
           AND bucket_date = $2::date
           AND balance_normalized > 0::numeric
       ),
       prev_balances AS (
         SELECT address, balance_normalized
         FROM wallet_daily_balances
         WHERE token_symbol = $1
           AND bucket_date = ($2::date - INTERVAL '1 day')::date
           AND balance_normalized > 0::numeric
       ),
       ranked_balances AS (
         SELECT
           address,
           balance_normalized,
           ROW_NUMBER() OVER (ORDER BY balance_normalized DESC, address ASC) AS rank_desc,
           ROW_NUMBER() OVER (ORDER BY balance_normalized ASC, address ASC) AS rank_asc,
           COUNT(*) OVER () AS holder_total,
           SUM(balance_normalized) OVER () AS balance_total
         FROM today_balances
       ),
       scoped_transactions AS (
         SELECT amount_normalized
         FROM transactions
         WHERE token_symbol = $1
           AND timestamp >= ($2::date)::timestamp
           AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
       )
       SELECT
         $1,
         $2::date,
         (SELECT COUNT(*) FROM today_balances),
         (
           SELECT COUNT(*)
           FROM today_balances t
           WHERE NOT EXISTS (
             SELECT 1 FROM prev_balances p WHERE p.address = t.address
           )
         ),
         (
           SELECT COUNT(*)
           FROM prev_balances p
           WHERE NOT EXISTS (
             SELECT 1 FROM today_balances t WHERE t.address = p.address
           )
         ),
         (
           SELECT COUNT(*)
           FROM wallet_daily_activity
           WHERE token_symbol = $1
             AND bucket_date = $2::date
             AND (incoming_tx_count + outgoing_tx_count) > 0
         ),
         (SELECT COUNT(*) FROM scoped_transactions),
         (SELECT COALESCE(SUM(amount_normalized), 0::numeric) FROM scoped_transactions),
         (
           SELECT COALESCE(current_supply_normalized::numeric, 0::numeric)
           FROM token_metadata
           WHERE token_symbol = $1
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(balance_total) > 0
                 THEN (SUM(CASE WHEN rank_desc <= 10 THEN balance_normalized ELSE 0::numeric END) / MAX(balance_total)) * 100::numeric
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(balance_total) > 0
                 THEN (SUM(CASE WHEN rank_desc <= 50 THEN balance_normalized ELSE 0::numeric END) / MAX(balance_total)) * 100::numeric
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(balance_total) > 0
                 THEN (MAX(CASE WHEN rank_desc = 1 THEN balance_normalized ELSE 0::numeric END) / MAX(balance_total)) * 100::numeric
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(holder_total) > 0 AND MAX(balance_total) > 0
                 THEN (
                   (2::numeric * SUM(rank_asc * balance_normalized) / (MAX(holder_total) * MAX(balance_total)))
                   - ((MAX(holder_total) + 1)::numeric / MAX(holder_total))
                 )
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (SELECT COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_normalized), 0::numeric) FROM scoped_transactions),
         (SELECT COALESCE(AVG(amount_normalized), 0::numeric) FROM scoped_transactions),
         NOW(),
         NOW()
       ON CONFLICT (token_symbol, bucket_date) DO UPDATE
         SET holder_count = EXCLUDED.holder_count,
             new_holder_count = EXCLUDED.new_holder_count,
             lost_holder_count = EXCLUDED.lost_holder_count,
             active_wallet_count = EXCLUDED.active_wallet_count,
             transaction_count = EXCLUDED.transaction_count,
             transfer_volume = EXCLUDED.transfer_volume,
             current_supply = EXCLUDED.current_supply,
             top10_share = EXCLUDED.top10_share,
             top50_share = EXCLUDED.top50_share,
             top_wallet_share = EXCLUDED.top_wallet_share,
             gini_coefficient = EXCLUDED.gini_coefficient,
             median_transfer_amount = EXCLUDED.median_transfer_amount,
             avg_transfer_amount = EXCLUDED.avg_transfer_amount,
             updated_at = NOW()`,
      [tokenSymbol, normalizedBucketDate],
    );
  });
}

export async function refreshAllTokenAnalyticsForDate(
  bucketDate = new Date(),
): Promise<number> {
  const tokenSymbols = await getAvailableTokens();

  for (const tokenSymbol of tokenSymbols) {
    await refreshTokenAnalyticsForDate(tokenSymbol, bucketDate);
  }

  return tokenSymbols.length;
}

export async function getTokenDailyMetrics(
  tokenSymbol: string,
  days: number,
): Promise<TokenDailyMetricsRecord[]> {
  const safeDays = Math.max(1, Math.min(3650, Math.floor(days || 30)));
  const result = await databasePool.query(
    `SELECT
       token_symbol,
       bucket_date::text AS bucket_date,
       holder_count,
       new_holder_count,
       lost_holder_count,
       active_wallet_count,
       transaction_count,
       transfer_volume,
       current_supply,
       top10_share,
       top50_share,
       top_wallet_share,
       gini_coefficient,
       median_transfer_amount,
       avg_transfer_amount,
       updated_at
     FROM token_daily_metrics
     WHERE token_symbol = $1
     ORDER BY bucket_date DESC
     LIMIT $2`,
    [tokenSymbol, safeDays],
  );

  return result.rows.reverse().map(mapTokenDailyMetricsRow);
}

export async function getTokenTopMovers(
  tokenSymbol: string,
  windowDays: number,
  limit: number,
): Promise<TokenTopMoverRecord[]> {
  const safeWindowDays = Math.max(
    1,
    Math.min(365, Math.floor(windowDays || 7)),
  );
  const safeLimit = Math.max(1, Math.min(200, Math.floor(limit || 20)));
  const result = await databasePool.query(
    `WITH latest_date AS (
       SELECT MAX(bucket_date) AS value
       FROM wallet_daily_balances
       WHERE token_symbol = $1
     ),
     baseline_target AS (
       SELECT (value - ($2::text || ' days')::interval)::date AS value
       FROM latest_date
       WHERE value IS NOT NULL
     ),
     baseline_date AS (
       SELECT COALESCE(
         (
           SELECT MAX(bucket_date)
           FROM wallet_daily_balances
           WHERE token_symbol = $1
             AND bucket_date <= (SELECT value FROM baseline_target)
         ),
         (
           SELECT MIN(bucket_date)
           FROM wallet_daily_balances
           WHERE token_symbol = $1
             AND bucket_date < (SELECT value FROM latest_date)
         )
       ) AS value
     ),
     latest_balances AS (
       SELECT address, COALESCE(balance_normalized, 0::numeric) AS balance
       FROM wallet_daily_balances
       WHERE token_symbol = $1
         AND bucket_date = (SELECT value FROM latest_date)
     ),
     baseline_balances AS (
       SELECT address, COALESCE(balance_normalized, 0::numeric) AS balance
       FROM wallet_daily_balances
       WHERE token_symbol = $1
         AND bucket_date = (SELECT value FROM baseline_date)
     )
     SELECT
       $1 AS token_symbol,
       COALESCE(l.address, b.address) AS address,
       (SELECT value::text FROM latest_date) AS latest_date,
       (SELECT value::text FROM baseline_date) AS baseline_date,
       COALESCE(l.balance, 0::numeric) AS latest_balance,
       COALESCE(b.balance, 0::numeric) AS baseline_balance,
       COALESCE(l.balance, 0::numeric) - COALESCE(b.balance, 0::numeric) AS delta_balance,
       CASE
         WHEN COALESCE(b.balance, 0::numeric) > 0
           THEN ((COALESCE(l.balance, 0::numeric) - COALESCE(b.balance, 0::numeric)) / b.balance) * 100::numeric
         WHEN COALESCE(b.balance, 0::numeric) = 0 AND COALESCE(l.balance, 0::numeric) > 0
           THEN 100::numeric
         ELSE 0::numeric
       END AS delta_pct
     FROM latest_balances l
     FULL OUTER JOIN baseline_balances b
       ON b.address = l.address
     WHERE (SELECT value FROM baseline_date) IS NOT NULL
       AND (COALESCE(l.balance, 0::numeric) > 0::numeric
        OR COALESCE(b.balance, 0::numeric) > 0::numeric
       )
     ORDER BY ABS(COALESCE(l.balance, 0::numeric) - COALESCE(b.balance, 0::numeric)) DESC,
              COALESCE(l.address, b.address) ASC
     LIMIT $3`,
    [tokenSymbol, safeWindowDays, safeLimit],
  );

  return result.rows.map(mapTokenTopMoverRow);
}

export async function getPrecomputedApiView(
  viewKey: string,
): Promise<Record<string, unknown> | null> {
  const result = await queryReadWithRetry<{
    payload: Record<string, unknown>;
  }>(
    `SELECT payload
       FROM public.api_precomputed_views
      WHERE view_key = $1
        AND expires_at > NOW()
      LIMIT 1`,
    [viewKey],
    "get_precomputed_api_view",
  );

  if (result.rowCount === 0) {
    return null;
  }

  return (result.rows[0]?.payload as Record<string, unknown>) ?? null;
}

export async function setPrecomputedApiView(options: {
  viewKey: string;
  tokenSymbol?: string;
  payload: Record<string, unknown>;
  ttlMs?: number;
}): Promise<void> {
  const ttlMs = Math.max(
    1000,
    Math.floor(Number(options.ttlMs ?? apiConfig.precomputeTtlMs) || 0),
  );

  await databasePool.query(
    `INSERT INTO public.api_precomputed_views (
       view_key,
       token_symbol,
       payload,
       expires_at,
       updated_at
     ) VALUES (
       $1,
       $2,
       $3::jsonb,
       NOW() + (($4::bigint) * INTERVAL '1 millisecond'),
       NOW()
     )
     ON CONFLICT (view_key) DO UPDATE
       SET token_symbol = EXCLUDED.token_symbol,
           payload = EXCLUDED.payload,
           expires_at = EXCLUDED.expires_at,
           updated_at = NOW()`,
    [
      options.viewKey,
      options.tokenSymbol ?? null,
      JSON.stringify(options.payload),
      ttlMs,
    ],
  );
}

export async function refreshTokenPrecomputedViews(
  tokenSymbol: string,
): Promise<void> {
  const [metadata, graphBase, topHolders, timeseries30] = await Promise.all([
    getTokenMetadata(tokenSymbol),
    getFullTokenGraph(tokenSymbol, {
      includeTopHoldersLimit: 0,
      edgeLimit: Math.min(
        apiConfig.tokenGraphMaxEdges,
        apiConfig.tokenGraphStageBaseEdgeLimit,
      ),
    }),
    getTopHolders(tokenSymbol, 25),
    getTokenDailyMetrics(tokenSymbol, 30),
  ]);

  const generatedAt = new Date().toISOString();
  const basePayload = {
    tokenSymbol,
    generatedAt,
    metadata,
    graphBase,
    topHolders,
    timeseries30,
  } as Record<string, unknown>;

  await Promise.all([
    setPrecomputedApiView({
      viewKey: `token-overview:${tokenSymbol}`,
      tokenSymbol,
      payload: basePayload,
    }),
    setPrecomputedApiView({
      viewKey: `token-graph-base:${tokenSymbol}`,
      tokenSymbol,
      payload: {
        tokenSymbol,
        generatedAt,
        graph: graphBase,
      },
    }),
    setPrecomputedApiView({
      viewKey: `token-top-holders:${tokenSymbol}:25`,
      tokenSymbol,
      payload: {
        tokenSymbol,
        generatedAt,
        topHolders,
      },
    }),
  ]);
}
