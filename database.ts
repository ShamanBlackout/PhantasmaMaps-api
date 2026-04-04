import {
  Pool,
  type PoolClient,
  type PoolConfig,
  type QueryResultRow,
} from "pg";
import { PhantasmaTS } from "phantasma-sdk-ts";
import { apiConfig, databaseConfig } from "./phantasma.config";
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
    const balanceRaw = balancesByNodeKey.get(nodeKey) ?? "0";
    const tokenDecimals = tokenDecimalsBySymbol.get(node.tokenSymbol) ?? 0;
    const balanceNormalized = normalizeRawAmount(balanceRaw, tokenDecimals);

    await client.query(
      `INSERT INTO nodes (address, token_symbol, balance, balance_normalized, metadata)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (address, token_symbol) DO UPDATE
         SET balance = EXCLUDED.balance,
             balance_normalized = EXCLUDED.balance_normalized,
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

export async function updateSyncStateForBlock(
  client: PoolClient,
  blockHeight: number,
  tokenSymbols: string[],
): Promise<void> {
  const checkpointTokens = new Set<string>([CHAIN_SYNC_TOKEN, ...tokenSymbols]);

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
          checkpointType: tokenSymbol === CHAIN_SYNC_TOKEN ? "chain" : "token",
          lastCommittedBlockHeight: blockHeight,
        },
      ],
    );
  }
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
       UNION
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
     limited_edges AS (
       SELECT DISTINCT ON (e.tx_hash, e.event_index)
              e.id,
              e.token_symbol,
              e.from_address,
              e.to_address,
              e.amount,
              e.amount_normalized,
              e.tx_hash,
              e.event_index,
              e.metadata
         FROM edges e
         JOIN walk
           ON e.token_symbol = $1
          AND (e.from_address = walk.address OR e.to_address = walk.address)
        ORDER BY e.tx_hash, e.event_index, e.id
        LIMIT $4
     )
     SELECT * FROM limited_edges`,
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
  page: number;
  pageSize: number;
}): Promise<PaginatedTransactionsResult> {
  const filters: string[] = [];
  const values: Array<string | number> = [];

  if (options.tokenSymbol) {
    values.push(options.tokenSymbol);
    filters.push(`token_symbol = $${values.length}`);
  }

  if (options.address) {
    values.push(options.address);
    filters.push(
      `(from_address = $${values.length} OR to_address = $${values.length})`,
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
      ORDER BY block_height DESC, tx_hash ASC
      LIMIT $${values.length - 1} OFFSET $${values.length}`,
    values,
  );

  return {
    page,
    pageSize,
    total: Number(countResult.rows[0]?.total ?? 0),
    items: result.rows.map(mapTransactionRow),
  };
}
