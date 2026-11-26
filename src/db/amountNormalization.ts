import { PhantasmaTS } from "phantasma-sdk-ts";
import { type PoolClient } from "pg";
import { databasePool } from "./connection";
import { normalizeRawAmount } from "./rowMappers";

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
