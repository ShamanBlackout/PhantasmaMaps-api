import {
  type SyncStateRecord,
  CHAIN_SYNC_TOKEN,
  type ParsedTransfer,
  type TokenMetadataUpsertInput,
} from "../phantasma.types";
import { type PoolClient } from "pg";
import { databasePool } from "./connection";
import {
  mapSyncStateRow,
  normalizeRawAmount,
  resolveStoredTransferAmounts,
} from "./rowMappers";

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
