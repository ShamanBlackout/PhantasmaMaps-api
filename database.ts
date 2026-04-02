import {
  Pool,
  type PoolClient,
  type PoolConfig,
  type QueryResultRow,
} from "pg";
import { apiConfig, databaseConfig } from "./phantasma.config";
import {
  CHAIN_SYNC_TOKEN,
  type AddressSubgraphResult,
  type GraphEdgeRecord,
  type GraphNodeRecord,
  type PaginatedTransactionsResult,
  type ParsedTransfer,
  type SyncStateRecord,
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
    label: row.label === null ? null : String(row.label),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  };
}

function mapGraphEdgeRow(row: QueryResultRow): GraphEdgeRecord {
  return {
    id: String(row.id),
    tokenSymbol: String(row.token_symbol),
    fromAddress: String(row.from_address),
    toAddress: String(row.to_address),
    amount: row.amount === null ? null : String(row.amount),
    txHash: String(row.tx_hash),
    eventIndex: Number(row.event_index),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
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

export async function upsertTransfers(
  client: PoolClient,
  transfers: ParsedTransfer[],
): Promise<void> {
  for (const transfer of transfers) {
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
         metadata
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (tx_hash, event_index) DO UPDATE
         SET token_symbol = EXCLUDED.token_symbol,
             block_height = EXCLUDED.block_height,
             timestamp = EXCLUDED.timestamp,
             from_address = EXCLUDED.from_address,
             to_address = EXCLUDED.to_address,
             amount = EXCLUDED.amount,
             metadata = EXCLUDED.metadata`,
      [
        transfer.txHash,
        transfer.eventIndex,
        transfer.tokenSymbol,
        transfer.blockHeight,
        transfer.timestamp,
        transfer.fromAddress,
        transfer.toAddress,
        transfer.amount,
        transfer.metadata,
      ],
    );
  }
}

export async function upsertNodes(
  client: PoolClient,
  transfers: ParsedTransfer[],
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
    await client.query(
      `INSERT INTO nodes (address, token_symbol, balance, metadata)
       VALUES ($1, $2, NULL, $3)
       ON CONFLICT (address, token_symbol) DO UPDATE
         SET metadata = COALESCE(nodes.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)`,
      [node.address, node.tokenSymbol, node.metadata],
    );
  }
}

export async function upsertEdges(
  client: PoolClient,
  transfers: ParsedTransfer[],
): Promise<void> {
  for (const transfer of transfers) {
    await client.query(
      `INSERT INTO edges (
         token_symbol,
         from_address,
         to_address,
         amount,
         tx_hash,
         event_index,
         metadata
       ) VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (tx_hash, event_index) DO NOTHING`,
      [
        transfer.tokenSymbol,
        transfer.fromAddress,
        transfer.toAddress,
        transfer.amount,
        transfer.txHash,
        transfer.eventIndex,
        transfer.metadata,
      ],
    );
  }
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

export async function getFullTokenGraph(
  tokenSymbol: string,
): Promise<AddressSubgraphResult> {
  const edgesResult = await databasePool.query(
    `SELECT id, token_symbol, from_address, to_address, amount, tx_hash, event_index, metadata
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
        `SELECT address, token_symbol, balance, label, metadata
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
    `SELECT address, token_symbol, balance, label, metadata
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
    `SELECT COUNT(*)::bigint AS total FROM transactions ${whereClause}`,
    values,
  );

  values.push(pageSize, offset);
  const result = await databasePool.query(
    `SELECT id, tx_hash, event_index, token_symbol, block_height, timestamp,
            from_address, to_address, amount, metadata
       FROM transactions
       ${whereClause}
      ORDER BY block_height DESC, tx_hash ASC, event_index ASC
      LIMIT $${values.length - 1} OFFSET $${values.length}`,
    values,
  );

  return {
    page,
    pageSize,
    total: Number(countResult.rows[0]?.total ?? 0),
    items: result.rows,
  };
}
