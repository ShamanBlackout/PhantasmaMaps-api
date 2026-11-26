import {
  type TopHoldersResult,
  type GraphNodeRecord,
  CHAIN_SYNC_TOKEN,
  type TokenMetadataRecord,
  type AddressSubgraphResult,
} from "../phantasma.types";
import { type QueryResultRow } from "pg";
import { apiConfig } from "../phantasma.config";
import { isPathTerminalLabel } from "../labelingRubric";
import { databasePool, queryReadWithRetry } from "./connection";
import {
  mapGraphEdgeRow,
  mapGraphNodeRow,
  mapTokenMetadataRow,
  normalizeRawAmount,
} from "./rowMappers";

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

      if (isPathTerminalLabel(row.label_type, row.label)) {
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
