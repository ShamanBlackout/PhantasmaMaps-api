import { apiConfig } from "../phantasma.config";
import {
  cacheQueryPool,
  databasePool,
  isUndefinedTableError,
  queryReadWithRetry,
} from "./connection";
import {
  getFullTokenGraph,
  getTokenMetadata,
  getTopHolders,
} from "./graphQueries";
import { getTokenDailyMetrics } from "./analytics";

type CacheLookupStatus = "hit" | "miss" | "stale";

type CacheLookupResult = {
  status: CacheLookupStatus;
  payload: string | null;
};

let queryCacheTableMissingLogged = false;

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
