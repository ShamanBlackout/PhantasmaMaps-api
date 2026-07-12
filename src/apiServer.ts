import express, {
  type NextFunction,
  type Request,
  type Response,
} from "express";
import cors from "cors";
import compression from "compression";
import { createHash, randomUUID } from "crypto";
import { apiConfig } from "./phantasma.config";
import { createPhantasmaRpcClient } from "./rpcClient";
import { cacheMiddleware, invalidateCache } from "./responseCache";
import {
  clearSubgraphCache,
  closeDatabasePool,
  findAddressPaths,
  getTokenDailyMetrics,
  getTokenTopMovers,
  getAddressActivity,
  getAddressConnections,
  getAddressSubgraph,
  getAvailableTokens,
  getBlockSyncClaimsView,
  getPrecomputedApiView,
  getFullTokenGraph,
  getSyncStates,
  getTokenMetadata,
  getLabeledNodes,
  refreshTokenPrecomputedViews,
  getTopHolders,
  getTransactionsPage,
  refreshTokenAnalyticsForDate,
  testDatabaseConnection,
} from "./database";
import type { Server } from "http";

type ApiErrorCode =
  | "INVALID_REQUEST"
  | "TOKEN_SYMBOL_INVALID"
  | "TOKEN_NOT_FOUND"
  | "ADDRESS_INVALID"
  | "GRAPH_DEPTH_LIMIT_EXCEEDED"
  | "GRAPH_EDGE_LIMIT_EXCEEDED"
  | "PAGINATION_INVALID"
  | "RATE_LIMITED"
  | "INTERNAL_ERROR";

class ApiError extends Error {
  readonly status: number;

  readonly code: ApiErrorCode;

  readonly details?: Record<string, unknown>;

  readonly retryAfterMs?: number;

  constructor(
    status: number,
    code: ApiErrorCode,
    message: string,
    details?: Record<string, unknown>,
    retryAfterMs?: number,
  ) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
    this.retryAfterMs = retryAfterMs;
  }
}

type ResponseMeta = Record<string, unknown>;

type CacheMiddleware = (
  cacheKey: string,
  ttlMs: number,
) => (
  request: Request,
  response: Response,
  next: NextFunction,
) => void | Promise<void>;

export type ApiServerDeps = {
  rpcClient: {
    getBlockHeight: () => Promise<number | string>;
    getAccount: (address: string) => Promise<unknown>;
  };
  cacheMiddlewareImpl: CacheMiddleware;
  invalidateCacheImpl: () => void;
  clearSubgraphCacheImpl: () => void;
  closeDatabasePoolImpl: () => Promise<void>;
  testDatabaseConnectionImpl: () => Promise<unknown>;
  getSyncStatesImpl: () => Promise<unknown[]>;
  getBlockSyncClaimsViewImpl: typeof getBlockSyncClaimsView;
  getAvailableTokensImpl: () => Promise<string[]>;
  getTokenMetadataImpl: (tokenSymbol: string) => Promise<unknown>;
  getAddressSubgraphImpl: (
    tokenSymbol: string,
    address: string,
    depth: number,
    edgeLimit: number,
  ) => Promise<unknown>;
  getAddressConnectionsImpl: (
    tokenSymbol: string,
    address: string,
  ) => Promise<unknown[]>;
  getPrecomputedApiViewImpl: (
    viewKey: string,
  ) => Promise<Record<string, unknown> | null>;
  refreshTokenPrecomputedViewsImpl: (tokenSymbol: string) => Promise<void>;
  findAddressPathsImpl: (options: {
    tokenSymbol: string;
    fromAddress: string;
    toAddress: string;
    maxHops: number;
    pathLimit: number;
    stopAtTerminals?: boolean;
  }) => Promise<unknown[]>;
  getTopHoldersImpl: (tokenSymbol: string, limit: number) => Promise<unknown>;
  getFullTokenGraphImpl: (
    tokenSymbol: string,
    options: { includeTopHoldersLimit: number; edgeLimit?: number },
  ) => Promise<unknown>;
  getTransactionsPageImpl: typeof getTransactionsPage;
  getAddressActivityImpl: (
    tokenSymbol: string,
    address: string,
    days: number,
  ) => Promise<unknown[]>;
  refreshTokenAnalyticsForDateImpl: (
    tokenSymbol: string,
    bucketDate?: Date,
  ) => Promise<void>;
  getTokenDailyMetricsImpl: (
    tokenSymbol: string,
    days: number,
  ) => Promise<unknown[]>;
  getTokenTopMoversImpl: (
    tokenSymbol: string,
    windowDays: number,
    limit: number,
  ) => Promise<unknown[]>;
  getLabeledNodesImpl: (options: {
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
  }) => Promise<unknown>;
};

const defaultDeps: ApiServerDeps = {
  rpcClient: createPhantasmaRpcClient(),
  cacheMiddlewareImpl: cacheMiddleware,
  invalidateCacheImpl: invalidateCache,
  clearSubgraphCacheImpl: clearSubgraphCache,
  closeDatabasePoolImpl: closeDatabasePool,
  testDatabaseConnectionImpl: testDatabaseConnection,
  getSyncStatesImpl: getSyncStates,
  getBlockSyncClaimsViewImpl: getBlockSyncClaimsView,
  getAvailableTokensImpl: getAvailableTokens,
  getTokenMetadataImpl: getTokenMetadata,
  getAddressSubgraphImpl: getAddressSubgraph,
  getAddressConnectionsImpl: getAddressConnections,
  getPrecomputedApiViewImpl: getPrecomputedApiView,
  refreshTokenPrecomputedViewsImpl: refreshTokenPrecomputedViews,
  findAddressPathsImpl: findAddressPaths,
  getTopHoldersImpl: getTopHolders,
  getFullTokenGraphImpl: getFullTokenGraph,
  getTransactionsPageImpl: getTransactionsPage,
  getAddressActivityImpl: getAddressActivity,
  refreshTokenAnalyticsForDateImpl: refreshTokenAnalyticsForDate,
  getTokenDailyMetricsImpl: getTokenDailyMetrics,
  getTokenTopMoversImpl: getTokenTopMovers,
  getLabeledNodesImpl: getLabeledNodes,
};

function readPositiveInt(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function ensurePositiveEdgeLimit(value: number | undefined): number {
  const parsed = Number(value);
  if (Number.isFinite(parsed) && parsed > 0) {
    return Math.floor(parsed);
  }

  return apiConfig.tokenGraphMaxEdges;
}

function readOptionalNumber(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function readOptionalIsoDate(value: string | undefined): Date | undefined {
  if (!value) {
    return undefined;
  }

  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date : undefined;
}

function readOptionalBucketDate(value: string | undefined): Date | undefined {
  if (!value) {
    return undefined;
  }

  const trimmed = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return undefined;
  }

  const parsed = new Date(`${trimmed}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) ? parsed : undefined;
}

function readStringList(value: string | undefined): string[] {
  if (!value) {
    return [];
  }

  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.floor(value)));
}

function addIntegerStrings(left: unknown, right: unknown): string {
  const leftValue = BigInt(String(left ?? "0"));
  const rightValue = BigInt(String(right ?? "0"));
  return (leftValue + rightValue).toString();
}

function readLiveTokenBalanceRaw(
  account: unknown,
  tokenSymbol: string,
): string {
  if (!account || typeof account !== "object") {
    return "0";
  }

  const accountRecord = account as {
    balances?: unknown;
    stake?: unknown;
    unclaimed?: unknown;
  };

  const rawBalances = accountRecord.balances;
  if (!Array.isArray(rawBalances)) {
    return tokenSymbol === "SOUL"
      ? addIntegerStrings("0", accountRecord.stake)
      : tokenSymbol === "KCAL"
        ? addIntegerStrings("0", accountRecord.unclaimed)
        : "0";
  }

  for (const item of rawBalances) {
    if (!item || typeof item !== "object") {
      continue;
    }

    const symbol = String((item as { symbol?: unknown }).symbol ?? "").trim();
    if (symbol !== tokenSymbol) {
      continue;
    }

    const amount = (item as { amount?: unknown }).amount;
    if (symbol === "SOUL") {
      return addIntegerStrings(amount, accountRecord.stake);
    }

    if (symbol === "KCAL") {
      return addIntegerStrings(amount, accountRecord.unclaimed);
    }

    return String(amount ?? "0");
  }

  return "0";
}

function normalizeRawAmount(rawAmount: string, decimals: number): string {
  const safeRaw = String(rawAmount ?? "0").trim();

  if (!/^-?\d+$/.test(safeRaw)) {
    return "0";
  }

  const negative = safeRaw.startsWith("-");
  const digits = negative ? safeRaw.slice(1) : safeRaw;
  const safeDigits = digits.replace(/^0+(?=\d)/, "") || "0";
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

function readPositiveNumberFromUnknown(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

async function reconcileTokenGraphWithLiveBalances(
  deps: ApiServerDeps,
  tokenSymbol: string,
  graph: unknown,
  metadata: unknown,
): Promise<unknown> {
  const graphRecord =
    graph && typeof graph === "object"
      ? (graph as {
          nodes?: Array<Record<string, unknown>>;
          edges?: Array<Record<string, unknown>>;
        })
      : null;

  const nodes = Array.isArray(graphRecord?.nodes) ? graphRecord.nodes : null;
  if (!nodes || nodes.length === 0) {
    return graph;
  }

  const metadataRecord =
    metadata && typeof metadata === "object"
      ? (metadata as { currentSupplyNormalized?: unknown; decimals?: unknown })
      : null;
  const currentSupply = readPositiveNumberFromUnknown(
    metadataRecord?.currentSupplyNormalized,
  );

  if (currentSupply === null || currentSupply <= 0) {
    return graph;
  }

  const nodeTotal = nodes.reduce((sum, node) => {
    const balance =
      readPositiveNumberFromUnknown(node.balanceNormalized) ??
      readPositiveNumberFromUnknown(node.balance) ??
      0;
    return sum + balance;
  }, 0);

  if (nodeTotal <= currentSupply + 1e-9) {
    return graph;
  }

  const decimals = Math.max(
    0,
    Math.floor(Number(metadataRecord?.decimals ?? 0) || 0),
  );

  const refreshedNodes = await Promise.all(
    nodes.map(async (node) => {
      const address = String(node.address ?? "").trim();
      if (!address) {
        return node;
      }

      try {
        const account = await deps.rpcClient.getAccount(address);
        const liveBalanceRaw = readLiveTokenBalanceRaw(account, tokenSymbol);
        return {
          ...node,
          balance: liveBalanceRaw,
          balanceNormalized: normalizeRawAmount(liveBalanceRaw, decimals),
        };
      } catch {
        return node;
      }
    }),
  );

  return {
    ...graphRecord,
    nodes: refreshedNodes,
  };
}

function isValidTokenSymbol(rawToken: string): boolean {
  const token = String(rawToken || "").trim();
  return /^[A-Z0-9_-]{1,16}$/i.test(token);
}

function isValidAddress(rawAddress: string): boolean {
  const address = String(rawAddress || "").trim();
  return /^[PS][a-zA-Z0-9]{20,}$/.test(address);
}

function getRequestId(request: Request): string {
  return String(request.headers["x-request-id"] || randomUUID());
}

function createEtagFromData(data: unknown): string {
  const digest = createHash("sha1").update(JSON.stringify(data)).digest("hex");
  return `W/"${digest}"`;
}

function sendSuccess(
  request: Request,
  response: Response,
  data: unknown,
  meta: ResponseMeta = {},
  status = 200,
): void {
  const requestId = response.locals.requestId as string;
  const body = {
    data,
    meta: {
      generatedAt: new Date().toISOString(),
      source: "live",
      ...meta,
    },
    requestId,
  };

  const etag = createEtagFromData(body.data);
  response.setHeader("Cache-Control", "public, max-age=0, must-revalidate");
  response.setHeader("ETag", etag);

  if (request.headers["if-none-match"] === etag) {
    response.status(304).end();
    return;
  }

  response.status(status).json(body);
}

type AddressGraphStage = "core" | "connections" | "full";

function normalizeAddressGraphStage(request: Request): AddressGraphStage {
  const raw = String(request.query.stage ?? "core")
    .trim()
    .toLowerCase();

  if (raw === "core" || raw === "connections" || raw === "full") {
    return raw;
  }

  throw new ApiError(
    400,
    "INVALID_REQUEST",
    "stage must be core, connections, or full",
    {
      received: raw || null,
    },
  );
}

type TokenGraphStage = "base" | "holders" | "full";

function normalizeTokenGraphStage(request: Request): TokenGraphStage {
  const raw = String(request.query.stage ?? "base")
    .trim()
    .toLowerCase();

  if (raw === "base" || raw === "holders" || raw === "full") {
    return raw;
  }

  throw new ApiError(
    400,
    "INVALID_REQUEST",
    "stage must be base, holders, or full",
    {
      received: raw || null,
    },
  );
}

function sendError(
  response: Response,
  status: number,
  code: ApiErrorCode,
  message: string,
  details?: Record<string, unknown>,
  retryAfterMs?: number,
): void {
  const requestId = response.locals.requestId as string;
  if (retryAfterMs && retryAfterMs > 0) {
    response.setHeader("Retry-After", String(Math.ceil(retryAfterMs / 1000)));
  }

  response.status(status).json({
    requestId,
    retryAfterMs: retryAfterMs ?? null,
    error: {
      code,
      message,
      details: details ?? null,
    },
  });
}

function normalizeWithTopHolders(request: Request): number {
  const includeTopHoldersRaw = String(
    request.query.includeTopHolders ?? "",
  ).trim();
  const withTopHoldersRaw = String(request.query.withTopHolders ?? "")
    .trim()
    .toLowerCase();
  const topHoldersLimitRaw = String(request.query.topHoldersLimit ?? "").trim();

  if (includeTopHoldersRaw) {
    throw new ApiError(
      400,
      "INVALID_REQUEST",
      "includeTopHolders is deprecated; use withTopHolders=true",
      {
        deprecatedQuery: "includeTopHolders",
        replacementQuery: "withTopHolders",
      },
    );
  }

  if (topHoldersLimitRaw) {
    const parsedLimit = Number(topHoldersLimitRaw);
    if (!Number.isFinite(parsedLimit) || parsedLimit < 0) {
      throw new ApiError(
        400,
        "INVALID_REQUEST",
        "topHoldersLimit must be a non-negative number",
        {
          topHoldersLimit: topHoldersLimitRaw,
        },
      );
    }

    return Math.floor(parsedLimit);
  }

  if (withTopHoldersRaw === "true" || withTopHoldersRaw === "1") {
    return 10;
  }

  return 0;
}

function normalizeTokenQueryForCache(request: Request): string {
  return String(request.query.token ?? "")
    .trim()
    .toUpperCase();
}

function normalizeAddressPathForCache(request: Request): string {
  return String(request.params.address ?? "").trim();
}

function buildAddressSubgraphCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const address = normalizeAddressPathForCache(request);
  const depth = readPositiveInt(
    String(request.query.depth ?? ""),
    apiConfig.graphDefaultDepth,
  );
  const edgeLimit = readPositiveInt(
    String(request.query.edgeLimit ?? ""),
    apiConfig.graphMaxEdgesPerRequest,
  );

  return `address-subgraph:${tokenSymbol}:${address}:${depth}:${edgeLimit}`;
}

function buildAddressConnectionsCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const address = normalizeAddressPathForCache(request);
  return `address-connections:${tokenSymbol}:${address}`;
}

function buildAddressStagedCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const address = normalizeAddressPathForCache(request);
  const stage = String(request.query.stage ?? "core")
    .trim()
    .toLowerCase();
  const depth = readPositiveInt(
    String(request.query.depth ?? ""),
    apiConfig.graphDefaultDepth,
  );
  const edgeLimit = readPositiveInt(
    String(request.query.edgeLimit ?? ""),
    apiConfig.graphMaxEdgesPerRequest,
  );
  const connectionsLimit = readPositiveInt(
    String(request.query.connectionsLimit ?? ""),
    25,
  );

  return `address-staged:${tokenSymbol}:${address}:${stage}:${depth}:${edgeLimit}:${connectionsLimit}`;
}

function buildTokenStagedCacheKey(request: Request): string {
  const tokenSymbol = String(request.params.tokenSymbol ?? "")
    .trim()
    .toUpperCase();
  const stage = String(request.query.stage ?? "base")
    .trim()
    .toLowerCase();
  const edgeLimit = readPositiveInt(
    String(request.query.edgeLimit ?? ""),
    apiConfig.tokenGraphStageBaseEdgeLimit,
  );
  const topHoldersLimit = readPositiveInt(
    String(request.query.topHoldersLimit ?? ""),
    10,
  );

  return `token-staged:${tokenSymbol}:${stage}:${edgeLimit}:${topHoldersLimit}`;
}

function buildTracePathsCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const fromAddress = String(request.query.from ?? "").trim();
  const toAddress = String(request.query.to ?? "").trim();
  const maxHops = clampInt(
    readPositiveInt(
      request.query.maxHops ? String(request.query.maxHops) : undefined,
      5,
    ),
    1,
    8,
  );
  const pathLimit = clampInt(
    readPositiveInt(
      request.query.limit ? String(request.query.limit) : undefined,
      20,
    ),
    1,
    100,
  );
  const stopAtTerminalsRaw = String(request.query.stopAtTerminals ?? "true")
    .trim()
    .toLowerCase();
  const stopAtTerminals = !["false", "0", "no", "off"].includes(
    stopAtTerminalsRaw,
  );

  return `trace-paths:${tokenSymbol}:${fromAddress}:${toAddress}:${maxHops}:${pathLimit}:${stopAtTerminals ? "1" : "0"}`;
}

function handleRouteError(response: Response, error: unknown): void {
  if (error instanceof ApiError) {
    sendError(
      response,
      error.status,
      error.code,
      error.message,
      error.details,
      error.retryAfterMs,
    );
    return;
  }

  sendError(
    response,
    500,
    "INTERNAL_ERROR",
    error instanceof Error ? error.message : String(error),
  );
}

async function sendTokenGraphResponse(
  request: Request,
  response: Response,
  deps: ApiServerDeps,
  tokenSymbol: string,
  includeTopHolders: number,
  edgeLimit?: number,
  mode: "standard" | "max" = "standard",
): Promise<void> {
  const effectiveEdgeLimit =
    mode === "standard" ? ensurePositiveEdgeLimit(edgeLimit) : undefined;

  let graph: unknown;
  let fallbackApplied = false;
  let appliedTopHoldersLimit = includeTopHolders;
  let appliedEdgeLimit = effectiveEdgeLimit;

  try {
    graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
      includeTopHoldersLimit: includeTopHolders,
      edgeLimit: effectiveEdgeLimit,
    });
  } catch (primaryError) {
    if (mode !== "standard") {
      throw primaryError;
    }

    const fallbackEdgeLimit = Math.max(
      200,
      Math.floor(
        Number(effectiveEdgeLimit ?? apiConfig.tokenGraphMaxEdges) / 2,
      ),
    );
    graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
      includeTopHoldersLimit: 0,
      edgeLimit: fallbackEdgeLimit,
    });
    fallbackApplied = true;
    appliedTopHoldersLimit = 0;
    appliedEdgeLimit = fallbackEdgeLimit;
  }

  const graphNodes = Array.isArray((graph as { nodes?: unknown[] })?.nodes)
    ? ((graph as { nodes?: unknown[] }).nodes ?? [])
    : [];
  const graphEdges = Array.isArray((graph as { edges?: unknown[] })?.edges)
    ? ((graph as { edges?: unknown[] }).edges ?? [])
    : [];

  if (mode === "standard" && graphNodes.length > 0) {
    const metadata = await deps.getTokenMetadataImpl(tokenSymbol);
    graph = await reconcileTokenGraphWithLiveBalances(
      deps,
      tokenSymbol,
      graph,
      metadata,
    );
  }

  const responseNodes = Array.isArray((graph as { nodes?: unknown[] })?.nodes)
    ? ((graph as { nodes?: unknown[] }).nodes ?? [])
    : [];
  const responseEdges = Array.isArray((graph as { edges?: unknown[] })?.edges)
    ? ((graph as { edges?: unknown[] }).edges ?? [])
    : [];

  sendSuccess(request, response, graph, {
    isPartial: fallbackApplied,
    mode,
    appliedLimits: {
      topHoldersLimit: appliedTopHoldersLimit,
      edgeLimit:
        mode === "standard"
          ? appliedEdgeLimit
          : Number.isFinite(Number(edgeLimit))
            ? edgeLimit
            : null,
    },
    degradedFrom:
      fallbackApplied && mode === "standard"
        ? {
            topHoldersLimit: includeTopHolders,
            edgeLimit: effectiveEdgeLimit,
          }
        : null,
    totalNodeCount: responseNodes.length,
    totalEdgeCount: responseEdges.length,
  });
}

export function createApiApp(deps: ApiServerDeps = defaultDeps) {
  const app = express();
  app.disable("x-powered-by");

  const allowedOrigins = String(process.env.PHANTASMA_API_CORS_ORIGINS ?? "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);

  app.use(
    cors({
      origin: allowedOrigins.length > 0 ? allowedOrigins : true,
    }),
  );

  app.use(
    compression({
      // Only compress responses larger than 1KB; small payloads have negligible gain
      threshold: 1024,
      level: 6,
      memLevel: 8,
    }),
  );

  app.use(express.json());

  app.use((request: Request, response: Response, next: NextFunction) => {
    response.locals.requestId = getRequestId(request);
    response.setHeader("X-Request-Id", response.locals.requestId);
    response.setHeader("X-RateLimit-Limit", "120");
    response.setHeader("X-RateLimit-Remaining", "120");
    response.setHeader(
      "X-RateLimit-Reset",
      String(Math.floor(Date.now() / 1000) + 60),
    );
    next();
  });

  app.get("/health", async (_request: Request, response: Response) => {
    try {
      await deps.testDatabaseConnectionImpl();
      sendSuccess(_request, response, {
        ok: true,
        status: "healthy",
        database: "up",
      });
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);

      sendSuccess(
        _request,
        response,
        {
          ok: false,
          status: "degraded",
          database: "down",
          error: {
            message: errorMessage,
          },
        },
        {
          degraded: true,
        },
        503,
      );
    }
  });

  app.get("/sync-status", async (_request: Request, response: Response) => {
    try {
      const [syncStatesResult, chainHeadResult] = await Promise.allSettled([
        deps.getSyncStatesImpl(),
        deps.rpcClient.getBlockHeight(),
      ]);

      if (syncStatesResult.status !== "fulfilled") {
        throw syncStatesResult.reason;
      }

      sendSuccess(_request, response, {
        items: syncStatesResult.value,
        chainHeadBlockHeight:
          chainHeadResult.status === "fulfilled"
            ? Number(chainHeadResult.value)
            : null,
      });
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });

  app.get("/sync-claims", async (request: Request, response: Response) => {
    try {
      const limit = readPositiveInt(String(request.query.limit ?? ""), 100);
      const result = await deps.getBlockSyncClaimsViewImpl({
        statuses: readStringList(
          request.query.status ? String(request.query.status) : undefined,
        ),
        fromBlock: request.query.fromBlock
          ? readPositiveInt(String(request.query.fromBlock), 0)
          : undefined,
        toBlock: request.query.toBlock
          ? readPositiveInt(String(request.query.toBlock), 0)
          : undefined,
        limit,
      });

      sendSuccess(request, response, result);
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });

  app.get(
    "/tokens",
    deps.cacheMiddlewareImpl("tokens-list", 10 * 60 * 1000),
    async (_request: Request, response: Response) => {
      try {
        const items = await deps.getAvailableTokensImpl();
        sendSuccess(_request, response, { items });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/tokens/:tokenSymbol/metadata",
    (request: Request, response: Response, next) => {
      const cacheKey = `token-metadata:${String(request.params.tokenSymbol).toUpperCase()}`;
      deps.cacheMiddlewareImpl(cacheKey, 10 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }
        const metadata = await deps.getTokenMetadataImpl(tokenSymbol);

        if (!metadata) {
          throw new ApiError(
            404,
            "TOKEN_NOT_FOUND",
            "token metadata not found",
            {
              tokenSymbol,
            },
          );
        }

        sendSuccess(request, response, metadata);
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/address/:address/staged",
    (request: Request, response: Response, next) => {
      const cacheKey = buildAddressStagedCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const address = String(request.params.address).trim();
        const stage = normalizeAddressGraphStage(request);

        if (!tokenSymbol) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token query parameter is required",
            {
              query: "token",
            },
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
            {
              tokenSymbol,
            },
          );
        }
        if (!isValidAddress(address)) {
          throw new ApiError(
            400,
            "ADDRESS_INVALID",
            "address path parameter is invalid",
            {
              address,
            },
          );
        }

        const requestedDepth = readPositiveInt(
          String(request.query.depth ?? ""),
          apiConfig.graphDefaultDepth,
        );
        if (requestedDepth > apiConfig.graphHardMaxDepth) {
          throw new ApiError(
            400,
            "GRAPH_DEPTH_LIMIT_EXCEEDED",
            `depth must be <= ${apiConfig.graphHardMaxDepth}`,
            { received: requestedDepth, max: apiConfig.graphHardMaxDepth },
          );
        }

        const requestedEdgeLimit = readPositiveInt(
          String(request.query.edgeLimit ?? ""),
          apiConfig.graphMaxEdgesPerRequest,
        );
        if (requestedEdgeLimit > apiConfig.graphMaxEdgesPerRequest) {
          throw new ApiError(
            400,
            "GRAPH_EDGE_LIMIT_EXCEEDED",
            `edgeLimit must be <= ${apiConfig.graphMaxEdgesPerRequest}`,
            {
              received: requestedEdgeLimit,
              max: apiConfig.graphMaxEdgesPerRequest,
            },
          );
        }

        const coreEdgeLimit = Math.min(
          requestedEdgeLimit,
          apiConfig.graphStageCoreEdgeLimit,
        );
        const coreGraph = await deps.getAddressSubgraphImpl(
          tokenSymbol,
          address,
          1,
          coreEdgeLimit,
        );

        if (stage === "core") {
          sendSuccess(request, response, {
            tokenSymbol,
            address,
            stage,
            core: coreGraph,
            nextStage: "connections",
          });
          return;
        }

        const connectionsLimit = clampInt(
          readPositiveInt(String(request.query.connectionsLimit ?? ""), 25),
          1,
          200,
        );
        const connections = (
          await deps.getAddressConnectionsImpl(tokenSymbol, address)
        ).slice(0, connectionsLimit);

        if (stage === "connections") {
          sendSuccess(request, response, {
            tokenSymbol,
            address,
            stage,
            core: coreGraph,
            connections,
            nextStage: "full",
          });
          return;
        }

        const fullGraph = await deps.getAddressSubgraphImpl(
          tokenSymbol,
          address,
          requestedDepth,
          requestedEdgeLimit,
        );

        sendSuccess(request, response, {
          tokenSymbol,
          address,
          stage,
          core: coreGraph,
          connections,
          full: fullGraph,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/address/:address",
    (request: Request, response: Response, next) => {
      const cacheKey = buildAddressSubgraphCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const address = String(request.params.address).trim();
        if (!tokenSymbol) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token query parameter is required",
            {
              query: "token",
            },
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
            {
              tokenSymbol,
            },
          );
        }
        if (!isValidAddress(address)) {
          throw new ApiError(
            400,
            "ADDRESS_INVALID",
            "address path parameter is invalid",
            {
              address,
            },
          );
        }

        const depth = readPositiveInt(
          String(request.query.depth ?? ""),
          apiConfig.graphDefaultDepth,
        );
        if (depth > apiConfig.graphHardMaxDepth) {
          throw new ApiError(
            400,
            "GRAPH_DEPTH_LIMIT_EXCEEDED",
            `depth must be <= ${apiConfig.graphHardMaxDepth}`,
            { received: depth, max: apiConfig.graphHardMaxDepth },
          );
        }

        const edgeLimit = readPositiveInt(
          String(request.query.edgeLimit ?? ""),
          apiConfig.graphMaxEdgesPerRequest,
        );
        if (edgeLimit > apiConfig.graphMaxEdgesPerRequest) {
          throw new ApiError(
            400,
            "GRAPH_EDGE_LIMIT_EXCEEDED",
            `edgeLimit must be <= ${apiConfig.graphMaxEdgesPerRequest}`,
            { received: edgeLimit, max: apiConfig.graphMaxEdgesPerRequest },
          );
        }

        const graph = await deps.getAddressSubgraphImpl(
          tokenSymbol,
          address,
          depth,
          edgeLimit,
        );
        sendSuccess(request, response, graph, {
          isPartial: false,
          appliedLimits: {
            depth,
            edgeLimit,
          },
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/connections/address/:address",
    (request: Request, response: Response, next) => {
      const cacheKey = buildAddressConnectionsCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const address = String(request.params.address);
        if (!tokenSymbol) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token query parameter is required",
            {
              query: "token",
            },
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
            {
              tokenSymbol,
            },
          );
        }
        if (!isValidAddress(address)) {
          throw new ApiError(
            400,
            "ADDRESS_INVALID",
            "address path parameter is invalid",
            {
              address,
            },
          );
        }

        const connections = await deps.getAddressConnectionsImpl(
          tokenSymbol,
          address,
        );
        sendSuccess(request, response, {
          tokenSymbol,
          address,
          items: connections,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/trace/paths",
    (request: Request, response: Response, next) => {
      const cacheKey = buildTracePathsCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const fromAddress = String(request.query.from ?? "").trim();
        const toAddress = String(request.query.to ?? "").trim();

        if (!tokenSymbol || !fromAddress || !toAddress) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token, from, and to query parameters are required",
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
            { tokenSymbol },
          );
        }
        if (!isValidAddress(fromAddress) || !isValidAddress(toAddress)) {
          throw new ApiError(
            400,
            "ADDRESS_INVALID",
            "from and to query parameters must be valid wallet addresses",
            {
              fromAddress,
              toAddress,
            },
          );
        }

        const maxHops = clampInt(
          readPositiveInt(
            request.query.maxHops ? String(request.query.maxHops) : undefined,
            5,
          ),
          1,
          8,
        );
        const pathLimit = clampInt(
          readPositiveInt(
            request.query.limit ? String(request.query.limit) : undefined,
            20,
          ),
          1,
          100,
        );
        const stopAtTerminalsRaw = String(
          request.query.stopAtTerminals ?? "true",
        )
          .trim()
          .toLowerCase();
        const stopAtTerminals = !["false", "0", "no", "off"].includes(
          stopAtTerminalsRaw,
        );

        const items = await deps.findAddressPathsImpl({
          tokenSymbol,
          fromAddress,
          toAddress,
          maxHops,
          pathLimit,
          stopAtTerminals,
        });

        sendSuccess(request, response, {
          tokenSymbol,
          fromAddress,
          toAddress,
          maxHops,
          limit: pathLimit,
          stopAtTerminals,
          totalPaths: items.length,
          items,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/tokens/:tokenSymbol/top-holders",
    (request: Request, response: Response, next) => {
      const cacheKey = `top-holders:${String(request.params.tokenSymbol).toUpperCase()}:${String(request.query.limit ?? "10")}`;
      deps.cacheMiddlewareImpl(cacheKey, 5 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      const limit = readPositiveInt(String(request.query.limit ?? ""), 10);

      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }
        const result = await deps.getTopHoldersImpl(
          tokenSymbol,
          Math.min(limit, 100),
        );
        sendSuccess(request, response, result);
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/token/:tokenSymbol/staged",
    (request: Request, response: Response, next) => {
      const cacheKey = buildTokenStagedCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const stage = normalizeTokenGraphStage(request);
        const baseEdgeLimit = clampInt(
          readPositiveInt(
            String(request.query.edgeLimit ?? ""),
            apiConfig.tokenGraphStageBaseEdgeLimit,
          ),
          1,
          apiConfig.tokenGraphMaxEdges,
        );

        if (stage === "base") {
          const graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
            includeTopHoldersLimit: 0,
            edgeLimit: baseEdgeLimit,
          });

          sendSuccess(request, response, {
            tokenSymbol,
            stage,
            graph,
            nextStage: "holders",
          });
          return;
        }

        const topHoldersLimit = clampInt(
          readPositiveInt(String(request.query.topHoldersLimit ?? ""), 10),
          1,
          100,
        );

        if (stage === "holders") {
          const [graph, topHolders] = await Promise.all([
            deps.getFullTokenGraphImpl(tokenSymbol, {
              includeTopHoldersLimit: 0,
              edgeLimit: baseEdgeLimit,
            }),
            deps.getTopHoldersImpl(tokenSymbol, topHoldersLimit),
          ]);

          sendSuccess(request, response, {
            tokenSymbol,
            stage,
            graph,
            topHolders,
            nextStage: "full",
          });
          return;
        }

        const includeTopHolders = normalizeWithTopHolders(request);
        const graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
          includeTopHoldersLimit: includeTopHolders,
          edgeLimit: apiConfig.tokenGraphMaxEdges,
        });

        sendSuccess(request, response, {
          tokenSymbol,
          stage,
          graph,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/token/:tokenSymbol/max",
    (request: Request, response: Response, next) => {
      const cacheKey = `token-graph-max:${String(request.params.tokenSymbol).toUpperCase()}`;
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        await sendTokenGraphResponse(
          request,
          response,
          deps,
          tokenSymbol,
          0,
          undefined,
          "max",
        );
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/token/:tokenSymbol",
    (request: Request, response: Response, next) => {
      let includeTopHolders = 0;
      try {
        includeTopHolders = normalizeWithTopHolders(request);
      } catch (error: unknown) {
        handleRouteError(response, error);
        return;
      }
      const cacheKey = `token-graph:${String(request.params.tokenSymbol).toUpperCase()}:${includeTopHolders}`;
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const includeTopHolders = normalizeWithTopHolders(request);
        await sendTokenGraphResponse(
          request,
          response,
          deps,
          tokenSymbol,
          includeTopHolders,
          apiConfig.tokenGraphMaxEdges,
          "standard",
        );
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get("/transactions", async (request: Request, response: Response) => {
    try {
      const tokenSymbol = request.query.token
        ? String(request.query.token).trim()
        : undefined;
      const address = request.query.address
        ? String(request.query.address).trim()
        : undefined;

      if (tokenSymbol && !isValidTokenSymbol(tokenSymbol)) {
        throw new ApiError(
          400,
          "TOKEN_SYMBOL_INVALID",
          "token query parameter is invalid",
          {
            tokenSymbol,
          },
        );
      }
      if (address && !isValidAddress(address)) {
        throw new ApiError(
          400,
          "ADDRESS_INVALID",
          "address query parameter is invalid",
          {
            address,
          },
        );
      }

      const page = readPositiveInt(String(request.query.page ?? ""), 1);
      const pageSize = readPositiveInt(
        String(request.query.pageSize ?? ""),
        apiConfig.transactionPageSizeDefault,
      );
      const clampedPageSize = clampInt(
        pageSize,
        1,
        apiConfig.transactionPageSizeMax,
      );
      const directionRaw = String(request.query.dir ?? "")
        .trim()
        .toLowerCase();
      const direction =
        directionRaw === "from" || directionRaw === "to"
          ? directionRaw
          : undefined;
      const counterparty = request.query.counterparty
        ? String(request.query.counterparty).trim()
        : undefined;
      const startTime = readOptionalIsoDate(
        request.query.startTime ? String(request.query.startTime) : undefined,
      );
      const endTime = readOptionalIsoDate(
        request.query.endTime ? String(request.query.endTime) : undefined,
      );
      const minAmount = readOptionalNumber(
        request.query.minAmount ? String(request.query.minAmount) : undefined,
      );
      const maxAmount = readOptionalNumber(
        request.query.maxAmount ? String(request.query.maxAmount) : undefined,
      );
      const minUsd = readOptionalNumber(
        request.query.minUsd ? String(request.query.minUsd) : undefined,
      );
      const maxUsd = readOptionalNumber(
        request.query.maxUsd ? String(request.query.maxUsd) : undefined,
      );
      const usdRateNow = readOptionalNumber(
        request.query.usdRateNow ? String(request.query.usdRateNow) : undefined,
      );
      const sortByRaw = String(request.query.sortBy ?? "")
        .trim()
        .toLowerCase();
      const sortBy =
        sortByRaw === "amount" || sortByRaw === "usd" || sortByRaw === "time"
          ? sortByRaw
          : undefined;
      const sortDirRaw = String(request.query.sortDir ?? "")
        .trim()
        .toLowerCase();
      const sortDir =
        sortDirRaw === "asc" || sortDirRaw === "desc" ? sortDirRaw : undefined;

      const result = await deps.getTransactionsPageImpl({
        tokenSymbol,
        address,
        fromBlock: request.query.fromBlock
          ? readPositiveInt(String(request.query.fromBlock), 0)
          : undefined,
        toBlock: request.query.toBlock
          ? readPositiveInt(String(request.query.toBlock), 0)
          : undefined,
        direction,
        counterparty,
        startTime,
        endTime,
        minAmount,
        maxAmount,
        minUsd,
        maxUsd,
        usdRateNow,
        sortBy,
        sortDir,
        page,
        pageSize: clampedPageSize,
      });

      sendSuccess(request, response, result, {
        pagination: {
          page,
          pageSize: clampedPageSize,
          total: Number(result?.total ?? 0),
        },
      });
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });

  app.get("/labels/nodes", async (request: Request, response: Response) => {
    try {
      const tokenSymbol = request.query.tokenSymbol
        ? String(request.query.tokenSymbol).trim()
        : undefined;
      const label = request.query.label
        ? String(request.query.label).trim()
        : undefined;
      const labelType = request.query.labelType
        ? String(request.query.labelType).trim()
        : undefined;
      const labelSource = request.query.labelSource
        ? String(request.query.labelSource).trim()
        : undefined;
      const minConfidence = readOptionalNumber(
        request.query.minConfidence
          ? String(request.query.minConfidence)
          : undefined,
      );
      const maxConfidence = readOptionalNumber(
        request.query.maxConfidence
          ? String(request.query.maxConfidence)
          : undefined,
      );
      const updatedSince = readOptionalIsoDate(
        request.query.updatedSince
          ? String(request.query.updatedSince)
          : undefined,
      );
      const windowDays = readPositiveInt(
        request.query.windowDays ? String(request.query.windowDays) : undefined,
        30,
      );
      const page = readPositiveInt(String(request.query.page ?? ""), 1);
      const pageSize = clampInt(
        readPositiveInt(String(request.query.pageSize ?? ""), 50),
        1,
        apiConfig.transactionPageSizeMax,
      );

      if (tokenSymbol && !isValidTokenSymbol(tokenSymbol)) {
        throw new ApiError(
          400,
          "TOKEN_SYMBOL_INVALID",
          "tokenSymbol query parameter is invalid",
          { tokenSymbol },
        );
      }

      if (
        minConfidence !== undefined &&
        (minConfidence < 0 || minConfidence > 1)
      ) {
        throw new ApiError(
          400,
          "INVALID_REQUEST",
          "minConfidence must be between 0 and 1",
          { minConfidence },
        );
      }

      if (
        maxConfidence !== undefined &&
        (maxConfidence < 0 || maxConfidence > 1)
      ) {
        throw new ApiError(
          400,
          "INVALID_REQUEST",
          "maxConfidence must be between 0 and 1",
          { maxConfidence },
        );
      }

      if (
        minConfidence !== undefined &&
        maxConfidence !== undefined &&
        minConfidence > maxConfidence
      ) {
        throw new ApiError(
          400,
          "INVALID_REQUEST",
          "minConfidence cannot exceed maxConfidence",
          { minConfidence, maxConfidence },
        );
      }

      const result = await deps.getLabeledNodesImpl({
        tokenSymbol,
        label,
        labelType,
        labelSource,
        minConfidence,
        maxConfidence,
        updatedSince,
        windowDays,
        page,
        pageSize,
      });

      sendSuccess(request, response, result, {
        pagination: {
          page,
          pageSize,
          total: Number((result as { total?: number })?.total ?? 0),
        },
      });
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });

  app.get(
    "/tokens/:tokenSymbol/activity/:address",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        const address = String(request.params.address).trim();
        const days = Math.min(
          readPositiveInt(
            request.query.days ? String(request.query.days) : undefined,
            30,
          ),
          365,
        );

        if (!tokenSymbol || !address) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "tokenSymbol and address are required",
          );
        }

        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            {
              tokenSymbol,
            },
          );
        }

        if (!isValidAddress(address)) {
          throw new ApiError(
            400,
            "ADDRESS_INVALID",
            "address path parameter is invalid",
            {
              address,
            },
          );
        }

        const items = await deps.getAddressActivityImpl(
          tokenSymbol,
          address,
          days,
        );
        sendSuccess(request, response, { tokenSymbol, address, days, items });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.post(
    "/analytics/tokens/:tokenSymbol/refresh",
    async (_request: Request, response: Response) => {
      try {
        throw new ApiError(
          403,
          "INVALID_REQUEST",
          "Analytics refresh via POST is disabled",
        );
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/analytics/tokens/:tokenSymbol/timeseries",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const days = Math.min(
          readPositiveInt(
            request.query.days ? String(request.query.days) : undefined,
            90,
          ),
          3650,
        );

        const items = await deps.getTokenDailyMetricsImpl(tokenSymbol, days);
        sendSuccess(request, response, { tokenSymbol, days, items });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/precomputed/tokens/:tokenSymbol/overview",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const viewKey = `token-overview:${tokenSymbol}`;
        let precomputed = await deps.getPrecomputedApiViewImpl(viewKey);
        if (!precomputed) {
          await deps.refreshTokenPrecomputedViewsImpl(tokenSymbol);
          precomputed = await deps.getPrecomputedApiViewImpl(viewKey);
        }

        sendSuccess(request, response, {
          tokenSymbol,
          source: precomputed ? "precomputed" : "generated",
          overview: precomputed,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/analytics/tokens/:tokenSymbol/top-movers",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const windowDays = Math.min(
          readPositiveInt(
            request.query.windowDays
              ? String(request.query.windowDays)
              : undefined,
            7,
          ),
          365,
        );
        const limit = Math.min(
          readPositiveInt(
            request.query.limit ? String(request.query.limit) : undefined,
            20,
          ),
          200,
        );

        const items = await deps.getTokenTopMoversImpl(
          tokenSymbol,
          windowDays,
          limit,
        );
        sendSuccess(request, response, {
          tokenSymbol,
          windowDays,
          limit,
          items,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.post("/admin/cache/clear", (_request: Request, response: Response) => {
    deps.invalidateCacheImpl();
    deps.clearSubgraphCacheImpl();
    sendSuccess(_request, response, {
      ok: true,
      message: "All caches cleared",
    });
  });

  return app;
}

export function startApiServer(deps: ApiServerDeps = defaultDeps): {
  app: ReturnType<typeof createApiApp>;
  server: Server;
  shutdown: () => Promise<void>;
} {
  const app = createApiApp(deps);
  const server = app.listen(apiConfig.port, () => {
    console.log(`API server listening on port ${apiConfig.port}`);
  });
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 66_000;
  server.requestTimeout = 75_000;

  const shutdown = async (): Promise<void> => {
    await deps.closeDatabasePoolImpl();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  };

  return {
    app,
    server,
    shutdown,
  };
}
