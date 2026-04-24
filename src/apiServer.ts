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
  getAddressActivity,
  getAddressConnections,
  getAddressSubgraph,
  getAvailableTokens,
  getBlockSyncClaimsView,
  getFullTokenGraph,
  getSyncStates,
  getTokenMetadata,
  getTopHolders,
  getTransactionsPage,
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
) => (request: Request, response: Response, next: NextFunction) => void;

export type ApiServerDeps = {
  rpcClient: {
    getBlockHeight: () => Promise<number | string>;
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
  getTopHoldersImpl: (tokenSymbol: string, limit: number) => Promise<unknown>;
  getFullTokenGraphImpl: (
    tokenSymbol: string,
    options: { includeTopHoldersLimit: number },
  ) => Promise<unknown>;
  getTransactionsPageImpl: typeof getTransactionsPage;
  getAddressActivityImpl: (
    tokenSymbol: string,
    address: string,
    days: number,
  ) => Promise<unknown[]>;
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
  getTopHoldersImpl: getTopHolders,
  getFullTokenGraphImpl: getFullTokenGraph,
  getTransactionsPageImpl: getTransactionsPage,
  getAddressActivityImpl: getAddressActivity,
};

function readPositiveInt(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
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

function isValidTokenSymbol(rawToken: string): boolean {
  const token = String(rawToken || "").trim();
  return /^[A-Z0-9_-]{2,16}$/i.test(token);
}

function isValidAddress(rawAddress: string): boolean {
  const address = String(rawAddress || "").trim();
  return /^P[a-zA-Z0-9]{20,}$/.test(address);
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
  response.setHeader("ETag", etag);

  if (request.headers["if-none-match"] === etag) {
    response.status(304).end();
    return;
  }

  response.status(status).json(body);
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
  mode: "standard" | "max" = "standard",
): Promise<void> {
  const graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
    includeTopHoldersLimit: includeTopHolders,
  });
  const graphNodes = Array.isArray((graph as { nodes?: unknown[] })?.nodes)
    ? ((graph as { nodes?: unknown[] }).nodes ?? [])
    : [];
  const graphEdges = Array.isArray((graph as { edges?: unknown[] })?.edges)
    ? ((graph as { edges?: unknown[] }).edges ?? [])
    : [];

  sendSuccess(request, response, graph, {
    isPartial: false,
    mode,
    appliedLimits: {
      topHoldersLimit: includeTopHolders,
    },
    totalNodeCount: graphNodes.length,
    totalEdgeCount: graphEdges.length,
  });
}

export function createApiApp(deps: ApiServerDeps = defaultDeps) {
  const app = express();

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
      sendSuccess(_request, response, { ok: true });
    } catch (error: unknown) {
      handleRouteError(response, error);
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
    "/graph/address/:address",
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
