import express, { type Request, type Response } from "express";
import cors from "cors";
import compression from "compression";
import { apiConfig } from "./phantasma.config";
import { createPhantasmaRpcClient } from "./rpcClient";
import { cacheMiddleware, invalidateCache } from "./responseCache";
import {
  clearSubgraphCache,
  closeDatabasePool,
  getAddressActivity,
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

const rpcClient = createPhantasmaRpcClient();

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

app.get("/health", async (_request: Request, response: Response) => {
  try {
    await testDatabaseConnection();
    response.json({ ok: true });
  } catch (error: unknown) {
    response.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

app.get("/sync-status", async (_request: Request, response: Response) => {
  try {
    const [syncStatesResult, chainHeadResult] = await Promise.allSettled([
      getSyncStates(),
      rpcClient.getBlockHeight(),
    ]);

    if (syncStatesResult.status !== "fulfilled") {
      throw syncStatesResult.reason;
    }

    response.json({
      items: syncStatesResult.value,
      chainHeadBlockHeight:
        chainHeadResult.status === "fulfilled"
          ? Number(chainHeadResult.value)
          : null,
    });
  } catch (error: unknown) {
    response
      .status(500)
      .json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/sync-claims", async (request: Request, response: Response) => {
  try {
    const limit = readPositiveInt(String(request.query.limit ?? ""), 100);
    const result = await getBlockSyncClaimsView({
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

    response.json(result);
  } catch (error: unknown) {
    response
      .status(500)
      .json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get(
  "/tokens",
  cacheMiddleware("tokens-list", 10 * 60 * 1000),
  async (_request: Request, response: Response) => {
    try {
      const items = await getAvailableTokens();
      response.json({ items });
    } catch (error: unknown) {
      response
        .status(500)
        .json({
          error: error instanceof Error ? error.message : String(error),
        });
    }
  },
);

app.get(
  "/tokens/:tokenSymbol/metadata",
  (request: Request, response: Response, next) => {
    const cacheKey = `token-metadata:${String(request.params.tokenSymbol).toUpperCase()}`;
    cacheMiddleware(cacheKey, 10 * 60 * 1000)(request, response, next);
  },
  async (request: Request, response: Response) => {
    try {
      const tokenSymbol = String(request.params.tokenSymbol);
      const metadata = await getTokenMetadata(tokenSymbol);

      if (!metadata) {
        response.status(404).json({ error: "token metadata not found" });
        return;
      }

      response.json(metadata);
    } catch (error: unknown) {
      response.status(500).json({
        error: error instanceof Error ? error.message : String(error),
      });
    }
  },
);

app.get(
  "/graph/address/:address",
  async (request: Request, response: Response) => {
    const tokenSymbol = String(request.query.token ?? "").trim();

    if (!tokenSymbol) {
      response.status(400).json({ error: "token query parameter is required" });
      return;
    }

    try {
      const depth = readPositiveInt(
        String(request.query.depth ?? ""),
        apiConfig.graphDefaultDepth,
      );
      const edgeLimit = readPositiveInt(
        String(request.query.edgeLimit ?? ""),
        apiConfig.graphMaxEdgesPerRequest,
      );
      const graph = await getAddressSubgraph(
        tokenSymbol,
        String(request.params.address),
        depth,
        edgeLimit,
      );
      response.json(graph);
    } catch (error: unknown) {
      response.status(500).json({
        error: error instanceof Error ? error.message : String(error),
      });
    }
  },
);

app.get(
  "/tokens/:tokenSymbol/top-holders",
  (request: Request, response: Response, next) => {
    const cacheKey = `top-holders:${String(request.params.tokenSymbol).toUpperCase()}:${String(request.query.limit ?? "10")}`;
    cacheMiddleware(cacheKey, 5 * 60 * 1000)(request, response, next);
  },
  async (request: Request, response: Response) => {
    const limit = readPositiveInt(String(request.query.limit ?? ""), 10);

    try {
      const result = await getTopHolders(
        String(request.params.tokenSymbol),
        Math.min(limit, 100),
      );
      response.json(result);
    } catch (error: unknown) {
      response.status(500).json({
        error: error instanceof Error ? error.message : String(error),
      });
    }
  },
);

app.get(
  "/graph/token/:tokenSymbol",
  (request: Request, response: Response, next) => {
    const cacheKey = `token-graph:${String(request.params.tokenSymbol).toUpperCase()}`;
    cacheMiddleware(cacheKey, 1 * 60 * 1000)(request, response, next);
  },
  async (request: Request, response: Response) => {
    try {
      const graph = await getFullTokenGraph(String(request.params.tokenSymbol));
      response.json(graph);
    } catch (error: unknown) {
      response.status(500).json({
        error: error instanceof Error ? error.message : String(error),
      });
    }
  },
);

app.get("/transactions", async (request: Request, response: Response) => {
  try {
    const page = readPositiveInt(String(request.query.page ?? ""), 1);
    const pageSize = readPositiveInt(
      String(request.query.pageSize ?? ""),
      apiConfig.transactionPageSizeDefault,
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

    const result = await getTransactionsPage({
      tokenSymbol: request.query.token
        ? String(request.query.token)
        : undefined,
      address: request.query.address
        ? String(request.query.address)
        : undefined,
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
      pageSize,
    });

    response.json(result);
  } catch (error: unknown) {
    response
      .status(500)
      .json({ error: error instanceof Error ? error.message : String(error) });
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
        response
          .status(400)
          .json({ error: "tokenSymbol and address are required" });
        return;
      }

      const items = await getAddressActivity(tokenSymbol, address, days);
      response.json({ tokenSymbol, address, days, items });
    } catch (error: unknown) {
      response.status(500).json({
        error: error instanceof Error ? error.message : String(error),
      });
    }
  },
);

app.post("/admin/cache/clear", (_request: Request, response: Response) => {
  invalidateCache();
  clearSubgraphCache();
  response.json({ ok: true, message: "All caches cleared" });
});

const server = app.listen(apiConfig.port, () => {
  console.log(`API server listening on port ${apiConfig.port}`);
});

async function shutdown(): Promise<void> {
  await closeDatabasePool();
  server.close();
}

process.on("SIGINT", () => {
  void shutdown();
});

process.on("SIGTERM", () => {
  void shutdown();
});
