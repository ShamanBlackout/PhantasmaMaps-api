import express, { type Request, type Response } from "express";
import { apiConfig } from "./phantasma.config";
import {
  closeDatabasePool,
  getAddressSubgraph,
  getAvailableTokens,
  getFullTokenGraph,
  getSyncStates,
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

const app = express();
app.use(express.json());

app.get("/health", async (_request: Request, response: Response) => {
  try {
    await testDatabaseConnection();
    response.json({ ok: true });
  } catch (error: unknown) {
    response
      .status(500)
      .json({
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
  }
});

app.get("/sync-status", async (_request: Request, response: Response) => {
  try {
    const syncStates = await getSyncStates();
    response.json({ items: syncStates });
  } catch (error: unknown) {
    response
      .status(500)
      .json({ error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/tokens", async (_request: Request, response: Response) => {
  try {
    const items = await getAvailableTokens();
    response.json({ items });
  } catch (error: unknown) {
    response
      .status(500)
      .json({ error: error instanceof Error ? error.message : String(error) });
  }
});

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
      response
        .status(500)
        .json({
          error: error instanceof Error ? error.message : String(error),
        });
    }
  },
);

app.get(
  "/graph/token/:tokenSymbol",
  async (request: Request, response: Response) => {
    try {
      const graph = await getFullTokenGraph(String(request.params.tokenSymbol));
      response.json(graph);
    } catch (error: unknown) {
      response
        .status(500)
        .json({
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
