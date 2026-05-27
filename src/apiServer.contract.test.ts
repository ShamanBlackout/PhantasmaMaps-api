import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import { createApiApp, type ApiServerDeps } from "./apiServer";
import { apiConfig } from "./phantasma.config";

function noCache() {
  return (_request: unknown, _response: unknown, next: () => void) => next();
}

function createDeps(): ApiServerDeps {
  return {
    rpcClient: {
      getBlockHeight: async () => 123,
    },
    cacheMiddlewareImpl: () => noCache(),
    invalidateCacheImpl: () => {},
    clearSubgraphCacheImpl: () => {},
    closeDatabasePoolImpl: async () => {},
    testDatabaseConnectionImpl: async () => {},
    getSyncStatesImpl: async () => [
      {
        tokenSymbol: "__chain__",
        lastBlockHeight: 120,
        updatedAt: new Date().toISOString(),
        metadata: null,
      },
    ],
    getBlockSyncClaimsViewImpl: async () => ({
      summary: {
        pending: 0,
        claimed: 0,
        completed: 0,
        failed: 0,
        exhausted: 0,
        retryBlocked: 0,
        nextRetryAt: null,
      },
      items: [],
    }),
    getAvailableTokensImpl: async () => ["SOUL", "KCAL"],
    getTokenMetadataImpl: async (tokenSymbol: string) => ({
      symbol: tokenSymbol,
      decimals: 8,
      currentSupplyNormalized: "1000",
    }),
    getAddressSubgraphImpl: async () => ({
      totalSupply: 1000,
      nodes: [],
      edges: [],
    }),
    getAddressConnectionsImpl: async () => [],
    getTopHoldersImpl: async () => ({ items: [] }),
    getFullTokenGraphImpl: async () => ({
      totalSupply: 1000,
      nodes: [],
      edges: [],
    }),
    getTransactionsPageImpl: async () => ({
      items: [],
      total: 0,
      page: 1,
      pageSize: 50,
    }),
    getAddressActivityImpl: async () => [],
    refreshTokenAnalyticsForDateImpl: async () => {},
    getTokenDailyMetricsImpl: async () => [],
    getTokenTopMoversImpl: async () => [],
    getLabeledNodesImpl: async () => ({
      page: 1,
      pageSize: 50,
      total: 0,
      appliedFilters: {},
      items: [],
    }),
  };
}

test("GET /tokens returns envelope contract", async () => {
  const app = createApiApp(createDeps());
  const response = await request(app).get("/tokens");

  assert.equal(response.status, 200);
  assert.equal(typeof response.body?.requestId, "string");
  assert.ok(response.body?.meta);
  assert.equal(response.body?.meta?.source, "live");
  assert.deepEqual(response.body?.data?.items, ["SOUL", "KCAL"]);
});

test("GET /graph/token/:tokenSymbol rejects deprecated includeTopHolders", async () => {
  const app = createApiApp(createDeps());
  const response = await request(app).get(
    "/graph/token/SOUL?includeTopHolders=1",
  );

  assert.equal(response.status, 400);
  assert.equal(response.body?.error?.code, "INVALID_REQUEST");
  assert.equal(
    response.body?.error?.details?.deprecatedQuery,
    "includeTopHolders",
  );
  assert.equal(
    response.body?.error?.details?.replacementQuery,
    "withTopHolders",
  );
});

test("GET /graph/token/:tokenSymbol accepts withTopHolders=true", async () => {
  const app = createApiApp(createDeps());
  const response = await request(app).get(
    "/graph/token/SOUL?withTopHolders=true",
  );

  assert.equal(response.status, 200);
  assert.equal(response.body?.meta?.appliedLimits?.topHoldersLimit, 10);
  assert.equal(
    response.body?.meta?.appliedLimits?.edgeLimit,
    apiConfig.tokenGraphMaxEdges,
  );
});

test("GET /graph/token/:tokenSymbol accepts single-character symbols", async () => {
  const deps = createDeps();
  let receivedTokenSymbol = "";
  deps.getFullTokenGraphImpl = async (tokenSymbol: string) => {
    receivedTokenSymbol = tokenSymbol;
    return {
      totalSupply: 1000,
      nodes: [],
      edges: [],
    };
  };

  const app = createApiApp(deps);
  const response = await request(app).get("/graph/token/D");

  assert.equal(response.status, 200);
  assert.equal(receivedTokenSymbol, "D");
});

test("GET /graph/token/:tokenSymbol accepts topHoldersLimit override", async () => {
  const app = createApiApp(createDeps());
  const response = await request(app).get(
    "/graph/token/SOUL?topHoldersLimit=25",
  );

  assert.equal(response.status, 200);
  assert.equal(response.body?.meta?.appliedLimits?.topHoldersLimit, 25);
  assert.equal(
    response.body?.meta?.appliedLimits?.edgeLimit,
    apiConfig.tokenGraphMaxEdges,
  );
  assert.equal(response.body?.meta?.totalNodeCount, 0);
  assert.equal(response.body?.meta?.totalEdgeCount, 0);
});

test("GET /graph/token/:tokenSymbol/max returns dedicated max mode graph", async () => {
  const app = createApiApp(createDeps());
  const response = await request(app).get("/graph/token/SOUL/max");

  assert.equal(response.status, 200);
  assert.equal(response.body?.meta?.mode, "max");
  assert.equal(response.body?.meta?.appliedLimits?.topHoldersLimit, 0);
  assert.equal(response.body?.meta?.appliedLimits?.edgeLimit, null);
  assert.equal(response.body?.meta?.totalNodeCount, 0);
  assert.equal(response.body?.meta?.totalEdgeCount, 0);
});

test("GET /graph/token/:tokenSymbol degrades to smaller graph when primary query fails", async () => {
  const deps = createDeps();
  let callCount = 0;
  deps.getFullTokenGraphImpl = async () => {
    callCount += 1;
    if (callCount === 1) {
      throw new Error("temporary graph failure");
    }

    return {
      totalSupply: 1000,
      nodes: [],
      edges: [],
    };
  };

  const app = createApiApp(deps);
  const response = await request(app).get(
    "/graph/token/KCAL?topHoldersLimit=25",
  );

  assert.equal(response.status, 200);
  assert.equal(response.body?.meta?.isPartial, true);
  assert.equal(response.body?.meta?.appliedLimits?.topHoldersLimit, 0);
  assert.equal(response.body?.meta?.degradedFrom?.topHoldersLimit, 25);
  assert.equal(
    response.body?.meta?.degradedFrom?.edgeLimit,
    apiConfig.tokenGraphMaxEdges,
  );
  assert.equal(response.body?.meta?.totalNodeCount, 0);
  assert.equal(response.body?.meta?.totalEdgeCount, 0);
});

test("POST /analytics/tokens/:tokenSymbol/refresh validates date format", async () => {
  const app = createApiApp(createDeps());
  const response = await request(app).post(
    "/analytics/tokens/SOUL/refresh?date=2026/01/01",
  );

  assert.equal(response.status, 400);
  assert.equal(response.body?.error?.code, "INVALID_REQUEST");
});

test("GET /analytics/tokens/:tokenSymbol/timeseries returns envelope", async () => {
  const deps = createDeps();
  deps.getTokenDailyMetricsImpl = async () => [
    {
      tokenSymbol: "SOUL",
      bucketDate: "2026-05-10",
      holderCount: 10,
      newHolderCount: 2,
      lostHolderCount: 1,
      activeWalletCount: 7,
      transactionCount: 15,
      transferVolume: 125.5,
      currentSupply: 1000,
      top10Share: 42.5,
      top50Share: 88.2,
      topWalletShare: 13.7,
      giniCoefficient: 0.62,
      medianTransferAmount: 3.1,
      avgTransferAmount: 8.4,
      updatedAt: new Date().toISOString(),
    },
  ];

  const app = createApiApp(deps);
  const response = await request(app).get(
    "/analytics/tokens/SOUL/timeseries?days=30",
  );

  assert.equal(response.status, 200);
  assert.equal(response.body?.data?.tokenSymbol, "SOUL");
  assert.equal(response.body?.data?.days, 30);
  assert.equal(Array.isArray(response.body?.data?.items), true);
  assert.equal(response.body?.data?.items?.length, 1);
});

test("GET /analytics/tokens/:tokenSymbol/top-movers returns envelope", async () => {
  const deps = createDeps();
  deps.getTokenTopMoversImpl = async () => [
    {
      tokenSymbol: "SOUL",
      address: "P2Kexample",
      latestDate: "2026-05-10",
      baselineDate: "2026-05-03",
      latestBalance: 100,
      baselineBalance: 70,
      deltaBalance: 30,
      deltaPct: 42.857,
    },
  ];

  const app = createApiApp(deps);
  const response = await request(app).get(
    "/analytics/tokens/SOUL/top-movers?windowDays=7&limit=5",
  );

  assert.equal(response.status, 200);
  assert.equal(response.body?.data?.tokenSymbol, "SOUL");
  assert.equal(response.body?.data?.windowDays, 7);
  assert.equal(response.body?.data?.limit, 5);
  assert.equal(Array.isArray(response.body?.data?.items), true);
});

test("GET /labels/nodes returns envelope and forwards filters", async () => {
  const deps = createDeps();
  let captured: Record<string, unknown> = {};
  deps.getLabeledNodesImpl = async (options) => {
    captured = options as Record<string, unknown>;
    return {
      page: Number(options.page),
      pageSize: Number(options.pageSize),
      total: 1,
      appliedFilters: {
        tokenSymbol: options.tokenSymbol,
      },
      items: [
        {
          tokenSymbol: "SOUL",
          address: "P2Kexample",
          label: "Hub",
          labelConfidence: 0.92,
        },
      ],
    };
  };

  const app = createApiApp(deps);
  const response = await request(app).get(
    "/labels/nodes?tokenSymbol=SOUL&label=Hub&minConfidence=0.8&page=2&pageSize=10",
  );

  assert.equal(response.status, 200);
  assert.equal(response.body?.data?.total, 1);
  assert.equal(response.body?.meta?.pagination?.page, 2);
  assert.equal(response.body?.meta?.pagination?.pageSize, 10);
  assert.equal(captured?.tokenSymbol, "SOUL");
  assert.equal(captured?.label, "Hub");
  assert.equal(captured?.minConfidence, 0.8);
});

test("GET /labels/nodes validates confidence bounds", async () => {
  const app = createApiApp(createDeps());
  const response = await request(app).get("/labels/nodes?minConfidence=1.5");

  assert.equal(response.status, 400);
  assert.equal(response.body?.error?.code, "INVALID_REQUEST");
});
