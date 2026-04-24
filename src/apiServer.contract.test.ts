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
