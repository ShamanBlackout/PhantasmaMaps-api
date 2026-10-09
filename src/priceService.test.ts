import test from "node:test";
import assert from "node:assert/strict";
import {
  createTokenPriceService,
  parseCoinGeckoIds,
  parseExplorerQuote,
  parseSaturnxQuote,
} from "./priceService";

type Route = (url: string) => unknown;

function createFetch(route: Route) {
  const calls: string[] = [];
  const fetchImpl = async (url: string) => {
    calls.push(url);
    const body = route(url);
    if (body instanceof Error) throw body;
    return { ok: true, status: 200, json: async () => body };
  };
  return { fetchImpl, calls };
}

const explorerKcal = { tokens: [{ symbol: "KCAL", price: { usd: 2.4738e-8 } }] };

test("parsers handle explorer, SaturnX and CoinGecko id config", () => {
  assert.deepEqual(parseExplorerQuote(explorerKcal, "kcal"), {
    priceUsd: 2.4738e-8,
    priceChange24h: null,
  });
  assert.equal(
    parseExplorerQuote({ tokens: [{ symbol: "CROWN", price: { usd: null } }] }, "CROWN"),
    null,
  );
  assert.deepEqual(
    parseSaturnxQuote({ data: { symbol: "KCAL", priceUsd: "3e-8", change24h: -2 } }, "KCAL"),
    { priceUsd: 3e-8, priceChange24h: -2 },
  );
  assert.deepEqual(parseCoinGeckoIds("soul:phantasma, kcal:phantasma-energy"), {
    SOUL: "phantasma",
    KCAL: "phantasma-energy",
  });
});

test("falls back to the explorer when SaturnX and CoinGecko fail", async () => {
  const { fetchImpl } = createFetch((url) =>
    url.includes("api-explorer") ? explorerKcal : new Error("blocked"),
  );
  const service = createTokenPriceService({ fetchImpl });

  const quote = await service.getQuote("kcal");

  assert.equal(quote.tokenSymbol, "KCAL");
  assert.equal(quote.priceUsd, 2.4738e-8);
  assert.equal(quote.source, "phantasma-explorer");
  assert.equal(quote.priceChange24h, null);
  assert.equal(quote.stale, false);
});

test("prefers SaturnX and fills a missing 24h change from CoinGecko", async () => {
  const { fetchImpl } = createFetch((url) => {
    if (url.includes("saturnx")) return { symbol: "SOUL", price: 0.011 };
    if (url.includes("coingecko"))
      return { phantasma: { usd: 0.0105, usd_24h_change: -6.8 } };
    return { tokens: [{ symbol: "SOUL", price: { usd: 0.0104 } }] };
  });
  const service = createTokenPriceService({ fetchImpl });

  const quote = await service.getQuote("SOUL");

  assert.equal(quote.source, "saturnx");
  assert.equal(quote.priceUsd, 0.011);
  assert.equal(quote.priceChange24h, -6.8);
});

test("caches quotes and coalesces concurrent requests", async () => {
  let currentTime = 1_000;
  const { fetchImpl, calls } = createFetch((url) =>
    url.includes("api-explorer") ? explorerKcal : new Error("down"),
  );
  const service = createTokenPriceService({
    fetchImpl,
    freshTtlMs: 60_000,
    now: () => currentTime,
  });

  await Promise.all([service.getQuote("KCAL"), service.getQuote("KCAL")]);
  const callsAfterFirst = calls.length;
  await service.getQuote("KCAL");
  assert.equal(calls.length, callsAfterFirst);

  currentTime += 61_000;
  await service.getQuote("KCAL");
  assert.equal(calls.length, callsAfterFirst * 2);
});

test("serves the last good quote as stale when every source fails", async () => {
  let currentTime = 0;
  let sourcesUp = true;
  const { fetchImpl } = createFetch((url) =>
    sourcesUp && url.includes("api-explorer") ? explorerKcal : new Error("down"),
  );
  const service = createTokenPriceService({
    fetchImpl,
    freshTtlMs: 1_000,
    staleMaxMs: 10_000,
    now: () => currentTime,
  });

  await service.getQuote("KCAL");
  sourcesUp = false;
  currentTime = 5_000;
  const stale = await service.getQuote("KCAL");
  assert.equal(stale.priceUsd, 2.4738e-8);
  assert.equal(stale.stale, true);

  currentTime = 60_000;
  const expired = await service.getQuote("KCAL");
  assert.equal(expired.priceUsd, null);
  assert.equal(expired.source, null);
});
