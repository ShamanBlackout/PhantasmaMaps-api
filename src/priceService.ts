export type PriceSourceName = "saturnx" | "coingecko" | "phantasma-explorer";

export type TokenPriceQuote = {
  tokenSymbol: string;
  priceUsd: number | null;
  priceChange24h: number | null;
  source: PriceSourceName | null;
  fetchedAt: string;
  stale: boolean;
};

type SourceQuote = {
  priceUsd: number;
  priceChange24h: number | null;
};

type FetchLike = (
  input: string,
  init?: { signal?: AbortSignal; headers?: Record<string, string> },
) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>;

export type TokenPriceServiceOptions = {
  fetchImpl?: FetchLike;
  saturnxBaseUrl?: string;
  saturnxNetwork?: string;
  explorerTokensUrl?: string;
  coingeckoSimplePriceUrl?: string;
  coingeckoIds?: Record<string, string>;
  freshTtlMs?: number;
  failureTtlMs?: number;
  staleMaxMs?: number;
  requestTimeoutMs?: number;
  maxCacheEntries?: number;
  now?: () => number;
};

type CacheEntry = {
  quote: TokenPriceQuote;
  expiresAt: number;
  lastGood: TokenPriceQuote | null;
  lastGoodAt: number;
};

export const DEFAULT_COINGECKO_IDS: Record<string, string> = {
  SOUL: "phantasma",
  KCAL: "phantasma-energy",
};

function toFiniteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function readSymbol(candidate: Record<string, unknown>): string {
  return String(
    candidate.symbol ??
      candidate.tokenSymbol ??
      candidate.token_symbol ??
      candidate.token ??
      "",
  )
    .trim()
    .toUpperCase();
}

// SaturnX's response shape is not formally documented, so accept the common variants.
export function parseSaturnxQuote(
  payload: unknown,
  tokenSymbol: string,
): SourceQuote | null {
  const symbol = tokenSymbol.toUpperCase();
  const root = asRecord(payload);
  if (!root) return null;

  const data = root.data;
  const collection =
    root.tokens ?? root.prices ?? root.quotes ?? asRecord(data)?.tokens ?? data;
  let candidates: Record<string, unknown>[] = [];
  if (asRecord(data) && !Array.isArray(collection)) {
    candidates = [asRecord(data)!];
  } else if (Array.isArray(collection)) {
    candidates = collection.map(asRecord).filter(Boolean) as Record<
      string,
      unknown
    >[];
  } else if (root.price !== undefined || root.priceUsd !== undefined) {
    candidates = [root];
  }

  const token = candidates.find((candidate) => {
    const candidateSymbol = readSymbol(candidate);
    return (
      candidateSymbol === symbol ||
      (!candidateSymbol && candidates.length === 1)
    );
  });
  if (!token) return null;

  const priceUsd = toFiniteNumber(
    token.priceUsd ??
      token.price_usd ??
      token.usdPrice ??
      token.usd_price ??
      token.currentPrice ??
      token.current_price ??
      (asRecord(token.price)?.usd ?? token.price) ??
      token.usd,
  );
  if (priceUsd === null || priceUsd < 0) return null;

  const change = asRecord(token.change);
  return {
    priceUsd,
    priceChange24h: toFiniteNumber(
      token.priceChange24h ??
        token.price_change_24h ??
        token.change24h ??
        token.change_24h ??
        token.changePercent24h ??
        token.change_percent_24h ??
        change?.h24 ??
        change?.h24Percent,
    ),
  };
}

export function parseExplorerQuote(
  payload: unknown,
  tokenSymbol: string,
): SourceQuote | null {
  const root = asRecord(payload);
  const tokens = Array.isArray(root?.tokens)
    ? root!.tokens
    : Array.isArray(payload)
      ? payload
      : [];
  const token = (tokens as unknown[])
    .map(asRecord)
    .find(
      (candidate) =>
        candidate && readSymbol(candidate) === tokenSymbol.toUpperCase(),
    );
  if (!token) return null;
  const priceUsd = toFiniteNumber(asRecord(token.price)?.usd ?? token.price);
  if (priceUsd === null || priceUsd < 0) return null;
  // The explorer publishes spot prices only, without 24h movement.
  return { priceUsd, priceChange24h: null };
}

export function parseCoinGeckoQuote(
  payload: unknown,
  coingeckoId: string,
): SourceQuote | null {
  const entry = asRecord(asRecord(payload)?.[coingeckoId]);
  if (!entry) return null;
  const priceUsd = toFiniteNumber(entry.usd);
  if (priceUsd === null || priceUsd < 0) return null;
  return { priceUsd, priceChange24h: toFiniteNumber(entry.usd_24h_change) };
}

export function parseCoinGeckoIds(
  rawValue: string | undefined,
): Record<string, string> {
  if (!rawValue || !rawValue.trim()) return { ...DEFAULT_COINGECKO_IDS };
  const ids: Record<string, string> = {};
  for (const pair of rawValue.split(",")) {
    const [symbol, id] = pair.split(":").map((part) => part.trim());
    if (symbol && id) ids[symbol.toUpperCase()] = id;
  }
  return ids;
}

export function createTokenPriceService(
  options: TokenPriceServiceOptions = {},
) {
  const fetchImpl: FetchLike =
    options.fetchImpl ?? (globalThis.fetch as unknown as FetchLike);
  const saturnxBaseUrl = (
    options.saturnxBaseUrl ?? "https://apiops.saturnx.cc/v1/tokens"
  ).replace(/\/$/, "");
  const saturnxNetwork = options.saturnxNetwork ?? "mainnet";
  const explorerTokensUrl = (
    options.explorerTokensUrl ??
    "https://api-explorer.phantasma.info/api/v1/tokens"
  ).replace(/\/$/, "");
  const coingeckoSimplePriceUrl =
    options.coingeckoSimplePriceUrl ??
    "https://api.coingecko.com/api/v3/simple/price";
  const coingeckoIds = options.coingeckoIds ?? DEFAULT_COINGECKO_IDS;
  const freshTtlMs = options.freshTtlMs ?? 60_000;
  const failureTtlMs = options.failureTtlMs ?? 30_000;
  const staleMaxMs = options.staleMaxMs ?? 60 * 60_000;
  const requestTimeoutMs = options.requestTimeoutMs ?? 6_000;
  const maxCacheEntries = options.maxCacheEntries ?? 500;
  const now = options.now ?? Date.now;

  const cache = new Map<string, CacheEntry>();
  const inFlight = new Map<string, Promise<TokenPriceQuote>>();

  async function fetchJson(url: string): Promise<unknown> {
    const response = await fetchImpl(url, {
      signal: AbortSignal.timeout(requestTimeoutMs),
      headers: { accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`Price source responded with ${response.status}`);
    }
    return response.json();
  }

  async function fetchSources(
    symbol: string,
  ): Promise<Array<{ source: PriceSourceName; quote: SourceQuote | null }>> {
    const coingeckoId = coingeckoIds[symbol];
    const tasks: Array<{
      source: PriceSourceName;
      run: () => Promise<SourceQuote | null>;
    }> = [
      {
        source: "saturnx",
        run: async () =>
          parseSaturnxQuote(
            await fetchJson(
              `${saturnxBaseUrl}/${encodeURIComponent(symbol)}?network=${encodeURIComponent(saturnxNetwork)}`,
            ),
            symbol,
          ),
      },
    ];
    if (coingeckoId) {
      tasks.push({
        source: "coingecko",
        run: async () =>
          parseCoinGeckoQuote(
            await fetchJson(
              `${coingeckoSimplePriceUrl}?ids=${encodeURIComponent(coingeckoId)}&vs_currencies=usd&include_24hr_change=true`,
            ),
            coingeckoId,
          ),
      });
    }
    tasks.push({
      source: "phantasma-explorer",
      run: async () =>
        parseExplorerQuote(
          await fetchJson(
            `${explorerTokensUrl}?symbol=${encodeURIComponent(symbol)}&with_price=1`,
          ),
          symbol,
        ),
    });

    const settled = await Promise.allSettled(tasks.map((task) => task.run()));
    return tasks.map((task, index) => {
      const outcome = settled[index];
      return {
        source: task.source,
        quote: outcome.status === "fulfilled" ? outcome.value : null,
      };
    });
  }

  async function refresh(symbol: string): Promise<TokenPriceQuote> {
    const results = await fetchSources(symbol);
    // Sources are ordered by preference; fill a missing 24h change from any other source.
    const winner = results.find((result) => result.quote);
    const fetchedAtMs = now();
    const fetchedAt = new Date(fetchedAtMs).toISOString();
    const previous = cache.get(symbol);

    let quote: TokenPriceQuote;
    if (winner?.quote) {
      const changeFromOther = results.find(
        (result) => result.quote && result.quote.priceChange24h !== null,
      )?.quote?.priceChange24h;
      quote = {
        tokenSymbol: symbol,
        priceUsd: winner.quote.priceUsd,
        priceChange24h:
          winner.quote.priceChange24h ?? changeFromOther ?? null,
        source: winner.source,
        fetchedAt,
        stale: false,
      };
    } else if (
      previous?.lastGood &&
      fetchedAtMs - previous.lastGoodAt <= staleMaxMs
    ) {
      quote = { ...previous.lastGood, stale: true };
    } else {
      quote = {
        tokenSymbol: symbol,
        priceUsd: null,
        priceChange24h: null,
        source: null,
        fetchedAt,
        stale: false,
      };
    }

    const succeeded = Boolean(winner?.quote);
    cache.delete(symbol);
    cache.set(symbol, {
      quote,
      expiresAt: fetchedAtMs + (succeeded ? freshTtlMs : failureTtlMs),
      lastGood: succeeded ? quote : (previous?.lastGood ?? null),
      lastGoodAt: succeeded ? fetchedAtMs : (previous?.lastGoodAt ?? 0),
    });
    while (cache.size > maxCacheEntries) {
      const oldestKey = cache.keys().next().value;
      if (oldestKey === undefined) break;
      cache.delete(oldestKey);
    }
    return quote;
  }

  async function getQuote(tokenSymbol: string): Promise<TokenPriceQuote> {
    const symbol = tokenSymbol.trim().toUpperCase();
    const cached = cache.get(symbol);
    if (cached && cached.expiresAt > now()) return cached.quote;

    const pending = inFlight.get(symbol);
    if (pending) return pending;

    const request = refresh(symbol).finally(() => inFlight.delete(symbol));
    inFlight.set(symbol, request);
    return request;
  }

  return { getQuote, clear: () => cache.clear() };
}
