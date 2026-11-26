import type { Response } from "express";
import { apiConfig } from "./phantasma.config";
import { getCachedApiResponse, setCachedApiResponse } from "./database";

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

const EXPIRED_SWEEP_INTERVAL_MS = 60_000;

/**
 * In-memory TTL cache bounded by entry count. When full, expired entries are
 * dropped first, then the oldest-written entry is evicted.
 */
export class SimpleCache<T> {
  private cache = new Map<string, CacheEntry<T>>();

  private sweepTimer: NodeJS.Timeout | null = null;

  constructor(private readonly maxEntries: number) {}

  set(key: string, value: T, ttlMs: number): void {
    // Re-insert so Map iteration order tracks write recency.
    this.cache.delete(key);

    if (this.cache.size >= this.maxEntries) {
      this.sweepExpired();
    }

    while (this.cache.size >= this.maxEntries) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey === undefined) {
        break;
      }
      this.cache.delete(oldestKey);
    }

    this.cache.set(key, {
      value,
      expiresAt: Date.now() + ttlMs,
    });
    this.ensureSweepTimer();
  }

  get(key: string): T | null {
    const entry = this.cache.get(key);

    if (!entry) {
      return null;
    }

    if (Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return null;
    }

    return entry.value;
  }

  sweepExpired(now = Date.now()): void {
    for (const [key, entry] of this.cache) {
      if (now > entry.expiresAt) {
        this.cache.delete(key);
      }
    }
  }

  size(): number {
    return this.cache.size;
  }

  private ensureSweepTimer(): void {
    if (this.sweepTimer) {
      return;
    }

    this.sweepTimer = setInterval(
      () => this.sweepExpired(),
      EXPIRED_SWEEP_INTERVAL_MS,
    );
    this.sweepTimer.unref();
  }
}

/**
 * Caches JSON responses with automatic TTL expiry.
 * Helps reduce database load for expensive queries.
 */
export const responseCache = new SimpleCache<string>(
  apiConfig.responseCacheMaxEntries,
);

/**
 * Middleware to serve cached response if available.
 * Call this before your route handler.
 */
export function cacheMiddleware(
  cacheKey: string,
  ttlMs: number,
): (request: any, response: Response, next: () => void) => void {
  return async (_request, response, next) => {
    const inMemoryCached = responseCache.get(cacheKey);

    if (inMemoryCached) {
      response.setHeader("X-Cache", "HIT");
      response.json(JSON.parse(inMemoryCached));
      return;
    }

    let databaseCached: string | null = null;
    let databaseLookupStatus: "hit" | "miss" | "stale" = "miss";
    try {
      const lookup = await getCachedApiResponse(cacheKey);
      databaseLookupStatus = lookup.status;
      databaseCached = lookup.payload;
    } catch {
      databaseCached = null;
      databaseLookupStatus = "miss";
    }

    if (databaseLookupStatus === "hit" && databaseCached) {
      responseCache.set(cacheKey, databaseCached, ttlMs);
      response.setHeader("X-Cache", "HIT");
      response.json(JSON.parse(databaseCached));
      return;
    }

    if (
      databaseLookupStatus === "stale" &&
      databaseCached &&
      apiConfig.cacheServeStale
    ) {
      const staleTtlMs = Math.max(1_000, Math.min(5_000, ttlMs));
      responseCache.set(cacheKey, databaseCached, staleTtlMs);
      response.setHeader("X-Cache", "STALE");
      response.json(JSON.parse(databaseCached));
      return;
    }

    // Override response.json to intercept and cache the response
    const originalJson = response.json.bind(response);
    response.json = function (data: any) {
      try {
        // Cache only successful responses to avoid persisting transient errors.
        if (response.statusCode >= 200 && response.statusCode < 300) {
          const payloadJson = JSON.stringify(data);
          responseCache.set(cacheKey, payloadJson, ttlMs);
          void setCachedApiResponse(cacheKey, payloadJson, ttlMs).catch(() => {
            // Ignore persistent cache errors; API response should still succeed.
          });
        }
      } catch {
        // Ignore cache errors, still send response
      }

      return originalJson(data);
    };

    response.setHeader(
      "X-Cache",
      databaseLookupStatus === "stale" ? "STALE-MISS" : "MISS",
    );
    next();
  };
}
