import type { Response } from "express";
import { apiConfig } from "./phantasma.config";
import {
  clearApiQueryCache,
  getCachedApiResponse,
  setCachedApiResponse,
} from "./database";

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

class SimpleCache<T> {
  private cache = new Map<string, CacheEntry<T>>();

  set(key: string, value: T, ttlMs: number): void {
    this.cache.set(key, {
      value,
      expiresAt: Date.now() + ttlMs,
    });
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

  clear(keyPattern?: RegExp): void {
    if (!keyPattern) {
      this.cache.clear();
      return;
    }

    for (const key of this.cache.keys()) {
      if (keyPattern.test(key)) {
        this.cache.delete(key);
      }
    }
  }

  size(): number {
    return this.cache.size;
  }
}

/**
 * Caches JSON responses with automatic TTL expiry.
 * Helps reduce database load for expensive queries.
 */
export const responseCache = new SimpleCache<string>();

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

/**
 * Invalidates all cache entries matching a pattern.
 * Useful when data changes (e.g., after sync completes).
 */
export function invalidateCache(pattern?: RegExp): void {
  responseCache.clear(pattern);

  // Pattern-based invalidation is in-memory only. Full invalidation also clears DB cache.
  if (!pattern) {
    void clearApiQueryCache().catch(() => {
      // Ignore persistent cache clear errors; data freshness still improves via TTL.
    });
  }
}
