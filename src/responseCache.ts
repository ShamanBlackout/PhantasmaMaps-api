import type { Response } from "express";

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
  return (_request, response, next) => {
    const cached = responseCache.get(cacheKey);

    if (cached) {
      response.setHeader("X-Cache", "HIT");
      response.json(JSON.parse(cached));
      return;
    }

    // Override response.json to intercept and cache the response
    const originalJson = response.json.bind(response);
    response.json = function (data: any) {
      try {
        responseCache.set(cacheKey, JSON.stringify(data), ttlMs);
      } catch {
        // Ignore cache errors, still send response
      }

      return originalJson(data);
    };

    response.setHeader("X-Cache", "MISS");
    next();
  };
}

/**
 * Invalidates all cache entries matching a pattern.
 * Useful when data changes (e.g., after sync completes).
 */
export function invalidateCache(pattern?: RegExp): void {
  responseCache.clear(pattern);
}
