import type { Request } from "express";
import { apiConfig } from "../phantasma.config";
import { clampInt, readPositiveInt } from "./params";

function normalizeTokenQueryForCache(request: Request): string {
  return String(request.query.token ?? "")
    .trim()
    .toUpperCase();
}

function normalizeAddressPathForCache(request: Request): string {
  return String(request.params.address ?? "").trim();
}

export function buildAddressSubgraphCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const address = normalizeAddressPathForCache(request);
  const depth = readPositiveInt(
    String(request.query.depth ?? ""),
    apiConfig.graphDefaultDepth,
  );
  const edgeLimit = readPositiveInt(
    String(request.query.edgeLimit ?? ""),
    apiConfig.graphMaxEdgesPerRequest,
  );

  return `address-subgraph:${tokenSymbol}:${address}:${depth}:${edgeLimit}`;
}

export function buildAddressConnectionsCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const address = normalizeAddressPathForCache(request);
  return `address-connections:${tokenSymbol}:${address}`;
}

export function buildAddressStagedCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const address = normalizeAddressPathForCache(request);
  const stage = String(request.query.stage ?? "core")
    .trim()
    .toLowerCase();
  const depth = readPositiveInt(
    String(request.query.depth ?? ""),
    apiConfig.graphDefaultDepth,
  );
  const edgeLimit = readPositiveInt(
    String(request.query.edgeLimit ?? ""),
    apiConfig.graphMaxEdgesPerRequest,
  );
  const connectionsLimit = readPositiveInt(
    String(request.query.connectionsLimit ?? ""),
    25,
  );

  return `address-staged:${tokenSymbol}:${address}:${stage}:${depth}:${edgeLimit}:${connectionsLimit}`;
}

export function buildTokenStagedCacheKey(request: Request): string {
  const tokenSymbol = String(request.params.tokenSymbol ?? "")
    .trim()
    .toUpperCase();
  const stage = String(request.query.stage ?? "base")
    .trim()
    .toLowerCase();
  const edgeLimit = readPositiveInt(
    String(request.query.edgeLimit ?? ""),
    apiConfig.tokenGraphStageBaseEdgeLimit,
  );
  const topHoldersLimit = readPositiveInt(
    String(request.query.topHoldersLimit ?? ""),
    10,
  );

  return `token-staged:${tokenSymbol}:${stage}:${edgeLimit}:${topHoldersLimit}`;
}

export function buildTracePathsCacheKey(request: Request): string {
  const tokenSymbol = normalizeTokenQueryForCache(request);
  const fromAddress = String(request.query.from ?? "").trim();
  const toAddress = String(request.query.to ?? "").trim();
  const maxHops = clampInt(
    readPositiveInt(
      request.query.maxHops ? String(request.query.maxHops) : undefined,
      5,
    ),
    1,
    8,
  );
  const pathLimit = clampInt(
    readPositiveInt(
      request.query.limit ? String(request.query.limit) : undefined,
      20,
    ),
    1,
    100,
  );
  const stopAtTerminalsRaw = String(request.query.stopAtTerminals ?? "true")
    .trim()
    .toLowerCase();
  const stopAtTerminals = !["false", "0", "no", "off"].includes(
    stopAtTerminalsRaw,
  );

  return `trace-paths:${tokenSymbol}:${fromAddress}:${toAddress}:${maxHops}:${pathLimit}:${stopAtTerminals ? "1" : "0"}`;
}
