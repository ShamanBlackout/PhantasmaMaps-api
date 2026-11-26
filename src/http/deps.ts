import type { Request, Response, NextFunction } from "express";
import {
  getBlockSyncClaimsView,
  getTransactionsPage,
  closeDatabasePool,
  testDatabaseConnection,
  getSyncStates,
  getAvailableTokens,
  getTokenMetadata,
  getAddressSubgraph,
  getAddressConnections,
  getPrecomputedApiView,
  refreshTokenPrecomputedViews,
  findAddressPaths,
  getTopHolders,
  getFullTokenGraph,
  getAddressActivity,
  refreshTokenAnalyticsForDate,
  getTokenDailyMetrics,
  getTokenTopMovers,
  getLabeledNodes,
} from "../database";
import {
  type TokenPriceQuote,
  createTokenPriceService,
  parseCoinGeckoIds,
} from "../priceService";
import { apiConfig } from "../phantasma.config";
import { createPhantasmaRpcClient } from "../rpcClient";
import { cacheMiddleware } from "../responseCache";

type CacheMiddleware = (
  cacheKey: string,
  ttlMs: number,
) => (
  request: Request,
  response: Response,
  next: NextFunction,
) => void | Promise<void>;

export type ApiServerDeps = {
  rpcClient: {
    getBlockHeight: () => Promise<number | string>;
    getAccount: (address: string) => Promise<unknown>;
  };
  cacheMiddlewareImpl: CacheMiddleware;
  closeDatabasePoolImpl: () => Promise<void>;
  testDatabaseConnectionImpl: () => Promise<unknown>;
  getSyncStatesImpl: () => Promise<unknown[]>;
  getBlockSyncClaimsViewImpl: typeof getBlockSyncClaimsView;
  getAvailableTokensImpl: () => Promise<string[]>;
  getTokenMetadataImpl: (tokenSymbol: string) => Promise<unknown>;
  getAddressSubgraphImpl: (
    tokenSymbol: string,
    address: string,
    depth: number,
    edgeLimit: number,
  ) => Promise<unknown>;
  getAddressConnectionsImpl: (
    tokenSymbol: string,
    address: string,
  ) => Promise<unknown[]>;
  getPrecomputedApiViewImpl: (
    viewKey: string,
  ) => Promise<Record<string, unknown> | null>;
  refreshTokenPrecomputedViewsImpl: (tokenSymbol: string) => Promise<void>;
  findAddressPathsImpl: (options: {
    tokenSymbol: string;
    fromAddress: string;
    toAddress: string;
    maxHops: number;
    pathLimit: number;
    stopAtTerminals?: boolean;
  }) => Promise<unknown[]>;
  getTopHoldersImpl: (tokenSymbol: string, limit: number) => Promise<unknown>;
  getFullTokenGraphImpl: (
    tokenSymbol: string,
    options: { includeTopHoldersLimit: number; edgeLimit?: number },
  ) => Promise<unknown>;
  getTransactionsPageImpl: typeof getTransactionsPage;
  getAddressActivityImpl: (
    tokenSymbol: string,
    address: string,
    days: number,
  ) => Promise<unknown[]>;
  refreshTokenAnalyticsForDateImpl: (
    tokenSymbol: string,
    bucketDate?: Date,
  ) => Promise<void>;
  getTokenDailyMetricsImpl: (
    tokenSymbol: string,
    days: number,
  ) => Promise<unknown[]>;
  getTokenTopMoversImpl: (
    tokenSymbol: string,
    windowDays: number,
    limit: number,
  ) => Promise<unknown[]>;
  getLabeledNodesImpl: (options: {
    tokenSymbol?: string;
    label?: string;
    labelType?: string;
    labelSource?: string;
    minConfidence?: number;
    maxConfidence?: number;
    updatedSince?: Date;
    windowDays?: number;
    page: number;
    pageSize: number;
  }) => Promise<unknown>;
  getTokenPriceImpl?: (tokenSymbol: string) => Promise<TokenPriceQuote>;
};

export const tokenPriceService = createTokenPriceService({
  saturnxBaseUrl: apiConfig.priceSaturnxBaseUrl,
  saturnxNetwork: apiConfig.priceSaturnxNetwork,
  explorerTokensUrl: apiConfig.priceExplorerTokensUrl,
  coingeckoIds: parseCoinGeckoIds(apiConfig.priceCoingeckoIds),
  freshTtlMs: apiConfig.priceCacheTtlMs,
  staleMaxMs: apiConfig.priceStaleMaxMs,
});

export const defaultDeps: ApiServerDeps = {
  rpcClient: createPhantasmaRpcClient(),
  cacheMiddlewareImpl: cacheMiddleware,
  closeDatabasePoolImpl: closeDatabasePool,
  testDatabaseConnectionImpl: testDatabaseConnection,
  getSyncStatesImpl: getSyncStates,
  getBlockSyncClaimsViewImpl: getBlockSyncClaimsView,
  getAvailableTokensImpl: getAvailableTokens,
  getTokenMetadataImpl: getTokenMetadata,
  getAddressSubgraphImpl: getAddressSubgraph,
  getAddressConnectionsImpl: getAddressConnections,
  getPrecomputedApiViewImpl: getPrecomputedApiView,
  refreshTokenPrecomputedViewsImpl: refreshTokenPrecomputedViews,
  findAddressPathsImpl: findAddressPaths,
  getTopHoldersImpl: getTopHolders,
  getFullTokenGraphImpl: getFullTokenGraph,
  getTransactionsPageImpl: getTransactionsPage,
  getAddressActivityImpl: getAddressActivity,
  refreshTokenAnalyticsForDateImpl: refreshTokenAnalyticsForDate,
  getTokenDailyMetricsImpl: getTokenDailyMetrics,
  getTokenTopMoversImpl: getTokenTopMovers,
  getLabeledNodesImpl: getLabeledNodes,
  getTokenPriceImpl: tokenPriceService.getQuote,
};
