// Public database API. Implementation lives in ./db/*; import from here.
export {
  databasePool,
  closeDatabasePool,
  testDatabaseConnection,
  withDatabaseTransaction,
} from "./db/connection";
export {
  getCachedApiResponse,
  setCachedApiResponse,
  getPrecomputedApiView,
  setPrecomputedApiView,
  refreshTokenPrecomputedViews,
} from "./db/apiCache";
export {
  getChainSyncHeight,
  seedBlockSyncClaims,
  resetStaleBlockSyncClaims,
  claimNextBlockHeight,
  getBlockSyncClaimWaitState,
  getExhaustedBlockSyncClaims,
  requeueExhaustedBlockSyncClaims,
  recoverCommitGapBlockClaim,
  getBlockSyncClaimsView,
  renewBlockSyncClaim,
  completeBlockSyncClaim,
  failBlockSyncClaim,
  advanceChainSyncHeightFromClaims,
} from "./db/blockClaims";
export {
  getSyncStates,
  getTrackedTokenSymbolsFromSyncState,
  upsertTransfers,
  upsertTokenLedgerEvents,
  upsertNodes,
  upsertEdges,
  upsertAddressConnections,
  updateTokenSyncStateForBlock,
  updateChainSyncHeight,
  upsertTokenMetadata,
} from "./db/ingestion";
export {
  getTrackedNodeAddressTokens,
  getTrackedPositiveNodeBalances,
  updateTrackedNodeBalances,
  syncNodeBalancesNormalizedForToken,
  syncTransactionAmountsNormalized,
  syncNodeBalancesNormalized,
  syncEdgeAmountsNormalized,
} from "./db/amountNormalization";
export {
  getTopHolders,
  getAvailableTokens,
  getTokenMetadata,
  getFullTokenGraph,
  getAddressSubgraph,
  getAddressActivity,
  getAddressConnections,
  findAddressPaths,
} from "./db/graphQueries";
export { getTransactionsPage, getLabeledNodes } from "./db/listQueries";
export {
  refreshTokenAnalyticsForDate,
  refreshAllTokenAnalyticsForDate,
  getTokenDailyMetrics,
  getTokenTopMovers,
} from "./db/analytics";
