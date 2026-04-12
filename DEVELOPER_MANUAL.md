# PHANTASMAMAPS-API-DEVELOPER(7)

## NAME

PhantasmaMaps-api developer manual - source file reference and function-by-function description for the ingestion workers, RPC client, parser, database layer, and API server.

## PURPOSE

This manual is the symbol-level companion to [README.md](f:/PhantasmaRepositories/PhantasmaMaps-api/README.md). The README explains how to run and operate the system. This document explains how the code is organized and what each function is responsible for.

## TABLE OF CONTENTS

- [NAME](#name)
- [PURPOSE](#purpose)
- [TABLE OF CONTENTS](#table-of-contents)
- [FILE REFERENCE](#file-reference)
- [MAINTENANCE NOTES](#maintenance-notes)

## FILE REFERENCE

### `apiServer.ts`

Purpose: defines the Express application and read-only HTTP API.

- `readPositiveInt(value, fallback)`: parse a positive integer query value or return the fallback.
- `readStringList(value)`: split a comma-separated query string into trimmed items.
- `/health` handler: verifies PostgreSQL connectivity.
- `/sync-status` handler: returns `sync_state` rows and the live chain head when RPC is available.
- `/sync-claims` handler: returns claim summary and detailed claim rows.
- `/tokens` handler: returns the tracked token list.
- `/tokens/:tokenSymbol/metadata` handler: returns one token metadata record.
- `/graph/address/:address` handler: returns an address-scoped graph for a token.
- `/tokens/:tokenSymbol/top-holders` handler: returns top holders for a token.
- `/graph/token/:tokenSymbol` handler: returns the stored graph for a token.
- `/transactions` handler: returns paginated transaction rows.
- `shutdown()`: closes the database pool and stops the HTTP server on `SIGINT` or `SIGTERM`.

### `phantasma.config.ts`

Purpose: parses environment variables into typed runtime configuration.

- `readNumber(name, fallback)`: parse a numeric env var or return fallback.
- `readBoolean(name, fallback)`: parse a boolean env var or return fallback.
- `readList(name, fallback)`: parse a comma-separated list env var.
- `attemptsPerRpc`: exported numeric retry count.
- `requestTimeoutMs`: exported request timeout.
- `rpcConfig`: exported RPC configuration object.
- `syncConfig`: exported worker and claim configuration object.
- `apiConfig`: exported API configuration object.
- `databaseConfig`: exported PostgreSQL configuration object.

### `phantasma.types.ts`

Purpose: shared application types and constants.

- `CHAIN_SYNC_TOKEN`: special token symbol used for the chain-wide checkpoint row.
- `RequestCategory`: RPC scheduling category type.
- `ParsedTransfer`: normalized transfer record type.
- `ParsedBlockResult`: parsed transfer summary for one block.
- `SyncStateRecord`: sync checkpoint record type.
- `GraphNodeRecord`: node row shape.
- `GraphEdgeRecord`: edge row shape.
- `AddressSubgraphResult`: address graph response type.
- `PaginatedTransactionsResult`: paged transactions response type.
- `TopHolderRecord`: top-holder row type.
- `TopHoldersResult`: top-holder response type.
- `TokenMetadataRecord`: token metadata row type.
- `TokenMetadataUpsertInput`: token metadata upsert payload type.
- `TokenEventMatch`: decoded event pair helper type.

### `rpcClient.ts`

Purpose: wraps `phantasma-sdk-ts` with timeout, retry, failover, and rate limiting.

- `unwrapRpcResult(value)`: return `value.result` when the SDK wraps responses, otherwise return the original value.
- `sleep(delayMs)`: internal delay helper.
- `withTimeout(promise, timeoutMs)`: reject an RPC operation if it exceeds the configured timeout.
- `RequestScheduler.constructor(minIntervalMs, maxConcurrent)`: configure a scheduler for one RPC category.
- `RequestScheduler.schedule(task)`: run a task subject to concurrency and pacing rules.
- `PhantasmaRpcClient.getApi(url)`: cache or create a `PhantasmaAPI` instance for a URL.
- `PhantasmaRpcClient.getPrioritizedUrls()`: prefer the last successful RPC URL.
- `PhantasmaRpcClient.getScheduler(category)`: select the block or metadata scheduler.
- `PhantasmaRpcClient.execute(category, label, operation)`: perform one RPC call with failover and retry.
- `PhantasmaRpcClient.getBlockHeight()`: fetch current chain height.
- `PhantasmaRpcClient.getBlockByHeight(height)`: fetch one block.
- `PhantasmaRpcClient.getNexus()`: fetch nexus details.
- `PhantasmaRpcClient.getChains()`: fetch available chains.
- `PhantasmaRpcClient.getAccount(address)`: fetch account balances and staking information.
- `PhantasmaRpcClient.getToken(symbol)`: fetch token metadata from RPC.
- `PhantasmaRpcClient.getConnectionSummary()`: return current client configuration and active RPC URL.
- `createPhantasmaRpcClient()`: exported factory for a new RPC client.

### `transferParser.ts`

Purpose: translate raw Phantasma blocks into normalized transfer records.

- `normalizeState(state)`: lowercases a transaction state string.
- `getTimestamp(tx, block)`: resolve the best timestamp for a transaction.
- `resolveBlockHeight(block, fallbackHeight)`: safely resolve a block height, using the requested height as fallback.
- `isTokenMovementEvent(event)`: return `true` for `TOKENSEND` and `TOKENRECEIVE`.
- `decodeTokenEvent(event, txHash, eventIndex)`: decode token event payloads using the SDK.
- `buildTransferMetadata(tx, sendEvent, receiveEvent)`: construct the metadata object stored with transfers.
- `pairTransferEvents(tx)`: pair send and receive events into transfer candidates.
- `extractTransfersFromBlock(block, requestedHeight)`: parse one block into transfers, token list, and counts.

### `database.ts`

Purpose: own the PostgreSQL pool, sync writes, maintenance routines, and read queries used by the API.

- `buildPoolConfig()`: construct `pg.Pool` configuration.
- `databasePool`: exported PostgreSQL connection pool.
- `readRawEventAmountFromMetadata(metadata)`: decode a raw event amount from stored metadata.
- `restoreFungibleTransactionAmountsFromMetadata()`: maintenance helper to repair fungible transaction amounts from metadata.
- `restoreFungibleEdgeAmountsFromMetadata()`: maintenance helper to repair fungible edge amounts from metadata.
- `mapSyncStateRow(row)`: map a database row to `SyncStateRecord`.
- `mapGraphNodeRow(row)`: map a database row to `GraphNodeRecord`.
- `normalizeRawAmount(rawAmount, decimals)`: normalize an integer token amount into a decimal string.
- `isTokenFungible(flags)`: test token metadata flags for fungibility.
- `resolveStoredTransferAmounts(rawAmount, tokenMetadata)`: choose stored raw and normalized amounts for a transfer.
- `mapGraphEdgeRow(row)`: map a database row to `GraphEdgeRecord`.
- `mapTokenMetadataRow(row)`: map a database row to `TokenMetadataRecord`.
- `mapTransactionRow(row)`: map a transaction row into the API response shape.
- `closeDatabasePool()`: close the connection pool.
- `testDatabaseConnection()`: execute `SELECT 1`.
- `withDatabaseTransaction(callback)`: run a callback in a SQL transaction.
- `getChainSyncHeight()`: return the `__chain__` checkpoint or `null`.
- `seedBlockSyncClaims(startHeight, endHeight)`: insert missing block claim rows for a range.
- `resetStaleBlockSyncClaims(staleAfterSeconds)`: move stale `claimed` rows back to `pending` when called manually or from maintenance code.
- `claimNextBlockHeight(workerId, maxAttempts, retryBaseDelaySeconds, retryMaxDelaySeconds, staleAfterSeconds)`: atomically claim the next eligible block, including stale `claimed` rows that have aged past the configured stale timeout.
- `getBlockSyncClaimWaitState(startHeight, endHeight, maxAttempts, retryBaseDelaySeconds, retryMaxDelaySeconds)`: summarize claim queue state.
- `getExhaustedBlockSyncClaims(startHeight, endHeight, maxAttempts, limit)`: list failed claims that reached the retry cap.
- `getBlockSyncClaimsView(options)`: return claim summary plus filtered rows for the API.
- `completeBlockSyncClaim(workerId, blockHeight)`: mark a claimed block complete.
- `failBlockSyncClaim(workerId, blockHeight, errorMessage)`: mark a claimed block failed.
- `advanceChainSyncHeightFromClaims(defaultPreviousHeight)`: advance `__chain__` to the highest contiguous completed block.
- `getSyncStates()`: fetch all sync checkpoint rows.
- `getTrackedTokenSymbolsFromSyncState()`: return non-chain token symbols tracked in sync state.
- `getTrackedNodeAddressTokens()`: return tracked address and token pairs from nodes.
- `updateTrackedNodeBalances(items)`: update node balances from freshly fetched account data.
- `upsertTransfers(client, transfers, tokenMetadataBySymbol)`: insert or update transaction rows for parsed transfers.
- `syncTransactionAmountsNormalized()`: recalculate normalized transaction amounts and optionally repair fungible raw amounts.
- `upsertNodes(client, transfers, nodeBalances, decimalsByToken)`: upsert node rows and balances.
- `syncNodeBalancesNormalized()`: recalculate normalized node balances.
- `upsertEdges(client, transfers, tokenMetadataBySymbol)`: insert graph edge rows.
- `syncEdgeAmountsNormalized()`: recalculate normalized edge amounts and optionally repair fungible raw amounts.
- `updateTokenSyncStateForBlock(client, blockHeight, tokenSymbols)`: upsert per-token block progress for a processed block.
- `updateChainSyncHeight(blockHeight)`: upsert the chain-level `__chain__` progress row.
- `upsertTokenMetadata(client, items)`: insert or update token metadata rows.
- `getTopHolders(tokenSymbol, limit)`: return top holders by net balance.
- `getAvailableTokens()`: return distinct token symbols from transactions.
- `getTokenMetadata(tokenSymbol)`: return a single token metadata record.
- `getFullTokenGraph(tokenSymbol)`: fetch full edges and participating nodes for one token.
- `getAddressSubgraph(tokenSymbol, rootAddress, depth, edgeLimit)`: traverse outward from an address and return a bounded subgraph.
- `getTransactionsPage(options)`: return paginated transaction rows with optional filters.

### `syncService.ts`

Purpose: orchestrate block processing across workers.

- `sleep(delayMs)`: worker delay helper.
- `mapWithConcurrency(items, requestedConcurrency, worker)`: run a fixed-size async worker pool.
- `readNumber(value, fallback)`: parse a numeric value.
- `readOptionalString(value)`: normalize nullable string input.
- `normalizeIntegerString(value)`: strip non-digits and preserve sign.
- `addIntegerStrings(left, right)`: add numeric strings using `BigInt`.
- `normalizeRawAmount(rawAmount, decimals)`: format raw integer amounts using decimals.
- `getRawSupply(token)`: extract current supply from an RPC token response.
- `getRawMaxSupply(token)`: extract maximum supply from an RPC token response.
- `readTokenFlagSet(token)`: parse token flags into a lowercase set.
- `mapRpcTokenToUpsert(tokenSymbol, tokenRaw)`: convert RPC token metadata into a database upsert payload.
- `readBalancesFromAccount(account)`: extract relevant balances from an account response, including SOUL stake and KCAL unclaimed amounts.
- `fetchNodeBalancesFromRpc(transfers)`: fetch balances for all addresses touched by a block.
- `fetchTokenMetadataFromRpc(tokenSymbols)`: fetch metadata for all token symbols in a block.
- `processBlockHeight(blockHeight, options)`: fetch, parse, enrich, and persist one block.
- `runBlockRange(startHeight, endHeight)`: drive the block claim loop for a height range using direct stale-claim reclamation inside `claimNextBlockHeight()`.
- `getResumeStartHeight()`: determine where sync should resume from.
- `runBackfillSync()`: sync from resume height to current tip in backfill mode.
- `runIncrementalSync()`: sync from resume height to current tip in incremental mode.

### `backfill.ts`

Purpose: main historical sync entrypoint.

- Module entrypoint: runs `runBackfillSync()`, then normalizes nodes, edges, and transactions.

### `backfillDryRun.ts`

Purpose: inspect parser output without writing to PostgreSQL.

- `runBackfillDryRun()`: fetch and parse five blocks, then write `backfill-dry-run.json`.

### `syncIncremental.ts`

Purpose: incremental worker entrypoint.

- Module entrypoint: runs `runIncrementalSync()` and closes the database pool.

### `syncNodeBalancesNormalized.ts`

Purpose: repair and refresh token metadata, tracked node balances, and normalized amount fields.

- `readNumber(value, fallback)`: parse numeric input.
- `readOptionalString(value)`: normalize nullable string input.
- `normalizeIntegerString(value)`: strip non-digits and preserve sign.
- `addIntegerStrings(left, right)`: add numeric strings using `BigInt`.
- `normalizeRawAmount(rawAmount, decimals)`: format raw amounts using decimals.
- `getRawSupply(token)`: extract current supply from a token response.
- `getRawMaxSupply(token)`: extract maximum supply from a token response.
- `readTokenFlagSet(token)`: parse token flags.
- `mapRpcTokenToUpsert(tokenSymbol, tokenRaw)`: turn RPC token metadata into an upsert payload.
- `readBalancesFromAccount(account)`: extract balances from an account response.
- `backfillTokenMetadataFromSyncState()`: fetch token metadata for already tracked tokens and upsert it.
- `refreshTrackedNodeBalancesFromRpc()`: refresh balances for addresses already present in the nodes table.
- `run()`: execute metadata refresh, balance refresh, and normalization passes.

### `connectPhantasma.ts`

Purpose: RPC smoke test utility.

- `resolveNexusName(nexusValue)`: derive a displayable nexus name from the RPC response.
- `resolveChainNames(chainsValue)`: derive chain names from the RPC response.
- `connectToPhantasma()`: connect to RPC and print current network details.

### `findFirstTransferBlock.ts`

Purpose: locate the first block containing transfers from a fixed starting block.

- `findFirstTransferBlock()`: scan blocks sequentially until a transfer-bearing block is found.

### `testDatabaseInserts.ts`

Purpose: create a JSON snapshot showing what a limited parsed insert set looks like.

- `runDatabaseInsertTest()`: fetch a fixed block range, derive transfer-like nodes, edges, and sync state, and write `test-database-snapshot.json`.

### `cleanupBlockClaims.ts`

Purpose: remove old completed claim rows.

- `readNumber(name, fallback)`: parse the cleanup retention period from env.
- `cleanupCompletedClaims()`: delete completed `block_sync_claims` older than the configured day count, which currently defaults to 2 days.

### `_temp_restore_fungible_amounts.ts`

Purpose: maintenance helper for repairing fungible edge and transaction amounts.

- `run()`: execute the edge and transaction normalization repair routines and print the result.

## MAINTENANCE NOTES

- `database.ts` contains both runtime query code and repair logic for previously mis-stored fungible amounts.
- `claimNextBlockHeight()` now treats stale `claimed` rows as directly claimable, which is stronger than the earlier idle-poll reset approach because workers do not need a separate reset pass before reclaiming abandoned work.
- `syncNodeBalancesNormalized.ts` duplicates some parsing helpers from `syncService.ts`; if the repository grows, those can be moved into a shared utility module.
