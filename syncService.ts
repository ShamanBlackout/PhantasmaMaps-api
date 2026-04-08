import type { Block } from "phantasma-sdk-ts";
import { randomUUID } from "node:crypto";
import { rpcConfig, syncConfig } from "./phantasma.config";
import {
  advanceChainSyncHeightFromClaims,
  claimNextBlockHeight,
  completeBlockSyncClaim,
  failBlockSyncClaim,
  getBlockSyncClaimWaitState,
  getExhaustedBlockSyncClaims,
  getChainSyncHeight,
  resetStaleBlockSyncClaims,
  seedBlockSyncClaims,
  updateChainSyncHeight,
  updateTokenSyncStateForBlock,
  upsertEdges,
  upsertNodes,
  upsertTokenMetadata,
  upsertTransfers,
  withDatabaseTransaction,
} from "./database";
import { createPhantasmaRpcClient } from "./rpcClient";
import { extractTransfersFromBlock } from "./transferParser";
import type { TokenMetadataUpsertInput } from "./phantasma.types";

const rpcClient = createPhantasmaRpcClient();

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

async function mapWithConcurrency<T, TResult>(
  items: T[],
  requestedConcurrency: number,
  worker: (item: T, index: number) => Promise<TResult>,
): Promise<TResult[]> {
  if (items.length === 0) {
    return [];
  }

  const results = new Array<TResult>(items.length);
  const concurrency = Math.min(
    Math.max(1, Math.floor(requestedConcurrency)),
    items.length,
  );
  let nextIndex = 0;

  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (true) {
        const currentIndex = nextIndex;
        nextIndex += 1;

        if (currentIndex >= items.length) {
          return;
        }

        results[currentIndex] = await worker(items[currentIndex], currentIndex);
      }
    }),
  );

  return results;
}

function readNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readOptionalString(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
}

function normalizeIntegerString(value: unknown): string {
  const normalized = readOptionalString(value) ?? "0";
  const negative = normalized.startsWith("-");
  const digitsOnly = (negative ? normalized.slice(1) : normalized).replace(
    /\D/g,
    "",
  );
  const safeDigits = digitsOnly.length > 0 ? digitsOnly : "0";
  return `${negative ? "-" : ""}${safeDigits}`;
}

function addIntegerStrings(left: unknown, right: unknown): string {
  return (
    BigInt(normalizeIntegerString(left)) + BigInt(normalizeIntegerString(right))
  ).toString();
}

function normalizeRawAmount(rawAmount: string, decimals: number): string {
  const cleanRaw = rawAmount.trim();
  const negative = cleanRaw.startsWith("-");
  const digitsOnly = (negative ? cleanRaw.slice(1) : cleanRaw).replace(
    /\D/g,
    "",
  );
  const safeDigits = digitsOnly.length > 0 ? digitsOnly : "0";
  const safeDecimals = Math.max(0, Math.floor(decimals));

  if (safeDecimals === 0) {
    return `${negative ? "-" : ""}${safeDigits}`;
  }

  const padded = safeDigits.padStart(safeDecimals + 1, "0");
  const splitAt = padded.length - safeDecimals;
  const integerPart = padded.slice(0, splitAt);
  const fractionalPart = padded.slice(splitAt).replace(/0+$/, "");

  if (!fractionalPart) {
    return `${negative ? "-" : ""}${integerPart}`;
  }

  return `${negative ? "-" : ""}${integerPart}.${fractionalPart}`;
}

function getRawSupply(token: Record<string, unknown>): string {
  const raw =
    readOptionalString(token.currentSupply) ??
    readOptionalString(token.supply) ??
    readOptionalString(token.totalSupply) ??
    "0";

  return raw;
}

function getRawMaxSupply(token: Record<string, unknown>): string | null {
  return (
    readOptionalString(token.maxSupply) ??
    readOptionalString(token.maximumSupply) ??
    readOptionalString(token.totalSupply)
  );
}

function readTokenFlagSet(token: Record<string, unknown>): Set<string> {
  const rawFlags = readOptionalString(token.flags);

  if (!rawFlags) {
    return new Set<string>();
  }

  return new Set(
    rawFlags
      .split(",")
      .map((value) => value.trim().toLowerCase())
      .filter(Boolean),
  );
}

function mapRpcTokenToUpsert(
  tokenSymbol: string,
  tokenRaw: unknown,
): TokenMetadataUpsertInput {
  const token =
    tokenRaw && typeof tokenRaw === "object"
      ? (tokenRaw as Record<string, unknown>)
      : {};

  const decimals = Math.max(0, Math.floor(readNumber(token.decimals, 0)));
  const tokenFlags = readTokenFlagSet(token);
  const currentSupplyRaw = getRawSupply(token);
  const currentSupplyNormalized = normalizeRawAmount(
    currentSupplyRaw,
    decimals,
  );
  const maxSupplyRaw = getRawMaxSupply(token);
  const maxSupplyNormalized =
    maxSupplyRaw === null ? null : normalizeRawAmount(maxSupplyRaw, decimals);

  return {
    tokenSymbol,
    name: readOptionalString(token.name),
    decimals,
    currentSupplyRaw,
    currentSupplyNormalized,
    maxSupplyRaw,
    maxSupplyNormalized,
    flags: {
      isBurnable: tokenFlags.has("burnable"),
      isFungible: tokenFlags.has("fungible"),
      isFinite: tokenFlags.has("finite"),
      isTransferable: tokenFlags.has("transferable"),
    },
    metadata: token,
  };
}

function readBalancesFromAccount(account: unknown): Map<string, string> {
  const balances = new Map<string, string>();

  if (!account || typeof account !== "object") {
    return balances;
  }

  const accountRecord = account as {
    balances?: unknown;
    stake?: unknown;
    unclaimed?: unknown;
  };

  const rawBalances = accountRecord.balances;

  if (!Array.isArray(rawBalances)) {
    return balances;
  }

  for (const item of rawBalances) {
    if (!item || typeof item !== "object") {
      continue;
    }

    const symbol = String((item as { symbol?: unknown }).symbol ?? "").trim();
    const amount = (item as { amount?: unknown }).amount;

    if (!symbol) {
      continue;
    }

    if (symbol === "SOUL") {
      balances.set(symbol, addIntegerStrings(amount, accountRecord.stake));
      continue;
    }

    if (symbol === "KCAL") {
      balances.set(symbol, addIntegerStrings(amount, accountRecord.unclaimed));
      continue;
    }

    balances.set(symbol, String(amount ?? "0"));
  }

  return balances;
}

async function fetchNodeBalancesFromRpc(
  transfers: Array<{
    tokenSymbol: string;
    fromAddress: string;
    toAddress: string;
  }>,
): Promise<Map<string, string>> {
  const tokenSetByAddress = new Map<string, Set<string>>();

  for (const transfer of transfers) {
    for (const address of [transfer.fromAddress, transfer.toAddress]) {
      const set = tokenSetByAddress.get(address) ?? new Set<string>();
      set.add(transfer.tokenSymbol);
      tokenSetByAddress.set(address, set);
    }
  }

  const balancesByNodeKey = new Map<string, string>();

  const balanceResults = await mapWithConcurrency(
    [...tokenSetByAddress.entries()],
    rpcConfig.metadataMaxConcurrent,
    async ([address, tokenSet]) => {
      const account = await rpcClient.getAccount(address);
      return {
        address,
        tokenSet,
        balanceBySymbol: readBalancesFromAccount(account),
      };
    },
  );

  for (const result of balanceResults) {
    for (const tokenSymbol of result.tokenSet) {
      const key = `${tokenSymbol}:${result.address}`;
      balancesByNodeKey.set(
        key,
        result.balanceBySymbol.get(tokenSymbol) ?? "0",
      );
    }
  }

  return balancesByNodeKey;
}

async function fetchTokenMetadataFromRpc(
  tokenSymbols: string[],
): Promise<TokenMetadataUpsertInput[]> {
  const uniqueSymbols = [...new Set(tokenSymbols.filter(Boolean))];

  return mapWithConcurrency(
    uniqueSymbols,
    rpcConfig.metadataMaxConcurrent,
    async (tokenSymbol) => {
      const token = await rpcClient.getToken(tokenSymbol);
      return mapRpcTokenToUpsert(tokenSymbol, token);
    },
  );
}

export async function processBlockHeight(blockHeight: number): Promise<{
  blockHeight: number;
  transferCount: number;
  tokenSymbols: string[];
}>;
export async function processBlockHeight(
  blockHeight: number,
  options: { updateChainSyncState?: boolean },
): Promise<{
  blockHeight: number;
  transferCount: number;
  tokenSymbols: string[];
}>;
export async function processBlockHeight(
  blockHeight: number,
  options?: { updateChainSyncState?: boolean },
): Promise<{
  blockHeight: number;
  transferCount: number;
  tokenSymbols: string[];
}> {
  if (blockHeight < 1) {
    throw new Error(`Invalid block height ${blockHeight}: blocks start at 1`);
  }

  const block = (await rpcClient.getBlockByHeight(blockHeight)) as Block;
  const parsedBlock = extractTransfersFromBlock(block, blockHeight);
  const nodeBalances =
    parsedBlock.transfers.length > 0
      ? await fetchNodeBalancesFromRpc(parsedBlock.transfers)
      : new Map<string, string>();
  const tokenMetadata =
    parsedBlock.tokenSymbols.length > 0
      ? await fetchTokenMetadataFromRpc(parsedBlock.tokenSymbols)
      : [];
  const tokenMetadataBySymbol = new Map(
    tokenMetadata.map((item) => [item.tokenSymbol, item]),
  );

  await withDatabaseTransaction(async (client) => {
    if (tokenMetadata.length > 0) {
      await upsertTokenMetadata(client, tokenMetadata);
    }

    if (parsedBlock.transfers.length > 0) {
      await upsertTransfers(
        client,
        parsedBlock.transfers,
        tokenMetadataBySymbol,
      );
      await upsertNodes(
        client,
        parsedBlock.transfers,
        nodeBalances,
        new Map(tokenMetadata.map((item) => [item.tokenSymbol, item.decimals])),
      );
      await upsertEdges(client, parsedBlock.transfers, tokenMetadataBySymbol);
    }

    await updateTokenSyncStateForBlock(
      client,
      parsedBlock.blockHeight,
      parsedBlock.tokenSymbols,
    );
  });

  if (options?.updateChainSyncState !== false) {
    await updateChainSyncHeight(parsedBlock.blockHeight);
  }

  return {
    blockHeight: parsedBlock.blockHeight,
    transferCount: parsedBlock.transferCount,
    tokenSymbols: parsedBlock.tokenSymbols,
  };
}

async function runBlockRange(
  startHeight: number,
  endHeight: number,
): Promise<void> {
  if (startHeight > endHeight) {
    return;
  }

  const totalBlocks = endHeight - startHeight + 1;
  const workerCount = Math.min(
    Math.max(1, syncConfig.workerCount),
    totalBlocks,
  );

  await resetStaleBlockSyncClaims(syncConfig.claimStaleAfterSeconds);
  await seedBlockSyncClaims(startHeight, endHeight);

  const exhaustedClaims = await getExhaustedBlockSyncClaims(
    startHeight,
    endHeight,
    syncConfig.claimMaxAttempts,
    5,
  );

  if (exhaustedClaims.length > 0) {
    const summary = exhaustedClaims
      .map(
        (item) =>
          `${item.blockHeight} (attempts=${item.attemptCount}${item.error ? `, error=${item.error}` : ""})`,
      )
      .join(", ");

    throw new Error(
      `Cannot continue sync because block claims exceeded retry limit (${syncConfig.claimMaxAttempts}): ${summary}`,
    );
  }

  let failure: unknown = null;
  const activeBlocks = new Map<number, number>();
  let commitQueue = Promise.resolve<number | null>(null);
  const commitFallbackHeight = startHeight - 1;

  const workers = Array.from({ length: workerCount }, (_, workerIndex) => {
    const workerId = workerIndex + 1;
    const workerClaimId = `${process.pid}:${workerId}:${randomUUID()}`;

    return (async () => {
      while (failure === null) {
        const blockHeight = await claimNextBlockHeight(
          workerClaimId,
          syncConfig.claimMaxAttempts,
          syncConfig.claimRetryBaseDelaySeconds,
          syncConfig.claimRetryMaxDelaySeconds,
        );

        if (blockHeight === null) {
          const resetCount = await resetStaleBlockSyncClaims(
            syncConfig.claimStaleAfterSeconds,
          );

          if (resetCount > 0) {
            continue;
          }

          const exhaustedDuringRun = await getExhaustedBlockSyncClaims(
            startHeight,
            endHeight,
            syncConfig.claimMaxAttempts,
            5,
          );

          if (exhaustedDuringRun.length > 0) {
            const summary = exhaustedDuringRun
              .map(
                (item) =>
                  `${item.blockHeight} (attempts=${item.attemptCount}${item.error ? `, error=${item.error}` : ""})`,
              )
              .join(", ");

            failure = new Error(
              `Cannot continue sync because block claims exceeded retry limit (${syncConfig.claimMaxAttempts}): ${summary}`,
            );
            return;
          }

          const waitState = await getBlockSyncClaimWaitState(
            startHeight,
            endHeight,
            syncConfig.claimMaxAttempts,
            syncConfig.claimRetryBaseDelaySeconds,
            syncConfig.claimRetryMaxDelaySeconds,
          );

          if (
            waitState.pendingCount === 0 &&
            waitState.claimedCount === 0 &&
            waitState.retryBlockedCount === 0
          ) {
            return;
          }

          const waitMs = waitState.nextRetryAt
            ? Math.max(
                250,
                Math.min(5000, waitState.nextRetryAt.getTime() - Date.now()),
              )
            : 1000;

          await sleep(waitMs);
          continue;
        }

        activeBlocks.set(workerId, blockHeight);

        try {
          const result = await processBlockHeight(blockHeight, {
            updateChainSyncState: false,
          });

          const completed = await completeBlockSyncClaim(
            workerClaimId,
            result.blockHeight,
          );

          if (!completed) {
            throw new Error(
              `Failed to mark block ${result.blockHeight} as completed for worker ${workerClaimId}`,
            );
          }

          activeBlocks.delete(workerId);

          commitQueue = commitQueue.then(async () => {
            const committedThrough =
              await advanceChainSyncHeightFromClaims(commitFallbackHeight);
            const effectiveCommitHeight =
              committedThrough ??
              (await getChainSyncHeight()) ??
              commitFallbackHeight;

            if (result.blockHeight % syncConfig.blockLogInterval === 0) {
              const activeSummary = [...activeBlocks.entries()]
                .sort((left, right) => left[0] - right[0])
                .map(([id, activeBlockHeight]) => `w${id}:${activeBlockHeight}`)
                .join(", ");

              console.log(
                `Processed block ${result.blockHeight} with ${result.transferCount} transfer(s) across ${result.tokenSymbols.length} token(s); committedThrough=${effectiveCommitHeight}; active=[${activeSummary}]`,
              );
            }

            return effectiveCommitHeight;
          });
        } catch (error: unknown) {
          activeBlocks.delete(workerId);
          await failBlockSyncClaim(
            workerClaimId,
            blockHeight,
            error instanceof Error ? error.message : String(error),
          );
          failure = error;
          return;
        }
      }
    })();
  });

  await Promise.all(workers);
  await commitQueue;

  if (failure !== null) {
    throw failure;
  }
}

async function getResumeStartHeight(): Promise<number> {
  const lastSyncedHeight = await getChainSyncHeight();

  if (lastSyncedHeight !== null) {
    return lastSyncedHeight + 1;
  }

  return syncConfig.initialBackfillStartBlock;
}

export async function runBackfillSync(): Promise<void> {
  const currentHeight = await rpcClient.getBlockHeight();
  const startHeight = await getResumeStartHeight();

  if (startHeight > currentHeight) {
    console.log(`No new blocks to process. Current height: ${currentHeight}`);
    return;
  }

  console.log(
    `Starting backfill from block ${startHeight} to ${currentHeight}`,
  );

  await runBlockRange(startHeight, currentHeight);
}

export async function runIncrementalSync(): Promise<void> {
  const currentHeight = await rpcClient.getBlockHeight();
  const startHeight = await getResumeStartHeight();

  if (startHeight > currentHeight) {
    console.log(`No new blocks to process. Current height: ${currentHeight}`);
    return;
  }

  console.log(
    `Starting incremental sync from block ${startHeight} to ${currentHeight}`,
  );

  await runBlockRange(startHeight, currentHeight);
}
