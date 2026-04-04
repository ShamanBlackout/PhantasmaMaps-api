import type { Block } from "phantasma-sdk-ts";
import { syncConfig } from "./phantasma.config";
import {
  getChainSyncHeight,
  updateSyncStateForBlock,
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

  for (const [address, tokenSet] of tokenSetByAddress.entries()) {
    const account = await rpcClient.getAccount(address);
    const balanceBySymbol = readBalancesFromAccount(account);

    for (const tokenSymbol of tokenSet) {
      const key = `${tokenSymbol}:${address}`;
      balancesByNodeKey.set(key, balanceBySymbol.get(tokenSymbol) ?? "0");
    }
  }

  return balancesByNodeKey;
}

async function fetchTokenMetadataFromRpc(
  tokenSymbols: string[],
): Promise<TokenMetadataUpsertInput[]> {
  const uniqueSymbols = [...new Set(tokenSymbols.filter(Boolean))];
  const items: TokenMetadataUpsertInput[] = [];

  for (const tokenSymbol of uniqueSymbols) {
    const token = await rpcClient.getToken(tokenSymbol);
    items.push(mapRpcTokenToUpsert(tokenSymbol, token));
  }

  return items;
}

export async function processBlockHeight(blockHeight: number): Promise<{
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

    await updateSyncStateForBlock(
      client,
      parsedBlock.blockHeight,
      parsedBlock.tokenSymbols,
    );
  });

  return {
    blockHeight: parsedBlock.blockHeight,
    transferCount: parsedBlock.transferCount,
    tokenSymbols: parsedBlock.tokenSymbols,
  };
}

export async function runBackfillSync(): Promise<void> {
  const currentHeight = await rpcClient.getBlockHeight();

  console.log(
    `Starting backfill from block ${syncConfig.initialBackfillStartBlock} to ${currentHeight}`,
  );

  for (
    let blockHeight = syncConfig.initialBackfillStartBlock;
    blockHeight <= currentHeight;
    blockHeight++
  ) {
    const result = await processBlockHeight(blockHeight);

    if (blockHeight % syncConfig.blockLogInterval === 0) {
      console.log(
        `Processed block ${result.blockHeight} with ${result.transferCount} transfer(s) across ${result.tokenSymbols.length} token(s)`,
      );
    }
  }
}

export async function runIncrementalSync(): Promise<void> {
  const currentHeight = await rpcClient.getBlockHeight();
  const lastSyncedHeight =
    (await getChainSyncHeight()) ?? syncConfig.initialBackfillStartBlock - 1;
  const startHeight = Math.max(
    syncConfig.initialBackfillStartBlock,
    lastSyncedHeight + 1,
  );

  if (startHeight > currentHeight) {
    console.log(`No new blocks to process. Current height: ${currentHeight}`);
    return;
  }

  console.log(
    `Starting incremental sync from block ${startHeight} to ${currentHeight}`,
  );

  for (
    let blockHeight = startHeight;
    blockHeight <= currentHeight;
    blockHeight++
  ) {
    const result = await processBlockHeight(blockHeight);

    if (blockHeight % syncConfig.blockLogInterval === 0) {
      console.log(
        `Processed block ${result.blockHeight} with ${result.transferCount} transfer(s) across ${result.tokenSymbols.length} token(s)`,
      );
    }
  }
}
