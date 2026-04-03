import {
  closeDatabasePool,
  getTrackedTokenSymbolsFromSyncState,
  syncEdgeAmountsNormalized,
  syncNodeBalancesNormalized,
  syncTransactionAmountsNormalized,
  testDatabaseConnection,
  upsertTokenMetadata,
  withDatabaseTransaction,
} from "./database";
import { createPhantasmaRpcClient } from "./rpcClient";
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
  return (
    readOptionalString(token.currentSupply) ??
    readOptionalString(token.supply) ??
    readOptionalString(token.totalSupply) ??
    "0"
  );
}

function getRawMaxSupply(token: Record<string, unknown>): string | null {
  return (
    readOptionalString(token.maxSupply) ??
    readOptionalString(token.maximumSupply) ??
    readOptionalString(token.totalSupply)
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
  const currentSupplyRaw = getRawSupply(token);
  const maxSupplyRaw = getRawMaxSupply(token);

  return {
    tokenSymbol,
    name: readOptionalString(token.name),
    decimals,
    currentSupplyRaw,
    currentSupplyNormalized: normalizeRawAmount(currentSupplyRaw, decimals),
    maxSupplyRaw,
    maxSupplyNormalized:
      maxSupplyRaw === null ? null : normalizeRawAmount(maxSupplyRaw, decimals),
    flags: {
      isBurnable:
        typeof token.isBurnable === "boolean" ? token.isBurnable : null,
      isFungible:
        typeof token.isFungible === "boolean" ? token.isFungible : null,
      isFinite: typeof token.isFinite === "boolean" ? token.isFinite : null,
      isTransferable:
        typeof token.isTransferable === "boolean" ? token.isTransferable : null,
    },
    metadata: token,
  };
}

async function backfillTokenMetadataFromSyncState(): Promise<{
  tokenCount: number;
}> {
  const tokenSymbols = await getTrackedTokenSymbolsFromSyncState();

  if (tokenSymbols.length === 0) {
    return { tokenCount: 0 };
  }

  const items: TokenMetadataUpsertInput[] = [];

  for (const tokenSymbol of tokenSymbols) {
    const token = await rpcClient.getToken(tokenSymbol);
    items.push(mapRpcTokenToUpsert(tokenSymbol, token));
  }

  await withDatabaseTransaction(async (client) => {
    await upsertTokenMetadata(client, items);
  });

  return { tokenCount: items.length };
}

async function run(): Promise<void> {
  const startedAt = Date.now();

  try {
    await testDatabaseConnection();

    const metadataBackfill = await backfillTokenMetadataFromSyncState();
    console.log(
      `Token metadata backfill complete. tokens=${metadataBackfill.tokenCount}`,
    );

    const result = await syncNodeBalancesNormalized();
    const edgeResult = await syncEdgeAmountsNormalized();
    const transactionResult = await syncTransactionAmountsNormalized();
    const elapsedMs = Date.now() - startedAt;

    console.log(
      `Node normalized balance sync complete. updated=${result.totalUpdated}, withMetadata=${result.updatedUsingMetadata}, fallback=${result.updatedFallback}, elapsedMs=${elapsedMs}`,
    );
    console.log(
      `Edge normalized amount sync complete. updated=${edgeResult.totalUpdated}, withMetadata=${edgeResult.updatedUsingMetadata}, fallback=${edgeResult.updatedFallback}, elapsedMs=${elapsedMs}`,
    );
    console.log(
      `Transaction normalized amount sync complete. updated=${transactionResult.totalUpdated}, withMetadata=${transactionResult.updatedUsingMetadata}, fallback=${transactionResult.updatedFallback}, elapsedMs=${elapsedMs}`,
    );
  } finally {
    await closeDatabasePool();
  }
}

run().catch((error: unknown) => {
  console.error(
    `Node normalized balance sync failed: ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exit(1);
});
