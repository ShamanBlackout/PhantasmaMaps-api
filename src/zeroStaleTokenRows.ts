import {
  closeDatabasePool,
  getTrackedPositiveNodeBalances,
  syncNodeBalancesNormalizedForToken,
  testDatabaseConnection,
  updateTrackedNodeBalances,
  withDatabaseTransaction,
} from "./database";
import { createPhantasmaRpcClient } from "./rpcClient";

const rpcClient = createPhantasmaRpcClient();

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

    balances.set(symbol, normalizeIntegerString(amount));
  }

  return balances;
}

function readTokenArgs(): string[] {
  return process.argv
    .slice(2)
    .map((value) => String(value).trim().toUpperCase())
    .filter(Boolean);
}

async function zeroStaleRowsForToken(tokenSymbol: string): Promise<void> {
  const trackedRows = await getTrackedPositiveNodeBalances(tokenSymbol);

  if (trackedRows.length === 0) {
    console.log(`No positive tracked rows found for ${tokenSymbol}.`);
    return;
  }

  const staleUpdates: Array<{
    address: string;
    tokenSymbol: string;
    balance: string;
  }> = [];

  for (const row of trackedRows) {
    const account = await rpcClient.getAccount(row.address);
    const balancesBySymbol = readBalancesFromAccount(account);
    const liveBalance = balancesBySymbol.get(tokenSymbol) ?? "0";

    if (liveBalance === "0") {
      staleUpdates.push({
        address: row.address,
        tokenSymbol,
        balance: "0",
      });
    }
  }

  if (staleUpdates.length === 0) {
    console.log(`No stale positive rows found for ${tokenSymbol}.`);
    return;
  }

  const updatedCount = await withDatabaseTransaction((client) => {
    return updateTrackedNodeBalances(client, staleUpdates);
  });

  const normalizedCount = await syncNodeBalancesNormalizedForToken(tokenSymbol);

  console.log(
    `Zeroed stale rows for ${tokenSymbol}. scanned=${trackedRows.length} zeroed=${updatedCount} normalized=${normalizedCount}`,
  );
}

async function run(): Promise<void> {
  const tokenSymbols = readTokenArgs();

  if (tokenSymbols.length === 0) {
    throw new Error(
      "Provide at least one token symbol. Example: npm run maintenance:zero-stale -- GLITCH",
    );
  }

  try {
    await testDatabaseConnection();

    for (const tokenSymbol of tokenSymbols) {
      await zeroStaleRowsForToken(tokenSymbol);
    }
  } finally {
    await closeDatabasePool();
  }
}

run().catch((error: unknown) => {
  console.error(
    `Zero stale row maintenance failed: ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exit(1);
});
