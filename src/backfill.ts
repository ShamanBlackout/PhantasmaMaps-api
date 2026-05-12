import {
  closeDatabasePool,
  syncEdgeAmountsNormalized,
  syncNodeBalancesNormalized,
  syncTransactionAmountsNormalized,
} from "./database";
import { runBackfillSync } from "./syncService";

function readNumber(name: string, fallback: number): number {
  const rawValue = process.env[name];

  if (!rawValue) {
    return fallback;
  }

  const parsed = Number(rawValue);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

const pollIntervalMs = Math.max(
  1000,
  readNumber("PHANTASMA_SYNC_POLL_INTERVAL_MS", 30000),
);

let shouldStop = false;

process.on("SIGINT", () => {
  shouldStop = true;
});

process.on("SIGTERM", () => {
  shouldStop = true;
});

async function runNormalizationPasses(): Promise<void> {
  const nodeResult = await syncNodeBalancesNormalized();
  console.log(
    `Node normalized balance sync complete. updated=${nodeResult.totalUpdated}, withMetadata=${nodeResult.updatedUsingMetadata}, fallback=${nodeResult.updatedFallback}`,
  );

  const edgeResult = await syncEdgeAmountsNormalized();
  console.log(
    `Edge normalized amount sync complete. updated=${edgeResult.totalUpdated}, withMetadata=${edgeResult.updatedUsingMetadata}, fallback=${edgeResult.updatedFallback}`,
  );

  const transactionResult = await syncTransactionAmountsNormalized();
  console.log(
    `Transaction normalized amount sync complete. updated=${transactionResult.totalUpdated}, withMetadata=${transactionResult.updatedUsingMetadata}, fallback=${transactionResult.updatedFallback}`,
  );
}

async function runContinuousBackfill(): Promise<void> {
  console.log(
    `Starting continuous backfill loop (pollIntervalMs=${pollIntervalMs}).`,
  );

  while (!shouldStop) {
    try {
      await runBackfillSync();
      await runNormalizationPasses();
    } catch (error: unknown) {
      console.error("Backfill cycle failed", error);
    }

    if (!shouldStop) {
      await sleep(pollIntervalMs);
    }
  }
}

runContinuousBackfill()
  .catch((error: unknown) => {
    console.error("Backfill worker terminated unexpectedly", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
