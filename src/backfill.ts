import {
  closeDatabasePool,
  syncEdgeAmountsNormalized,
  syncNodeBalancesNormalized,
  syncTransactionAmountsNormalized,
} from "./database";
import { runBackfillSync } from "./syncService";

runBackfillSync()
  .then(async () => {
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
  })
  .catch((error: unknown) => {
    console.error("Backfill failed", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
