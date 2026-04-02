import { closeDatabasePool } from "./database";
import { runBackfillSync } from "./syncService";

runBackfillSync()
  .catch((error: unknown) => {
    console.error("Backfill failed", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
