import { closeDatabasePool } from "./database";
import { runIncrementalSync } from "./syncService";

runIncrementalSync()
  .catch((error: unknown) => {
    console.error("Incremental sync failed", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
