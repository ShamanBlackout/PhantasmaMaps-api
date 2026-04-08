import { databasePool } from "./database";

function readNumber(name: string, fallback: number): number {
  const rawValue = process.env[name];

  if (!rawValue) {
    return fallback;
  }

  const parsed = Number(rawValue);
  return Number.isFinite(parsed) ? parsed : fallback;
}

// Reads from env: PHANTASMA_CLAIM_CLEANUP_DAYS (default 7)
const days = readNumber("PHANTASMA_CLAIM_CLEANUP_DAYS", 7);

async function cleanupCompletedClaims() {
  const result = await databasePool.query(
    `DELETE FROM block_sync_claims
      WHERE status = 'completed'
        AND (completed_at IS NULL OR completed_at < NOW() - INTERVAL '${days} days')`,
  );
  console.log(
    `Deleted ${result.rowCount} completed block_sync_claims older than ${days} days.`,
  );
  await databasePool.end();
}

cleanupCompletedClaims().catch((err) => {
  console.error("Error during claim cleanup:", err);
  process.exit(1);
});
