import {
  closeDatabasePool,
  syncEdgeAmountsNormalized,
  syncTransactionAmountsNormalized,
} from "./database";

async function run(): Promise<void> {
  const edgeResult = await syncEdgeAmountsNormalized();
  const transactionResult = await syncTransactionAmountsNormalized();

  console.log(
    JSON.stringify(
      {
        edgeResult,
        transactionResult,
      },
      null,
      2,
    ),
  );
}

run()
  .catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
