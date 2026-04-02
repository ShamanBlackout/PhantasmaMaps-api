import type { Block } from "phantasma-sdk-ts";
import { syncConfig } from "./phantasma.config";
import {
  getChainSyncHeight,
  updateSyncStateForBlock,
  upsertEdges,
  upsertNodes,
  upsertTransfers,
  withDatabaseTransaction,
} from "./database";
import { createPhantasmaRpcClient } from "./rpcClient";
import { extractTransfersFromBlock } from "./transferParser";

const rpcClient = createPhantasmaRpcClient();

export async function processBlockHeight(blockHeight: number): Promise<{
  blockHeight: number;
  transferCount: number;
  tokenSymbols: string[];
}> {
  const block = (await rpcClient.getBlockByHeight(blockHeight)) as Block;
  const parsedBlock = extractTransfersFromBlock(block);

  await withDatabaseTransaction(async (client) => {
    if (parsedBlock.transfers.length > 0) {
      await upsertTransfers(client, parsedBlock.transfers);
      await upsertNodes(client, parsedBlock.transfers);
      await upsertEdges(client, parsedBlock.transfers);
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
