import type { Block } from "phantasma-sdk-ts";
import { createPhantasmaRpcClient } from "./rpcClient";
import { extractTransfersFromBlock } from "./transferParser";

const rpcClient = createPhantasmaRpcClient();
const BLOCKS_PER_SECOND = 100;

async function findFirstTransferBlock(): Promise<void> {
  let blockHeight = 6422400;
  let blocksProcessed = 0;
  const startTime = Date.now();

  console.log(
    `Scanning for first block with transfers starting at block 34000...`,
  );

  while (true) {
    const block = (await rpcClient.getBlockByHeight(
      blockHeight,
    )) as Block | null;

    if (!block) {
      console.log(`Block ${blockHeight} not found, stopping.`);
      break;
    }

    const parsed = extractTransfersFromBlock(block, blockHeight);
    blocksProcessed++;

    if (parsed.transferCount > 0) {
      const elapsedSeconds = (Date.now() - startTime) / 1000;
      const blocksPerSecond = blocksProcessed / elapsedSeconds;

      console.log(`\n✓ Found first block with transfers!`);
      console.log(`  Block height: ${parsed.blockHeight}`);
      console.log(`  Transfer count: ${parsed.transferCount}`);
      console.log(`  Token symbols: ${parsed.tokenSymbols.join(", ")}`);
      console.log(`\nScan stats:`);
      console.log(`  Scanned: ${blocksProcessed} blocks`);
      console.log(`  Time: ${elapsedSeconds.toFixed(2)}s`);
      console.log(`  Rate: ${blocksPerSecond.toFixed(1)} blocks/sec`);
      break;
    }

    if (blockHeight % 100 === 0) {
      const elapsedSeconds = (Date.now() - startTime) / 1000;
      const rate = blocksProcessed / elapsedSeconds;
      console.log(`  Block ${blockHeight}... (${rate.toFixed(1)} blocks/sec)`);
    }

    blockHeight++;
  }
}

findFirstTransferBlock().catch((error: unknown) => {
  console.error("Error:", error);
  process.exitCode = 1;
});
