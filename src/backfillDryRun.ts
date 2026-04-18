import * as fs from "fs";
import * as path from "path";
import type { Block } from "phantasma-sdk-ts";
import { syncConfig } from "./phantasma.config";
import { createPhantasmaRpcClient } from "./rpcClient";
import { extractTransfersFromBlock } from "./transferParser";
import type { ParsedBlockResult } from "./phantasma.types";

const DRY_RUN_BLOCK_COUNT = 5;
const OUTPUT_FILE = path.resolve("backfill-dry-run.json");

interface DryRunLog {
  metadata: {
    timestamp: string;
    startBlock: number;
    endBlock: number;
    blocksProcessed: number;
    totalTransfers: number;
  };
  blocks: ParsedBlockResult[];
}

async function runBackfillDryRun(): Promise<void> {
  const rpc = createPhantasmaRpcClient();

  const startBlock = syncConfig.initialBackfillStartBlock;
  const endBlock = startBlock + DRY_RUN_BLOCK_COUNT - 1;

  console.log(
    `Dry-run backfill: blocks ${startBlock} to ${endBlock} (no DB writes)`,
  );

  const blocks: ParsedBlockResult[] = [];

  for (let height = startBlock; height <= endBlock; height++) {
    console.log(`  Fetching block ${height}...`);
    const raw = (await rpc.getBlockByHeight(height)) as Block | null;

    if (!raw) {
      console.log(`    Block ${height} not found, skipping`);
      continue;
    }

    const parsed = extractTransfersFromBlock(raw, height);
    blocks.push(parsed);
    console.log(`    ${parsed.transferCount} transfer(s) found`);
  }

  const log: DryRunLog = {
    metadata: {
      timestamp: new Date().toISOString(),
      startBlock,
      endBlock,
      blocksProcessed: blocks.length,
      totalTransfers: blocks.reduce((sum, b) => sum + b.transferCount, 0),
    },
    blocks,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(log, null, 2), "utf-8");
  console.log(`\nDry-run complete. Results written to ${OUTPUT_FILE}`);
  console.log(`Blocks processed : ${log.metadata.blocksProcessed}`);
  console.log(`Total transfers  : ${log.metadata.totalTransfers}`);
}

runBackfillDryRun().catch((error: unknown) => {
  console.error("Dry-run backfill failed", error);
  process.exitCode = 1;
});
