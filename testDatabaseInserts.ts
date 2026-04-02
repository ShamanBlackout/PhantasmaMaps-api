import { createPhantasmaRpcClient } from "./rpcClient";
import { extractTransfersFromBlock } from "./transferParser";
import {
  ParsedTransfer,
  GraphNodeRecord,
  GraphEdgeRecord,
  SyncStateRecord,
} from "./phantasma.types";
import type { Block } from "phantasma-sdk-ts";
import * as fs from "fs";
import * as path from "path";

interface TestDatabaseSnapshot {
  metadata: {
    timestamp: string;
    blocksProcessed: number;
    startBlock: number;
    endBlock: number;
  };
  transfers: ParsedTransfer[];
  nodes: GraphNodeRecord[];
  edges: GraphEdgeRecord[];
  syncState: SyncStateRecord[];
}

async function runDatabaseInsertTest() {
  const metadata = {
    timestamp: new Date().toISOString(),
    blocksProcessed: 0,
    startBlock: 0,
    endBlock: 0,
  };

  try {
    const rpc = createPhantasmaRpcClient();

    console.log("Fetching current block height...");
    const currentHeight = await rpc.getBlockHeight();
    console.log(`Current block height: ${currentHeight}`);

    // Test with blocks known to have transfers (~block 8681517 area)
    // or use recent blocks
    const startBlock = 8681510;
    const endBlock = 8681520;

    console.log(`Processing blocks ${startBlock} to ${endBlock}...`);

    const transfers: ParsedTransfer[] = [];
    const nodeMap = new Map<string, GraphNodeRecord>();
    const edgeMap = new Map<string, GraphEdgeRecord>();
    const syncStateMap = new Map<string, SyncStateRecord>();

    for (let height = startBlock; height <= endBlock; height++) {
      try {
        console.log(`  Fetching block ${height}...`);
        const block = (await rpc.getBlockByHeight(height)) as Block | null;

        if (!block) {
          console.log(`    Block ${height} not found, skipping`);
          continue;
        }

        // Extract transfers
        const blockResult = extractTransfersFromBlock(block);
        const blockTransfers = blockResult.transfers;
        transfers.push(...blockTransfers);

        console.log(`    Found ${blockTransfers.length} transfers`);

        // Prepare node records (unique addresses)
        for (const transfer of blockTransfers) {
          if (!nodeMap.has(transfer.fromAddress)) {
            nodeMap.set(transfer.fromAddress, {
              address: transfer.fromAddress,
              tokenSymbol: transfer.tokenSymbol,
              balance: null,
              label: null,
              metadata: null,
            });
          }
          if (!nodeMap.has(transfer.toAddress)) {
            nodeMap.set(transfer.toAddress, {
              address: transfer.toAddress,
              tokenSymbol: transfer.tokenSymbol,
              balance: null,
              label: null,
              metadata: null,
            });
          }
        }

        // Prepare edge records (transfer relationships)
        for (const transfer of blockTransfers) {
          const edgeKey = `${transfer.txHash}-${transfer.eventIndex}`;
          if (!edgeMap.has(edgeKey)) {
            edgeMap.set(edgeKey, {
              id: edgeKey,
              tokenSymbol: transfer.tokenSymbol,
              fromAddress: transfer.fromAddress,
              toAddress: transfer.toAddress,
              amount: transfer.amount,
              txHash: transfer.txHash,
              eventIndex: transfer.eventIndex,
              metadata: transfer.metadata || null,
            });
          }
        }

        // Update sync state per token
        for (const transfer of blockTransfers) {
          const key = transfer.tokenSymbol;
          if (!syncStateMap.has(key)) {
            syncStateMap.set(key, {
              tokenSymbol: transfer.tokenSymbol,
              lastBlockHeight: height,
              updatedAt: new Date(),
              metadata: { transfersInRange: 1 },
            });
          } else {
            const existing = syncStateMap.get(key)!;
            existing.lastBlockHeight = height;
            existing.updatedAt = new Date();
            if (existing.metadata && typeof existing.metadata === "object") {
              (existing.metadata as any).transfersInRange =
                ((existing.metadata as any).transfersInRange || 0) + 1;
            }
          }
        }

        metadata.blocksProcessed++;
      } catch (error) {
        console.error(`Error processing block ${height}:`, error);
      }
    }

    // Build final snapshot
    const snapshot: TestDatabaseSnapshot = {
      metadata: {
        ...metadata,
        startBlock,
        endBlock,
      },
      transfers,
      nodes: Array.from(nodeMap.values()),
      edges: Array.from(edgeMap.values()),
      syncState: Array.from(syncStateMap.values()),
    };

    // Write to JSON file
    const outputPath = path.join(process.cwd(), "test-database-snapshot.json");
    fs.writeFileSync(outputPath, JSON.stringify(snapshot, null, 2), "utf-8");

    console.log("\n✅ Test complete!");
    console.log(`📊 Summary:`);
    console.log(`   Blocks processed: ${snapshot.metadata.blocksProcessed}`);
    console.log(`   Transfers found: ${snapshot.transfers.length}`);
    console.log(`   Unique addresses: ${snapshot.nodes.length}`);
    console.log(`   Edges (relationships): ${snapshot.edges.length}`);
    console.log(`   Tokens tracked: ${snapshot.syncState.length}`);
    console.log(`\n📁 Output saved to: ${outputPath}`);
    console.log(`\nTokens tracked:`);
    for (const state of snapshot.syncState) {
      const transferCount = (state.metadata as any)?.transfersInRange || 0;
      console.log(
        `   ${state.tokenSymbol}: ${transferCount} transfers, last block: ${state.lastBlockHeight}`,
      );
    }
  } catch (error) {
    console.error("Test failed:", error);
    process.exit(1);
  }
}

runDatabaseInsertTest().catch((error) => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
