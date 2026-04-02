import { createPhantasmaRpcClient, unwrapRpcResult } from "./rpcClient";

function resolveNexusName(nexusValue: unknown): string {
  const unwrapped = unwrapRpcResult<unknown>(nexusValue);

  if (
    unwrapped !== null &&
    typeof unwrapped === "object" &&
    "name" in unwrapped &&
    typeof (unwrapped as { name?: unknown }).name === "string"
  ) {
    return (unwrapped as { name: string }).name;
  }

  return typeof unwrapped === "string" ? unwrapped : "unknown";
}

function resolveChainNames(chainsValue: unknown): string[] {
  const unwrapped = unwrapRpcResult<unknown>(chainsValue);

  if (!Array.isArray(unwrapped)) {
    return [];
  }

  return unwrapped
    .map((item) => {
      if (
        item !== null &&
        typeof item === "object" &&
        "name" in item &&
        typeof (item as { name?: unknown }).name === "string"
      ) {
        return (item as { name: string }).name;
      }

      return undefined;
    })
    .filter((name): name is string => Boolean(name));
}

async function connectToPhantasma(): Promise<void> {
  const rpcClient = createPhantasmaRpcClient();
  const [blockHeight, nexusInfo, chains] = await Promise.all([
    rpcClient.getBlockHeight(),
    rpcClient.getNexus(),
    rpcClient.getChains(),
  ]);
  const nexusName = resolveNexusName(nexusInfo);
  const chainNames = resolveChainNames(chains);
  const connectionSummary = rpcClient.getConnectionSummary();

  console.log("Connected to Phantasma API");
  console.log(
    `Configured RPC endpoints (${connectionSummary.configuredRpcUrls.length}):`,
  );
  console.log(
    `Rate limits: block ${connectionSummary.blockRequestIntervalMs}ms, metadata ${connectionSummary.metadataRequestIntervalMs}ms`,
  );
  for (const url of connectionSummary.configuredRpcUrls) {
    console.log(`- ${url}`);
  }
  console.log(`Active RPC: ${connectionSummary.activeRpcUrl}`);
  console.log(`RPC: ${connectionSummary.activeRpcUrl}`);
  console.log(`Nexus: ${nexusName}`);
  console.log(`Chain: ${connectionSummary.chain}`);
  console.log(`Block height: ${blockHeight}`);
  console.log(`Available chains: ${chainNames.join(", ") || "none returned"}`);
}

connectToPhantasma().catch((error: unknown) => {
  console.error("Failed to connect to Phantasma API", error);
  process.exitCode = 1;
});
