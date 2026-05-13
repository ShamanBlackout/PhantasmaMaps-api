import { PhantasmaAPI } from 'phantasma-sdk-ts/public';

async function main(): Promise<void> {
  const rpcUrl = process.env.PHANTASMA_RPC_URL ?? 'http://localhost:5172/rpc';
  const nexus = process.env.PHANTASMA_NEXUS ?? 'localnet';
  const chain = process.env.PHANTASMA_CHAIN ?? 'main';

  const api = new PhantasmaAPI(rpcUrl, null, nexus);
  const height = await api.getBlockHeight(chain);
  const latestBlock = await api.getLatestBlock(chain);

  console.log({ chain, height, latestBlockHash: latestBlock.hash });
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
