import { getTokenEventData, PhantasmaAPI } from 'phantasma-sdk-ts/public';

async function main(): Promise<void> {
  const rpcUrl = process.env.PHANTASMA_RPC_URL ?? 'http://localhost:5172/rpc';
  const nexus = process.env.PHANTASMA_NEXUS ?? 'localnet';
  const chain = process.env.PHANTASMA_CHAIN ?? 'main';
  const height = Number(process.env.PHANTASMA_BLOCK_HEIGHT ?? 1);

  const api = new PhantasmaAPI(rpcUrl, null, nexus);
  const block = await api.getBlockByHeight(chain, height);

  const tokenEvents = block.txs.flatMap((tx) =>
    tx.events
      .filter((event) => event.kind === 'TokenReceive' || event.kind === 'TokenSend')
      .map((event) => ({
        txHash: tx.hash,
        eventKind: event.kind,
        address: event.address,
        data: getTokenEventData(event.data),
      }))
  );

  console.log({ chain, height, tokenEvents });
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
