import { decodeVMObject, PhantasmaAPI, ScriptBuilder } from 'phantasma-sdk-ts/public';

async function main(): Promise<void> {
  const rpcUrl = process.env.PHANTASMA_RPC_URL ?? 'http://localhost:5172/rpc';
  const nexus = process.env.PHANTASMA_NEXUS ?? 'localnet';
  const chain = process.env.PHANTASMA_CHAIN ?? 'main';

  const api = new PhantasmaAPI(rpcUrl, null, nexus);
  const script = new ScriptBuilder()
    .beginScript()
    .callInterop('Runtime.GetTokenDecimals', ['SOUL'])
    .endScript();

  const response = await api.invokeRawScript(chain, script);
  if (response.error) {
    throw new Error(response.error);
  }

  const decoded = decodeVMObject(response.result);

  console.log({ chain, decoded });
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
