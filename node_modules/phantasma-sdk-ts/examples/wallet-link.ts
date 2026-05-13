import { EasyConnect, PhantasmaLink, ScriptBuilder } from 'phantasma-sdk-ts/public';

export function connectWithPhantasmaLink(): PhantasmaLink {
  const link = new PhantasmaLink('example-dapp', false);

  link.login(
    (connected) => {
      console.log({ connected, account: link.account?.address ?? null });
    },
    (message) => {
      console.error(message ?? 'Wallet connection failed');
    },
    4,
    'phantasma',
    'poltergeist'
  );

  return link;
}

export function invokeReadOnlyWithEasyConnect(): EasyConnect {
  const easy = new EasyConnect(['4', 'phantasma', 'poltergeist']);
  const script = new ScriptBuilder()
    .beginScript()
    .callInterop('Runtime.GetTokenDecimals', ['SOUL'])
    .endScript();

  void easy.invokeScript(script, (result) => {
    console.log(result);
  });

  return easy;
}
