import { PhantasmaKeys, setLogger } from 'phantasma-sdk-ts/public';

setLogger(console);

const keys = PhantasmaKeys.generate();

console.log({
  address: keys.address.text,
  publicKey: keys.publicKey,
});

setLogger();
