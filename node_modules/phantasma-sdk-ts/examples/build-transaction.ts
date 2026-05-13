import { PhantasmaKeys, ScriptBuilder, Transaction } from 'phantasma-sdk-ts/public';

const keys = PhantasmaKeys.generate();
const script = new ScriptBuilder().beginScript().emitVarString('example').endScript();
const transaction = new Transaction(
  'localnet',
  'main',
  script,
  new Date(Date.now() + 5 * 60 * 1000),
  'example'
);

transaction.signWithKeys(keys);

console.log({
  address: keys.address.text,
  unsigned: transaction.toStringEncoded(false),
  signed: transaction.toStringEncoded(true),
});
