# Phantasma TypeScript SDK

TypeScript SDK for Phantasma RPC access, transaction construction, VM script helpers, wallet/link integration, and Carbon binary types.

## Install

```bash
npm install phantasma-sdk-ts
```

The package ships CommonJS, ESM, and type declarations.

## Entry Points

Use the curated public entrypoint for new code:

```ts
import {
  Address,
  PhantasmaAPI,
  PhantasmaKeys,
  ScriptBuilder,
  Transaction,
} from 'phantasma-sdk-ts/public';
```

Deep imports are available for stable source areas:

```ts
import { Transaction } from 'phantasma-sdk-ts/tx/transaction';
import { ScriptBuilder } from 'phantasma-sdk-ts/vm';
import { Bytes32 } from 'phantasma-sdk-ts/types/carbon/bytes32';
```

The root export and `core/**` deep imports remain for existing consumers that still depend on the old SDK layout:

```ts
import { PhantasmaTS } from 'phantasma-sdk-ts';
import { Transaction } from 'phantasma-sdk-ts/core/tx/Transaction';
```

Those compatibility paths are deprecated. New code should use `/public` or the lowercase module paths shown above.

## RPC Example

```ts
import { PhantasmaAPI } from 'phantasma-sdk-ts/public';

const api = new PhantasmaAPI('http://localhost:5172/rpc', null, 'localnet');
const height = await api.getBlockHeight('main');
const latestBlock = await api.getLatestBlock('main');

console.log({ height, latestBlockHash: latestBlock.hash });
```

High-level RPC methods keep the historical `Promise<T>` shape for existing consumers. New code that wants explicit result-style handling can call `JSONRPCResult<T>()` and check `isRpcErrorResult(result)` or use `unwrapRpcResult(result)`.

## Transaction Example

This builds and signs a local transaction object. It does not broadcast anything.

```ts
import { PhantasmaKeys, ScriptBuilder, Transaction } from 'phantasma-sdk-ts/public';

const keys = PhantasmaKeys.generate();
const script = new ScriptBuilder().beginScript().emitVarString('example').endScript();
const tx = new Transaction('localnet', 'main', script, new Date(Date.now() + 5 * 60 * 1000), '');

tx.signWithKeys(keys);

console.log(tx.toStringEncoded(true));
```

## Examples

The `examples/` folder contains TypeScript examples that are compiled by `npm run test:package-exports`.

- `examples/read-only-rpc.ts` reads block height and latest block data from an RPC endpoint.
- `examples/build-transaction.ts` builds and signs a local transaction object without broadcasting.
- `examples/logging.ts` enables and disables SDK logging.
- `examples/invoke-raw-script.ts` invokes `Runtime.GetTokenDecimals("SOUL")` through RPC and decodes the VM result.
- `examples/scan-token-events.ts` reads one block and decodes token send/receive event data.
- `examples/wallet-link.ts` shows the Phantasma Link and EasyConnect wallet connection shape.
- `examples/carbon-link-signing.ts` builds a Carbon transfer message and forwards it to a Link v4 wallet for signing.

## Compatibility Aliases

The SDK keeps legacy PascalCase methods and typoed aliases where existing consumers may still import them, for example `Transaction.FromBytes`, `ScriptBuilder.BeginScript`, `GetAddressFromLedeger`, `CarbonBlob.Serialize`, and `Serialization.Unserialize`.

For new code, prefer the canonical camelCase APIs:

```ts
Transaction.fromBytes(bytes);
new ScriptBuilder().beginScript().endScript();
CarbonBlob.serialize(value);
Serialization.deserialize(bytes, SomeType);
```

Deprecated aliases are intentionally covered by compatibility tests and deprecated-usage checks so new SDK code does not keep spreading them internally.

For consumer projects that want CI-visible diagnostics for legacy SDK calls,
enable the type-aware ESLint rule `@typescript-eslint/no-deprecated`. TypeScript
and editors read the SDK's `@deprecated` declarations, but `tsc` does not fail
builds on deprecated API use by itself.

## Development

```bash
npm install
npm run typecheck
npm run lint
npm test
npm run test:package-exports
```

Examples under `examples/` are type-checked by `npm run test:package-exports`.
