import {
  Bytes32,
  PhantasmaLink,
  SmallString,
  TxMsg,
  TxMsgTransferFungible,
  TxTypes,
} from 'phantasma-sdk-ts/public';

export function buildCarbonTransferForWalletSigning(
  senderPublicKeyBytes: Uint8Array,
  receiverPublicKeyBytes: Uint8Array
): TxMsg {
  return new TxMsg(
    TxTypes.TransferFungible,
    BigInt(Math.floor(Date.now() / 1000) + 300),
    100000n,
    0n,
    new Bytes32(senderPublicKeyBytes),
    SmallString.empty,
    new TxMsgTransferFungible(new Bytes32(receiverPublicKeyBytes), 1n, 10_00000000n)
  );
}

export function requestCarbonSignature(link: PhantasmaLink, txMsg: TxMsg): void {
  link.version = 4;

  link.signCarbonTxAndBroadcast(
    txMsg,
    (result) => {
      console.log(result);
    },
    (message) => {
      console.error(message ?? 'Carbon transaction signing failed');
    }
  );
}
