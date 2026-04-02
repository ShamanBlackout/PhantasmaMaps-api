import {
  PhantasmaTS,
  type Block,
  type Event,
  type TransactionData,
} from "phantasma-sdk-ts";
import { syncConfig } from "./phantasma.config";
import type {
  ParsedBlockResult,
  ParsedTransfer,
  TokenEventMatch,
} from "./phantasma.types";

function normalizeState(state: string | undefined): string {
  return (state ?? "").toLowerCase();
}

function getTimestamp(tx: TransactionData, block: Block): Date {
  const timestampSeconds = Number(tx.timestamp ?? block.timestamp ?? 0);
  return new Date(timestampSeconds * 1000);
}

function resolveBlockHeight(
  block: Block,
  fallbackBlockHeight?: number,
): number {
  const parsedBlockHeight = Number(block.height ?? fallbackBlockHeight ?? 0);

  if (!Number.isInteger(parsedBlockHeight) || parsedBlockHeight < 1) {
    throw new Error(
      `Invalid block height ${parsedBlockHeight}: blocks start at 1`,
    );
  }

  return parsedBlockHeight;
}

function isTokenMovementEvent(event: Event): boolean {
  const kind = event.kind.toLowerCase();
  return kind === "tokensend" || kind === "tokenreceive";
}

function decodeTokenEvent(
  event: Event,
  eventIndex: number,
): TokenEventMatch | null {
  try {
    const decoded = PhantasmaTS.getTokenEventData(event.data);
    return {
      event,
      eventIndex,
      symbol: decoded.symbol,
      value: decoded.value,
      chainName: decoded.chainName,
    };
  } catch {
    return null;
  }
}

function buildTransferMetadata(
  tx: TransactionData,
  sendEvent: TokenEventMatch,
  receiveEvent: TokenEventMatch,
): Record<string, unknown> {
  const metadata: Record<string, unknown> = {
    sender: tx.sender,
    gasPayer: tx.gasPayer,
    state: tx.state,
    contract: receiveEvent.event.contract,
    eventKinds: [sendEvent.event.kind, receiveEvent.event.kind],
  };

  if (syncConfig.captureRawEvents) {
    metadata.rawEvents = [sendEvent.event, receiveEvent.event];
  }

  return metadata;
}

function pairTransferEvents(tx: TransactionData): ParsedTransfer[] {
  const transfers: ParsedTransfer[] = [];
  const pendingSends = new Map<string, TokenEventMatch[]>();
  const txEvents = tx.events ?? [];

  for (const [eventIndex, event] of txEvents.entries()) {
    if (!isTokenMovementEvent(event)) {
      continue;
    }

    const decodedEvent = decodeTokenEvent(event, eventIndex);

    if (!decodedEvent) {
      continue;
    }

    const key = [
      decodedEvent.symbol,
      decodedEvent.value,
      decodedEvent.chainName,
    ].join(":");
    const kind = event.kind.toLowerCase();

    if (kind === "tokensend") {
      const existing = pendingSends.get(key) ?? [];
      existing.push(decodedEvent);
      pendingSends.set(key, existing);
      continue;
    }

    const matchingSend = pendingSends.get(key)?.shift();

    if (!matchingSend) {
      continue;
    }

    const timestampSeconds = Number(tx.timestamp ?? 0);
    transfers.push({
      eventIndex: decodedEvent.eventIndex,
      txHash: tx.hash,
      blockHeight: Number(tx.blockHeight ?? 0),
      timestamp: new Date(timestampSeconds * 1000),
      tokenSymbol: decodedEvent.symbol,
      chainName: decodedEvent.chainName,
      fromAddress: matchingSend.event.address,
      toAddress: decodedEvent.event.address,
      amount: decodedEvent.value,
      metadata: buildTransferMetadata(tx, matchingSend, decodedEvent),
    });
  }

  return transfers;
}

export function extractTransfersFromBlock(
  block: Block,
  fallbackBlockHeight?: number,
): ParsedBlockResult {
  const resolvedBlockHeight = resolveBlockHeight(block, fallbackBlockHeight);
  const transfers: ParsedTransfer[] = [];

  for (const tx of block.txs ?? []) {
    if (normalizeState(tx.state) === "fault") {
      continue;
    }

    for (const transfer of pairTransferEvents(tx)) {
      transfers.push({
        ...transfer,
        blockHeight: resolvedBlockHeight,
        timestamp: getTimestamp(tx, block),
      });
    }
  }

  return {
    blockHeight: resolvedBlockHeight,
    transferCount: transfers.length,
    tokenSymbols: [
      ...new Set(transfers.map((transfer) => transfer.tokenSymbol)),
    ],
    transfers,
  };
}
