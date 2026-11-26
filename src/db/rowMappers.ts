import { type QueryResultRow } from "pg";
import {
  type SyncStateRecord,
  type GraphNodeRecord,
  type TokenMetadataUpsertInput,
  type GraphEdgeRecord,
  type TokenMetadataRecord,
} from "../phantasma.types";

export function mapSyncStateRow(row: QueryResultRow): SyncStateRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    lastBlockHeight: Number(row.last_block_height),
    updatedAt: row.updated_at ? new Date(row.updated_at) : null,
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  };
}

export function mapGraphNodeRow(row: QueryResultRow): GraphNodeRecord {
  return {
    address: String(row.address),
    tokenSymbol: String(row.token_symbol),
    balance: row.balance === null ? null : String(row.balance),
    balanceNormalized:
      row.balance_normalized === null ? null : String(row.balance_normalized),
    label: row.label === null ? null : String(row.label),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  };
}

export function normalizeRawAmount(
  rawAmount: string,
  decimals: number,
): string {
  const cleanRaw = rawAmount.trim();
  const negative = cleanRaw.startsWith("-");
  const digitsOnly = (negative ? cleanRaw.slice(1) : cleanRaw).replace(
    /\D/g,
    "",
  );
  const safeDigits = digitsOnly.length > 0 ? digitsOnly : "0";
  const safeDecimals = Math.max(0, Math.floor(decimals));

  if (safeDecimals === 0) {
    return `${negative ? "-" : ""}${safeDigits}`;
  }

  const padded = safeDigits.padStart(safeDecimals + 1, "0");
  const splitAt = padded.length - safeDecimals;
  const integerPart = padded.slice(0, splitAt);
  const fractionalPart = padded.slice(splitAt).replace(/0+$/, "");

  if (!fractionalPart) {
    return `${negative ? "-" : ""}${integerPart}`;
  }

  return `${negative ? "-" : ""}${integerPart}.${fractionalPart}`;
}

function isTokenFungible(
  flags: Record<string, unknown> | null | undefined,
): boolean {
  if (!flags) {
    return false;
  }

  const directFlag = flags.isFungible;

  if (typeof directFlag !== "boolean") {
    return false;
  }

  return directFlag;
}

export function resolveStoredTransferAmounts(
  rawAmount: string,
  tokenMetadata:
    Pick<TokenMetadataUpsertInput, "decimals" | "flags"> | undefined,
): { amount: string; amountNormalized: string } {
  if (!tokenMetadata) {
    return {
      amount: "1",
      amountNormalized: "1",
    };
  }

  if (!isTokenFungible(tokenMetadata.flags)) {
    return {
      amount: "1",
      amountNormalized: "1",
    };
  }

  return {
    amount: rawAmount,
    amountNormalized: normalizeRawAmount(rawAmount, tokenMetadata.decimals),
  };
}

export function mapGraphEdgeRow(row: QueryResultRow): GraphEdgeRecord {
  return {
    id: String(row.id),
    tokenSymbol: String(row.token_symbol),
    fromAddress: String(row.from_address),
    toAddress: String(row.to_address),
    amount: row.amount === null ? null : String(row.amount),
    amountNormalized:
      row.amount_normalized === null ? null : String(row.amount_normalized),
    txHash: String(row.tx_hash),
    eventIndex: Number(row.event_index),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
  };
}

export function mapTokenMetadataRow(row: QueryResultRow): TokenMetadataRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    name: row.name === null ? null : String(row.name),
    decimals: Number(row.decimals),
    holderCount: Number(row.holder_count ?? 0),
    currentSupplyRaw: String(row.current_supply_raw),
    currentSupplyNormalized: String(row.current_supply_normalized),
    maxSupplyRaw:
      row.max_supply_raw === null ? null : String(row.max_supply_raw),
    maxSupplyNormalized:
      row.max_supply_normalized === null
        ? null
        : String(row.max_supply_normalized),
    flags: (row.flags as Record<string, unknown> | null) ?? null,
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
    updatedAt: row.updated_at ? new Date(row.updated_at) : null,
  };
}

export function mapTransactionRow(
  row: QueryResultRow,
): Record<string, unknown> {
  return {
    id: String(row.id),
    txHash: String(row.tx_hash),
    eventIndex:
      row.event_index === null || row.event_index === undefined
        ? null
        : Number(row.event_index),
    eventIndexes: Array.isArray(row.event_indexes)
      ? row.event_indexes.map((value: unknown) => Number(value))
      : [],
    transferCount: Number(row.transfer_count ?? 1),
    eventKind: row.event_kind === null ? "transfer" : String(row.event_kind),
    tokenSymbol: String(row.token_symbol),
    blockHeight: Number(row.block_height),
    timestamp: row.timestamp,
    fromAddress: String(row.from_address),
    toAddress: String(row.to_address),
    relatedAddress:
      row.related_address === null ? null : String(row.related_address),
    amount: row.amount === null ? null : String(row.amount),
    amountNormalized:
      row.amount_normalized === null ? null : String(row.amount_normalized),
    metadata: (row.metadata as Record<string, unknown> | null) ?? null,
    tx_hash: String(row.tx_hash),
    event_index:
      row.event_index === null || row.event_index === undefined
        ? null
        : Number(row.event_index),
    event_indexes: Array.isArray(row.event_indexes)
      ? row.event_indexes.map((value: unknown) => Number(value))
      : [],
    transfer_count: Number(row.transfer_count ?? 1),
    event_kind: row.event_kind === null ? "transfer" : String(row.event_kind),
    token_symbol: String(row.token_symbol),
    block_height: Number(row.block_height),
    from_address: String(row.from_address),
    to_address: String(row.to_address),
    related_address:
      row.related_address === null ? null : String(row.related_address),
    amount_normalized:
      row.amount_normalized === null ? null : String(row.amount_normalized),
  };
}
