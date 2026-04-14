import type { Event } from "phantasma-sdk-ts";

export const CHAIN_SYNC_TOKEN = "__chain__";

export type RequestCategory = "block" | "metadata";

export interface ParsedTransfer {
  eventIndex: number;
  txHash: string;
  blockHeight: number;
  timestamp: Date;
  tokenSymbol: string;
  chainName: string;
  fromAddress: string;
  toAddress: string;
  amount: string;
  metadata: Record<string, unknown>;
}

export interface ParsedBlockResult {
  blockHeight: number;
  transferCount: number;
  tokenSymbols: string[];
  transfers: ParsedTransfer[];
}

export interface SyncStateRecord {
  tokenSymbol: string;
  lastBlockHeight: number;
  updatedAt: Date | null;
  metadata: Record<string, unknown> | null;
}

export interface GraphNodeRecord {
  address: string;
  tokenSymbol: string;
  balance: string | null;
  balanceNormalized: string | null;
  label: string | null;
  metadata: Record<string, unknown> | null;
}

export interface GraphEdgeRecord {
  id: string;
  tokenSymbol: string;
  fromAddress: string;
  toAddress: string;
  amount: string | null;
  amountNormalized: string | null;
  txHash: string;
  eventIndex: number;
  metadata: Record<string, unknown> | null;
}

export interface AddressSubgraphResult {
  tokenSymbol: string;
  rootAddress: string;
  depth: number;
  nodes: GraphNodeRecord[];
  edges: GraphEdgeRecord[];
}

export interface PaginatedTransactionsResult {
  page: number;
  pageSize: number;
  total: number;
  appliedFilters?: Record<string, unknown>;
  items: Array<Record<string, unknown>>;
}

export interface TopHolderRecord {
  address: string;
  tokenSymbol: string;
  netBalance: string;
}

export interface TopHoldersResult {
  tokenSymbol: string;
  limit: number;
  items: TopHolderRecord[];
}

export interface ActivityBucket {
  date: string;
  txCount: number;
  volume: number;
}

export interface AddressActivityResult {
  tokenSymbol: string;
  address: string;
  days: number;
  items: ActivityBucket[];
}

export interface TokenMetadataRecord {
  tokenSymbol: string;
  name: string | null;
  decimals: number;
  currentSupplyRaw: string;
  currentSupplyNormalized: string;
  maxSupplyRaw: string | null;
  maxSupplyNormalized: string | null;
  flags: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
  updatedAt: Date | null;
}

export interface TokenMetadataUpsertInput {
  tokenSymbol: string;
  name: string | null;
  decimals: number;
  currentSupplyRaw: string;
  currentSupplyNormalized: string;
  maxSupplyRaw: string | null;
  maxSupplyNormalized: string | null;
  flags: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
}

export interface TokenEventMatch {
  event: Event;
  eventIndex: number;
  symbol: string;
  value: string;
  chainName: string;
}
