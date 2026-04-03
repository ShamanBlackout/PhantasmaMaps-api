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
  label: string | null;
  metadata: Record<string, unknown> | null;
}

export interface GraphEdgeRecord {
  id: string;
  tokenSymbol: string;
  fromAddress: string;
  toAddress: string;
  amount: string | null;
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

export interface TokenEventMatch {
  event: Event;
  eventIndex: number;
  symbol: string;
  value: string;
  chainName: string;
}
