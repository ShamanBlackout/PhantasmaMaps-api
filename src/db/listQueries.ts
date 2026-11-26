import { type PaginatedTransactionsResult } from "../phantasma.types";
import { apiConfig } from "../phantasma.config";
import { databasePool, queryReadWithRetry } from "./connection";
import { mapTransactionRow } from "./rowMappers";

export async function getTransactionsPage(options: {
  tokenSymbol?: string;
  address?: string;
  fromBlock?: number;
  toBlock?: number;
  direction?: "from" | "to";
  counterparty?: string;
  startTime?: Date;
  endTime?: Date;
  minAmount?: number;
  maxAmount?: number;
  minUsd?: number;
  maxUsd?: number;
  usdRateNow?: number;
  sortBy?: "amount" | "usd" | "time";
  sortDir?: "asc" | "desc";
  page: number;
  pageSize: number;
}): Promise<PaginatedTransactionsResult> {
  const filters: string[] = [];
  const values: Array<string | number> = [];
  let addressParamIndex: number | null = null;

  if (options.tokenSymbol) {
    values.push(options.tokenSymbol);
    filters.push(`token_symbol = $${values.length}`);
  }

  if (options.address) {
    values.push(options.address);
    addressParamIndex = values.length;
    filters.push(
      `(from_address = $${values.length} OR to_address = $${values.length})`,
    );
  }

  if (options.direction === "from" && addressParamIndex !== null) {
    filters.push(`to_address = $${addressParamIndex}`);
  }

  if (options.direction === "to" && addressParamIndex !== null) {
    filters.push(`from_address = $${addressParamIndex}`);
  }

  if (options.counterparty) {
    values.push(`%${options.counterparty}%`);
    if (addressParamIndex !== null) {
      filters.push(
        `(
          (from_address = $${addressParamIndex} AND to_address ILIKE $${values.length})
          OR
          (to_address = $${addressParamIndex} AND from_address ILIKE $${values.length})
        )`,
      );
    } else {
      filters.push(
        `(from_address ILIKE $${values.length} OR to_address ILIKE $${values.length})`,
      );
    }
  }

  if (options.startTime) {
    values.push(options.startTime.toISOString());
    filters.push(`timestamp >= $${values.length}::timestamptz`);
  }

  if (options.endTime) {
    values.push(options.endTime.toISOString());
    filters.push(`timestamp <= $${values.length}::timestamptz`);
  }

  if (options.minAmount !== undefined) {
    values.push(options.minAmount);
    filters.push(`amount_normalized >= $${values.length}::numeric`);
  }

  if (options.maxAmount !== undefined) {
    values.push(options.maxAmount);
    filters.push(`amount_normalized <= $${values.length}::numeric`);
  }

  if (options.usdRateNow !== undefined && options.minUsd !== undefined) {
    values.push(options.usdRateNow, options.minUsd);
    filters.push(
      `(amount_normalized * $${values.length - 1}::numeric) >= $${values.length}::numeric`,
    );
  }

  if (options.usdRateNow !== undefined && options.maxUsd !== undefined) {
    values.push(options.usdRateNow, options.maxUsd);
    filters.push(
      `(amount_normalized * $${values.length - 1}::numeric) <= $${values.length}::numeric`,
    );
  }

  if (options.fromBlock !== undefined) {
    values.push(options.fromBlock);
    filters.push(`block_height >= $${values.length}`);
  }

  if (options.toBlock !== undefined) {
    values.push(options.toBlock);
    filters.push(`block_height <= $${values.length}`);
  }

  const whereClause = filters.length ? `WHERE ${filters.join(" AND ")}` : "";
  const groupByClause = `tx_hash, event_kind, token_symbol, block_height,
                         timestamp, from_address, to_address, related_address`;
  const pageSize = Math.min(
    Math.max(options.pageSize, 1),
    apiConfig.transactionPageSizeMax,
  );
  const page = Math.max(options.page, 1);
  const offset = (page - 1) * pageSize;
  const appliedFilters: Record<string, unknown> = {};
  const normalizedSortDir = options.sortDir === "asc" ? "ASC" : "DESC";
  const orderByClause =
    options.sortBy === "amount"
      ? `SUM(amount_normalized) ${normalizedSortDir}, block_height DESC, tx_hash ASC`
      : options.sortBy === "usd"
        ? `(SUM(amount_normalized) * ${
            options.usdRateNow !== undefined
              ? `${Number(options.usdRateNow)}`
              : "1"
          }::numeric) ${normalizedSortDir}, block_height DESC, tx_hash ASC`
        : options.sortBy === "time"
          ? `block_height ${normalizedSortDir}, tx_hash ASC`
          : "block_height DESC, tx_hash ASC";

  if (options.tokenSymbol) {
    appliedFilters.token = options.tokenSymbol;
  }

  if (options.address) {
    appliedFilters.address = options.address;
  }

  if (
    options.address &&
    (options.direction === "from" || options.direction === "to")
  ) {
    appliedFilters.dir = options.direction;
  }

  if (options.counterparty) {
    appliedFilters.counterparty = options.counterparty;
  }

  if (options.startTime) {
    appliedFilters.startTime = options.startTime.toISOString();
  }

  if (options.endTime) {
    appliedFilters.endTime = options.endTime.toISOString();
  }

  if (options.minAmount !== undefined) {
    appliedFilters.minAmount = options.minAmount;
  }

  if (options.maxAmount !== undefined) {
    appliedFilters.maxAmount = options.maxAmount;
  }

  if (options.fromBlock !== undefined) {
    appliedFilters.fromBlock = options.fromBlock;
  }

  if (options.toBlock !== undefined) {
    appliedFilters.toBlock = options.toBlock;
  }

  if (
    options.usdRateNow !== undefined &&
    (options.minUsd !== undefined || options.maxUsd !== undefined)
  ) {
    appliedFilters.usdRateNow = options.usdRateNow;
    if (options.minUsd !== undefined) {
      appliedFilters.minUsd = options.minUsd;
    }
    if (options.maxUsd !== undefined) {
      appliedFilters.maxUsd = options.maxUsd;
    }
  }

  if (options.sortBy === "amount" || options.sortBy === "usd") {
    appliedFilters.sortBy = options.sortBy;
    appliedFilters.sortDir = options.sortDir === "asc" ? "asc" : "desc";
  }

  // Combine count and data queries into a single query using window function
  // This reduces database round trips by 50% for this endpoint
  values.push(pageSize, offset);
  const result = await queryReadWithRetry<{
    id: string;
    tx_hash: string;
    event_index: number;
    event_indexes: number[];
    transfer_count: number;
    event_kind: string | null;
    token_symbol: string;
    block_height: string;
    timestamp: string;
    from_address: string;
    to_address: string;
    related_address: string | null;
    amount: string;
    amount_normalized: string;
    metadata: unknown;
    total: string;
  }>(
    `SELECT MIN(id) AS id,
            tx_hash,
            NULL::integer AS event_index,
            ARRAY_AGG(event_index ORDER BY event_index) AS event_indexes,
            COUNT(*)::integer AS transfer_count,
            MIN(event_kind) AS event_kind,
            token_symbol,
            block_height,
            timestamp,
            from_address,
            to_address,
            MIN(related_address) AS related_address,
            SUM(amount) AS amount,
            SUM(amount_normalized) AS amount_normalized,
            CASE
              WHEN COUNT(*) = 1 THEN (JSONB_AGG(metadata ORDER BY event_index))->0
              ELSE JSONB_AGG(metadata ORDER BY event_index)
            END AS metadata,
            COUNT(*) OVER () AS total
      FROM public.transactions
       ${whereClause}
      GROUP BY ${groupByClause}
      ORDER BY ${orderByClause}
      LIMIT $${values.length - 1} OFFSET $${values.length}`,
    values,
    "get_transactions_page",
  );

  let total = result.rows.length > 0 ? Number(result.rows[0].total) : 0;
  if (result.rows.length === 0 && offset > 0) {
    // A window count has no row to carry the total on an out-of-range page.
    const countResult = await queryReadWithRetry<{ total: string }>(
      `SELECT COUNT(*) AS total
         FROM (
           SELECT 1 FROM public.transactions
           ${whereClause}
           GROUP BY ${groupByClause}
         ) grouped_transactions`,
      values.slice(0, -2),
      "get_transactions_page_total",
    );
    total = Number(countResult.rows[0].total);
  }

  return {
    page,
    pageSize,
    total,
    appliedFilters,
    items: result.rows.map(mapTransactionRow),
  };
}

export async function getLabeledNodes(options: {
  tokenSymbol?: string;
  label?: string;
  labelType?: string;
  labelSource?: string;
  minConfidence?: number;
  maxConfidence?: number;
  updatedSince?: Date;
  windowDays?: number;
  page: number;
  pageSize: number;
}): Promise<{
  page: number;
  pageSize: number;
  total: number;
  appliedFilters: Record<string, unknown>;
  items: Array<Record<string, unknown>>;
}> {
  const filters: string[] = ["n.label IS NOT NULL"];
  const values: Array<string | number> = [];
  const appliedFilters: Record<string, unknown> = {};

  if (options.tokenSymbol) {
    values.push(options.tokenSymbol);
    filters.push(`n.token_symbol = $${values.length}`);
    appliedFilters.tokenSymbol = options.tokenSymbol;
  }

  if (options.label) {
    values.push(options.label);
    filters.push(`n.label = $${values.length}`);
    appliedFilters.label = options.label;
  }

  if (options.labelType) {
    values.push(options.labelType);
    filters.push(`n.label_type = $${values.length}`);
    appliedFilters.labelType = options.labelType;
  }

  if (options.labelSource) {
    values.push(options.labelSource);
    filters.push(`n.label_source = $${values.length}`);
    appliedFilters.labelSource = options.labelSource;
  }

  if (options.minConfidence !== undefined) {
    values.push(options.minConfidence);
    filters.push(
      `COALESCE(n.label_confidence, 0::numeric) >= $${values.length}::numeric`,
    );
    appliedFilters.minConfidence = options.minConfidence;
  }

  if (options.maxConfidence !== undefined) {
    values.push(options.maxConfidence);
    filters.push(
      `COALESCE(n.label_confidence, 0::numeric) <= $${values.length}::numeric`,
    );
    appliedFilters.maxConfidence = options.maxConfidence;
  }

  if (options.updatedSince) {
    values.push(options.updatedSince.toISOString());
    filters.push(`n.label_updated_at >= $${values.length}::timestamptz`);
    appliedFilters.updatedSince = options.updatedSince.toISOString();
  }

  const page = Math.max(options.page, 1);
  const pageSize = Math.max(options.pageSize, 1);
  const offset = (page - 1) * pageSize;
  const windowDays = Math.max(0, Math.floor(Number(options.windowDays ?? 30)));

  values.push(windowDays, pageSize, offset);

  const result = await databasePool.query<{
    token_symbol: string;
    address: string;
    label: string | null;
    label_type: string | null;
    label_source: string | null;
    label_version: string | null;
    label_confidence: string | null;
    label_updated_at: Date | null;
    label_evidence: Record<string, unknown> | null;
    score_window_days: number | null;
    in_tx_count: string | null;
    out_tx_count: string | null;
    in_unique_counterparties: string | null;
    out_unique_counterparties: string | null;
    in_volume: string | null;
    out_volume: string | null;
    in_percent_rank: string | null;
    out_percent_rank: string | null;
    in_z_score_log: string | null;
    out_z_score_log: string | null;
    in_mad_score_log: string | null;
    out_mad_score_log: string | null;
    computed_at: Date | null;
    total: string;
  }>(
    `WITH filtered AS (
       SELECT n.token_symbol,
              n.address,
              n.label,
              n.label_type,
              n.label_source,
              n.label_version,
              n.label_confidence,
              n.label_updated_at,
              n.label_evidence,
              s.window_days AS score_window_days,
              s.in_tx_count,
              s.out_tx_count,
              s.in_unique_counterparties,
              s.out_unique_counterparties,
              s.in_volume,
              s.out_volume,
              s.in_percent_rank,
              s.out_percent_rank,
              s.in_z_score_log,
              s.out_z_score_log,
              s.in_mad_score_log,
              s.out_mad_score_log,
              s.computed_at
         FROM nodes n
         LEFT JOIN node_label_scores s
           ON s.token_symbol = n.token_symbol
          AND s.address = n.address
          AND s.window_days = $${values.length - 2}::int
        WHERE ${filters.join(" AND ")}
     )
     SELECT token_symbol,
            address,
            label,
            label_type,
            label_source,
            label_version,
            label_confidence::text,
            label_updated_at,
            label_evidence,
            score_window_days,
            in_tx_count::text,
            out_tx_count::text,
            in_unique_counterparties::text,
            out_unique_counterparties::text,
            in_volume::text,
            out_volume::text,
            in_percent_rank::text,
            out_percent_rank::text,
            in_z_score_log::text,
            out_z_score_log::text,
            in_mad_score_log::text,
            out_mad_score_log::text,
            computed_at,
            COUNT(*) OVER ()::text AS total
       FROM filtered
      ORDER BY COALESCE(label_confidence, 0::numeric) DESC,
               COALESCE(label_updated_at, computed_at) DESC NULLS LAST,
               token_symbol ASC,
               address ASC
      LIMIT $${values.length - 1} OFFSET $${values.length}`,
    values,
  );

  const total = result.rows.length > 0 ? Number(result.rows[0].total) : 0;

  return {
    page,
    pageSize,
    total,
    appliedFilters: {
      ...appliedFilters,
      windowDays,
    },
    items: result.rows.map((row) => ({
      tokenSymbol: String(row.token_symbol),
      address: String(row.address),
      label: row.label === null ? null : String(row.label),
      labelType: row.label_type === null ? null : String(row.label_type),
      labelSource: row.label_source === null ? null : String(row.label_source),
      labelVersion:
        row.label_version === null ? null : String(row.label_version),
      labelConfidence:
        row.label_confidence === null ? null : Number(row.label_confidence),
      labelUpdatedAt: row.label_updated_at,
      labelEvidence:
        (row.label_evidence as Record<string, unknown> | null) ?? null,
      scoreSnapshot:
        row.score_window_days === null
          ? null
          : {
              windowDays: Number(row.score_window_days),
              inTxCount:
                row.in_tx_count === null ? null : Number(row.in_tx_count),
              outTxCount:
                row.out_tx_count === null ? null : Number(row.out_tx_count),
              inUniqueCounterparties:
                row.in_unique_counterparties === null
                  ? null
                  : Number(row.in_unique_counterparties),
              outUniqueCounterparties:
                row.out_unique_counterparties === null
                  ? null
                  : Number(row.out_unique_counterparties),
              inVolume: row.in_volume === null ? null : Number(row.in_volume),
              outVolume:
                row.out_volume === null ? null : Number(row.out_volume),
              inPercentRank:
                row.in_percent_rank === null
                  ? null
                  : Number(row.in_percent_rank),
              outPercentRank:
                row.out_percent_rank === null
                  ? null
                  : Number(row.out_percent_rank),
              inZScoreLog:
                row.in_z_score_log === null ? null : Number(row.in_z_score_log),
              outZScoreLog:
                row.out_z_score_log === null
                  ? null
                  : Number(row.out_z_score_log),
              inMadScoreLog:
                row.in_mad_score_log === null
                  ? null
                  : Number(row.in_mad_score_log),
              outMadScoreLog:
                row.out_mad_score_log === null
                  ? null
                  : Number(row.out_mad_score_log),
              computedAt: row.computed_at,
            },
    })),
  };
}
