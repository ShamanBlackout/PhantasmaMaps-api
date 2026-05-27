import * as fs from "fs";
import * as path from "path";
import { CHAIN_SYNC_TOKEN } from "./phantasma.types";
import {
  closeDatabasePool,
  databasePool,
  withDatabaseTransaction,
} from "./database";

const OUTPUT_FILE = path.resolve("labeling-dry-run.json");

function toNpmConfigKey(name: string): string {
  return `npm_config_${name.replace(/-/g, "_")}`;
}

function readOptionalNpmConfig(name: string): string | null {
  const value = process.env[toNpmConfigKey(name)];
  if (!value) {
    return null;
  }

  const trimmed = String(value).trim();
  return trimmed.length > 0 ? trimmed : null;
}

interface LabelingCandidateRow {
  token_symbol: string;
  address: string;
  in_tx_count: string;
  out_tx_count: string;
  in_unique_counterparties: string;
  out_unique_counterparties: string;
  in_volume: string;
  out_volume: string;
  in_percent_rank: string;
  out_percent_rank: string;
  in_z_score_log: string | null;
  out_z_score_log: string | null;
  in_mad_score_log: string | null;
  out_mad_score_log: string | null;
  high_inbound: boolean;
  high_outbound: boolean;
  high_in_counterparties: boolean;
  high_out_counterparties: boolean;
  candidate_label: string;
}

interface DryRunSummaryByToken {
  tokenSymbol: string;
  addressesScored: number;
  highInboundCount: number;
  highOutboundCount: number;
  hubReceiverCount: number;
  distributorCount: number;
  routerLikeCount: number;
}

interface LabelingDryRunOutput {
  metadata: {
    timestamp: string;
    windowDays: number | null;
    tokenFilter: string | null;
    topLimitPerToken: number;
    persistScores: boolean;
    persistedRows: number;
    applyLabels: boolean;
    appliedRows: number;
    applyAttempts: number;
    applySkippedByConfidence: number;
    applySkippedByLimit: number;
    applySkippedByNormal: number;
    minConfidence: number;
    maxUpdates: number;
    protectManualLabels: boolean;
    includeNormalLabels: boolean;
    labelVersion: string;
    batchTokens: boolean;
    tokensProcessed: number;
    skippedTokens: string[];
    totalCandidates: number;
  };
  summary: DryRunSummaryByToken[];
  topCandidates: Array<{
    tokenSymbol: string;
    address: string;
    candidateLabel: string;
    inTxCount: number;
    outTxCount: number;
    inUniqueCounterparties: number;
    outUniqueCounterparties: number;
    inVolume: number;
    outVolume: number;
    inPercentRank: number;
    outPercentRank: number;
    inZScoreLog: number | null;
    outZScoreLog: number | null;
    inMadScoreLog: number | null;
    outMadScoreLog: number | null;
    highInbound: boolean;
    highOutbound: boolean;
    highInCounterparties: boolean;
    highOutCounterparties: boolean;
  }>;
}

function readOptionalIntArg(name: string): number | null {
  const prefix = `--${name}=`;
  const arg = process.argv.find((value) => value.startsWith(prefix));

  const valueFromArg = arg ? arg.slice(prefix.length) : null;
  const valueFromNpmConfig = readOptionalNpmConfig(name);
  const rawValue = valueFromArg ?? valueFromNpmConfig;

  if (!rawValue) {
    return null;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(
      `Invalid value for --${name}. Expected non-negative integer.`,
    );
  }

  return Math.floor(parsed);
}

function readOptionalIntArgAny(names: string[]): number | null {
  for (const name of names) {
    const value = readOptionalIntArg(name);
    if (value !== null) {
      return value;
    }
  }

  return null;
}

function readOptionalStringArg(name: string): string | null {
  const prefix = `--${name}=`;
  const arg = process.argv.find((value) => value.startsWith(prefix));

  const valueFromArg = arg ? arg.slice(prefix.length) : null;
  const valueFromNpmConfig = readOptionalNpmConfig(name);
  const value = (valueFromArg ?? valueFromNpmConfig ?? "").trim();

  return value.length > 0 ? value : null;
}

function readOptionalStringArgAny(names: string[]): string | null {
  for (const name of names) {
    const value = readOptionalStringArg(name);
    if (value !== null) {
      return value;
    }
  }

  return null;
}

function readOptionalNumberArg(name: string): number | null {
  const prefix = `--${name}=`;
  const arg = process.argv.find((value) => value.startsWith(prefix));

  const valueFromArg = arg ? arg.slice(prefix.length) : null;
  const valueFromNpmConfig = readOptionalNpmConfig(name);
  const rawValue = valueFromArg ?? valueFromNpmConfig;

  if (!rawValue) {
    return null;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid value for --${name}. Expected number.`);
  }

  return parsed;
}

function readOptionalNumberArgAny(names: string[]): number | null {
  for (const name of names) {
    const value = readOptionalNumberArg(name);
    if (value !== null) {
      return value;
    }
  }

  return null;
}

function readBooleanArg(name: string): boolean {
  const flag = `--${name}`;

  if (process.argv.includes(flag)) {
    return true;
  }

  const valueFromArg = process.argv.find((value) =>
    value.startsWith(`${flag}=`),
  );
  const fromArg = valueFromArg
    ? valueFromArg.slice(`${flag}=`.length).toLowerCase()
    : null;
  const fromNpm = readOptionalNpmConfig(name)?.toLowerCase() ?? null;
  const raw = fromArg ?? fromNpm;

  if (!raw) {
    return false;
  }

  return ["1", "true", "yes", "on"].includes(raw);
}

function readBooleanArgAny(names: string[]): boolean {
  return names.some((name) => readBooleanArg(name));
}

function toNumber(value: string | number | null | undefined): number {
  if (typeof value === "number") {
    return value;
  }

  if (value === null || value === undefined) {
    return 0;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toNullableNumber(
  value: string | number | null | undefined,
): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isStatementTimeoutError(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const maybeCode = (error as { code?: unknown }).code;
  if (typeof maybeCode === "string" && maybeCode === "57014") {
    return true;
  }

  const maybeMessage = (error as { message?: unknown }).message;
  if (typeof maybeMessage === "string") {
    return /statement timeout/i.test(maybeMessage);
  }

  return false;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

interface LabelApplyCandidate {
  tokenSymbol: string;
  address: string;
  label: string;
  labelType: string;
  labelSource: string;
  labelVersion: string;
  labelConfidence: number;
  labelEvidenceJson: string;
  changedBy: string;
}

interface ApplyLabelsOptions {
  minConfidence: number;
  maxUpdates: number;
  protectManualLabels: boolean;
  includeNormalLabels: boolean;
  labelSource: string;
  labelVersion: string;
  changedBy: string;
}

interface ApplyLabelsResult {
  appliedRows: number;
  applyAttempts: number;
  applySkippedByConfidence: number;
  applySkippedByLimit: number;
  applySkippedByNormal: number;
}

function mapLabelType(candidateLabel: string): string {
  if (candidateLabel === "hub_receiver") {
    return "receiver";
  }

  if (candidateLabel === "distributor") {
    return "distributor";
  }

  if (candidateLabel === "Hub") {
    return "hub";
  }

  if (candidateLabel === "high_inbound_activity") {
    return "inbound_activity";
  }

  if (candidateLabel === "high_outbound_activity") {
    return "outbound_activity";
  }

  return "normal";
}

function computeLabelConfidence(row: LabelingCandidateRow): number {
  const candidateLabel = String(row.candidate_label);
  const baseByLabel: Record<string, number> = {
    hub_receiver: 0.86,
    distributor: 0.86,
    Hub: 0.9,
    high_inbound_activity: 0.78,
    high_outbound_activity: 0.78,
    normal: 0.35,
  };

  let confidence = baseByLabel[candidateLabel] ?? 0.4;

  const inMad = toNullableNumber(row.in_mad_score_log) ?? 0;
  const outMad = toNullableNumber(row.out_mad_score_log) ?? 0;
  const inZ = toNullableNumber(row.in_z_score_log) ?? 0;
  const outZ = toNullableNumber(row.out_z_score_log) ?? 0;
  const inRank = toNumber(row.in_percent_rank);
  const outRank = toNumber(row.out_percent_rank);

  if (Math.max(inMad, outMad) >= 3) {
    confidence += 0.05;
  }

  if (Math.max(inZ, outZ) >= 2) {
    confidence += 0.03;
  }

  if (Math.max(inRank, outRank) >= 0.99) {
    confidence += 0.03;
  }

  if (row.high_inbound && row.high_outbound) {
    confidence += 0.02;
  }

  return clamp(Number(confidence.toFixed(4)), 0, 0.99);
}

function buildApplyCandidate(
  row: LabelingCandidateRow,
  options: Pick<
    ApplyLabelsOptions,
    "labelSource" | "labelVersion" | "changedBy"
  >,
): LabelApplyCandidate {
  const label = String(row.candidate_label);
  const labelType = mapLabelType(label);
  const labelConfidence = computeLabelConfidence(row);

  const labelEvidence = {
    model: "labelingDryRun",
    rubricVersion: options.labelVersion,
    computedAt: new Date().toISOString(),
    candidateLabel: label,
    labelType,
    metrics: {
      inTxCount: toNumber(row.in_tx_count),
      outTxCount: toNumber(row.out_tx_count),
      inUniqueCounterparties: toNumber(row.in_unique_counterparties),
      outUniqueCounterparties: toNumber(row.out_unique_counterparties),
      inVolume: toNumber(row.in_volume),
      outVolume: toNumber(row.out_volume),
      inPercentRank: toNumber(row.in_percent_rank),
      outPercentRank: toNumber(row.out_percent_rank),
      inZScoreLog: toNullableNumber(row.in_z_score_log),
      outZScoreLog: toNullableNumber(row.out_z_score_log),
      inMadScoreLog: toNullableNumber(row.in_mad_score_log),
      outMadScoreLog: toNullableNumber(row.out_mad_score_log),
    },
    flags: {
      highInbound: row.high_inbound,
      highOutbound: row.high_outbound,
      highInCounterparties: row.high_in_counterparties,
      highOutCounterparties: row.high_out_counterparties,
    },
  };

  return {
    tokenSymbol: String(row.token_symbol),
    address: String(row.address),
    label,
    labelType,
    labelSource: options.labelSource,
    labelVersion: options.labelVersion,
    labelConfidence,
    labelEvidenceJson: JSON.stringify(labelEvidence),
    changedBy: options.changedBy,
  };
}

async function applyLabelsToNodes(
  rows: LabelingCandidateRow[],
  options: ApplyLabelsOptions,
): Promise<ApplyLabelsResult> {
  let applySkippedByNormal = 0;
  let applySkippedByConfidence = 0;

  const candidates = rows
    .map((row) =>
      buildApplyCandidate(row, {
        labelSource: options.labelSource,
        labelVersion: options.labelVersion,
        changedBy: options.changedBy,
      }),
    )
    .filter((candidate) => {
      if (!options.includeNormalLabels && candidate.label === "normal") {
        applySkippedByNormal += 1;
        return false;
      }

      if (candidate.labelConfidence < options.minConfidence) {
        applySkippedByConfidence += 1;
        return false;
      }

      return true;
    })
    .sort((a, b) => b.labelConfidence - a.labelConfidence);

  const applyAttempts = candidates.length;
  const boundedCandidates = candidates.slice(0, options.maxUpdates);
  const applySkippedByLimit = Math.max(
    0,
    candidates.length - options.maxUpdates,
  );

  if (boundedCandidates.length === 0) {
    return {
      appliedRows: 0,
      applyAttempts,
      applySkippedByConfidence,
      applySkippedByLimit,
      applySkippedByNormal,
    };
  }

  const BATCH_SIZE = 150;
  let appliedRows = 0;

  await withDatabaseTransaction(async (client) => {
    for (let i = 0; i < boundedCandidates.length; i += BATCH_SIZE) {
      const batch = boundedCandidates.slice(i, i + BATCH_SIZE);
      const placeholders: string[] = [];
      const values: Array<string | number> = [];

      for (let rowIndex = 0; rowIndex < batch.length; rowIndex++) {
        const candidate = batch[rowIndex];
        const base = rowIndex * 10;

        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10})`,
        );

        values.push(
          candidate.tokenSymbol,
          candidate.address,
          candidate.label,
          candidate.labelType,
          candidate.labelSource,
          candidate.labelVersion,
          candidate.labelConfidence,
          candidate.labelEvidenceJson,
          candidate.changedBy,
          options.minConfidence,
        );
      }

      const protectManualClause = options.protectManualLabels
        ? "COALESCE(n.label_source, '') <> 'manual'"
        : "TRUE";

      const queryResult = await client.query<{ updated_count: string }>(
        `WITH incoming AS (
           SELECT
             v.token_symbol::text AS token_symbol,
             v.address::text AS address,
             v.new_label::text AS new_label,
             v.new_label_type::text AS new_label_type,
             v.label_source::text AS label_source,
             v.label_version::text AS label_version,
             v.label_confidence::numeric AS label_confidence,
             v.label_evidence::jsonb AS label_evidence,
             v.changed_by::text AS changed_by
           FROM (VALUES ${placeholders.join(", ")}) AS v(
             token_symbol,
             address,
             new_label,
             new_label_type,
             label_source,
             label_version,
             label_confidence,
             label_evidence,
             changed_by,
             min_confidence
           )
         ),
         to_update AS (
           SELECT
             n.address,
             n.token_symbol,
             n.label AS previous_label,
             n.label_type AS previous_label_type,
             i.new_label,
             i.new_label_type,
             i.label_source,
             i.label_confidence,
             i.label_evidence,
             i.label_version,
             i.changed_by
           FROM incoming i
           JOIN nodes n
             ON n.address = i.address
            AND n.token_symbol = i.token_symbol
          WHERE ${protectManualClause}
            AND (
              n.label IS DISTINCT FROM i.new_label
              OR n.label_type IS DISTINCT FROM i.new_label_type
              OR n.label_source IS DISTINCT FROM i.label_source
              OR n.label_version IS DISTINCT FROM i.label_version
              OR n.label_confidence IS NULL
              OR n.label_confidence < i.label_confidence
              OR n.label_evidence IS DISTINCT FROM i.label_evidence
            )
         ),
         updated AS (
           UPDATE nodes n
              SET label = t.new_label,
                  label_type = t.new_label_type,
                  label_source = t.label_source,
                  label_confidence = t.label_confidence,
                  label_evidence = t.label_evidence,
                  label_version = t.label_version,
                  label_updated_at = NOW()
             FROM to_update t
            WHERE n.address = t.address
              AND n.token_symbol = t.token_symbol
         RETURNING
           t.address,
           t.token_symbol,
           t.previous_label,
           t.previous_label_type,
           t.new_label,
           t.new_label_type,
           t.label_source,
           t.label_confidence,
           t.label_evidence,
           t.label_version,
           t.changed_by
         ),
         history_insert AS (
           INSERT INTO node_label_history (
             address,
             token_symbol,
             previous_label,
             previous_label_type,
             new_label,
             new_label_type,
             label_source,
             label_confidence,
             label_evidence,
             label_version,
             changed_by,
             changed_at
           )
           SELECT
             address,
             token_symbol,
             previous_label,
             previous_label_type,
             new_label,
             new_label_type,
             label_source,
             label_confidence,
             label_evidence,
             label_version,
             changed_by,
             NOW()
           FROM updated
         RETURNING 1
         )
         SELECT COUNT(*)::text AS updated_count
           FROM history_insert`,
        values,
      );

      appliedRows += Number(queryResult.rows[0]?.updated_count ?? 0);
    }
  });

  return {
    appliedRows,
    applyAttempts,
    applySkippedByConfidence,
    applySkippedByLimit,
    applySkippedByNormal,
  };
}

async function persistLabelScores(
  rows: LabelingCandidateRow[],
  windowDays: number,
): Promise<number> {
  if (rows.length === 0) {
    return 0;
  }

  const BATCH_SIZE = 250;
  let persistedRows = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const placeholders: string[] = [];
    const values: Array<string | number | boolean | null> = [];

    for (let rowIndex = 0; rowIndex < batch.length; rowIndex++) {
      const row = batch[rowIndex];
      const base = rowIndex * 20;

      placeholders.push(
        `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, $${base + 14}, $${base + 15}, $${base + 16}, $${base + 17}, $${base + 18}, $${base + 19}, $${base + 20})`,
      );

      values.push(
        row.token_symbol,
        row.address,
        windowDays,
        row.in_tx_count,
        row.out_tx_count,
        row.in_unique_counterparties,
        row.out_unique_counterparties,
        row.in_volume,
        row.out_volume,
        row.in_percent_rank,
        row.out_percent_rank,
        row.in_z_score_log,
        row.out_z_score_log,
        row.in_mad_score_log,
        row.out_mad_score_log,
        row.high_inbound,
        row.high_outbound,
        row.high_in_counterparties,
        row.high_out_counterparties,
        row.candidate_label,
      );
    }

    const result = await databasePool.query(
      `INSERT INTO node_label_scores (
         token_symbol,
         address,
         window_days,
         in_tx_count,
         out_tx_count,
         in_unique_counterparties,
         out_unique_counterparties,
         in_volume,
         out_volume,
         in_percent_rank,
         out_percent_rank,
         in_z_score_log,
         out_z_score_log,
         in_mad_score_log,
         out_mad_score_log,
         high_inbound,
         high_outbound,
         high_in_counterparties,
         high_out_counterparties,
         candidate_label,
         computed_at
       )
       SELECT v.token_symbol,
              v.address,
        v.window_days::int,
        v.in_tx_count::bigint,
        v.out_tx_count::bigint,
        v.in_unique_counterparties::bigint,
        v.out_unique_counterparties::bigint,
        v.in_volume::numeric,
        v.out_volume::numeric,
        v.in_percent_rank::numeric,
        v.out_percent_rank::numeric,
        v.in_z_score_log::numeric,
        v.out_z_score_log::numeric,
        v.in_mad_score_log::numeric,
        v.out_mad_score_log::numeric,
        v.high_inbound::boolean,
        v.high_outbound::boolean,
        v.high_in_counterparties::boolean,
        v.high_out_counterparties::boolean,
        v.candidate_label::text,
              NOW()
         FROM (VALUES ${placeholders.join(", ")}) AS v(
           token_symbol,
           address,
           window_days,
           in_tx_count,
           out_tx_count,
           in_unique_counterparties,
           out_unique_counterparties,
           in_volume,
           out_volume,
           in_percent_rank,
           out_percent_rank,
           in_z_score_log,
           out_z_score_log,
           in_mad_score_log,
           out_mad_score_log,
           high_inbound,
           high_outbound,
           high_in_counterparties,
           high_out_counterparties,
           candidate_label
         )
         JOIN nodes n
           ON n.token_symbol = v.token_symbol
          AND n.address = v.address
       ON CONFLICT (token_symbol, address, window_days) DO UPDATE
         SET in_tx_count = EXCLUDED.in_tx_count,
             out_tx_count = EXCLUDED.out_tx_count,
             in_unique_counterparties = EXCLUDED.in_unique_counterparties,
             out_unique_counterparties = EXCLUDED.out_unique_counterparties,
             in_volume = EXCLUDED.in_volume,
             out_volume = EXCLUDED.out_volume,
             in_percent_rank = EXCLUDED.in_percent_rank,
             out_percent_rank = EXCLUDED.out_percent_rank,
             in_z_score_log = EXCLUDED.in_z_score_log,
             out_z_score_log = EXCLUDED.out_z_score_log,
             in_mad_score_log = EXCLUDED.in_mad_score_log,
             out_mad_score_log = EXCLUDED.out_mad_score_log,
             high_inbound = EXCLUDED.high_inbound,
             high_outbound = EXCLUDED.high_outbound,
             high_in_counterparties = EXCLUDED.high_in_counterparties,
             high_out_counterparties = EXCLUDED.high_out_counterparties,
             candidate_label = EXCLUDED.candidate_label,
             computed_at = NOW()`,
      values,
    );

    persistedRows += result.rowCount ?? 0;
  }

  return persistedRows;
}

async function runLabelingQuery(
  text: string,
  values: Array<string | number>,
  queryTimeoutMs: number,
): Promise<LabelingCandidateRow[]> {
  const queryConfig = {
    text,
    values,
    query_timeout: queryTimeoutMs,
    statement_timeout: queryTimeoutMs,
  } as any;
  const result = await databasePool.query<LabelingCandidateRow>(queryConfig);

  return result.rows;
}

async function fetchLabelingCandidates(options: {
  effectiveWindowDays: number | null;
  tokenFilter: string | null;
  batchTokens: boolean;
  queryTimeoutMs: number;
}): Promise<{
  rows: LabelingCandidateRow[];
  tokensProcessed: number;
  skippedTokens: string[];
}> {
  const { effectiveWindowDays, tokenFilter, batchTokens, queryTimeoutMs } =
    options;

  if (tokenFilter) {
    const values: Array<string | number> = [CHAIN_SYNC_TOKEN];
    if (effectiveWindowDays !== null) {
      values.push(effectiveWindowDays);
    }
    values.push(tokenFilter);

    const queryText = buildLabelingQuery(effectiveWindowDays !== null, true);
    const rows = await runLabelingQuery(queryText, values, queryTimeoutMs);

    return {
      rows,
      tokensProcessed: 1,
      skippedTokens: [],
    };
  }

  if (!batchTokens) {
    const values: Array<string | number> = [CHAIN_SYNC_TOKEN];
    if (effectiveWindowDays !== null) {
      values.push(effectiveWindowDays);
    }

    const queryText = buildLabelingQuery(effectiveWindowDays !== null, false);
    const rows = await runLabelingQuery(queryText, values, queryTimeoutMs);

    return {
      rows,
      tokensProcessed: 0,
      skippedTokens: [],
    };
  }

  const tokenResult = await databasePool.query<{ token_symbol: string }>(
    `SELECT DISTINCT token_symbol
       FROM transactions
      WHERE token_symbol <> $1
      ORDER BY token_symbol ASC`,
    [CHAIN_SYNC_TOKEN],
  );

  const tokens = tokenResult.rows.map((row) => String(row.token_symbol));
  const allRows: LabelingCandidateRow[] = [];
  const skippedTokens: string[] = [];

  for (const token of tokens) {
    const values: Array<string | number> = [CHAIN_SYNC_TOKEN];
    if (effectiveWindowDays !== null) {
      values.push(effectiveWindowDays);
    }
    values.push(token);

    const queryText = buildLabelingQuery(effectiveWindowDays !== null, true);
    try {
      const rows = await runLabelingQuery(queryText, values, queryTimeoutMs);
      allRows.push(...rows);
    } catch (error) {
      if (!isStatementTimeoutError(error)) {
        throw error;
      }

      skippedTokens.push(token);
      console.warn(
        `[labelingDryRun] Token ${token} skipped due to statement timeout; continuing with remaining tokens.`,
      );
    }
  }

  return {
    rows: allRows,
    tokensProcessed: tokens.length,
    skippedTokens,
  };
}

function buildLabelingQuery(
  hasWindow: boolean,
  hasTokenFilter: boolean,
): string {
  const params: string[] = ["token_symbol <> $1"];
  let nextParam = 2;

  if (hasWindow) {
    params.push(
      `timestamp >= NOW() - make_interval(days => $${nextParam}::int)`,
    );
    nextParam += 1;
  }

  if (hasTokenFilter) {
    params.push(`token_symbol = $${nextParam}`);
  }

  const whereClause = params.join(" AND ");

  return `
    WITH scoped_transactions AS (
      SELECT token_symbol,
             from_address,
             to_address,
             COALESCE(amount_normalized, 0::numeric) AS amount_normalized
        FROM transactions
       WHERE ${whereClause}
    ),
    expanded AS (
      SELECT token_symbol,
             to_address AS address,
             from_address AS counterparty,
             1::int AS in_tx,
             0::int AS out_tx,
             amount_normalized AS in_volume,
             0::numeric AS out_volume
        FROM scoped_transactions
      UNION ALL
      SELECT token_symbol,
             from_address AS address,
             to_address AS counterparty,
             0::int AS in_tx,
             1::int AS out_tx,
             0::numeric AS in_volume,
             amount_normalized AS out_volume
        FROM scoped_transactions
    ),
    wallet_metrics AS (
      SELECT token_symbol,
             address,
             SUM(in_tx)::bigint AS in_tx_count,
             SUM(out_tx)::bigint AS out_tx_count,
             COUNT(DISTINCT CASE WHEN in_tx = 1 THEN counterparty END)::bigint AS in_unique_counterparties,
             COUNT(DISTINCT CASE WHEN out_tx = 1 THEN counterparty END)::bigint AS out_unique_counterparties,
             COALESCE(SUM(in_volume), 0::numeric) AS in_volume,
             COALESCE(SUM(out_volume), 0::numeric) AS out_volume
        FROM expanded
       WHERE COALESCE(address, '') <> ''
         AND COALESCE(counterparty, '') <> ''
       GROUP BY token_symbol, address
    ),
    log_metrics AS (
      SELECT token_symbol,
             address,
             in_tx_count,
             out_tx_count,
             in_unique_counterparties,
             out_unique_counterparties,
             in_volume,
             out_volume,
             LN(1 + in_tx_count::numeric) AS in_log,
             LN(1 + out_tx_count::numeric) AS out_log,
             PERCENT_RANK() OVER (PARTITION BY token_symbol ORDER BY in_tx_count) AS in_percent_rank,
             PERCENT_RANK() OVER (PARTITION BY token_symbol ORDER BY out_tx_count) AS out_percent_rank
        FROM wallet_metrics
    ),
    distribution AS (
      SELECT token_symbol,
             PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY in_tx_count) AS in_tx_p95,
             PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY out_tx_count) AS out_tx_p95,
             PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY in_unique_counterparties) AS in_cp_p99,
             PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY out_unique_counterparties) AS out_cp_p99,
             AVG(in_log) AS in_log_mean,
             AVG(out_log) AS out_log_mean,
             STDDEV_SAMP(in_log) AS in_log_stddev,
             STDDEV_SAMP(out_log) AS out_log_stddev,
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY in_log) AS in_log_median,
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY out_log) AS out_log_median
        FROM log_metrics
       GROUP BY token_symbol
    ),
    mad AS (
      SELECT lm.token_symbol,
             PERCENTILE_CONT(0.5) WITHIN GROUP (
               ORDER BY ABS(lm.in_log - d.in_log_median)
             ) AS in_log_mad,
             PERCENTILE_CONT(0.5) WITHIN GROUP (
               ORDER BY ABS(lm.out_log - d.out_log_median)
             ) AS out_log_mad
        FROM log_metrics lm
        JOIN distribution d
          ON d.token_symbol = lm.token_symbol
       GROUP BY lm.token_symbol
    ),
    scored AS (
      SELECT lm.token_symbol,
             lm.address,
             lm.in_tx_count,
             lm.out_tx_count,
             lm.in_unique_counterparties,
             lm.out_unique_counterparties,
             lm.in_volume,
             lm.out_volume,
             lm.in_percent_rank,
             lm.out_percent_rank,
             CASE
               WHEN COALESCE(d.in_log_stddev, 0::numeric) > 0::numeric
                 THEN (lm.in_log - d.in_log_mean) / d.in_log_stddev
               ELSE NULL
             END AS in_z_score_log,
             CASE
               WHEN COALESCE(d.out_log_stddev, 0::numeric) > 0::numeric
                 THEN (lm.out_log - d.out_log_mean) / d.out_log_stddev
               ELSE NULL
             END AS out_z_score_log,
             CASE
               WHEN COALESCE(m.in_log_mad, 0::numeric) > 0::numeric
                 THEN (lm.in_log - d.in_log_median) / (1.4826::numeric * m.in_log_mad)
               ELSE NULL
             END AS in_mad_score_log,
             CASE
               WHEN COALESCE(m.out_log_mad, 0::numeric) > 0::numeric
                 THEN (lm.out_log - d.out_log_median) / (1.4826::numeric * m.out_log_mad)
               ELSE NULL
             END AS out_mad_score_log,
             (lm.in_tx_count >= d.in_tx_p95 OR COALESCE((lm.in_log - d.in_log_mean) / NULLIF(d.in_log_stddev, 0::numeric), 0::numeric) >= 2::numeric OR COALESCE((lm.in_log - d.in_log_median) / NULLIF(1.4826::numeric * m.in_log_mad, 0::numeric), 0::numeric) >= 3::numeric) AS high_inbound,
             (lm.out_tx_count >= d.out_tx_p95 OR COALESCE((lm.out_log - d.out_log_mean) / NULLIF(d.out_log_stddev, 0::numeric), 0::numeric) >= 2::numeric OR COALESCE((lm.out_log - d.out_log_median) / NULLIF(1.4826::numeric * m.out_log_mad, 0::numeric), 0::numeric) >= 3::numeric) AS high_outbound,
             (lm.in_unique_counterparties >= d.in_cp_p99) AS high_in_counterparties,
             (lm.out_unique_counterparties >= d.out_cp_p99) AS high_out_counterparties
        FROM log_metrics lm
        JOIN distribution d
          ON d.token_symbol = lm.token_symbol
        JOIN mad m
          ON m.token_symbol = lm.token_symbol
    )
    SELECT token_symbol,
           address,
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
           high_inbound,
           high_outbound,
           high_in_counterparties,
           high_out_counterparties,
           CASE
             WHEN high_inbound
              AND high_outbound
              AND high_in_counterparties
              AND high_out_counterparties
              AND COALESCE(in_volume, 0::numeric) > 0::numeric
              AND COALESCE(out_volume, 0::numeric) > 0::numeric
              AND (
                ABS(COALESCE(in_volume, 0::numeric) - COALESCE(out_volume, 0::numeric))
                / NULLIF(GREATEST(COALESCE(in_volume, 0::numeric), COALESCE(out_volume, 0::numeric)), 0::numeric)
              ) <= 0.35::numeric
              THEN 'Hub'
             WHEN high_outbound AND high_out_counterparties THEN 'distributor'
             WHEN high_inbound AND high_in_counterparties THEN 'hub_receiver'
             WHEN high_inbound THEN 'high_inbound_activity'
             WHEN high_outbound THEN 'high_outbound_activity'
             ELSE 'normal'
           END AS candidate_label
      FROM scored
     ORDER BY token_symbol ASC,
              GREATEST(
                COALESCE(in_mad_score_log, 0::numeric),
                COALESCE(out_mad_score_log, 0::numeric),
                COALESCE(in_z_score_log, 0::numeric),
                COALESCE(out_z_score_log, 0::numeric)
              ) DESC,
              address ASC`;
}

function summarizeByToken(
  rows: LabelingCandidateRow[],
): DryRunSummaryByToken[] {
  const buckets = new Map<string, DryRunSummaryByToken>();

  for (const row of rows) {
    const tokenSymbol = String(row.token_symbol);
    const existing = buckets.get(tokenSymbol) ?? {
      tokenSymbol,
      addressesScored: 0,
      highInboundCount: 0,
      highOutboundCount: 0,
      hubReceiverCount: 0,
      distributorCount: 0,
      routerLikeCount: 0,
    };

    existing.addressesScored += 1;
    if (row.high_inbound) {
      existing.highInboundCount += 1;
    }
    if (row.high_outbound) {
      existing.highOutboundCount += 1;
    }
    if (row.candidate_label === "hub_receiver") {
      existing.hubReceiverCount += 1;
    }
    if (row.candidate_label === "distributor") {
      existing.distributorCount += 1;
    }
    if (row.candidate_label === "Hub") {
      existing.routerLikeCount += 1;
    }

    buckets.set(tokenSymbol, existing);
  }

  return [...buckets.values()].sort((a, b) =>
    a.tokenSymbol.localeCompare(b.tokenSymbol),
  );
}

function selectTopCandidates(
  rows: LabelingCandidateRow[],
  topLimitPerToken: number,
): LabelingDryRunOutput["topCandidates"] {
  const grouped = new Map<string, LabelingCandidateRow[]>();

  for (const row of rows) {
    const tokenSymbol = String(row.token_symbol);
    if (!grouped.has(tokenSymbol)) {
      grouped.set(tokenSymbol, []);
    }

    grouped.get(tokenSymbol)?.push(row);
  }

  const topCandidates: LabelingDryRunOutput["topCandidates"] = [];

  for (const [tokenSymbol, tokenRows] of grouped.entries()) {
    const sorted = [...tokenRows].sort((a, b) => {
      const scoreA = Math.max(
        toNumber(a.in_mad_score_log),
        toNumber(a.out_mad_score_log),
        toNumber(a.in_z_score_log),
        toNumber(a.out_z_score_log),
      );
      const scoreB = Math.max(
        toNumber(b.in_mad_score_log),
        toNumber(b.out_mad_score_log),
        toNumber(b.in_z_score_log),
        toNumber(b.out_z_score_log),
      );

      if (scoreA === scoreB) {
        return String(a.address).localeCompare(String(b.address));
      }

      return scoreB - scoreA;
    });

    for (const row of sorted.slice(0, topLimitPerToken)) {
      topCandidates.push({
        tokenSymbol,
        address: String(row.address),
        candidateLabel: String(row.candidate_label),
        inTxCount: toNumber(row.in_tx_count),
        outTxCount: toNumber(row.out_tx_count),
        inUniqueCounterparties: toNumber(row.in_unique_counterparties),
        outUniqueCounterparties: toNumber(row.out_unique_counterparties),
        inVolume: toNumber(row.in_volume),
        outVolume: toNumber(row.out_volume),
        inPercentRank: toNumber(row.in_percent_rank),
        outPercentRank: toNumber(row.out_percent_rank),
        inZScoreLog: toNullableNumber(row.in_z_score_log),
        outZScoreLog: toNullableNumber(row.out_z_score_log),
        inMadScoreLog: toNullableNumber(row.in_mad_score_log),
        outMadScoreLog: toNullableNumber(row.out_mad_score_log),
        highInbound: Boolean(row.high_inbound),
        highOutbound: Boolean(row.high_outbound),
        highInCounterparties: Boolean(row.high_in_counterparties),
        highOutCounterparties: Boolean(row.high_out_counterparties),
      });
    }
  }

  return topCandidates.sort((a, b) => {
    const tokenCmp = a.tokenSymbol.localeCompare(b.tokenSymbol);
    if (tokenCmp !== 0) {
      return tokenCmp;
    }

    const scoreA = Math.max(
      a.inMadScoreLog ?? 0,
      a.outMadScoreLog ?? 0,
      a.inZScoreLog ?? 0,
      a.outZScoreLog ?? 0,
    );
    const scoreB = Math.max(
      b.inMadScoreLog ?? 0,
      b.outMadScoreLog ?? 0,
      b.inZScoreLog ?? 0,
      b.outZScoreLog ?? 0,
    );

    return scoreB - scoreA;
  });
}

async function runLabelingDryRun(): Promise<void> {
  const parsedDays = readOptionalIntArgAny(["label-days", "days"]);
  const windowDays = parsedDays === null ? 0 : parsedDays;
  const allTime =
    readBooleanArgAny(["label-all-time", "all-time"]) || windowDays === 0;
  const effectiveWindowDays = allTime ? null : windowDays;
  const tokenFilter =
    readOptionalStringArgAny(["label-token", "token"])?.toUpperCase() ?? null;
  const topLimitPerToken =
    readOptionalIntArgAny(["label-limit", "limit"]) ?? 25;
  const persistScores = readBooleanArgAny([
    "label-persist-scores",
    "persist-scores",
  ]);
  const applyLabels = readBooleanArgAny(["label-apply", "apply-labels"]);
  const includeNormalLabels = readBooleanArgAny([
    "label-include-normal",
    "include-normal",
  ]);
  const protectManualLabels = !readBooleanArgAny([
    "label-overwrite-manual",
    "overwrite-manual",
  ]);
  const minConfidence = clamp(
    readOptionalNumberArgAny(["label-min-confidence", "min-confidence"]) ??
      0.75,
    0,
    1,
  );
  const maxUpdates =
    readOptionalIntArgAny(["label-max-updates", "max-updates"]) ?? 500;
  const labelSource =
    readOptionalStringArgAny(["label-source"]) ?? "heuristic_rubric_v1";
  const labelVersion =
    readOptionalStringArgAny(["label-version"]) ?? "label-rubric-v1";
  const changedBy =
    readOptionalStringArgAny(["label-changed-by", "changed-by"]) ??
    "labelingDryRun";
  const disableBatching = readBooleanArgAny([
    "label-disable-batch-tokens",
    "disable-batch-tokens",
  ]);
  const batchTokens = tokenFilter === null && !disableBatching;
  const queryTimeoutMs = Math.max(
    30_000,
    readOptionalIntArgAny(["label-query-timeout-ms", "query-timeout-ms"]) ??
      300_000,
  );

  console.log(
    `Labeling dry-run started (window=${
      effectiveWindowDays === null ? "all-time" : `${effectiveWindowDays}d`
    }, token=${tokenFilter ?? "all"}, top-per-token=${topLimitPerToken}, persist-scores=${persistScores}, apply=${applyLabels}, batchTokens=${batchTokens}, queryTimeoutMs=${queryTimeoutMs})`,
  );

  const candidateResult = await fetchLabelingCandidates({
    effectiveWindowDays,
    tokenFilter,
    batchTokens,
    queryTimeoutMs,
  });

  const summary = summarizeByToken(candidateResult.rows);
  const topCandidates = selectTopCandidates(
    candidateResult.rows,
    topLimitPerToken,
  );
  const persistedRows = persistScores
    ? await persistLabelScores(candidateResult.rows, effectiveWindowDays ?? 0)
    : 0;
  const applyResult = applyLabels
    ? await applyLabelsToNodes(candidateResult.rows, {
        minConfidence,
        maxUpdates,
        protectManualLabels,
        includeNormalLabels,
        labelSource,
        labelVersion,
        changedBy,
      })
    : {
        appliedRows: 0,
        applyAttempts: 0,
        applySkippedByConfidence: 0,
        applySkippedByLimit: 0,
        applySkippedByNormal: 0,
      };

  const output: LabelingDryRunOutput = {
    metadata: {
      timestamp: new Date().toISOString(),
      windowDays: effectiveWindowDays,
      tokenFilter,
      topLimitPerToken,
      persistScores,
      persistedRows,
      applyLabels,
      appliedRows: applyResult.appliedRows,
      applyAttempts: applyResult.applyAttempts,
      applySkippedByConfidence: applyResult.applySkippedByConfidence,
      applySkippedByLimit: applyResult.applySkippedByLimit,
      applySkippedByNormal: applyResult.applySkippedByNormal,
      minConfidence,
      maxUpdates,
      protectManualLabels,
      includeNormalLabels,
      labelVersion,
      batchTokens,
      tokensProcessed: candidateResult.tokensProcessed,
      skippedTokens: candidateResult.skippedTokens,
      totalCandidates: candidateResult.rows.length,
    },
    summary,
    topCandidates,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2), "utf-8");

  console.log(`Labeling dry-run complete. Results written to ${OUTPUT_FILE}`);
  console.log(`Total addresses scored: ${output.metadata.totalCandidates}`);
  if (batchTokens) {
    console.log(`Token batches processed: ${output.metadata.tokensProcessed}`);
    if (output.metadata.skippedTokens.length > 0) {
      console.log(
        `Token batches skipped due to timeout: ${output.metadata.skippedTokens.length} (${output.metadata.skippedTokens.join(", ")})`,
      );
    }
  }
  if (persistScores) {
    console.log(`Score rows upserted: ${output.metadata.persistedRows}`);
  }
  if (applyLabels) {
    console.log(
      `Labels applied: ${output.metadata.appliedRows} (eligible=${output.metadata.applyAttempts}, skipped-confidence=${output.metadata.applySkippedByConfidence}, skipped-normal=${output.metadata.applySkippedByNormal}, skipped-limit=${output.metadata.applySkippedByLimit})`,
    );
  }
  console.log(`Tokens scored: ${summary.length}`);

  for (const token of summary) {
    console.log(
      `  ${token.tokenSymbol}: scored=${token.addressesScored}, highIn=${token.highInboundCount}, highOut=${token.highOutboundCount}, hubReceiver=${token.hubReceiverCount}, dist=${token.distributorCount}, Hub=${token.routerLikeCount}`,
    );
  }
}

runLabelingDryRun()
  .catch((error: unknown) => {
    console.error("Labeling dry-run failed", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
