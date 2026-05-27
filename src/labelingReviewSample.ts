import * as fs from "fs";
import * as path from "path";
import { closeDatabasePool, databasePool } from "./database";

const OUTPUT_FILE = path.resolve("labeling-review-sample.json");

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

function readOptionalStringArg(name: string): string | null {
  const prefix = `--${name}=`;
  const arg = process.argv.find((value) => value.startsWith(prefix));
  const valueFromArg = arg ? arg.slice(prefix.length) : null;
  const valueFromNpmConfig = readOptionalNpmConfig(name);
  const value = (valueFromArg ?? valueFromNpmConfig ?? "").trim();
  return value.length > 0 ? value : null;
}

function readOptionalIntArg(name: string): number | null {
  const raw = readOptionalStringArg(name);
  if (!raw) {
    return null;
  }

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(
      `Invalid value for --${name}. Expected non-negative integer.`,
    );
  }

  return Math.floor(parsed);
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

type PopulationRow = {
  token_symbol: string;
  label: string;
  count: string;
};

type SampleRow = {
  token_symbol: string;
  address: string;
  label: string;
  label_type: string | null;
  label_confidence: string | null;
  label_source: string | null;
  label_version: string | null;
  label_updated_at: string | null;
  window_days: number | null;
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
  top_counterparties: Array<{
    counterparty: string;
    totalVolume: number;
    transactionCount: number;
  }> | null;
  tx_count_30d: string | null;
  volume_30d: string | null;
  last_change_at: string | null;
  last_previous_label: string | null;
};

function toNumber(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function runLabelReviewSample(): Promise<void> {
  const labelSource =
    readOptionalStringArg("label-source") ?? "heuristic_rubric_v1";
  const labelVersion = readOptionalStringArg("label-version");
  const tokenFilter =
    readOptionalStringArg("label-token")?.toUpperCase() ?? null;
  const perLabel = readOptionalIntArg("label-per-label") ?? 5;
  const maxTotal = readOptionalIntArg("label-max-total") ?? 80;
  const windowDays = readOptionalIntArg("label-window-days") ?? 30;
  const historyDays = readOptionalIntArg("label-history-days") ?? 30;
  const minConfidence =
    toNumber(readOptionalStringArg("label-min-confidence")) ?? 0.75;
  const includeNormal = readBooleanArg("label-include-normal");

  const filters: string[] = ["n.label_source = $1"];
  const values: Array<string | number> = [labelSource];

  if (tokenFilter) {
    values.push(tokenFilter);
    filters.push(`n.token_symbol = $${values.length}`);
  }

  values.push(minConfidence);
  filters.push(
    `COALESCE(n.label_confidence, 0::numeric) >= $${values.length}::numeric`,
  );

  if (!includeNormal) {
    filters.push("n.label <> 'normal'");
  }

  if (labelVersion) {
    values.push(labelVersion);
    filters.push(`n.label_version = $${values.length}`);
  }

  values.push(historyDays);
  const historyDaysParam = values.length;

  values.push(windowDays);
  const windowDaysParam = values.length;

  values.push(perLabel);
  const perLabelParam = values.length;

  values.push(maxTotal);
  const maxTotalParam = values.length;

  const whereClause = filters.join(" AND ");

  console.log(
    `Label review sample started (source=${labelSource}, token=${tokenFilter ?? "all"}, per-label=${perLabel}, max-total=${maxTotal})`,
  );

  const populationResult = await databasePool.query<PopulationRow>(
    `SELECT n.token_symbol,
            n.label,
            COUNT(*)::text AS count
       FROM nodes n
      WHERE ${whereClause}
      GROUP BY n.token_symbol, n.label
      ORDER BY n.token_symbol ASC, n.label ASC`,
    values.slice(0, historyDaysParam - 1),
  );

  const sampleResult = await databasePool.query<SampleRow>(
    `WITH base AS (
       SELECT
         n.token_symbol,
         n.address,
         n.label,
         n.label_type,
         n.label_confidence,
         n.label_source,
         n.label_version,
         n.label_updated_at,
         ROW_NUMBER() OVER (
           PARTITION BY n.token_symbol, n.label
           ORDER BY random()
         ) AS sample_rank
       FROM nodes n
       WHERE ${whereClause}
     ),
     sampled AS (
       SELECT *
       FROM base
       WHERE sample_rank <= $${perLabelParam}
       ORDER BY token_symbol ASC, label ASC, sample_rank ASC
       LIMIT $${maxTotalParam}
     )
     SELECT
       s.token_symbol,
       s.address,
       s.label,
       s.label_type,
       s.label_confidence::text,
       s.label_source,
       s.label_version,
       s.label_updated_at::text,
       sc.window_days,
       sc.in_tx_count::text,
       sc.out_tx_count::text,
       sc.in_unique_counterparties::text,
       sc.out_unique_counterparties::text,
       sc.in_volume::text,
       sc.out_volume::text,
       sc.in_percent_rank::text,
       sc.out_percent_rank::text,
       sc.in_z_score_log::text,
       sc.out_z_score_log::text,
       sc.in_mad_score_log::text,
       sc.out_mad_score_log::text,
       cp.top_counterparties,
       tx.tx_count_30d::text,
       tx.volume_30d::text,
       h.last_change_at::text,
       h.last_previous_label
     FROM sampled s
     LEFT JOIN node_label_scores sc
       ON sc.token_symbol = s.token_symbol
      AND sc.address = s.address
      AND sc.window_days = $${windowDaysParam}
     LEFT JOIN LATERAL (
       SELECT COALESCE(
         JSONB_AGG(
           JSONB_BUILD_OBJECT(
             'counterparty', ranked.counterparty,
             'totalVolume', ranked.total_volume,
             'transactionCount', ranked.transaction_count
           )
           ORDER BY ranked.total_volume DESC
         ),
         '[]'::jsonb
       ) AS top_counterparties
       FROM (
         SELECT counterparty, total_volume, transaction_count
         FROM address_connections
         WHERE token_symbol = s.token_symbol
           AND address = s.address
         ORDER BY total_volume DESC
         LIMIT 5
       ) ranked
     ) cp ON TRUE
     LEFT JOIN LATERAL (
       SELECT
         COUNT(*)::bigint AS tx_count_30d,
         COALESCE(SUM(amount_normalized), 0::numeric) AS volume_30d
       FROM transactions t
       WHERE t.token_symbol = s.token_symbol
         AND (t.from_address = s.address OR t.to_address = s.address)
         AND t.timestamp >= NOW() - make_interval(days => $${historyDaysParam}::int)
     ) tx ON TRUE
     LEFT JOIN LATERAL (
       SELECT
         changed_at AS last_change_at,
         previous_label AS last_previous_label
       FROM node_label_history h
       WHERE h.token_symbol = s.token_symbol
         AND h.address = s.address
         AND h.changed_at >= NOW() - make_interval(days => $${historyDaysParam}::int)
       ORDER BY h.changed_at DESC
       LIMIT 1
     ) h ON TRUE
     ORDER BY s.token_symbol ASC, s.label ASC, s.address ASC`,
    values,
  );

  const sampleItems = sampleResult.rows.map((row) => ({
    tokenSymbol: row.token_symbol,
    address: row.address,
    label: row.label,
    labelType: row.label_type,
    labelConfidence: toNumber(row.label_confidence),
    labelSource: row.label_source,
    labelVersion: row.label_version,
    labelUpdatedAt: row.label_updated_at,
    windowDays: row.window_days,
    scoreSnapshot:
      row.window_days === null
        ? null
        : {
            inTxCount: toNumber(row.in_tx_count),
            outTxCount: toNumber(row.out_tx_count),
            inUniqueCounterparties: toNumber(row.in_unique_counterparties),
            outUniqueCounterparties: toNumber(row.out_unique_counterparties),
            inVolume: toNumber(row.in_volume),
            outVolume: toNumber(row.out_volume),
            inPercentRank: toNumber(row.in_percent_rank),
            outPercentRank: toNumber(row.out_percent_rank),
            inZScoreLog: toNumber(row.in_z_score_log),
            outZScoreLog: toNumber(row.out_z_score_log),
            inMadScoreLog: toNumber(row.in_mad_score_log),
            outMadScoreLog: toNumber(row.out_mad_score_log),
          },
    recentActivity: {
      txCountWindow: toNumber(row.tx_count_30d),
      volumeWindow: toNumber(row.volume_30d),
      topCounterparties: row.top_counterparties ?? [],
    },
    latestHistory: {
      lastChangedAt: row.last_change_at,
      lastPreviousLabel: row.last_previous_label,
    },
    review: {
      verdict: null,
      confidenceAccepted: null,
      notes: "",
      reviewer: "",
      reviewedAt: null,
    },
  }));

  const output = {
    metadata: {
      generatedAt: new Date().toISOString(),
      labelSource,
      labelVersion,
      tokenFilter,
      perLabel,
      maxTotal,
      windowDays,
      historyDays,
      minConfidence,
      includeNormal,
      populationGroups: populationResult.rows.length,
      sampleSize: sampleItems.length,
    },
    population: populationResult.rows.map((row) => ({
      tokenSymbol: row.token_symbol,
      label: row.label,
      count: Number(row.count),
    })),
    reviewItems: sampleItems,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2), "utf-8");

  console.log(
    `Label review sample complete. Results written to ${OUTPUT_FILE}`,
  );
  console.log(`Population groups: ${output.metadata.populationGroups}`);
  console.log(`Sample size: ${output.metadata.sampleSize}`);
}

runLabelReviewSample()
  .catch((error: unknown) => {
    console.error("Label review sample failed", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
