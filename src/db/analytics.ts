import { type QueryResultRow } from "pg";
import {
  type TokenDailyMetricsRecord,
  type TokenTopMoverRecord,
} from "../phantasma.types";
import { databasePool, withDatabaseTransaction } from "./connection";
import { getAvailableTokens } from "./graphQueries";

function normalizeBucketDate(bucketDate: Date): string {
  const year = bucketDate.getUTCFullYear();
  const month = String(bucketDate.getUTCMonth() + 1).padStart(2, "0");
  const day = String(bucketDate.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function mapTokenDailyMetricsRow(row: QueryResultRow): TokenDailyMetricsRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    bucketDate: String(row.bucket_date),
    holderCount: Number(row.holder_count ?? 0),
    newHolderCount: Number(row.new_holder_count ?? 0),
    lostHolderCount: Number(row.lost_holder_count ?? 0),
    activeWalletCount: Number(row.active_wallet_count ?? 0),
    transactionCount: Number(row.transaction_count ?? 0),
    transferVolume: Number(row.transfer_volume ?? 0),
    currentSupply: Number(row.current_supply ?? 0),
    top10Share: Number(row.top10_share ?? 0),
    top50Share: Number(row.top50_share ?? 0),
    topWalletShare: Number(row.top_wallet_share ?? 0),
    giniCoefficient: Number(row.gini_coefficient ?? 0),
    medianTransferAmount: Number(row.median_transfer_amount ?? 0),
    avgTransferAmount: Number(row.avg_transfer_amount ?? 0),
    updatedAt: row.updated_at ? new Date(row.updated_at) : null,
  };
}

function mapTokenTopMoverRow(row: QueryResultRow): TokenTopMoverRecord {
  return {
    tokenSymbol: String(row.token_symbol),
    address: String(row.address),
    latestDate: String(row.latest_date),
    baselineDate: String(row.baseline_date),
    latestBalance: Number(row.latest_balance ?? 0),
    baselineBalance: Number(row.baseline_balance ?? 0),
    deltaBalance: Number(row.delta_balance ?? 0),
    deltaPct: Number(row.delta_pct ?? 0),
  };
}

export async function refreshTokenAnalyticsForDate(
  tokenSymbol: string,
  bucketDate = new Date(),
): Promise<void> {
  const normalizedBucketDate = normalizeBucketDate(bucketDate);

  await withDatabaseTransaction(async (client) => {
    await client.query(
      `DELETE FROM wallet_daily_balances
        WHERE token_symbol = $1
          AND bucket_date = $2::date`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `INSERT INTO wallet_daily_balances (
         token_symbol,
         address,
         bucket_date,
         balance,
         balance_normalized,
         share_of_supply,
         wallet_type,
         first_seen_at,
         last_seen_at,
         created_at,
         updated_at
       )
       WITH balance_ledger AS (
         SELECT
           address,
           SUM(delta) AS balance_normalized
         FROM (
           SELECT
             to_address AS address,
             COALESCE(amount_normalized, 0::numeric) AS delta
           FROM transactions
           WHERE token_symbol = $1
             AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
             AND COALESCE(to_address, '') <> ''
           UNION ALL
           SELECT
             from_address AS address,
             -COALESCE(amount_normalized, 0::numeric) AS delta
           FROM transactions
           WHERE token_symbol = $1
             AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
             AND COALESCE(from_address, '') <> ''
         ) deltas
         GROUP BY address
       )
       SELECT
         $1 AS token_symbol,
         bl.address,
         $2::date AS bucket_date,
         COALESCE(bl.balance_normalized, 0::numeric) AS balance,
         COALESCE(bl.balance_normalized, 0::numeric) AS balance_normalized,
         CASE
           WHEN COALESCE(tm.current_supply_normalized::numeric, 0::numeric) > 0
             THEN (COALESCE(bl.balance_normalized, 0::numeric) / tm.current_supply_normalized::numeric) * 100::numeric
           ELSE 0::numeric
         END AS share_of_supply,
         NULL::text AS wallet_type,
         NULL::timestamp AS first_seen_at,
         NULL::timestamp AS last_seen_at,
         NOW(),
         NOW()
       FROM balance_ledger bl
       LEFT JOIN token_metadata tm
         ON tm.token_symbol = $1
       WHERE COALESCE(bl.balance_normalized, 0::numeric) > 0::numeric
       ON CONFLICT (token_symbol, address, bucket_date) DO UPDATE
         SET balance = EXCLUDED.balance,
             balance_normalized = EXCLUDED.balance_normalized,
             share_of_supply = EXCLUDED.share_of_supply,
             wallet_type = EXCLUDED.wallet_type,
             first_seen_at = EXCLUDED.first_seen_at,
             last_seen_at = EXCLUDED.last_seen_at,
             updated_at = NOW()`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `DELETE FROM wallet_daily_activity
        WHERE token_symbol = $1
          AND bucket_date = $2::date`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `WITH scoped_transactions AS (
         SELECT
           token_symbol,
           from_address,
           to_address,
           COALESCE(amount_normalized, 0::numeric) AS amount_normalized,
           timestamp
         FROM transactions
         WHERE token_symbol = $1
           AND timestamp >= ($2::date)::timestamp
           AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
       ),
       expanded AS (
         SELECT
           token_symbol,
           to_address AS address,
           from_address AS counterparty,
           1 AS incoming_tx_count,
           0 AS outgoing_tx_count,
           amount_normalized AS incoming_volume,
           0::numeric AS outgoing_volume,
           timestamp
         FROM scoped_transactions
         UNION ALL
         SELECT
           token_symbol,
           from_address AS address,
           to_address AS counterparty,
           0 AS incoming_tx_count,
           1 AS outgoing_tx_count,
           0::numeric AS incoming_volume,
           amount_normalized AS outgoing_volume,
           timestamp
         FROM scoped_transactions
       )
       INSERT INTO wallet_daily_activity (
         token_symbol,
         address,
         bucket_date,
         incoming_tx_count,
         outgoing_tx_count,
         incoming_volume,
         outgoing_volume,
         net_flow,
         counterparty_count,
         last_tx_at,
         created_at,
         updated_at
       )
       SELECT
         token_symbol,
         address,
         $2::date AS bucket_date,
         SUM(incoming_tx_count)::integer,
         SUM(outgoing_tx_count)::integer,
         SUM(incoming_volume),
         SUM(outgoing_volume),
         SUM(incoming_volume) - SUM(outgoing_volume) AS net_flow,
         COUNT(DISTINCT counterparty)::integer AS counterparty_count,
         MAX(timestamp) AS last_tx_at,
         NOW(),
         NOW()
       FROM expanded
       GROUP BY token_symbol, address
       ON CONFLICT (token_symbol, address, bucket_date) DO UPDATE
         SET incoming_tx_count = EXCLUDED.incoming_tx_count,
             outgoing_tx_count = EXCLUDED.outgoing_tx_count,
             incoming_volume = EXCLUDED.incoming_volume,
             outgoing_volume = EXCLUDED.outgoing_volume,
             net_flow = EXCLUDED.net_flow,
             counterparty_count = EXCLUDED.counterparty_count,
             last_tx_at = EXCLUDED.last_tx_at,
             updated_at = NOW()`,
      [tokenSymbol, normalizedBucketDate],
    );

    await client.query(
      `INSERT INTO token_daily_metrics (
         token_symbol,
         bucket_date,
         holder_count,
         new_holder_count,
         lost_holder_count,
         active_wallet_count,
         transaction_count,
         transfer_volume,
         current_supply,
         top10_share,
         top50_share,
         top_wallet_share,
         gini_coefficient,
         median_transfer_amount,
         avg_transfer_amount,
         created_at,
         updated_at
       )
       WITH today_balances AS (
         SELECT address, balance_normalized
         FROM wallet_daily_balances
         WHERE token_symbol = $1
           AND bucket_date = $2::date
           AND balance_normalized > 0::numeric
       ),
       prev_balances AS (
         SELECT address, balance_normalized
         FROM wallet_daily_balances
         WHERE token_symbol = $1
           AND bucket_date = ($2::date - INTERVAL '1 day')::date
           AND balance_normalized > 0::numeric
       ),
       ranked_balances AS (
         SELECT
           address,
           balance_normalized,
           ROW_NUMBER() OVER (ORDER BY balance_normalized DESC, address ASC) AS rank_desc,
           ROW_NUMBER() OVER (ORDER BY balance_normalized ASC, address ASC) AS rank_asc,
           COUNT(*) OVER () AS holder_total,
           SUM(balance_normalized) OVER () AS balance_total
         FROM today_balances
       ),
       scoped_transactions AS (
         SELECT amount_normalized
         FROM transactions
         WHERE token_symbol = $1
           AND timestamp >= ($2::date)::timestamp
           AND timestamp < (($2::date + INTERVAL '1 day'))::timestamp
       )
       SELECT
         $1,
         $2::date,
         (SELECT COUNT(*) FROM today_balances),
         (
           SELECT COUNT(*)
           FROM today_balances t
           WHERE NOT EXISTS (
             SELECT 1 FROM prev_balances p WHERE p.address = t.address
           )
         ),
         (
           SELECT COUNT(*)
           FROM prev_balances p
           WHERE NOT EXISTS (
             SELECT 1 FROM today_balances t WHERE t.address = p.address
           )
         ),
         (
           SELECT COUNT(*)
           FROM wallet_daily_activity
           WHERE token_symbol = $1
             AND bucket_date = $2::date
             AND (incoming_tx_count + outgoing_tx_count) > 0
         ),
         (SELECT COUNT(*) FROM scoped_transactions),
         (SELECT COALESCE(SUM(amount_normalized), 0::numeric) FROM scoped_transactions),
         (
           SELECT COALESCE(current_supply_normalized::numeric, 0::numeric)
           FROM token_metadata
           WHERE token_symbol = $1
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(balance_total) > 0
                 THEN (SUM(CASE WHEN rank_desc <= 10 THEN balance_normalized ELSE 0::numeric END) / MAX(balance_total)) * 100::numeric
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(balance_total) > 0
                 THEN (SUM(CASE WHEN rank_desc <= 50 THEN balance_normalized ELSE 0::numeric END) / MAX(balance_total)) * 100::numeric
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(balance_total) > 0
                 THEN (MAX(CASE WHEN rank_desc = 1 THEN balance_normalized ELSE 0::numeric END) / MAX(balance_total)) * 100::numeric
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (
           SELECT COALESCE(
             CASE
               WHEN MAX(holder_total) > 0 AND MAX(balance_total) > 0
                 THEN (
                   (2::numeric * SUM(rank_asc * balance_normalized) / (MAX(holder_total) * MAX(balance_total)))
                   - ((MAX(holder_total) + 1)::numeric / MAX(holder_total))
                 )
               ELSE 0::numeric
             END,
             0::numeric
           )
           FROM ranked_balances
         ),
         (SELECT COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_normalized), 0::numeric) FROM scoped_transactions),
         (SELECT COALESCE(AVG(amount_normalized), 0::numeric) FROM scoped_transactions),
         NOW(),
         NOW()
       ON CONFLICT (token_symbol, bucket_date) DO UPDATE
         SET holder_count = EXCLUDED.holder_count,
             new_holder_count = EXCLUDED.new_holder_count,
             lost_holder_count = EXCLUDED.lost_holder_count,
             active_wallet_count = EXCLUDED.active_wallet_count,
             transaction_count = EXCLUDED.transaction_count,
             transfer_volume = EXCLUDED.transfer_volume,
             current_supply = EXCLUDED.current_supply,
             top10_share = EXCLUDED.top10_share,
             top50_share = EXCLUDED.top50_share,
             top_wallet_share = EXCLUDED.top_wallet_share,
             gini_coefficient = EXCLUDED.gini_coefficient,
             median_transfer_amount = EXCLUDED.median_transfer_amount,
             avg_transfer_amount = EXCLUDED.avg_transfer_amount,
             updated_at = NOW()`,
      [tokenSymbol, normalizedBucketDate],
    );
  });
}

export async function refreshAllTokenAnalyticsForDate(
  bucketDate = new Date(),
): Promise<number> {
  const tokenSymbols = await getAvailableTokens();

  for (const tokenSymbol of tokenSymbols) {
    await refreshTokenAnalyticsForDate(tokenSymbol, bucketDate);
  }

  return tokenSymbols.length;
}

export async function getTokenDailyMetrics(
  tokenSymbol: string,
  days: number,
): Promise<TokenDailyMetricsRecord[]> {
  const safeDays = Math.max(1, Math.min(3650, Math.floor(days || 30)));
  const result = await databasePool.query(
    `SELECT
       token_symbol,
       bucket_date::text AS bucket_date,
       holder_count,
       new_holder_count,
       lost_holder_count,
       active_wallet_count,
       transaction_count,
       transfer_volume,
       current_supply,
       top10_share,
       top50_share,
       top_wallet_share,
       gini_coefficient,
       median_transfer_amount,
       avg_transfer_amount,
       updated_at
     FROM token_daily_metrics
     WHERE token_symbol = $1
     ORDER BY bucket_date DESC
     LIMIT $2`,
    [tokenSymbol, safeDays],
  );

  return result.rows.reverse().map(mapTokenDailyMetricsRow);
}

export async function getTokenTopMovers(
  tokenSymbol: string,
  windowDays: number,
  limit: number,
): Promise<TokenTopMoverRecord[]> {
  const safeWindowDays = Math.max(
    1,
    Math.min(365, Math.floor(windowDays || 7)),
  );
  const safeLimit = Math.max(1, Math.min(200, Math.floor(limit || 20)));
  const result = await databasePool.query(
    `WITH latest_date AS (
       SELECT MAX(bucket_date) AS value
       FROM wallet_daily_balances
       WHERE token_symbol = $1
     ),
     baseline_target AS (
       SELECT (value - ($2::text || ' days')::interval)::date AS value
       FROM latest_date
       WHERE value IS NOT NULL
     ),
     baseline_date AS (
       SELECT COALESCE(
         (
           SELECT MAX(bucket_date)
           FROM wallet_daily_balances
           WHERE token_symbol = $1
             AND bucket_date <= (SELECT value FROM baseline_target)
         ),
         (
           SELECT MIN(bucket_date)
           FROM wallet_daily_balances
           WHERE token_symbol = $1
             AND bucket_date < (SELECT value FROM latest_date)
         )
       ) AS value
     ),
     latest_balances AS (
       SELECT address, COALESCE(balance_normalized, 0::numeric) AS balance
       FROM wallet_daily_balances
       WHERE token_symbol = $1
         AND bucket_date = (SELECT value FROM latest_date)
     ),
     baseline_balances AS (
       SELECT address, COALESCE(balance_normalized, 0::numeric) AS balance
       FROM wallet_daily_balances
       WHERE token_symbol = $1
         AND bucket_date = (SELECT value FROM baseline_date)
     )
     SELECT
       $1 AS token_symbol,
       COALESCE(l.address, b.address) AS address,
       (SELECT value::text FROM latest_date) AS latest_date,
       (SELECT value::text FROM baseline_date) AS baseline_date,
       COALESCE(l.balance, 0::numeric) AS latest_balance,
       COALESCE(b.balance, 0::numeric) AS baseline_balance,
       COALESCE(l.balance, 0::numeric) - COALESCE(b.balance, 0::numeric) AS delta_balance,
       CASE
         WHEN COALESCE(b.balance, 0::numeric) > 0
           THEN ((COALESCE(l.balance, 0::numeric) - COALESCE(b.balance, 0::numeric)) / b.balance) * 100::numeric
         WHEN COALESCE(b.balance, 0::numeric) = 0 AND COALESCE(l.balance, 0::numeric) > 0
           THEN 100::numeric
         ELSE 0::numeric
       END AS delta_pct
     FROM latest_balances l
     FULL OUTER JOIN baseline_balances b
       ON b.address = l.address
     WHERE (SELECT value FROM baseline_date) IS NOT NULL
       AND (COALESCE(l.balance, 0::numeric) > 0::numeric
        OR COALESCE(b.balance, 0::numeric) > 0::numeric
       )
     ORDER BY ABS(COALESCE(l.balance, 0::numeric) - COALESCE(b.balance, 0::numeric)) DESC,
              COALESCE(l.address, b.address) ASC
     LIMIT $3`,
    [tokenSymbol, safeWindowDays, safeLimit],
  );

  return result.rows.map(mapTokenTopMoverRow);
}
