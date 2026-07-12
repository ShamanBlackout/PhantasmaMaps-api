import { databasePool, closeDatabasePool } from "../src/database";

type IndexCheck = {
  tableName: string;
  indexName: string;
};

const REQUIRED_INDEXES: IndexCheck[] = [
  { tableName: "edges", indexName: "idx_edges_token_from_amount_id" },
  { tableName: "edges", indexName: "idx_edges_token_to_amount_id" },
  { tableName: "edges", indexName: "idx_edges_token_id" },
  {
    tableName: "address_connections",
    indexName: "idx_address_connections_token_address_volume",
  },
  { tableName: "transactions", indexName: "idx_tx_token_block_hash" },
  { tableName: "transactions", indexName: "idx_tx_token_from_time" },
  { tableName: "transactions", indexName: "idx_tx_token_to_time" },
  {
    tableName: "wallet_daily_balances",
    indexName: "idx_wallet_daily_balances_token_date_balance",
  },
  {
    tableName: "wallet_daily_activity",
    indexName: "idx_wallet_daily_activity_token_date_address",
  },
  {
    tableName: "token_daily_metrics",
    indexName: "idx_token_daily_metrics_token_date",
  },
];

async function getKnownIndexes(): Promise<Set<string>> {
  const result = await databasePool.query<{
    tablename: string;
    indexname: string;
  }>(
    `SELECT tablename, indexname
       FROM pg_indexes
      WHERE schemaname = 'public'`,
  );

  return new Set(
    result.rows.map((row) => `${row.tablename.toLowerCase()}.${row.indexname}`),
  );
}

async function explainHotQueries(
  tokenSymbol: string,
  address: string,
): Promise<void> {
  const queries = [
    {
      name: "address_subgraph_depth1",
      text: `EXPLAIN (FORMAT TEXT)
             SELECT id, token_symbol, from_address, to_address, amount_normalized
               FROM public.edges
              WHERE token_symbol = $1
                AND (from_address = $2 OR to_address = $2)
              ORDER BY amount_normalized DESC, id ASC
              LIMIT 300`,
      values: [tokenSymbol, address],
    },
    {
      name: "address_connections",
      text: `EXPLAIN (FORMAT TEXT)
             SELECT counterparty, total_volume, transaction_count
               FROM public.address_connections
              WHERE token_symbol = $1
                AND address = $2
              ORDER BY total_volume DESC
              LIMIT 50`,
      values: [tokenSymbol, address],
    },
    {
      name: "transactions_page",
      text: `EXPLAIN (FORMAT TEXT)
             SELECT tx_hash, event_index, block_height
               FROM public.transactions
              WHERE token_symbol = $1
                AND (from_address = $2 OR to_address = $2)
              ORDER BY block_height DESC, tx_hash ASC
              LIMIT 100`,
      values: [tokenSymbol, address],
    },
  ];

  for (const query of queries) {
    const result = await databasePool.query<{ "QUERY PLAN": string }>(
      query.text,
      query.values,
    );
    console.log(`\n[EXPLAIN] ${query.name}`);
    for (const row of result.rows) {
      console.log(row["QUERY PLAN"]);
    }
  }
}

async function main(): Promise<void> {
  const knownIndexes = await getKnownIndexes();
  const missing = REQUIRED_INDEXES.filter(
    (item) => !knownIndexes.has(`${item.tableName}.${item.indexName}`),
  );

  if (missing.length > 0) {
    console.error("Missing required indexes:");
    for (const item of missing) {
      console.error(`- ${item.tableName}.${item.indexName}`);
    }
    process.exitCode = 1;
  } else {
    console.log("All required composite indexes are present.");
  }

  const tokenResult = await databasePool.query<{ token_symbol: string }>(
    `SELECT token_symbol
       FROM public.token_metadata
      ORDER BY updated_at DESC NULLS LAST, token_symbol ASC
      LIMIT 1`,
  );
  const tokenSymbol = tokenResult.rows[0]?.token_symbol;

  if (!tokenSymbol) {
    console.log(
      "No token metadata rows found; skipped EXPLAIN verification run.",
    );
    return;
  }

  const addressResult = await databasePool.query<{ address: string }>(
    `SELECT address
       FROM public.nodes
      WHERE token_symbol = $1
      ORDER BY COALESCE(balance_normalized, 0::numeric) DESC, address ASC
      LIMIT 1`,
    [tokenSymbol],
  );
  const address = addressResult.rows[0]?.address;

  if (!address) {
    console.log(
      `No node rows found for token ${tokenSymbol}; skipped EXPLAIN verification run.`,
    );
    return;
  }

  await explainHotQueries(tokenSymbol, address);
}

main()
  .catch((error) => {
    console.error("verifyIndexes failed", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeDatabasePool();
  });
