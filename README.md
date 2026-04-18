# PHANTASMAMAPS-API(7)

## NAME

PhantasmaMaps-api - Phantasma blockchain ingestion workers, PostgreSQL graph store, and HTTP API for token graph, holder, and transaction queries.

## SYNOPSIS

```text
npm run api
npm run sync
npm run backfill
npm run backfill:dry-run
npm run sync:nodes-normalized
npm run test:rpc
npm run test:db-snapshot
npm run cleanup:claims
```

## TABLE OF CONTENTS

- [NAME](#name)
- [SYNOPSIS](#synopsis)
- [TABLE OF CONTENTS](#table-of-contents)
- [DESCRIPTION](#description)
- [PROJECT STRUCTURE](#project-structure)
- [ARCHITECTURE](#architecture)
- [REQUIREMENTS](#requirements)
- [INSTALLATION](#installation)
- [ENVIRONMENT](#environment)
- [COMMANDS](#commands)
- [DEPLOYMENT](#deployment)
- [CLEANUP TIMER](#cleanup-timer)
- [HTTP API](#http-api)
- [DATABASE LAYOUT](#database-layout)
- [MIGRATIONS AND ROLLBACK](#migrations-and-rollback)
- [OPERATIONAL MODEL](#operational-model)
- [DEVELOPER MANUAL](#developer-manual)
- [OPERATOR RUNBOOK](#operator-runbook)
- [DEVELOPMENT NOTES](#development-notes)
- [SEE ALSO](#see-also)

## DESCRIPTION

PhantasmaMaps-api ingests transfer activity from the Phantasma chain, normalizes token metadata and balances, stores the result in PostgreSQL, and exposes an HTTP API for graph and transaction lookups. The codebase is split into four main concerns:

1. RPC access and throttling.
2. Transfer extraction and normalization.
3. Database upsert, sync-state, and block-claim management.
4. API read endpoints for consumers.

The worker side processes blocks by height. Each block is claimed through the `block_sync_claims` table, fetched from RPC, parsed into transfers, enriched with balances and token metadata, then written into `transactions`, `nodes`, `edges`, `token_metadata`, and `sync_state`. The chain-wide checkpoint uses the synthetic token symbol `__chain__` to represent the highest contiguous completed block.

## PROJECT STRUCTURE

```text
src/
  apiServer.ts
  database.ts
  rpcClient.ts
  syncService.ts
  transferParser.ts
  ...other worker and utility entrypoints
sql/
  migrations/
    001_init.sql
    ...
    010_address_connections.sql
  maintenance/
    004_truncate_all.sql
README.md
DEVELOPER_MANUAL.md
docker-compose.yml
Dockerfile
```

## ARCHITECTURE

```text
Phantasma RPC
    |
    v
src/rpcClient.ts
    |
    v
src/transferParser.ts ----> src/syncService.ts ----> src/database.ts ----> PostgreSQL
                                                |
                                                v
                                           src/apiServer.ts
```

### Data Flow

1. A worker determines the next block to process from `block_sync_claims`.
2. `src/rpcClient.ts` fetches the block, account balances, and token metadata.
3. `src/transferParser.ts` pairs `TOKENSEND` and `TOKENRECEIVE` events into transfers.
4. `src/syncService.ts` enriches those transfers with node balances and token metadata.
5. `src/database.ts` persists transfers, nodes, edges, token metadata, and sync checkpoints.
6. `advanceChainSyncHeightFromClaims()` advances `__chain__` only when the completed range is contiguous.
7. `src/apiServer.ts` serves query endpoints from the stored data.

## REQUIREMENTS

- Node.js 22 or newer.
- PostgreSQL reachable through `DATABASE_URL` or discrete `PG*` settings.
- Network access to one or more Phantasma RPC endpoints.

## INSTALLATION

1. Install dependencies.

```bash
npm install
```

2. Create `.env` from `.env.example` and fill in database credentials.

3. Apply SQL files in ascending order:

```text
sql/migrations/001_init.sql
sql/migrations/002_transactions.sql
sql/migrations/003_sync_state.sql
sql/migrations/004_token_metadata.sql
sql/migrations/005_nodes_balance_normalized.sql
sql/migrations/006_edges_amount_normalized.sql
sql/migrations/007_transactions_amount_normalized.sql
sql/migrations/008_block_sync_claims.sql
```

4. Start the API or one of the worker commands.

## MIGRATIONS AND ROLLBACK

There is no built-in migration runner in this repository. Apply the SQL files manually or from external deployment tooling.

### Manual Migration Commands

PowerShell example using `psql` and `DATABASE_URL` from `.env`:

```powershell
$env:DATABASE_URL = "postgres://USER:PASSWORD@HOST:25060/DBNAME?sslmode=require"
psql $env:DATABASE_URL -f .\sql/migrations/001_init.sql
psql $env:DATABASE_URL -f .\sql/migrations/002_transactions.sql
psql $env:DATABASE_URL -f .\sql/migrations/003_sync_state.sql
psql $env:DATABASE_URL -f .\sql/migrations/004_token_metadata.sql
psql $env:DATABASE_URL -f .\sql/migrations/005_nodes_balance_normalized.sql
psql $env:DATABASE_URL -f .\sql/migrations/006_edges_amount_normalized.sql
psql $env:DATABASE_URL -f .\sql/migrations/007_transactions_amount_normalized.sql
psql $env:DATABASE_URL -f .\sql/migrations/008_block_sync_claims.sql
```

Bash example:

```bash
export DATABASE_URL="postgres://USER:PASSWORD@HOST:25060/DBNAME?sslmode=require"
for file in \
  sql/migrations/001_init.sql \
  sql/migrations/002_transactions.sql \
  sql/migrations/003_sync_state.sql \
  sql/migrations/004_token_metadata.sql \
  sql/migrations/005_nodes_balance_normalized.sql \
  sql/migrations/006_edges_amount_normalized.sql \
  sql/migrations/007_transactions_amount_normalized.sql \
  sql/migrations/008_block_sync_claims.sql; do
  psql "$DATABASE_URL" -f "$file"
done
```

### Recommended Pre-Deployment Backup

Create a backup before applying schema changes:

```bash
pg_dump "$DATABASE_URL" > phantasmamaps-predeploy.sql
```

### Rollback Strategy

This repository does not ship reversible `down` migrations. In practice, rollback means one of these paths:

1. Restore the database from a backup taken before the migration.
2. If the issue is data-only and you intend to rebuild derived state, truncate runtime tables with [sql/maintenance/004_truncate_all.sql](sql/maintenance/004_truncate_all.sql), then rerun backfill.
3. If a migration only adds nullable columns or indexes and the application still runs, fix forward with a follow-up migration instead of dropping production data.

Restore from a SQL backup:

```bash
psql "$DATABASE_URL" -f phantasmamaps-predeploy.sql
```

Reset runtime tables and rebuild derived state:

```bash
psql "$DATABASE_URL" -f sql/maintenance/004_truncate_all.sql
npm run backfill
```

Important note: [sql/maintenance/004_truncate_all.sql](sql/maintenance/004_truncate_all.sql) clears `edges`, `nodes`, `transactions`, `sync_state`, and `graph_versions`. It does not remove `token_metadata` or `block_sync_claims`, so use it only when that partial reset matches the repair you need.

## ENVIRONMENT

The application loads environment variables through `process.loadEnvFile?.()` in Node 22, so a local `.env` file is read automatically when present.

### Database

`DATABASE_URL`
: Full PostgreSQL connection string. If set, it takes precedence over discrete `PG*` settings.

`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
: Optional discrete PostgreSQL connection settings.

`PGSSL`
: `true` enables TLS with `rejectUnauthorized: false`.

### API

`PHANTASMA_API_PORT`
: HTTP port. Default: `3000`.

`PHANTASMA_API_CORS_ORIGINS`
: Comma-separated allowed origins. If omitted, CORS is permissive.

### RPC

`PHANTASMA_RPC_URLS`
: Comma-separated list of Phantasma RPC URLs.

`PHANTASMA_NEXUS`
: Nexus name. Default: `mainnet`.

`PHANTASMA_CHAIN`
: Chain name. Default: `main`.

`PHANTASMA_RPC_ATTEMPTS`
: Retry count per RPC URL. Default: `2`.

`PHANTASMA_RPC_TIMEOUT_MS`
: Per-request timeout. Default: `6000`.

`PHANTASMA_BLOCK_REQUEST_INTERVAL_MS`
: Minimum delay between block request start times. Default: `1000`.

`PHANTASMA_RPC_BLOCK_MAX_CONCURRENT`
: Maximum concurrent block RPC requests. Default: `4`.

`PHANTASMA_METADATA_REQUEST_INTERVAL_MS`
: Minimum delay between metadata request start times. Default: `500`.

`PHANTASMA_RPC_METADATA_MAX_CONCURRENT`
: Maximum concurrent metadata RPC requests. Default: `8`.

### Sync Behavior

`PHANTASMA_SYNC_WORKER_COUNT`
: Worker count for block processing. Default: `4`.

`PHANTASMA_SYNC_BLOCK_LOG_INTERVAL`
: Log every N processed blocks. Default: `100`.

`PHANTASMA_SYNC_CLAIM_MAX_ATTEMPTS`
: Maximum attempts before a claim is considered exhausted. Default: `3`.

`PHANTASMA_SYNC_CLAIM_RETRY_BASE_DELAY_SECONDS`
: Retry backoff base delay. Default: `30`.

`PHANTASMA_SYNC_CLAIM_RETRY_MAX_DELAY_SECONDS`
: Retry backoff cap. Default: `900`.

`PHANTASMA_SYNC_CLAIM_STALE_AFTER_SECONDS`
: Claim age after which a `claimed` row is reset to `pending`. Default: `1800`.

`PHANTASMA_CAPTURE_RAW_EVENTS`
: Store raw event metadata for transfers. Default: `true`.

### API Limits

`PHANTASMA_GRAPH_MAX_EDGES_PER_REQUEST`
: Per-request cap for address graph traversal. Default: `1200`.

`PHANTASMA_TOKEN_GRAPH_MAX_EDGES`
: Maximum full-token graph edge count. Default: `5000`.

`PHANTASMA_TX_PAGE_SIZE`
: Default transaction page size. Default: `50`.

`PHANTASMA_TX_PAGE_SIZE_MAX`
: Maximum transaction page size. Default: `250`.

### Maintenance

`PHANTASMA_CLAIM_CLEANUP_DAYS`
: Days of completed block claims to retain before cleanup. Default: `2`.

## COMMANDS

`npm run api`
: Starts the Express API server.

`npm run sync`
: Runs incremental sync from the current committed chain height to the latest RPC height.

`npm run backfill`
: Runs a historical backfill, then normalizes node, edge, and transaction amounts.

`npm run backfill:dry-run`
: Parses five blocks starting from the configured backfill block and writes a JSON report without touching the database.

`npm run sync:nodes-normalized`
: Refreshes token metadata and tracked node balances, then normalizes node, edge, and transaction amounts.

`npm run find-first-transfer`
: Scans blocks upward from a hardcoded starting point until a block with transfers is found.

`npm run test:rpc`
: Verifies the RPC connection and prints basic network information.

`npm run test:db-snapshot`
: Creates a JSON snapshot of parsed transfer-like records from a fixed block range.

`npm run cleanup:claims`
: Deletes old completed block claims.

## CLEANUP TIMER

The cleanup script in [src/cleanupBlockClaims.ts](src/cleanupBlockClaims.ts) is intended to run as a scheduled maintenance job. It deletes `completed` rows from `block_sync_claims` when `completed_at` is older than `PHANTASMA_CLAIM_CLEANUP_DAYS`.

### Linux Cron

Example weekly cron job:

```cron
0 3 * * 0 cd /opt/apps/PhantasmaMaps-api && PHANTASMA_CLAIM_CLEANUP_DAYS=2 /usr/bin/npm run cleanup:claims >> /var/log/phantasma-cleanup.log 2>&1
```

This runs every Sunday at 03:00.

### systemd Service And Timer

Service file:

```ini
# /etc/systemd/system/phantasma-cleanup.service
[Unit]
Description=Phantasma block_sync_claims cleanup job
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/opt/apps/PhantasmaMaps-api
Environment=PHANTASMA_CLAIM_CLEANUP_DAYS=2
ExecStart=/usr/bin/npm run cleanup:claims
User=root
```

Timer file:

```ini
# /etc/systemd/system/phantasma-cleanup.timer
[Unit]
Description=Run Phantasma cleanup weekly

[Timer]
OnCalendar=Sun 03:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and verify:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now phantasma-cleanup.timer
sudo systemctl status phantasma-cleanup.timer
sudo systemctl list-timers --all | grep phantasma-cleanup
```

Manual test:

```bash
sudo systemctl start phantasma-cleanup.service
journalctl -u phantasma-cleanup.service -n 100 --no-pager
```

Operational note: verify the `npm` path on the server with `which npm`. If it is not `/usr/bin/npm`, update `ExecStart` accordingly.

## DEPLOYMENT

### Local Node Process

1. Install dependencies with `npm install`.
2. Populate `.env` with a reachable PostgreSQL database and RPC URLs.
3. Apply the SQL migrations manually.
4. Start the API with `npm run api`.
5. Start a sync worker separately with `npm run sync` or `npm run backfill`.

The API process is read-only. Keeping the HTTP server running does not ingest new blocks by itself. A worker process must also be running if new chain activity should appear in the database.

### Docker Image

The repository includes [Dockerfile](Dockerfile). It:

- uses `node:22-bookworm-slim`
- runs `npm ci`
- copies the repository into `/app`
- exposes port `3000`
- defaults to `npm run api`

Build the image:

```bash
docker build -t phantasmamaps-api .
```

Run the API container:

```bash
docker run --rm -p 3000:3000 --env-file .env phantasmamaps-api
```

Run a one-off sync job from the same image:

```bash
docker run --rm --env-file .env phantasmamaps-api npm run sync
```

### Docker Compose

The repository includes [docker-compose.yml](docker-compose.yml) with three services:

- `api`: long-running HTTP server on port `3000`
- `backfill`: one-shot historical sync job under the `jobs` profile
- `sync`: one-shot incremental sync job under the `jobs` profile

Start the API service:

```bash
docker compose up --build api
```

Run a one-off incremental sync job:

```bash
docker compose --profile jobs run --rm sync
```

Run a one-off backfill job:

```bash
docker compose --profile jobs run --rm backfill
```

Important deployment note: the compose file does not provision PostgreSQL. The containers expect an external database reachable through `.env`.

### Production Notes

- Keep API and worker processes separate so API restarts do not interrupt ingestion.
- Tune `PHANTASMA_SYNC_WORKER_COUNT`, `PHANTASMA_RPC_BLOCK_MAX_CONCURRENT`, and `PHANTASMA_RPC_METADATA_MAX_CONCURRENT` conservatively before scaling up.
- Set `PHANTASMA_SYNC_CLAIM_STALE_AFTER_SECONDS` to a value that matches expected block processing time so abandoned claims recover promptly.
- Backups matter because `transactions`, `edges`, and `nodes` are the durable derived state for the API.

## HTTP API

### `GET /health`

Returns `{ ok: true }` when the API can reach PostgreSQL.

Example request:

```bash
curl http://localhost:3000/health
```

Example response:

```json
{
  "ok": true
}
```

### `GET /sync-status`

Returns all rows from `sync_state`, including the synthetic `__chain__` checkpoint.

Example request:

```bash
curl http://localhost:3000/sync-status
```

Example response:

```json
{
  "items": [
    {
      "tokenSymbol": "__chain__",
      "lastBlockHeight": 8681520,
      "updatedAt": "2026-04-08T09:40:12.000Z",
      "metadata": null
    },
    {
      "tokenSymbol": "SOUL",
      "lastBlockHeight": 8681520,
      "updatedAt": "2026-04-08T09:40:12.000Z",
      "metadata": null
    }
  ],
  "chainHeadBlockHeight": 8681544
}
```

### `GET /sync-claims`

Returns block claim status summary and rows.

Query parameters:

- `status`: comma-separated statuses.
- `fromBlock`: inclusive lower block bound.
- `toBlock`: inclusive upper block bound.
- `limit`: row cap.

Example request:

```bash
curl "http://localhost:3000/sync-claims?status=claimed,failed&fromBlock=8681400&limit=20"
```

Example response:

```json
{
  "summary": {
    "pending": 0,
    "claimed": 1,
    "completed": 120,
    "failed": 2,
    "exhausted": 0,
    "retryBlocked": 1,
    "nextRetryAt": "2026-04-08T09:45:00.000Z"
  },
  "items": [
    {
      "blockHeight": 8681444,
      "status": "claimed",
      "claimedBy": "12345:2:worker-uuid",
      "claimedAt": "2026-04-08T09:42:00.000Z",
      "completedAt": null,
      "attemptCount": 1,
      "error": null,
      "createdAt": "2026-04-08T09:35:00.000Z",
      "updatedAt": "2026-04-08T09:42:00.000Z",
      "nextRetryAt": null,
      "retryBlocked": false,
      "exhausted": false
    }
  ]
}
```

### `GET /tokens`

Returns distinct token symbols available in stored transactions.

Example request:

```bash
curl http://localhost:3000/tokens
```

Example response:

```json
{
  "items": ["BNB", "KCAL", "SOUL"]
}
```

### `GET /tokens/:tokenSymbol/metadata`

Returns one token metadata record or `404` if absent.

Example request:

```bash
curl http://localhost:3000/tokens/SOUL/metadata
```

Example response:

```json
{
  "tokenSymbol": "SOUL",
  "name": "Phantasma Stake",
  "decimals": 8,
  "currentSupplyRaw": "124512345678900000",
  "currentSupplyNormalized": "1245123456.789",
  "maxSupplyRaw": "1000000000000000000",
  "maxSupplyNormalized": "10000000000",
  "flags": {
    "isBurnable": true,
    "isFungible": true,
    "isFinite": true,
    "isTransferable": true
  },
  "metadata": {},
  "updatedAt": "2026-04-08T09:40:12.000Z"
}
```

### `GET /tokens/:tokenSymbol/top-holders`

Returns top holders by net balance.

Query parameters:

- `limit`: maximum returned holders, capped at `100`.

Example request:

```bash
curl "http://localhost:3000/tokens/SOUL/top-holders?limit=5"
```

Example response:

```json
{
  "tokenSymbol": "SOUL",
  "limit": 5,
  "items": [
    {
      "address": "P2KExampleAddress",
      "tokenSymbol": "SOUL",
      "netBalance": "502341230000"
    }
  ]
}
```

### `GET /graph/token/:tokenSymbol`

Returns the full stored graph for a token up to the configured edge cap.

Example request:

```bash
curl http://localhost:3000/graph/token/SOUL
```

Example response:

```json
{
  "tokenSymbol": "SOUL",
  "rootAddress": "",
  "depth": 0,
  "nodes": [
    {
      "address": "P2KExampleFrom",
      "tokenSymbol": "SOUL",
      "balance": "1200000000",
      "balanceNormalized": "12",
      "label": null,
      "metadata": null
    }
  ],
  "edges": [
    {
      "id": "8681520-0",
      "tokenSymbol": "SOUL",
      "fromAddress": "P2KExampleFrom",
      "toAddress": "P2KExampleTo",
      "amount": "100000000",
      "amountNormalized": "1",
      "txHash": "0xexample",
      "eventIndex": 0,
      "metadata": {}
    }
  ]
}
```

### `GET /graph/address/:address`

Returns an address-centered subgraph.

Query parameters:

- `token`: required token symbol.
- `depth`: traversal depth.
- `edgeLimit`: edge cap.

Example request:

```bash
curl "http://localhost:3000/graph/address/P2KExampleAddress?token=SOUL&depth=2&edgeLimit=250"
```

Example response:

```json
{
  "tokenSymbol": "SOUL",
  "rootAddress": "P2KExampleAddress",
  "depth": 2,
  "nodes": [
    {
      "address": "P2KExampleAddress",
      "tokenSymbol": "SOUL",
      "balance": "4200000000",
      "balanceNormalized": "42",
      "label": null,
      "metadata": null
    }
  ],
  "edges": [
    {
      "id": "8681520-0",
      "tokenSymbol": "SOUL",
      "fromAddress": "P2KNeighborA",
      "toAddress": "P2KExampleAddress",
      "amount": "100000000",
      "amountNormalized": "1",
      "txHash": "0xexample",
      "eventIndex": 0,
      "metadata": {}
    }
  ]
}
```

### `GET /transactions`

Returns paginated transaction data.

Query parameters:

- `token`: optional token symbol.
- `address`: optional source or destination address.
- `dir`: optional direction relative to `address`. Allowed values: `from` or `to`.
- `counterparty`: optional partial match for the opposite address.
- `startTime`: optional inclusive UTC ISO datetime lower bound.
- `endTime`: optional inclusive UTC ISO datetime upper bound.
- `minAmount`: optional inclusive lower bound on `amountNormalized`.
- `maxAmount`: optional inclusive upper bound on `amountNormalized`.
- `minUsd`: optional inclusive lower USD bound. Requires `usdRateNow`.
- `maxUsd`: optional inclusive upper USD bound. Requires `usdRateNow`.
- `usdRateNow`: optional USD rate used for USD filters (`amountNormalized * usdRateNow`).
- `sortBy`: optional sort key. Allowed values: `amount` or `usd`.
- `sortDir`: optional sort direction. Allowed values: `asc` or `desc`.
- `fromBlock`: inclusive lower block bound.
- `toBlock`: inclusive upper block bound.
- `page`: page number.
- `pageSize`: page size.

Example request:

```bash
curl "http://localhost:3000/transactions?token=SOUL&address=P2KExampleAddress&dir=from&counterparty=P2K&startTime=2026-01-01T00:00:00.000Z&endTime=2026-12-31T23:59:59.000Z&minAmount=1&maxAmount=10000&sortBy=usd&sortDir=desc&page=1&pageSize=100"
```

Example response:

```json
{
  "page": 1,
  "pageSize": 100,
  "total": 184,
  "appliedFilters": {
    "token": "SOUL",
    "address": "P2KExampleAddress",
    "dir": "from",
    "counterparty": "P2K",
    "startTime": "2026-01-01T00:00:00.000Z",
    "endTime": "2026-12-31T23:59:59.000Z",
    "minAmount": 1,
    "sortBy": "usd",
    "sortDir": "desc"
  },
  "items": [
    {
      "id": "101",
      "txHash": "0xexample",
      "eventIndex": null,
      "eventIndexes": [0, 1],
      "transferCount": 2,
      "tokenSymbol": "SOUL",
      "blockHeight": 8681520,
      "timestamp": "2026-04-08T09:38:00.000Z",
      "fromAddress": "P2KExampleFrom",
      "toAddress": "P2KExampleAddress",
      "amount": "250000000",
      "amountNormalized": "2.5",
      "metadata": []
    }
  ]
}
```

## DATABASE LAYOUT

### Core Tables

`nodes`
: Address and token scoped balance view.

`edges`
: Directed transfer relationships, typically one row per matched transfer event pair.

`transactions`
: Transfer ledger with block height, token, participants, and optional raw metadata.

`token_metadata`
: Token decimals, supply, flags, and raw token metadata.

`sync_state`
: Per-token and chain-level sync checkpoints.

`block_sync_claims`
: Worker coordination table with `pending`, `claimed`, `completed`, and `failed` states.

### Migration Files

`sql/migrations/001_init.sql`
: Creates `graph_versions`, `nodes`, and `edges`.

`sql/migrations/002_transactions.sql`
: Creates `transactions` and its uniqueness/indexing strategy.

`sql/migrations/003_sync_state.sql`
: Creates `sync_state` for checkpoint tracking.

`sql/migrations/004_token_metadata.sql`
: Creates `token_metadata`.

`sql/maintenance/004_truncate_all.sql`
: Development utility to truncate graph, transaction, and sync tables.

`sql/migrations/005_nodes_balance_normalized.sql`
: Adds normalized node balances.

`sql/migrations/006_edges_amount_normalized.sql`
: Adds normalized edge amounts.

`sql/migrations/007_transactions_amount_normalized.sql`
: Adds normalized transaction amounts.

`sql/migrations/008_block_sync_claims.sql`
: Creates the distributed claim table used by workers.

## OPERATIONAL MODEL

### Worker Claim Lifecycle

1. `seedBlockSyncClaims()` ensures the target range exists as rows.
2. `claimNextBlockHeight()` atomically picks the next eligible block, including stale `claimed` rows that have aged past `PHANTASMA_SYNC_CLAIM_STALE_AFTER_SECONDS`.
3. `processBlockHeight()` fetches and persists data for that block.
4. `completeBlockSyncClaim()` marks success.
5. `failBlockSyncClaim()` marks failure and stores the error.
6. `advanceChainSyncHeightFromClaims()` moves `__chain__` to the highest contiguous completed block.
7. `resetStaleBlockSyncClaims()` still exists as a maintenance helper, but workers now reclaim stale rows directly in the claim query.

### RPC Scheduling

The client uses separate schedulers for block and metadata requests. Each scheduler enforces both concurrency limits and a minimum delay between request start times. This keeps the workers from flooding the RPC provider or burning CPU on uncontrolled request bursts.

### Token Amount Handling

Fungibility is determined from `token_metadata.flags.isFungible`. If a token is not explicitly marked fungible, the graph and transaction layers store amount fields as `1` and `1`. When fungible metadata is available, raw integer amounts are normalized according to the token's decimals.

### Chain Sync Height

The `__chain__` row in `sync_state` is not simply the highest completed block. It is the highest contiguous completed block. If block `N` is stuck in `claimed`, blocks `N+1` and above may complete, but `__chain__` will not advance beyond `N-1` until the gap is cleared or reset.

Stale-claim recovery is now handled directly by the claim path. When a worker asks for the next block, stale `claimed` rows are treated as claimable candidates before newer pending blocks, which prevents old abandoned work from permanently blocking contiguous chain advancement.

## DEVELOPER MANUAL

The full file-by-file function catalog was moved to [DEVELOPER_MANUAL.md](DEVELOPER_MANUAL.md). Use that document when you need symbol-level descriptions rather than operational guidance.

## OPERATOR RUNBOOK

### API Health Returns `500`

Symptoms:

- `GET /health` returns `500`
- API process starts but data endpoints fail

Checks:

1. Verify `DATABASE_URL` or the discrete `PG*` settings in `.env`.
2. Confirm the database is reachable from the host or container network.
3. Confirm `PGSSL=true` is set when your provider requires TLS.

Actions:

1. Run `npm run test:rpc` only if you also suspect RPC issues; it does not validate PostgreSQL.
2. Fix database connectivity first, then restart `npm run api`.

### `__chain__` Stops Advancing

Symptoms:

- `/sync-status` shows token rows moving but `__chain__` lags behind
- `/sync-claims` shows a low block stuck in `claimed` or repeatedly `failed`

Checks:

1. Call `/sync-claims?fromBlock=<stalled range>&limit=100`.
2. Look for the lowest non-`completed` block.
3. Check whether `PHANTASMA_SYNC_CLAIM_STALE_AFTER_SECONDS` is too large for your workload.

Actions:

1. Restart the worker only if it is still running old code and does not contain the direct stale-claim reclaim logic.
2. Reduce `PHANTASMA_SYNC_CLAIM_STALE_AFTER_SECONDS` if abandoned work is recovering too slowly.
3. If the row is `failed`, inspect the stored error and retry path rather than forcing chain height manually.
4. If the row is `claimed` but not yet stale, wait for the threshold or manually reset that specific row if you are certain the claiming worker died.

### Worker CPU Is Too High

Symptoms:

- Sync process consumes more CPU than expected
- RPC provider starts throttling or timing out

Checks:

1. Review `PHANTASMA_SYNC_WORKER_COUNT`.
2. Review `PHANTASMA_RPC_BLOCK_MAX_CONCURRENT` and `PHANTASMA_RPC_METADATA_MAX_CONCURRENT`.
3. Review `PHANTASMA_BLOCK_REQUEST_INTERVAL_MS` and `PHANTASMA_METADATA_REQUEST_INTERVAL_MS`.

Actions:

1. Lower worker count first.
2. Lower RPC concurrency second.
3. Increase request intervals if the host still spikes or the provider rate-limits you.

### Graph Responses Are Too Large

Symptoms:

- Graph endpoints return too much data
- API latency spikes on `/graph/address/...` or `/graph/token/...`

Checks:

1. Review `PHANTASMA_GRAPH_MAX_EDGES_PER_REQUEST`.
2. Review `PHANTASMA_TOKEN_GRAPH_MAX_EDGES`.
3. Review caller-supplied `depth` and `edgeLimit` values.

Actions:

1. Lower the configured graph caps.
2. Keep address graph depth at `1` or `2`.
3. Prefer paginated transaction lookups when graph traversal is not necessary.

### New Deployment Has Bad Derived Data

Symptoms:

- API responds but balances, amounts, or sync state look wrong after a deploy or repair

Checks:

1. Confirm migrations were applied in order.
2. Confirm workers finished running after the schema change.
3. Confirm token metadata and normalization jobs were run if your repair required them.

Actions:

1. Restore from backup if the deployment changed schema in an unsafe way.
2. If the schema is fine and the data is rebuildable, run [sql/maintenance/004_truncate_all.sql](sql/maintenance/004_truncate_all.sql) and then `npm run backfill`.
3. Run `npm run sync:nodes-normalized` when you need to refresh normalized balances and amounts without a full schema rollback.

## DEVELOPMENT NOTES

- Type checking is provided by `npm run check`.
- There is no migration runner in the repository; SQL files are intended to be applied manually or from external deployment tooling.
- The API is read-only; all write paths are worker and utility scripts.
- `sql/maintenance/004_truncate_all.sql` is destructive and intended for reset scenarios only.
- `src/cleanupBlockClaims.ts` currently defaults to a 2-day retention window unless `PHANTASMA_CLAIM_CLEANUP_DAYS` is overridden.

## SEE ALSO

- `phantasma-sdk-ts`
- PostgreSQL `jsonb` and indexing documentation
- Express 5 routing and middleware documentation
