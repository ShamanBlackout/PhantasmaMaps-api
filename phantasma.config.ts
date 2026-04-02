function readNumber(name: string, fallback: number): number {
  const rawValue = process.env[name];

  if (!rawValue) {
    return fallback;
  }

  const parsed = Number(rawValue);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readBoolean(name: string, fallback: boolean): boolean {
  const rawValue = process.env[name];

  if (!rawValue) {
    return fallback;
  }

  return rawValue.toLowerCase() === "true";
}

function readList(name: string, fallback: string[]): string[] {
  const rawValue = process.env[name];

  if (!rawValue) {
    return fallback;
  }

  return rawValue
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export const attemptsPerRpc = readNumber("PHANTASMA_RPC_ATTEMPTS", 2);
export const requestTimeoutMs = readNumber("PHANTASMA_RPC_TIMEOUT_MS", 6000);

export const rpcConfig = {
  urls: readList("PHANTASMA_RPC_URLS", [
    "https://pharpc1.phantasma.info/rpc",
    "https://pharpc2.phantasma.info/rpc",
  ]),
  nexus: process.env.PHANTASMA_NEXUS ?? "mainnet",
  chain: process.env.PHANTASMA_CHAIN ?? "main",
  attemptsPerRpc,
  requestTimeoutMs,
  blockRequestIntervalMs: readNumber(
    "PHANTASMA_BLOCK_REQUEST_INTERVAL_MS",
    1000,
  ),
  metadataRequestIntervalMs: readNumber(
    "PHANTASMA_METADATA_REQUEST_INTERVAL_MS",
    500,
  ),
} as const;

export const syncConfig = {
  initialBackfillStartBlock: 0,
  blockLogInterval: readNumber("PHANTASMA_SYNC_BLOCK_LOG_INTERVAL", 100),
  captureRawEvents: readBoolean("PHANTASMA_CAPTURE_RAW_EVENTS", true),
} as const;

export const apiConfig = {
  port: readNumber("PHANTASMA_API_PORT", 3000),
  graphDefaultDepth: 1,
  graphHardMaxDepth: 2,
  graphMaxEdgesPerRequest: 500,
  tokenGraphMaxEdges: readNumber("PHANTASMA_TOKEN_GRAPH_MAX_EDGES", 5000),
  transactionPageSizeDefault: readNumber("PHANTASMA_TX_PAGE_SIZE", 50),
  transactionPageSizeMax: readNumber("PHANTASMA_TX_PAGE_SIZE_MAX", 250),
} as const;

export const databaseConfig = {
  connectionString: process.env.DATABASE_URL,
  host: process.env.PGHOST,
  port: process.env.PGPORT ? readNumber("PGPORT", 5432) : undefined,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  ssl: readBoolean("PGSSL", false),
} as const;
