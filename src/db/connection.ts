import {
  type PoolConfig,
  Pool,
  type QueryResultRow,
  type QueryResult,
  type PoolClient,
} from "pg";
import { readFileSync } from "node:fs";
import {
  databaseConfig,
  connectionPoolConfig,
  cacheDatabaseConfig,
} from "../phantasma.config";

type DbConnectionConfig = {
  connectionString?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  ssl: boolean;
  sslCaCert?: string;
  sslRootCertPath?: string;
};

const isApiProcess = process.argv.some((arg) =>
  /(^|[\\/])startApiServer(\.ts|\.js)?$/i.test(String(arg || "")),
);

const isDirectDatabaseProcess = process.argv.some((arg) =>
  /(^|[\\/])(backfill|backfillDryRun|syncNodeBalancesNormalized|cleanupBlockClaims|testDatabaseInserts|labelingDryRun|labelingReviewSample)(\.ts|\.js)?$/i.test(
    String(arg || ""),
  ),
);

let hasWarnedUnverifiedTls = false;

function buildSslConfig(config: DbConnectionConfig): PoolConfig["ssl"] {
  const ca =
    config.sslCaCert?.replace(/\\n/g, "\n") ??
    (config.sslRootCertPath
      ? readFileSync(config.sslRootCertPath, "utf8")
      : undefined);

  if (ca) {
    return { ca, rejectUnauthorized: true };
  }

  if (!config.ssl) {
    return undefined;
  }

  if (!hasWarnedUnverifiedTls) {
    hasWarnedUnverifiedTls = true;
    console.warn(
      JSON.stringify({
        level: "warn",
        event: "database_tls_unverified",
        timestamp: new Date().toISOString(),
        message:
          "PGSSL is enabled without a CA certificate; the server certificate is not verified. Set DATABASE_CA_CERT or PGSSLROOTCERT to enable verification.",
      }),
    );
  }

  return { rejectUnauthorized: false };
}

function buildPoolConfig(
  config: DbConnectionConfig,
  options: { useExternalPooler?: boolean } = {},
): PoolConfig {
  const baseConfig: PoolConfig = {
    // Keep a very small local queue when the upstream DATABASE_URL is already a pooler.
    min: options.useExternalPooler ? 0 : 2,
    max: options.useExternalPooler ? 4 : 20,
    idleTimeoutMillis: 30000, // Close idle connections after 30 seconds
    connectionTimeoutMillis: 5000, // Wait up to 5 seconds to acquire a connection
    query_timeout: 30000, // Query timeout of 30 seconds
  };
  const ssl = buildSslConfig(config);

  if (config.connectionString) {
    return {
      connectionString: config.connectionString,
      ssl,
      ...baseConfig,
    };
  }

  return {
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    ssl,
    ...baseConfig,
  };
}

if (
  isDirectDatabaseProcess &&
  !databaseConfig.connectionString &&
  !(
    databaseConfig.host &&
    databaseConfig.port &&
    databaseConfig.user &&
    databaseConfig.database
  )
) {
  throw new Error(
    "Direct database connection is required for worker/sync jobs. Set DATABASE_URL (or PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE). CONNECTION_POOL is API-only.",
  );
}

if (isDirectDatabaseProcess && connectionPoolConfig.connectionString) {
  console.info(
    JSON.stringify({
      level: "info",
      event: "direct_database_mode",
      timestamp: new Date().toISOString(),
      message:
        "Worker/sync process detected; using DATABASE_URL/direct PG settings and ignoring CONNECTION_POOL.",
    }),
  );
}

const mainDatabaseConfig = isApiProcess ? connectionPoolConfig : databaseConfig;
const mainUsesExternalPooler =
  isApiProcess &&
  Boolean(connectionPoolConfig.connectionString) &&
  connectionPoolConfig.connectionString !== databaseConfig.connectionString;

export const databasePool = new Pool(
  buildPoolConfig(mainDatabaseConfig, {
    useExternalPooler: mainUsesExternalPooler,
  }),
);

const hasDedicatedCacheDatabase = Boolean(
  cacheDatabaseConfig.connectionString ||
  cacheDatabaseConfig.host ||
  cacheDatabaseConfig.database,
);

const cachePoolConfig = hasDedicatedCacheDatabase
  ? buildPoolConfig(cacheDatabaseConfig)
  : buildPoolConfig(mainDatabaseConfig, {
      useExternalPooler: mainUsesExternalPooler,
    });

// Use a smaller pool for cache because entries are short-lived and low-cost.
if (hasDedicatedCacheDatabase) {
  cachePoolConfig.min = 1;
  cachePoolConfig.max = 5;
}

export const cacheQueryPool = new Pool(cachePoolConfig);

// Prevent process crashes when an idle pooled client disconnects unexpectedly.
databasePool.on("error", (error: Error) => {
  const maybeError = error as Error & {
    code?: string;
    errno?: string | number;
    syscall?: string;
    address?: string;
    port?: number;
  };
  console.error(
    JSON.stringify({
      level: "error",
      event: "database_pool_error",
      timestamp: new Date().toISOString(),
      message: maybeError.message,
      code: maybeError.code ?? null,
      errno: maybeError.errno ?? null,
      syscall: maybeError.syscall ?? null,
      address: maybeError.address ?? null,
      port: maybeError.port ?? null,
      pool: {
        totalCount: databasePool.totalCount,
        idleCount: databasePool.idleCount,
        waitingCount: databasePool.waitingCount,
      },
    }),
  );
});

cacheQueryPool.on("error", (error: Error) => {
  const maybeError = error as Error & {
    code?: string;
    errno?: string | number;
    syscall?: string;
    address?: string;
    port?: number;
  };
  console.error(
    JSON.stringify({
      level: "error",
      event: "cache_database_pool_error",
      timestamp: new Date().toISOString(),
      message: maybeError.message,
      code: maybeError.code ?? null,
      errno: maybeError.errno ?? null,
      syscall: maybeError.syscall ?? null,
      address: maybeError.address ?? null,
      port: maybeError.port ?? null,
      pool: {
        totalCount: cacheQueryPool.totalCount,
        idleCount: cacheQueryPool.idleCount,
        waitingCount: cacheQueryPool.waitingCount,
      },
    }),
  );
});
const READ_RETRY_ATTEMPTS = 3;
const READ_RETRY_BASE_DELAY_MS = 150;

type RetryableDbError = Error & {
  code?: string;
  errno?: string | number;
  syscall?: string;
};

type DatabaseError = Error & {
  code?: string;
};

export function isUndefinedTableError(error: unknown): boolean {
  return (error as DatabaseError | undefined)?.code === "42P01";
}

function isRetryableReadError(error: unknown): boolean {
  const candidate = error as RetryableDbError;
  const code = String(candidate?.code ?? "").toUpperCase();
  const errno = String(candidate?.errno ?? "").toUpperCase();
  const message = String(candidate?.message ?? "").toLowerCase();

  if (
    [
      "ECONNRESET",
      "ETIMEDOUT",
      "ECONNREFUSED",
      "EPIPE",
      "EHOSTUNREACH",
      "57P01",
      "57P02",
      "57P03",
      "53300",
      "08000",
      "08001",
      "08003",
      "08006",
    ].includes(code)
  ) {
    return true;
  }

  if (["ECONNRESET", "ETIMEDOUT", "ECONNREFUSED", "EPIPE"].includes(errno)) {
    return true;
  }

  return (
    message.includes("connection terminated unexpectedly") ||
    message.includes("server closed the connection unexpectedly") ||
    message.includes("connection reset") ||
    message.includes("timeout")
  );
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function queryReadWithRetry<
  T extends QueryResultRow = QueryResultRow,
>(
  text: string,
  values?: unknown[],
  context = "query",
): Promise<QueryResult<T>> {
  let attempt = 0;

  while (attempt < READ_RETRY_ATTEMPTS) {
    attempt += 1;

    try {
      return await databasePool.query<T>(text, values);
    } catch (error) {
      const retryable = isRetryableReadError(error);
      const isLastAttempt = attempt >= READ_RETRY_ATTEMPTS;

      if (!retryable || isLastAttempt) {
        throw error;
      }

      const backoffMs = READ_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1);
      const candidate = error as RetryableDbError;
      console.warn(
        JSON.stringify({
          level: "warn",
          event: "database_read_retry",
          timestamp: new Date().toISOString(),
          context,
          attempt,
          maxAttempts: READ_RETRY_ATTEMPTS,
          backoffMs,
          code: candidate?.code ?? null,
          message: candidate?.message ?? String(error),
        }),
      );
      await delay(backoffMs);
    }
  }

  throw new Error("unreachable retry state");
}

export async function closeDatabasePool(): Promise<void> {
  await Promise.all([databasePool.end(), cacheQueryPool.end()]);
}

export async function testDatabaseConnection(): Promise<void> {
  await queryReadWithRetry("SELECT 1", [], "health_check");
}

export async function withDatabaseTransaction<T>(
  callback: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await databasePool.connect();

  try {
    await client.query("BEGIN");
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
