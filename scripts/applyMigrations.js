const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

function readEnvFile(envPath) {
  const values = {};
  if (!fs.existsSync(envPath)) {
    return values;
  }

  for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const splitIndex = trimmed.indexOf("=");
    if (splitIndex < 0) {
      continue;
    }

    const key = trimmed.slice(0, splitIndex).trim();
    const value = trimmed.slice(splitIndex + 1).trim();
    values[key] = value;
  }

  return values;
}

function splitSqlStatements(sqlText) {
  return sqlText
    .split(/;\s*(?:\r?\n|$)/)
    .map((statement) => statement.trim())
    .filter(Boolean);
}

async function main() {
  const cwd = process.cwd();
  const envValues = readEnvFile(path.join(cwd, ".env"));
  const connectionString = process.env.DATABASE_URL || envValues.DATABASE_URL;

  if (!connectionString) {
    throw new Error("DATABASE_URL not found in process environment or .env");
  }

  const migrationFiles = process.argv.slice(2);
  if (migrationFiles.length === 0) {
    throw new Error("Provide one or more migration file names.");
  }

  const client = new Client({
    connectionString,
    ssl: { rejectUnauthorized: false },
  });

  await client.connect();

  try {
    const lockTimeoutMs = Number(
      process.env.PHANTASMA_MIGRATION_LOCK_TIMEOUT_MS || 0,
    );
    const lockTimeout =
      Number.isFinite(lockTimeoutMs) && lockTimeoutMs > 0
        ? `${Math.floor(lockTimeoutMs)}ms`
        : "0";
    await client.query(`SET lock_timeout = '${lockTimeout}';`);
    await client.query("SET statement_timeout = '0';");

    for (const migrationFile of migrationFiles) {
      const migrationPath = path.join(cwd, "sql", "migrations", migrationFile);
      const sqlText = fs.readFileSync(migrationPath, "utf8");
      const statements = splitSqlStatements(sqlText);

      for (const statement of statements) {
        await client.query(statement);
      }

      console.log(`Applied ${migrationFile}`);
    }
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
