const { spawnSync } = require("child_process");
const path = require("path");

const repoRoot = path.resolve(__dirname, "..");
const cliPath = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
const entryPath = path.join(repoRoot, "src", "startApiServer.ts");

const result = spawnSync(process.execPath, [cliPath, entryPath], {
  cwd: repoRoot,
  stdio: "inherit",
  env: process.env,
  shell: false,
});

if (result.error) {
  console.error("Failed to start API server:", result.error.message);
  process.exit(1);
}

if (typeof result.status === "number") {
  process.exit(result.status);
}
