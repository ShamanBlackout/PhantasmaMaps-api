const { spawn } = require("child_process");
const path = require("path");

const repoRoot = path.resolve(__dirname, "..");
const cliPath = path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs");
const entryPath = path.join(repoRoot, "src", "startApiServer.ts");

const child = spawn(process.execPath, [cliPath, entryPath], {
  cwd: repoRoot,
  stdio: "inherit",
  env: process.env,
  shell: false,
});

// Forward termination signals so the API can drain requests and close pools.
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill(signal);
    }
  });
}

child.on("error", (error) => {
  console.error("Failed to start API server:", error.message);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  process.exit(typeof code === "number" ? code : signal ? 1 : 0);
});
