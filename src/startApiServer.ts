import { startApiServer } from "./apiServer";

const runtime = startApiServer();

function handleShutdownSignal(signal: NodeJS.Signals): void {
  runtime.shutdown().then(
    () => process.exit(0),
    (error: unknown) => {
      console.error(
        JSON.stringify({
          level: "error",
          event: "api_shutdown_failed",
          timestamp: new Date().toISOString(),
          signal,
          message: error instanceof Error ? error.message : String(error),
        }),
      );
      process.exit(1);
    },
  );
}

process.on("SIGINT", handleShutdownSignal);
process.on("SIGTERM", handleShutdownSignal);
