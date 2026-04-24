import { startApiServer } from "./apiServer";

const runtime = startApiServer();

process.on("SIGINT", () => {
  void runtime.shutdown();
});

process.on("SIGTERM", () => {
  void runtime.shutdown();
});
