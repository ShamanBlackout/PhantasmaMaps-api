import express, {
  type Request,
  type Response,
  type NextFunction,
} from "express";
import cors from "cors";
import compression from "compression";
import type { Server } from "http";
import { apiConfig } from "./phantasma.config";
import { type ApiServerDeps, defaultDeps } from "./http/deps";
import { getRequestId } from "./http/responses";
import { registerSystemRoutes } from "./routes/system";
import { registerTokenRoutes } from "./routes/tokens";
import { registerAddressGraphRoutes } from "./routes/addressGraph";
import { registerTokenGraphRoutes } from "./routes/tokenGraph";
import { registerListRoutes } from "./routes/lists";
import { registerAnalyticsRoutes } from "./routes/analytics";

export type { ApiServerDeps } from "./http/deps";

export function createApiApp(deps: ApiServerDeps = defaultDeps) {
  const app = express();
  app.disable("x-powered-by");

  const allowedOrigins = String(process.env.PHANTASMA_API_CORS_ORIGINS ?? "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);

  app.use(
    cors({
      origin: allowedOrigins.length > 0 ? allowedOrigins : true,
    }),
  );

  app.use(
    compression({
      // Only compress responses larger than 1KB; small payloads have negligible gain
      threshold: 1024,
      level: 6,
      memLevel: 8,
    }),
  );

  app.use((request: Request, response: Response, next: NextFunction) => {
    response.locals.requestId = getRequestId(request);
    response.setHeader("X-Request-Id", response.locals.requestId);
    next();
  });

  registerSystemRoutes(app, deps);
  registerTokenRoutes(app, deps);
  registerAddressGraphRoutes(app, deps);
  registerTokenGraphRoutes(app, deps);
  registerListRoutes(app, deps);
  registerAnalyticsRoutes(app, deps);

  return app;
}

// Keep below Docker's default 10s stop grace period.
const SHUTDOWN_DRAIN_TIMEOUT_MS = 8_000;

export function startApiServer(deps: ApiServerDeps = defaultDeps): {
  app: ReturnType<typeof createApiApp>;
  server: Server;
  shutdown: () => Promise<void>;
} {
  const app = createApiApp(deps);
  const server = app.listen(apiConfig.port, () => {
    console.log(`API server listening on port ${apiConfig.port}`);
  });
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 66_000;
  server.requestTimeout = 75_000;

  let shutdownPromise: Promise<void> | null = null;
  const shutdown = (): Promise<void> => {
    if (!shutdownPromise) {
      shutdownPromise = (async () => {
        // Drain in-flight requests before closing the pools they depend on.
        await new Promise<void>((resolve) => {
          const forceCloseTimer = setTimeout(
            () => server.closeAllConnections(),
            SHUTDOWN_DRAIN_TIMEOUT_MS,
          );
          forceCloseTimer.unref();
          server.close(() => {
            clearTimeout(forceCloseTimer);
            resolve();
          });
          server.closeIdleConnections();
        });
        await deps.closeDatabasePoolImpl();
      })();
    }

    return shutdownPromise;
  };

  return {
    app,
    server,
    shutdown,
  };
}
