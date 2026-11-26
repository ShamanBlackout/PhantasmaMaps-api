import type { Request, Response, Express } from "express";
import { handleRouteError, sendSuccess } from "../http/responses";
import type { ApiServerDeps } from "../http/deps";
import { readPositiveInt, readStringList } from "../http/params";

export function registerSystemRoutes(app: Express, deps: ApiServerDeps): void {
  app.get("/health", async (_request: Request, response: Response) => {
    try {
      await deps.testDatabaseConnectionImpl();
      sendSuccess(_request, response, {
        ok: true,
        status: "healthy",
        database: "up",
      });
    } catch (error: unknown) {
      console.error(
        JSON.stringify({
          level: "error",
          event: "api_health_database_down",
          timestamp: new Date().toISOString(),
          requestId: response.locals.requestId ?? null,
          message: error instanceof Error ? error.message : String(error),
        }),
      );

      sendSuccess(
        _request,
        response,
        {
          ok: false,
          status: "degraded",
          database: "down",
          error: {
            message: "Database connection check failed.",
          },
        },
        {
          degraded: true,
        },
        503,
      );
    }
  });

  app.get("/sync-status", async (_request: Request, response: Response) => {
    try {
      const [syncStatesResult, chainHeadResult] = await Promise.allSettled([
        deps.getSyncStatesImpl(),
        deps.rpcClient.getBlockHeight(),
      ]);

      if (syncStatesResult.status !== "fulfilled") {
        throw syncStatesResult.reason;
      }

      sendSuccess(_request, response, {
        items: syncStatesResult.value,
        chainHeadBlockHeight:
          chainHeadResult.status === "fulfilled"
            ? Number(chainHeadResult.value)
            : null,
      });
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });

  app.get("/sync-claims", async (request: Request, response: Response) => {
    try {
      const limit = readPositiveInt(String(request.query.limit ?? ""), 100);
      const result = await deps.getBlockSyncClaimsViewImpl({
        statuses: readStringList(
          request.query.status ? String(request.query.status) : undefined,
        ),
        fromBlock: request.query.fromBlock
          ? readPositiveInt(String(request.query.fromBlock), 0)
          : undefined,
        toBlock: request.query.toBlock
          ? readPositiveInt(String(request.query.toBlock), 0)
          : undefined,
        limit,
      });

      sendSuccess(request, response, result);
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });
}
