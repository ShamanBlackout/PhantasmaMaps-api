import type { Request, Response, Express } from "express";
import { ApiError } from "../http/apiError";
import { handleRouteError, sendSuccess } from "../http/responses";
import type { ApiServerDeps } from "../http/deps";
import { isValidTokenSymbol, readPositiveInt } from "../http/params";

export function registerAnalyticsRoutes(
  app: Express,
  deps: ApiServerDeps,
): void {
  app.post(
    "/analytics/tokens/:tokenSymbol/refresh",
    async (_request: Request, response: Response) => {
      try {
        throw new ApiError(
          403,
          "INVALID_REQUEST",
          "Analytics refresh via POST is disabled",
        );
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/analytics/tokens/:tokenSymbol/timeseries",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const days = Math.min(
          readPositiveInt(
            request.query.days ? String(request.query.days) : undefined,
            90,
          ),
          3650,
        );

        const items = await deps.getTokenDailyMetricsImpl(tokenSymbol, days);
        sendSuccess(request, response, { tokenSymbol, days, items });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/precomputed/tokens/:tokenSymbol/overview",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const viewKey = `token-overview:${tokenSymbol}`;
        let precomputed = await deps.getPrecomputedApiViewImpl(viewKey);
        if (!precomputed) {
          await deps.refreshTokenPrecomputedViewsImpl(tokenSymbol);
          precomputed = await deps.getPrecomputedApiViewImpl(viewKey);
        }

        sendSuccess(request, response, {
          tokenSymbol,
          source: precomputed ? "precomputed" : "generated",
          overview: precomputed,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/analytics/tokens/:tokenSymbol/top-movers",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            { tokenSymbol },
          );
        }

        const windowDays = Math.min(
          readPositiveInt(
            request.query.windowDays
              ? String(request.query.windowDays)
              : undefined,
            7,
          ),
          365,
        );
        const limit = Math.min(
          readPositiveInt(
            request.query.limit ? String(request.query.limit) : undefined,
            20,
          ),
          200,
        );

        const items = await deps.getTokenTopMoversImpl(
          tokenSymbol,
          windowDays,
          limit,
        );
        sendSuccess(request, response, {
          tokenSymbol,
          windowDays,
          limit,
          items,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );
}
