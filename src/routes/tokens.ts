import type { Request, Response, Express } from "express";
import { handleRouteError, sendSuccess } from "../http/responses";
import { type ApiServerDeps, tokenPriceService } from "../http/deps";
import {
  isValidAddress,
  isValidTokenSymbol,
  readPositiveInt,
} from "../http/params";
import { ApiError } from "../http/apiError";

export function registerTokenRoutes(app: Express, deps: ApiServerDeps): void {
  app.get(
    "/tokens",
    deps.cacheMiddlewareImpl("tokens-list", 10 * 60 * 1000),
    async (_request: Request, response: Response) => {
      try {
        const items = await deps.getAvailableTokensImpl();
        sendSuccess(_request, response, { items });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/tokens/:tokenSymbol/metadata",
    (request: Request, response: Response, next) => {
      const cacheKey = `token-metadata:${String(request.params.tokenSymbol).toUpperCase()}`;
      deps.cacheMiddlewareImpl(cacheKey, 10 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
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
        const metadata = await deps.getTokenMetadataImpl(tokenSymbol);

        if (!metadata) {
          throw new ApiError(
            404,
            "TOKEN_NOT_FOUND",
            "token metadata not found",
            {
              tokenSymbol,
            },
          );
        }

        sendSuccess(request, response, metadata);
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/tokens/:tokenSymbol/top-holders",
    (request: Request, response: Response, next) => {
      const cacheKey = `top-holders:${String(request.params.tokenSymbol).toUpperCase()}:${String(request.query.limit ?? "10")}`;
      deps.cacheMiddlewareImpl(cacheKey, 5 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      const limit = readPositiveInt(String(request.query.limit ?? ""), 10);

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
        const result = await deps.getTopHoldersImpl(
          tokenSymbol,
          Math.min(limit, 100),
        );
        sendSuccess(request, response, result);
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/tokens/:tokenSymbol/activity/:address",
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.params.tokenSymbol).trim();
        const address = String(request.params.address).trim();
        const days = Math.min(
          readPositiveInt(
            request.query.days ? String(request.query.days) : undefined,
            30,
          ),
          365,
        );

        if (!tokenSymbol || !address) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "tokenSymbol and address are required",
          );
        }

        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "tokenSymbol path parameter is invalid",
            {
              tokenSymbol,
            },
          );
        }

        if (!isValidAddress(address)) {
          throw new ApiError(
            400,
            "ADDRESS_INVALID",
            "address path parameter is invalid",
            {
              address,
            },
          );
        }

        const items = await deps.getAddressActivityImpl(
          tokenSymbol,
          address,
          days,
        );
        sendSuccess(request, response, { tokenSymbol, address, days, items });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/prices/:tokenSymbol",
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

        const getTokenPrice =
          deps.getTokenPriceImpl ?? tokenPriceService.getQuote;
        const quote = await getTokenPrice(tokenSymbol);
        sendSuccess(request, response, quote, {
          source: quote.source ?? "none",
          stale: quote.stale,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );
}
