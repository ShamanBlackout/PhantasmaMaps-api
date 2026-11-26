import type { Request, Response, Express } from "express";
import { apiConfig } from "../phantasma.config";
import {
  buildAddressConnectionsCacheKey,
  buildAddressStagedCacheKey,
  buildAddressSubgraphCacheKey,
  buildTracePathsCacheKey,
} from "../http/cacheKeys";
import {
  clampInt,
  isValidAddress,
  isValidTokenSymbol,
  normalizeAddressGraphStage,
  readPositiveInt,
} from "../http/params";
import { ApiError } from "../http/apiError";
import { handleRouteError, sendSuccess } from "../http/responses";
import type { ApiServerDeps } from "../http/deps";

export function registerAddressGraphRoutes(
  app: Express,
  deps: ApiServerDeps,
): void {
  app.get(
    "/graph/address/:address/staged",
    (request: Request, response: Response, next) => {
      const cacheKey = buildAddressStagedCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const address = String(request.params.address).trim();
        const stage = normalizeAddressGraphStage(request);

        if (!tokenSymbol) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token query parameter is required",
            {
              query: "token",
            },
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
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

        const requestedDepth = readPositiveInt(
          String(request.query.depth ?? ""),
          apiConfig.graphDefaultDepth,
        );
        if (requestedDepth > apiConfig.graphHardMaxDepth) {
          throw new ApiError(
            400,
            "GRAPH_DEPTH_LIMIT_EXCEEDED",
            `depth must be <= ${apiConfig.graphHardMaxDepth}`,
            { received: requestedDepth, max: apiConfig.graphHardMaxDepth },
          );
        }

        const requestedEdgeLimit = readPositiveInt(
          String(request.query.edgeLimit ?? ""),
          apiConfig.graphMaxEdgesPerRequest,
        );
        if (requestedEdgeLimit > apiConfig.graphMaxEdgesPerRequest) {
          throw new ApiError(
            400,
            "GRAPH_EDGE_LIMIT_EXCEEDED",
            `edgeLimit must be <= ${apiConfig.graphMaxEdgesPerRequest}`,
            {
              received: requestedEdgeLimit,
              max: apiConfig.graphMaxEdgesPerRequest,
            },
          );
        }

        const coreEdgeLimit = Math.min(
          requestedEdgeLimit,
          apiConfig.graphStageCoreEdgeLimit,
        );
        const coreGraph = await deps.getAddressSubgraphImpl(
          tokenSymbol,
          address,
          1,
          coreEdgeLimit,
        );

        if (stage === "core") {
          sendSuccess(request, response, {
            tokenSymbol,
            address,
            stage,
            core: coreGraph,
            nextStage: "connections",
          });
          return;
        }

        const connectionsLimit = clampInt(
          readPositiveInt(String(request.query.connectionsLimit ?? ""), 25),
          1,
          200,
        );
        const connections = (
          await deps.getAddressConnectionsImpl(tokenSymbol, address)
        ).slice(0, connectionsLimit);

        if (stage === "connections") {
          sendSuccess(request, response, {
            tokenSymbol,
            address,
            stage,
            core: coreGraph,
            connections,
            nextStage: "full",
          });
          return;
        }

        const fullGraph = await deps.getAddressSubgraphImpl(
          tokenSymbol,
          address,
          requestedDepth,
          requestedEdgeLimit,
        );

        sendSuccess(request, response, {
          tokenSymbol,
          address,
          stage,
          core: coreGraph,
          connections,
          full: fullGraph,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/address/:address",
    (request: Request, response: Response, next) => {
      const cacheKey = buildAddressSubgraphCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const address = String(request.params.address).trim();
        if (!tokenSymbol) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token query parameter is required",
            {
              query: "token",
            },
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
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

        const depth = readPositiveInt(
          String(request.query.depth ?? ""),
          apiConfig.graphDefaultDepth,
        );
        if (depth > apiConfig.graphHardMaxDepth) {
          throw new ApiError(
            400,
            "GRAPH_DEPTH_LIMIT_EXCEEDED",
            `depth must be <= ${apiConfig.graphHardMaxDepth}`,
            { received: depth, max: apiConfig.graphHardMaxDepth },
          );
        }

        const edgeLimit = readPositiveInt(
          String(request.query.edgeLimit ?? ""),
          apiConfig.graphMaxEdgesPerRequest,
        );
        if (edgeLimit > apiConfig.graphMaxEdgesPerRequest) {
          throw new ApiError(
            400,
            "GRAPH_EDGE_LIMIT_EXCEEDED",
            `edgeLimit must be <= ${apiConfig.graphMaxEdgesPerRequest}`,
            { received: edgeLimit, max: apiConfig.graphMaxEdgesPerRequest },
          );
        }

        const graph = await deps.getAddressSubgraphImpl(
          tokenSymbol,
          address,
          depth,
          edgeLimit,
        );
        sendSuccess(request, response, graph, {
          isPartial: false,
          appliedLimits: {
            depth,
            edgeLimit,
          },
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/connections/address/:address",
    (request: Request, response: Response, next) => {
      const cacheKey = buildAddressConnectionsCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const address = String(request.params.address);
        if (!tokenSymbol) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token query parameter is required",
            {
              query: "token",
            },
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
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

        const connections = await deps.getAddressConnectionsImpl(
          tokenSymbol,
          address,
        );
        sendSuccess(request, response, {
          tokenSymbol,
          address,
          items: connections,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/trace/paths",
    (request: Request, response: Response, next) => {
      const cacheKey = buildTracePathsCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
        request,
        response,
        next,
      );
    },
    async (request: Request, response: Response) => {
      try {
        const tokenSymbol = String(request.query.token ?? "").trim();
        const fromAddress = String(request.query.from ?? "").trim();
        const toAddress = String(request.query.to ?? "").trim();

        if (!tokenSymbol || !fromAddress || !toAddress) {
          throw new ApiError(
            400,
            "INVALID_REQUEST",
            "token, from, and to query parameters are required",
          );
        }
        if (!isValidTokenSymbol(tokenSymbol)) {
          throw new ApiError(
            400,
            "TOKEN_SYMBOL_INVALID",
            "token query parameter is invalid",
            { tokenSymbol },
          );
        }
        if (!isValidAddress(fromAddress) || !isValidAddress(toAddress)) {
          throw new ApiError(
            400,
            "ADDRESS_INVALID",
            "from and to query parameters must be valid wallet addresses",
            {
              fromAddress,
              toAddress,
            },
          );
        }

        const maxHops = clampInt(
          readPositiveInt(
            request.query.maxHops ? String(request.query.maxHops) : undefined,
            5,
          ),
          1,
          8,
        );
        const pathLimit = clampInt(
          readPositiveInt(
            request.query.limit ? String(request.query.limit) : undefined,
            20,
          ),
          1,
          100,
        );
        const stopAtTerminalsRaw = String(
          request.query.stopAtTerminals ?? "true",
        )
          .trim()
          .toLowerCase();
        const stopAtTerminals = !["false", "0", "no", "off"].includes(
          stopAtTerminalsRaw,
        );

        const items = await deps.findAddressPathsImpl({
          tokenSymbol,
          fromAddress,
          toAddress,
          maxHops,
          pathLimit,
          stopAtTerminals,
        });

        sendSuccess(request, response, {
          tokenSymbol,
          fromAddress,
          toAddress,
          maxHops,
          limit: pathLimit,
          stopAtTerminals,
          totalPaths: items.length,
          items,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );
}
