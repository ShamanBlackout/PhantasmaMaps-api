import type { Request, Response, Express } from "express";
import { apiConfig } from "../phantasma.config";
import type { ApiServerDeps } from "../http/deps";
import {
  clampInt,
  ensurePositiveEdgeLimit,
  isValidTokenSymbol,
  normalizeTokenGraphStage,
  normalizeWithTopHolders,
  readPositiveInt,
} from "../http/params";
import { handleRouteError, sendSuccess } from "../http/responses";
import { buildTokenStagedCacheKey } from "../http/cacheKeys";
import { ApiError } from "../http/apiError";

function addIntegerStrings(left: unknown, right: unknown): string {
  const leftValue = BigInt(String(left ?? "0"));
  const rightValue = BigInt(String(right ?? "0"));
  return (leftValue + rightValue).toString();
}

function readLiveTokenBalanceRaw(
  account: unknown,
  tokenSymbol: string,
): string {
  if (!account || typeof account !== "object") {
    return "0";
  }

  const accountRecord = account as {
    balances?: unknown;
    stake?: unknown;
    unclaimed?: unknown;
  };

  const rawBalances = accountRecord.balances;
  if (!Array.isArray(rawBalances)) {
    return tokenSymbol === "SOUL"
      ? addIntegerStrings("0", accountRecord.stake)
      : tokenSymbol === "KCAL"
        ? addIntegerStrings("0", accountRecord.unclaimed)
        : "0";
  }

  for (const item of rawBalances) {
    if (!item || typeof item !== "object") {
      continue;
    }

    const symbol = String((item as { symbol?: unknown }).symbol ?? "").trim();
    if (symbol !== tokenSymbol) {
      continue;
    }

    const amount = (item as { amount?: unknown }).amount;
    if (symbol === "SOUL") {
      return addIntegerStrings(amount, accountRecord.stake);
    }

    if (symbol === "KCAL") {
      return addIntegerStrings(amount, accountRecord.unclaimed);
    }

    return String(amount ?? "0");
  }

  return "0";
}

function normalizeRawAmount(rawAmount: string, decimals: number): string {
  const safeRaw = String(rawAmount ?? "0").trim();

  if (!/^-?\d+$/.test(safeRaw)) {
    return "0";
  }

  const negative = safeRaw.startsWith("-");
  const digits = negative ? safeRaw.slice(1) : safeRaw;
  const safeDigits = digits.replace(/^0+(?=\d)/, "") || "0";
  const safeDecimals = Math.max(0, Math.floor(decimals));

  if (safeDecimals === 0) {
    return `${negative ? "-" : ""}${safeDigits}`;
  }

  const padded = safeDigits.padStart(safeDecimals + 1, "0");
  const splitAt = padded.length - safeDecimals;
  const integerPart = padded.slice(0, splitAt);
  const fractionalPart = padded.slice(splitAt).replace(/0+$/, "");

  if (!fractionalPart) {
    return `${negative ? "-" : ""}${integerPart}`;
  }

  return `${negative ? "-" : ""}${integerPart}.${fractionalPart}`;
}

function readPositiveNumberFromUnknown(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

async function reconcileTokenGraphWithLiveBalances(
  deps: ApiServerDeps,
  tokenSymbol: string,
  graph: unknown,
  metadata: unknown,
): Promise<unknown> {
  const graphRecord =
    graph && typeof graph === "object"
      ? (graph as {
          nodes?: Array<Record<string, unknown>>;
          edges?: Array<Record<string, unknown>>;
        })
      : null;

  const nodes = Array.isArray(graphRecord?.nodes) ? graphRecord.nodes : null;
  if (!nodes || nodes.length === 0) {
    return graph;
  }

  const metadataRecord =
    metadata && typeof metadata === "object"
      ? (metadata as { currentSupplyNormalized?: unknown; decimals?: unknown })
      : null;
  const currentSupply = readPositiveNumberFromUnknown(
    metadataRecord?.currentSupplyNormalized,
  );

  if (currentSupply === null || currentSupply <= 0) {
    return graph;
  }

  const nodeTotal = nodes.reduce((sum, node) => {
    const balance =
      readPositiveNumberFromUnknown(node.balanceNormalized) ??
      readPositiveNumberFromUnknown(node.balance) ??
      0;
    return sum + balance;
  }, 0);

  if (nodeTotal <= currentSupply + 1e-9) {
    return graph;
  }

  const decimals = Math.max(
    0,
    Math.floor(Number(metadataRecord?.decimals ?? 0) || 0),
  );

  const refreshedNodes = await Promise.all(
    nodes.map(async (node) => {
      const address = String(node.address ?? "").trim();
      if (!address) {
        return node;
      }

      try {
        const account = await deps.rpcClient.getAccount(address);
        const liveBalanceRaw = readLiveTokenBalanceRaw(account, tokenSymbol);
        return {
          ...node,
          balance: liveBalanceRaw,
          balanceNormalized: normalizeRawAmount(liveBalanceRaw, decimals),
        };
      } catch {
        return node;
      }
    }),
  );

  return {
    ...graphRecord,
    nodes: refreshedNodes,
  };
}

async function sendTokenGraphResponse(
  request: Request,
  response: Response,
  deps: ApiServerDeps,
  tokenSymbol: string,
  includeTopHolders: number,
  edgeLimit?: number,
  mode: "standard" | "max" = "standard",
): Promise<void> {
  const effectiveEdgeLimit =
    mode === "standard" ? ensurePositiveEdgeLimit(edgeLimit) : undefined;

  let graph: unknown;
  let fallbackApplied = false;
  let appliedTopHoldersLimit = includeTopHolders;
  let appliedEdgeLimit = effectiveEdgeLimit;

  try {
    graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
      includeTopHoldersLimit: includeTopHolders,
      edgeLimit: effectiveEdgeLimit,
    });
  } catch (primaryError) {
    if (mode !== "standard") {
      throw primaryError;
    }

    const fallbackEdgeLimit = Math.max(
      200,
      Math.floor(
        Number(effectiveEdgeLimit ?? apiConfig.tokenGraphMaxEdges) / 2,
      ),
    );
    graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
      includeTopHoldersLimit: 0,
      edgeLimit: fallbackEdgeLimit,
    });
    fallbackApplied = true;
    appliedTopHoldersLimit = 0;
    appliedEdgeLimit = fallbackEdgeLimit;
  }

  const graphNodes = Array.isArray((graph as { nodes?: unknown[] })?.nodes)
    ? ((graph as { nodes?: unknown[] }).nodes ?? [])
    : [];
  const graphEdges = Array.isArray((graph as { edges?: unknown[] })?.edges)
    ? ((graph as { edges?: unknown[] }).edges ?? [])
    : [];

  if (mode === "standard" && graphNodes.length > 0) {
    const metadata = await deps.getTokenMetadataImpl(tokenSymbol);
    graph = await reconcileTokenGraphWithLiveBalances(
      deps,
      tokenSymbol,
      graph,
      metadata,
    );
  }

  const responseNodes = Array.isArray((graph as { nodes?: unknown[] })?.nodes)
    ? ((graph as { nodes?: unknown[] }).nodes ?? [])
    : [];
  const responseEdges = Array.isArray((graph as { edges?: unknown[] })?.edges)
    ? ((graph as { edges?: unknown[] }).edges ?? [])
    : [];

  sendSuccess(request, response, graph, {
    isPartial: fallbackApplied,
    mode,
    appliedLimits: {
      topHoldersLimit: appliedTopHoldersLimit,
      edgeLimit:
        mode === "standard"
          ? appliedEdgeLimit
          : Number.isFinite(Number(edgeLimit))
            ? edgeLimit
            : null,
    },
    degradedFrom:
      fallbackApplied && mode === "standard"
        ? {
            topHoldersLimit: includeTopHolders,
            edgeLimit: effectiveEdgeLimit,
          }
        : null,
    totalNodeCount: responseNodes.length,
    totalEdgeCount: responseEdges.length,
  });
}

export function registerTokenGraphRoutes(
  app: Express,
  deps: ApiServerDeps,
): void {
  app.get(
    "/graph/token/:tokenSymbol/staged",
    (request: Request, response: Response, next) => {
      const cacheKey = buildTokenStagedCacheKey(request);
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
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

        const stage = normalizeTokenGraphStage(request);
        const baseEdgeLimit = clampInt(
          readPositiveInt(
            String(request.query.edgeLimit ?? ""),
            apiConfig.tokenGraphStageBaseEdgeLimit,
          ),
          1,
          apiConfig.tokenGraphMaxEdges,
        );

        if (stage === "base") {
          const graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
            includeTopHoldersLimit: 0,
            edgeLimit: baseEdgeLimit,
          });

          sendSuccess(request, response, {
            tokenSymbol,
            stage,
            graph,
            nextStage: "holders",
          });
          return;
        }

        const topHoldersLimit = clampInt(
          readPositiveInt(String(request.query.topHoldersLimit ?? ""), 10),
          1,
          100,
        );

        if (stage === "holders") {
          const [graph, topHolders] = await Promise.all([
            deps.getFullTokenGraphImpl(tokenSymbol, {
              includeTopHoldersLimit: 0,
              edgeLimit: baseEdgeLimit,
            }),
            deps.getTopHoldersImpl(tokenSymbol, topHoldersLimit),
          ]);

          sendSuccess(request, response, {
            tokenSymbol,
            stage,
            graph,
            topHolders,
            nextStage: "full",
          });
          return;
        }

        const includeTopHolders = normalizeWithTopHolders(request);
        const graph = await deps.getFullTokenGraphImpl(tokenSymbol, {
          includeTopHoldersLimit: includeTopHolders,
          edgeLimit: apiConfig.tokenGraphMaxEdges,
        });

        sendSuccess(request, response, {
          tokenSymbol,
          stage,
          graph,
        });
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/token/:tokenSymbol/max",
    (request: Request, response: Response, next) => {
      const cacheKey = `token-graph-max:${String(request.params.tokenSymbol).toUpperCase()}`;
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
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

        await sendTokenGraphResponse(
          request,
          response,
          deps,
          tokenSymbol,
          0,
          undefined,
          "max",
        );
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );

  app.get(
    "/graph/token/:tokenSymbol",
    (request: Request, response: Response, next) => {
      let includeTopHolders = 0;
      try {
        includeTopHolders = normalizeWithTopHolders(request);
      } catch (error: unknown) {
        handleRouteError(response, error);
        return;
      }
      const cacheKey = `token-graph:${String(request.params.tokenSymbol).toUpperCase()}:${includeTopHolders}`;
      deps.cacheMiddlewareImpl(cacheKey, 1 * 60 * 1000)(
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

        const includeTopHolders = normalizeWithTopHolders(request);
        await sendTokenGraphResponse(
          request,
          response,
          deps,
          tokenSymbol,
          includeTopHolders,
          apiConfig.tokenGraphMaxEdges,
          "standard",
        );
      } catch (error: unknown) {
        handleRouteError(response, error);
      }
    },
  );
}
