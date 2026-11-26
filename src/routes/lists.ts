import type { Request, Response, Express } from "express";
import { apiConfig } from "../phantasma.config";
import {
  clampInt,
  isValidAddress,
  isValidTokenSymbol,
  readOptionalIsoDate,
  readOptionalNumber,
  readPageNumber,
  readPositiveInt,
} from "../http/params";
import { ApiError } from "../http/apiError";
import { handleRouteError, sendSuccess } from "../http/responses";
import type { ApiServerDeps } from "../http/deps";

export function registerListRoutes(app: Express, deps: ApiServerDeps): void {
  app.get("/transactions", async (request: Request, response: Response) => {
    try {
      const tokenSymbol = request.query.token
        ? String(request.query.token).trim()
        : undefined;
      const address = request.query.address
        ? String(request.query.address).trim()
        : undefined;

      if (tokenSymbol && !isValidTokenSymbol(tokenSymbol)) {
        throw new ApiError(
          400,
          "TOKEN_SYMBOL_INVALID",
          "token query parameter is invalid",
          {
            tokenSymbol,
          },
        );
      }
      if (address && !isValidAddress(address)) {
        throw new ApiError(
          400,
          "ADDRESS_INVALID",
          "address query parameter is invalid",
          {
            address,
          },
        );
      }

      const pageSize = readPositiveInt(
        String(request.query.pageSize ?? ""),
        apiConfig.transactionPageSizeDefault,
      );
      const clampedPageSize = clampInt(
        pageSize,
        1,
        apiConfig.transactionPageSizeMax,
      );
      const page = readPageNumber(request.query.page, clampedPageSize);
      const directionRaw = String(request.query.dir ?? "")
        .trim()
        .toLowerCase();
      const direction =
        directionRaw === "from" || directionRaw === "to"
          ? directionRaw
          : undefined;
      const counterparty = request.query.counterparty
        ? String(request.query.counterparty).trim()
        : undefined;
      const startTime = readOptionalIsoDate(
        request.query.startTime ? String(request.query.startTime) : undefined,
      );
      const endTime = readOptionalIsoDate(
        request.query.endTime ? String(request.query.endTime) : undefined,
      );
      const minAmount = readOptionalNumber(
        request.query.minAmount ? String(request.query.minAmount) : undefined,
      );
      const maxAmount = readOptionalNumber(
        request.query.maxAmount ? String(request.query.maxAmount) : undefined,
      );
      const minUsd = readOptionalNumber(
        request.query.minUsd ? String(request.query.minUsd) : undefined,
      );
      const maxUsd = readOptionalNumber(
        request.query.maxUsd ? String(request.query.maxUsd) : undefined,
      );
      const usdRateNow = readOptionalNumber(
        request.query.usdRateNow ? String(request.query.usdRateNow) : undefined,
      );
      const sortByRaw = String(request.query.sortBy ?? "")
        .trim()
        .toLowerCase();
      const sortBy =
        sortByRaw === "amount" || sortByRaw === "usd" || sortByRaw === "time"
          ? sortByRaw
          : undefined;
      const sortDirRaw = String(request.query.sortDir ?? "")
        .trim()
        .toLowerCase();
      const sortDir =
        sortDirRaw === "asc" || sortDirRaw === "desc" ? sortDirRaw : undefined;

      const result = await deps.getTransactionsPageImpl({
        tokenSymbol,
        address,
        fromBlock: request.query.fromBlock
          ? readPositiveInt(String(request.query.fromBlock), 0)
          : undefined,
        toBlock: request.query.toBlock
          ? readPositiveInt(String(request.query.toBlock), 0)
          : undefined,
        direction,
        counterparty,
        startTime,
        endTime,
        minAmount,
        maxAmount,
        minUsd,
        maxUsd,
        usdRateNow,
        sortBy,
        sortDir,
        page,
        pageSize: clampedPageSize,
      });

      sendSuccess(request, response, result, {
        pagination: {
          page,
          pageSize: clampedPageSize,
          total: Number(result?.total ?? 0),
        },
      });
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });

  app.get("/labels/nodes", async (request: Request, response: Response) => {
    try {
      const tokenSymbol = request.query.tokenSymbol
        ? String(request.query.tokenSymbol).trim()
        : undefined;
      const label = request.query.label
        ? String(request.query.label).trim()
        : undefined;
      const labelType = request.query.labelType
        ? String(request.query.labelType).trim()
        : undefined;
      const labelSource = request.query.labelSource
        ? String(request.query.labelSource).trim()
        : undefined;
      const minConfidence = readOptionalNumber(
        request.query.minConfidence
          ? String(request.query.minConfidence)
          : undefined,
      );
      const maxConfidence = readOptionalNumber(
        request.query.maxConfidence
          ? String(request.query.maxConfidence)
          : undefined,
      );
      const updatedSince = readOptionalIsoDate(
        request.query.updatedSince
          ? String(request.query.updatedSince)
          : undefined,
      );
      const windowDays = readPositiveInt(
        request.query.windowDays ? String(request.query.windowDays) : undefined,
        30,
      );
      const pageSize = clampInt(
        readPositiveInt(String(request.query.pageSize ?? ""), 50),
        1,
        apiConfig.transactionPageSizeMax,
      );
      const page = readPageNumber(request.query.page, pageSize);

      if (tokenSymbol && !isValidTokenSymbol(tokenSymbol)) {
        throw new ApiError(
          400,
          "TOKEN_SYMBOL_INVALID",
          "tokenSymbol query parameter is invalid",
          { tokenSymbol },
        );
      }

      if (
        minConfidence !== undefined &&
        (minConfidence < 0 || minConfidence > 1)
      ) {
        throw new ApiError(
          400,
          "INVALID_REQUEST",
          "minConfidence must be between 0 and 1",
          { minConfidence },
        );
      }

      if (
        maxConfidence !== undefined &&
        (maxConfidence < 0 || maxConfidence > 1)
      ) {
        throw new ApiError(
          400,
          "INVALID_REQUEST",
          "maxConfidence must be between 0 and 1",
          { maxConfidence },
        );
      }

      if (
        minConfidence !== undefined &&
        maxConfidence !== undefined &&
        minConfidence > maxConfidence
      ) {
        throw new ApiError(
          400,
          "INVALID_REQUEST",
          "minConfidence cannot exceed maxConfidence",
          { minConfidence, maxConfidence },
        );
      }

      const result = await deps.getLabeledNodesImpl({
        tokenSymbol,
        label,
        labelType,
        labelSource,
        minConfidence,
        maxConfidence,
        updatedSince,
        windowDays,
        page,
        pageSize,
      });

      sendSuccess(request, response, result, {
        pagination: {
          page,
          pageSize,
          total: Number((result as { total?: number })?.total ?? 0),
        },
      });
    } catch (error: unknown) {
      handleRouteError(response, error);
    }
  });
}
