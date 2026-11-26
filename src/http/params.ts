import { apiConfig } from "../phantasma.config";
import type { Request } from "express";
import { ApiError } from "./apiError";

export function readPositiveInt(
  value: string | undefined,
  fallback: number,
): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function ensurePositiveEdgeLimit(value: number | undefined): number {
  const parsed = Number(value);
  if (Number.isFinite(parsed) && parsed > 0) {
    return Math.max(1, Math.floor(parsed));
  }

  return apiConfig.tokenGraphMaxEdges;
}

export function normalizeTokenGraphEdgeLimit(request: Request): number {
  const edgeLimit = ensurePositiveEdgeLimit(
    readPositiveInt(
      String(request.query.edgeLimit ?? ""),
      apiConfig.tokenGraphMaxEdges,
    ),
  );
  if (edgeLimit > apiConfig.tokenGraphMaxEdges) {
    throw new ApiError(
      400,
      "GRAPH_EDGE_LIMIT_EXCEEDED",
      `edgeLimit must be <= ${apiConfig.tokenGraphMaxEdges}`,
      { received: edgeLimit, max: apiConfig.tokenGraphMaxEdges },
    );
  }
  return edgeLimit;
}

export function readOptionalNumber(
  value: string | undefined,
): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export function readOptionalIsoDate(
  value: string | undefined,
): Date | undefined {
  if (!value) {
    return undefined;
  }

  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date : undefined;
}

function readOptionalBucketDate(value: string | undefined): Date | undefined {
  if (!value) {
    return undefined;
  }

  const trimmed = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return undefined;
  }

  const parsed = new Date(`${trimmed}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) ? parsed : undefined;
}

export function readStringList(value: string | undefined): string[] {
  if (!value) {
    return [];
  }

  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.floor(value)));
}

export function readPageNumber(value: unknown, pageSize: number): number {
  const page = Math.max(1, Math.floor(readPositiveInt(String(value ?? ""), 1)));
  const maxOffset = apiConfig.paginationMaxOffset;

  if ((page - 1) * pageSize > maxOffset) {
    throw new ApiError(
      400,
      "PAGINATION_INVALID",
      `page exceeds the maximum offset of ${maxOffset} rows`,
      { page, pageSize, maxOffset },
    );
  }

  return page;
}

export function isValidTokenSymbol(rawToken: string): boolean {
  const token = String(rawToken || "").trim();
  return /^[A-Z0-9_-]{1,16}$/i.test(token);
}

export function isValidAddress(rawAddress: string): boolean {
  const address = String(rawAddress || "").trim();
  return /^[PS][a-zA-Z0-9]{20,}$/.test(address);
}

type AddressGraphStage = "core" | "connections" | "full";

export function normalizeAddressGraphStage(
  request: Request,
): AddressGraphStage {
  const raw = String(request.query.stage ?? "core")
    .trim()
    .toLowerCase();

  if (raw === "core" || raw === "connections" || raw === "full") {
    return raw;
  }

  throw new ApiError(
    400,
    "INVALID_REQUEST",
    "stage must be core, connections, or full",
    {
      received: raw || null,
    },
  );
}

type TokenGraphStage = "base" | "holders" | "full";

export function normalizeTokenGraphStage(request: Request): TokenGraphStage {
  const raw = String(request.query.stage ?? "base")
    .trim()
    .toLowerCase();

  if (raw === "base" || raw === "holders" || raw === "full") {
    return raw;
  }

  throw new ApiError(
    400,
    "INVALID_REQUEST",
    "stage must be base, holders, or full",
    {
      received: raw || null,
    },
  );
}

export function normalizeWithTopHolders(request: Request): number {
  const includeTopHoldersRaw = String(
    request.query.includeTopHolders ?? "",
  ).trim();
  const withTopHoldersRaw = String(request.query.withTopHolders ?? "")
    .trim()
    .toLowerCase();
  const topHoldersLimitRaw = String(request.query.topHoldersLimit ?? "").trim();

  if (includeTopHoldersRaw) {
    throw new ApiError(
      400,
      "INVALID_REQUEST",
      "includeTopHolders is deprecated; use withTopHolders=true",
      {
        deprecatedQuery: "includeTopHolders",
        replacementQuery: "withTopHolders",
      },
    );
  }

  if (topHoldersLimitRaw) {
    const parsedLimit = Number(topHoldersLimitRaw);
    if (!Number.isFinite(parsedLimit) || parsedLimit < 0) {
      throw new ApiError(
        400,
        "INVALID_REQUEST",
        "topHoldersLimit must be a non-negative number",
        {
          topHoldersLimit: topHoldersLimitRaw,
        },
      );
    }

    return Math.floor(parsedLimit);
  }

  if (withTopHoldersRaw === "true" || withTopHoldersRaw === "1") {
    return 10;
  }

  return 0;
}
