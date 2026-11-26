import type { Request, Response } from "express";
import { randomUUID, createHash } from "crypto";
import { ApiError, type ApiErrorCode } from "./apiError";

type ResponseMeta = Record<string, unknown>;

export function getRequestId(request: Request): string {
  return String(request.headers["x-request-id"] || randomUUID());
}

function createEtagFromData(data: unknown): string {
  const digest = createHash("sha1").update(JSON.stringify(data)).digest("hex");
  return `W/"${digest}"`;
}

export function sendSuccess(
  request: Request,
  response: Response,
  data: unknown,
  meta: ResponseMeta = {},
  status = 200,
): void {
  const requestId = response.locals.requestId as string;
  const body = {
    data,
    meta: {
      generatedAt: new Date().toISOString(),
      source: "live",
      ...meta,
    },
    requestId,
  };

  const etag = createEtagFromData(body.data);
  response.setHeader("Cache-Control", "public, max-age=0, must-revalidate");
  response.setHeader("ETag", etag);

  if (request.headers["if-none-match"] === etag) {
    response.status(304).end();
    return;
  }

  response.status(status).json(body);
}

function sendError(
  response: Response,
  status: number,
  code: ApiErrorCode,
  message: string,
  details?: Record<string, unknown>,
  retryAfterMs?: number,
): void {
  const requestId = response.locals.requestId as string;
  if (retryAfterMs && retryAfterMs > 0) {
    response.setHeader("Retry-After", String(Math.ceil(retryAfterMs / 1000)));
  }

  response.status(status).json({
    requestId,
    retryAfterMs: retryAfterMs ?? null,
    error: {
      code,
      message,
      details: details ?? null,
    },
  });
}

export function handleRouteError(response: Response, error: unknown): void {
  if (error instanceof ApiError) {
    sendError(
      response,
      error.status,
      error.code,
      error.message,
      error.details,
      error.retryAfterMs,
    );
    return;
  }

  console.error(
    JSON.stringify({
      level: "error",
      event: "api_unhandled_route_error",
      timestamp: new Date().toISOString(),
      requestId: response.locals.requestId ?? null,
      message: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    }),
  );

  // Never echo internal error text (SQL, upstream URLs) back to clients.
  sendError(
    response,
    500,
    "INTERNAL_ERROR",
    "Internal server error. Use requestId when reporting this issue.",
  );
}
