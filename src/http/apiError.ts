export type ApiErrorCode =
  | "INVALID_REQUEST"
  | "TOKEN_SYMBOL_INVALID"
  | "TOKEN_NOT_FOUND"
  | "ADDRESS_INVALID"
  | "GRAPH_DEPTH_LIMIT_EXCEEDED"
  | "GRAPH_EDGE_LIMIT_EXCEEDED"
  | "PAGINATION_INVALID"
  | "RATE_LIMITED"
  | "INTERNAL_ERROR";

export class ApiError extends Error {
  readonly status: number;

  readonly code: ApiErrorCode;

  readonly details?: Record<string, unknown>;

  readonly retryAfterMs?: number;

  constructor(
    status: number,
    code: ApiErrorCode,
    message: string,
    details?: Record<string, unknown>,
    retryAfterMs?: number,
  ) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
    this.retryAfterMs = retryAfterMs;
  }
}
