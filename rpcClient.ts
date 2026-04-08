import { PhantasmaAPI } from "phantasma-sdk-ts";
import { rpcConfig } from "./phantasma.config";
import type { RequestCategory } from "./phantasma.types";

export interface ConnectionSummary {
  activeRpcUrl: string | null;
  configuredRpcUrls: string[];
  nexus: string;
  chain: string;
  blockRequestIntervalMs: number;
  metadataRequestIntervalMs: number;
  attemptsPerRpc: number;
  requestTimeoutMs: number;
}

function unwrapRpcResult<T>(value: unknown): T {
  if (
    value !== null &&
    typeof value === "object" &&
    "result" in value &&
    (value as { result?: unknown }).result !== undefined
  ) {
    return (value as { result: T }).result;
  }

  return value as T;
}

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;

  try {
    const timeoutPromise = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        reject(new Error(`Timed out after ${timeoutMs}ms`));
      }, timeoutMs);
    });

    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

class RequestScheduler {
  private readonly minIntervalMs: number;
  private readonly maxConcurrent: number;
  private lastStartAt = 0;
  private activeCount = 0;
  private startGate = Promise.resolve();
  private readonly waiting: Array<() => void> = [];

  constructor(minIntervalMs: number, maxConcurrent: number) {
    this.minIntervalMs = minIntervalMs;
    this.maxConcurrent = Math.max(1, Math.floor(maxConcurrent));
  }

  async schedule<T>(task: () => Promise<T>): Promise<T> {
    await new Promise<void>((resolve) => {
      const attemptStart = () => {
        if (this.activeCount < this.maxConcurrent) {
          this.activeCount += 1;
          resolve();
          return;
        }

        this.waiting.push(attemptStart);
      };

      attemptStart();
    });

    try {
      let releaseStartGate!: () => void;
      const nextStartGate = new Promise<void>((resolve) => {
        releaseStartGate = resolve;
      });
      const previousStartGate = this.startGate;
      this.startGate = nextStartGate;

      await previousStartGate;

      try {
        const waitMs = Math.max(
          0,
          this.lastStartAt + this.minIntervalMs - Date.now(),
        );

        if (waitMs > 0) {
          await sleep(waitMs);
        }

        this.lastStartAt = Date.now();
      } finally {
        releaseStartGate();
      }

      return await task();
    } finally {
      this.activeCount = Math.max(0, this.activeCount - 1);
      const next = this.waiting.shift();

      if (next) {
        next();
      }
    }
  }
}

export class PhantasmaRpcClient {
  private readonly apiCache = new Map<string, PhantasmaAPI>();
  private readonly blockScheduler = new RequestScheduler(
    rpcConfig.blockRequestIntervalMs,
    rpcConfig.blockMaxConcurrent,
  );
  private readonly metadataScheduler = new RequestScheduler(
    rpcConfig.metadataRequestIntervalMs,
    rpcConfig.metadataMaxConcurrent,
  );
  private activeRpcUrl: string | null = null;

  private getApi(url: string): PhantasmaAPI {
    const existing = this.apiCache.get(url);

    if (existing) {
      return existing;
    }

    const api = new PhantasmaAPI(url, null, rpcConfig.nexus);
    this.apiCache.set(url, api);
    return api;
  }

  private getPrioritizedUrls(): string[] {
    if (!this.activeRpcUrl) {
      return [...rpcConfig.urls];
    }

    return [
      this.activeRpcUrl,
      ...rpcConfig.urls.filter((url) => url !== this.activeRpcUrl),
    ];
  }

  private getScheduler(category: RequestCategory): RequestScheduler {
    return category === "block" ? this.blockScheduler : this.metadataScheduler;
  }

  private async execute<T>(
    category: RequestCategory,
    label: string,
    operation: (api: PhantasmaAPI) => Promise<T>,
  ): Promise<T> {
    const errors: string[] = [];

    for (const url of this.getPrioritizedUrls()) {
      const api = this.getApi(url);

      for (let attempt = 1; attempt <= rpcConfig.attemptsPerRpc; attempt++) {
        try {
          const result = await this.getScheduler(category).schedule(
            async () => {
              return withTimeout(operation(api), rpcConfig.requestTimeoutMs);
            },
          );

          this.activeRpcUrl = url;
          return unwrapRpcResult<T>(result);
        } catch (error: unknown) {
          const reason = error instanceof Error ? error.message : String(error);
          errors.push(
            `${label} @ ${url} (${attempt}/${rpcConfig.attemptsPerRpc}) -> ${reason}`,
          );
        }
      }
    }

    throw new Error(errors.join("\n"));
  }

  async getBlockHeight(): Promise<number> {
    return this.execute<number>("metadata", "getBlockHeight", (api) => {
      return api.getBlockHeight(rpcConfig.chain);
    });
  }

  async getBlockByHeight(height: number): Promise<unknown> {
    return this.execute<unknown>(
      "block",
      `getBlockByHeight(${height})`,
      (api) => {
        return api.getBlockByHeight(rpcConfig.chain, height);
      },
    );
  }

  async getNexus(): Promise<unknown> {
    return this.execute<unknown>("metadata", "getNexus", (api) => {
      return api.getNexus(true);
    });
  }

  async getChains(): Promise<unknown> {
    return this.execute<unknown>("metadata", "getChains", (api) => {
      return api.getChains(false);
    });
  }

  async getAccount(address: string): Promise<unknown> {
    return this.execute<unknown>(
      "metadata",
      `getAccount(${address})`,
      (api) => {
        return api.getAccount(address, true);
      },
    );
  }

  async getToken(symbol: string): Promise<unknown> {
    return this.execute<unknown>("metadata", `getToken(${symbol})`, (api) => {
      return api.getToken(symbol, true);
    });
  }

  getConnectionSummary(): ConnectionSummary {
    return {
      activeRpcUrl: this.activeRpcUrl,
      configuredRpcUrls: [...rpcConfig.urls],
      nexus: rpcConfig.nexus,
      chain: rpcConfig.chain,
      blockRequestIntervalMs: rpcConfig.blockRequestIntervalMs,
      metadataRequestIntervalMs: rpcConfig.metadataRequestIntervalMs,
      attemptsPerRpc: rpcConfig.attemptsPerRpc,
      requestTimeoutMs: rpcConfig.requestTimeoutMs,
    };
  }
}

export function createPhantasmaRpcClient(): PhantasmaRpcClient {
  return new PhantasmaRpcClient();
}

export { unwrapRpcResult };
