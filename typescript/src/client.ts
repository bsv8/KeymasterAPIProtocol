import { HTTPError, ParseError, TransportError, VersionError } from "./errors.js";
import type {
  AddRootRequest,
  AddRootResponse,
  BrowseView,
  CatalogRefreshRequest,
  CollectorConfigRequest,
  CollectorDescriptionRequest,
  CollectorFlushResponse,
  CollectorHistory,
  CollectorRetryResponse,
  CollectorStatus,
  CreateTaskRequest,
  FilesView,
  Info,
  Ok,
  PricingBatchRequest,
  PricingBatchResponse,
  PricingClearResponse,
  PricingDefaultsRequest,
  PricingDefaultsResponse,
  PricingFileRequest,
  PricingFileResponse,
  RevisionedOk,
  RevisionRequest,
  RootsView,
  SetDefaultRootRequest,
  Settings,
  SettingsUpdate,
  Status,
  Task,
  TaskList,
} from "./types.gen.js";

export const ProbeTimeoutMs = 3000;
export const ShortTimeoutMs = 10000;
export const LongTimeoutMs = 30000;

/** 可注入的 fetch 形态（Web/Node/内存 transport 统一经此进入）。 */
export type FetchFn = typeof fetch;

export interface ClientOptions {
  baseURL: string;
  fetchFn?: FetchFn;
}

function trimBase(url: string): string {
  return url.replace(/\/+$/, "");
}

async function readJson<T>(op: string, resp: Response): Promise<T> {
  const text = await resp.text();
  if (!resp.ok && resp.status !== 201) {
    throw new HTTPError(op, resp.status, text.trim());
  }
  if (!text) return undefined as T;
  try {
    return JSON.parse(text) as T;
  } catch (e) {
    throw new ParseError(op, e);
  }
}

function toAbort(base: AbortSignal | undefined, ms: number): { signal: AbortSignal; cancel: () => void } {
  const ctrl = new AbortController();
  const onBase = () => ctrl.abort(base?.reason);
  base?.addEventListener("abort", onBase, { once: true });
  const timer = setTimeout(() => ctrl.abort(new Error("timeout")), ms);
  return {
    signal: ctrl.signal,
    cancel: () => {
      clearTimeout(timer);
      base?.removeEventListener("abort", onBase);
    },
  };
}

/** Keymaster v1 强类型客户端（与 Go SDK 同契约）。 */
export class KmClient {
  readonly baseURL: string;
  private readonly fetchFn: FetchFn;

  constructor(opts: ClientOptions) {
    this.baseURL = trimBase(opts.baseURL);
    this.fetchFn = opts.fetchFn ?? fetch;
  }

  private async request<T>(method: string, path: string, body: unknown, timeoutMs: number, signal?: AbortSignal): Promise<T> {
    const op = `${method} ${path}`;
    const t = toAbort(signal, timeoutMs);
    try {
      let resp: Response;
      try {
        resp = await this.fetchFn(this.baseURL + path, {
          method,
          headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
          body: body === undefined ? undefined : JSON.stringify(body),
          signal: t.signal,
        });
      } catch (e) {
        if (signal?.aborted) throw e;
        throw new TransportError(op, e);
      }
      return await readJson<T>(op, resp);
    } finally {
      t.cancel();
    }
  }

  status(signal?: AbortSignal): Promise<Status> {
    return this.request<Status>("GET", "/api/status", undefined, ProbeTimeoutMs, signal);
  }

  info(signal?: AbortSignal): Promise<Info> {
    return this.request<Info>("GET", "/api/info", undefined, ShortTimeoutMs, signal);
  }

  unlock(password: string, signal?: AbortSignal): Promise<void> {
    return this.request<void>("POST", "/api/unlock", { password }, LongTimeoutMs, signal);
  }

  listRoots(signal?: AbortSignal): Promise<RootsView> {
    return this.request<RootsView>("GET", "/api/roots", undefined, ShortTimeoutMs, signal);
  }

  addRoot(req: AddRootRequest, signal?: AbortSignal): Promise<AddRootResponse> {
    return this.request<AddRootResponse>("POST", "/api/roots", req, ShortTimeoutMs, signal);
  }

  deleteRoot(id: string, revision: number, signal?: AbortSignal): Promise<RevisionedOk> {
    const path = `/api/roots?id=${encodeURIComponent(id)}&revision=${revision}`;
    return this.request<RevisionedOk>("DELETE", path, undefined, ShortTimeoutMs, signal);
  }

  setDefaultRoot(req: SetDefaultRootRequest, signal?: AbortSignal): Promise<RevisionedOk> {
    return this.request<RevisionedOk>("PUT", "/api/roots/default", req, ShortTimeoutMs, signal);
  }

  listFiles(signal?: AbortSignal): Promise<FilesView> {
    return this.request<FilesView>("GET", "/api/files", undefined, ShortTimeoutMs, signal);
  }

  browse(path: string, signal?: AbortSignal): Promise<BrowseView> {
    return this.request<BrowseView>("GET", `/api/browse?path=${encodeURIComponent(path)}`, undefined, ShortTimeoutMs, signal);
  }

  getSettings(signal?: AbortSignal): Promise<Settings> {
    return this.request<Settings>("GET", "/api/settings", undefined, ShortTimeoutMs, signal);
  }

  updateSettings(req: SettingsUpdate, signal?: AbortSignal): Promise<Settings> {
    return this.request<Settings>("PUT", "/api/settings", req, ShortTimeoutMs, signal);
  }

  resetSettings(revision: number, signal?: AbortSignal): Promise<Settings> {
    const body: RevisionRequest = { revision };
    return this.request<Settings>("POST", "/api/settings/reset", body, ShortTimeoutMs, signal);
  }

  /** 单根返回原生报告对象，多根返回 {refreshed}；v1 直通不冻结内部字段。 */
  refreshCatalog(req: CatalogRefreshRequest, signal?: AbortSignal): Promise<unknown> {
    return this.request<unknown>("POST", "/api/catalog/refresh", req, ShortTimeoutMs, signal);
  }

  collectorStatus(signal?: AbortSignal): Promise<CollectorStatus> {
    return this.request<CollectorStatus>("GET", "/api/collector/status", undefined, ShortTimeoutMs, signal);
  }

  collectorHistory(signal?: AbortSignal): Promise<CollectorHistory> {
    return this.request<CollectorHistory>("GET", "/api/collector/history", undefined, ShortTimeoutMs, signal);
  }

  collectorDescribe(req: CollectorDescriptionRequest, signal?: AbortSignal): Promise<Ok> {
    return this.request<Ok>("POST", "/api/collector/description", req, ShortTimeoutMs, signal);
  }

  collectorConfig(req: CollectorConfigRequest, signal?: AbortSignal): Promise<Ok> {
    return this.request<Ok>("POST", "/api/collector/config", req, ShortTimeoutMs, signal);
  }

  collectorFlush(signal?: AbortSignal): Promise<CollectorFlushResponse> {
    return this.request<CollectorFlushResponse>("POST", "/api/collector/flush", undefined, ShortTimeoutMs, signal);
  }

  collectorRetry(signal?: AbortSignal): Promise<CollectorRetryResponse> {
    return this.request<CollectorRetryResponse>("POST", "/api/collector/retry", undefined, ShortTimeoutMs, signal);
  }

  setPricingDefaults(req: PricingDefaultsRequest, signal?: AbortSignal): Promise<PricingDefaultsResponse> {
    return this.request<PricingDefaultsResponse>("PUT", "/api/pricing/defaults", req, ShortTimeoutMs, signal);
  }

  pricingBatch(req: PricingBatchRequest, signal?: AbortSignal): Promise<PricingBatchResponse> {
    return this.request<PricingBatchResponse>("POST", "/api/pricing/batch", req, ShortTimeoutMs, signal);
  }

  setPricingFile(req: PricingFileRequest, signal?: AbortSignal): Promise<PricingFileResponse> {
    return this.request<PricingFileResponse>("PUT", "/api/pricing/file", req, ShortTimeoutMs, signal);
  }

  clearPricingFile(rootId: string, path: string, signal?: AbortSignal): Promise<PricingClearResponse> {
    return this.request<PricingClearResponse>(
      "DELETE",
      `/api/pricing/file?rootId=${encodeURIComponent(rootId)}&path=${encodeURIComponent(path)}`,
      undefined,
      ShortTimeoutMs,
      signal,
    );
  }

  listTasks(signal?: AbortSignal): Promise<TaskList> {
    return this.request<TaskList>("GET", "/api/tasks", undefined, ShortTimeoutMs, signal);
  }

  createTask(seedHashHex: string, signal?: AbortSignal): Promise<Task> {
    const body: CreateTaskRequest = { seedHashHex };
    return this.request<Task>("POST", "/api/tasks", body, LongTimeoutMs, signal);
  }

  getTask(seed: string, signal?: AbortSignal): Promise<Task> {
    return this.request<Task>("GET", `/api/tasks/${seed}`, undefined, ShortTimeoutMs, signal);
  }

  cancelTask(seed: string, signal?: AbortSignal): Promise<Ok> {
    return this.request<Ok>("POST", `/api/tasks/${seed}/cancel`, undefined, ShortTimeoutMs, signal);
  }
}

/** 校验 product/apiVersion 精确匹配。 */
export function checkIdentity(st: Status): void {
  if (st.product !== "km") throw new VersionError("GET /api/status", "product", String(st.product), "km");
  if (st.apiVersion !== "v1") throw new VersionError("GET /api/status", "apiVersion", String(st.apiVersion), "v1");
}
