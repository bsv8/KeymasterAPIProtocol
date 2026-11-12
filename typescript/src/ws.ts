import { Product, APIVersion, WSPath, MethodTimeout, KnownTopics } from "./methods.gen.js";
import { ProtocolError, TransportError, ParseError, VersionError } from "./errors.js";
import { defaultLimits, decodeAttachment, encodeAttachment, type Limits, type ClientInfo } from "./envelope.js";

/** 完整二进制帧允许的协议头开销；与服务端 SetReadLimit 的附加额度一致。 */
const BinaryFrameOverheadMaxBytes = 64 * 1024;

export interface SnapshotMsg {
  type: string;
  id?: string;
  instanceId: string;
  topics: string[];
  revisions?: Record<string, unknown>;
  state?: Record<string, unknown>;
}

export interface EventMsg {
  type: string;
  instanceId: string;
  seq: number;
  topic: string;
  entityId?: string;
  revision?: number;
  data?: Record<string, unknown>;
}

function toWSURL(base: string): string {
  const b = base.replace(/\/+$/, "");
  if (b.startsWith("ws://") || b.startsWith("wss://")) {
    return b.endsWith(WSPath) ? b : b + WSPath;
  }
  if (b.startsWith("http://")) return "ws://" + b.slice("http://".length) + WSPath;
  if (b.startsWith("https://")) return "wss://" + b.slice("https://".length) + WSPath;
  if (b.includes("://")) return b + WSPath;
  return "ws://" + b + WSPath;
}

function timeoutFor(cat: string | undefined): number | null {
  if (cat === "probe") return 3000;
  if (cat === "short") return 10000;
  if (cat === "long") return 30000;
  return null;
}

let reqSeq = 0;
function newRequestID(): string {
  reqSeq += 1;
  return `r${reqSeq}`;
}

/** 单 WS 连接：串行写入、并发请求关联、超时/取消/关闭清理。 */
export class KmWSClient {
  private ws: WebSocket | null = null;
  private pending = new Map<string, { resolve: (v: unknown) => void; reject: (e: unknown) => void; timer?: ReturnType<typeof setTimeout>; method: string; binaries?: Map<string, Uint8Array>; expected?: Map<string, number> }>();
  private inflight = 0;
  private closed = false;
  private instanceId = "";
  private connectionId = "";
  private limits: Limits = defaultLimits();
  // lastSeq 按主题记录；server 保证同一主题的 EventMsg.Seq 连续。
  private subs = new Map<string, { topics: string[]; onSnapshot?: (s: SnapshotMsg) => void; onEvent?: (e: EventMsg) => boolean | void; onResync?: () => void; lastSeq: Map<string, number> }>();
  private writeQueue: Promise<void> = Promise.resolve();
  private connStatus: 'connecting' | 'connected' | 'disconnected' = 'connecting';
  private statusListeners = new Set<(s: 'connecting' | 'connected' | 'disconnected') => void>();

  private constructor() {}

  /** 订阅连接状态变化；立即以当前状态回调一次，返回取消订阅函数。 */
  onConnectionChange(fn: (s: 'connecting' | 'connected' | 'disconnected') => void): () => void {
    this.statusListeners.add(fn);
    try { fn(this.connStatus); } catch { /* ignore */ }
    return () => { this.statusListeners.delete(fn); };
  }

  getStatus(): 'connecting' | 'connected' | 'disconnected' {
    return this.connStatus;
  }

  private setStatus(s: 'connecting' | 'connected' | 'disconnected'): void {
    if (this.connStatus === s) return;
    this.connStatus = s;
    for (const fn of this.statusListeners) {
      try { fn(s); } catch { /* ignore */ }
    }
  }

  static async connect(baseURL: string, client: ClientInfo): Promise<KmWSClient> {
    const c = new KmWSClient();
    const url = toWSURL(baseURL);
    const ws = new WebSocket(url);
    ws.binaryType = 'arraybuffer';
    c.ws = ws;
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const cleanup = () => {
        if (timer) clearTimeout(timer);
        ws.removeEventListener("open", onOpen);
        ws.removeEventListener("message", onMsg);
        ws.removeEventListener("error", onError);
        ws.removeEventListener("close", onClose);
      };
      const fail = (err: unknown) => {
        if (settled) return;
        settled = true;
        cleanup();
        try { ws.close(); } catch { /* ignore */ }
        reject(err);
      };
      const onOpen = () => {
        try {
          ws.send(JSON.stringify({ type: "hello", product: Product, apiVersion: APIVersion, client }));
        } catch (e) {
          fail(new TransportError("hello", e));
        }
      };
      const onMsg = (ev: MessageEvent) => {
        let msg: Record<string, any>;
        try {
          msg = JSON.parse(String(ev.data));
        } catch (e) {
          fail(new ParseError("welcome", e));
          return;
        }
        if (msg.type === "welcome") {
          if (msg.product !== Product || msg.apiVersion !== APIVersion) {
            fail(new VersionError("hello", "apiVersion", String(msg.apiVersion), APIVersion));
            return;
          }
          if (settled) return;
          settled = true;
          cleanup();
          c.instanceId = msg.instanceId;
          c.connectionId = msg.connectionId;
          if (msg.limits?.jsonControlMaxBytes) c.limits = msg.limits;
          c.setStatus('connected');
          resolve();
        } else if (msg.type === "response" && msg.error) {
          if (msg.error.code === "VERSION_MISMATCH") fail(new VersionError("hello", "apiVersion", "", APIVersion));
          else fail(new ProtocolError("hello", msg.error.code, msg.error.message));
        }
      };
      const onError = () => fail(new TransportError("dial " + url, "websocket error"));
      const onClose = () => fail(new TransportError("dial " + url, "连接在握手完成前关闭"));
      // 期限覆盖整个握手（open + welcome），不能在 open 时提前清除。
      const timer: ReturnType<typeof setTimeout> = setTimeout(
        () => fail(new TransportError("dial " + url, "等待 welcome 超时")),
        10000,
      );
      ws.addEventListener("open", onOpen);
      ws.addEventListener("message", onMsg);
      ws.addEventListener("error", onError);
      ws.addEventListener("close", onClose);
    });
    if (ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
      throw new TransportError("dial " + url, "连接在握手完成前关闭");
    }
    ws.addEventListener("message", (ev) => c.onMessage(ev));
    ws.addEventListener("close", () => c.failAll(new TransportError("read", "连接已关闭")));
    return c;
  }

  getInstanceId(): string {
    return this.instanceId;
  }
  getConnectionId(): string {
    return this.connectionId;
  }

  private sendJSON(v: unknown): void {
    const raw = JSON.stringify(v);
    if (raw.length > this.limits.jsonControlMaxBytes) {
      throw new ProtocolError("send", "TOO_LARGE", "控制消息过大");
    }
    const ws = this.ws;
    if (!ws || this.closed) throw new TransportError("send", "连接已关闭");
    const next = this.writeQueue.then(
      () =>
        new Promise<void>((resolve, reject) => {
          try {
            ws.send(raw);
            resolve();
          } catch (e) {
            reject(new TransportError("send", e));
          }
        }),
    );
    // 单次写失败不能毒化后续写队列；当前请求由自身超时收尾，后续请求仍可发送。
    this.writeQueue = next.catch(() => {});
  }

  private sendBinary(frame: Uint8Array, bodyBytes: number): void {
    // 附件正文与完整帧分别限制：帧还包含 4 字节头长和 JSON 头，不能把它
    // 计入正文上限，否则恰好 16 MiB 的合法附件会被误拒。
    if (bodyBytes > this.limits.binarySingleMaxBytes) {
      throw new ProtocolError("send", "TOO_LARGE", "附件过大");
    }
    if (frame.length > this.limits.binarySingleMaxBytes + BinaryFrameOverheadMaxBytes) {
      throw new ProtocolError("send", "TOO_LARGE", "附件帧过大");
    }
    const ws = this.ws;
    if (!ws || this.closed) throw new TransportError("send", "连接已关闭");
    const buf = frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength);
    const next = this.writeQueue.then(
      () =>
        new Promise<void>((resolve, reject) => {
          try {
            ws.send(buf);
            resolve();
          } catch (e) {
            reject(new TransportError("send", e));
          }
        }),
    );
    this.writeQueue = next.catch(() => {});
  }

  /**
   * 带二进制附件的调用：先发送 request，再逐个发送 AttachBegin + 二进制帧。
   * 服务端先回 AttachBegin/二进制，最后回 Response；收集齐后以
   * `{ result, binaries }` 形式返回。附件较大时超时放宽到 45 秒。
   */
  async callWithBinary(
    ctx: { signal?: AbortSignal } | undefined,
    method: string,
    params: unknown,
    attachments: Record<string, Uint8Array> = {},
  ): Promise<{ result: unknown; binaries: Record<string, Uint8Array> }> {
    const cat = (MethodTimeout as Record<string, string>)[method];
    if (cat === undefined) throw new ProtocolError(method, "BAD_REQUEST", "未知方法");
    if (this.closed) throw new TransportError(method, "连接已关闭");
    if (this.inflight >= this.limits.connInflightRequests) {
      throw new ProtocolError(method, "TOO_LARGE", "在途请求过多");
    }
    for (const [name, data] of Object.entries(attachments)) {
      if (data.length > this.limits.binarySingleMaxBytes) {
        throw new ProtocolError(method, "TOO_LARGE", `附件 ${name} 过大`);
      }
    }
    const id = newRequestID();
    // 附件上传需要声明期限（30s）+ 服务端执行；固定 45 秒上限。
    const timeoutMs = 45000;
    return new Promise<{ result: unknown; binaries: Record<string, Uint8Array> }>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const expected = new Map<string, number>();
      const binaries = new Map<string, Uint8Array>();
      const cleanup = () => {
        // onMessage 已删除 pending 并扣减 inflight；只有仍存在时才清理，
        // 避免带附件调用在响应路径被重复扣减。
        if (this.pending.delete(id)) this.inflight -= 1;
        if (timer) clearTimeout(timer);
        ctx?.signal?.removeEventListener("abort", onAbort);
      };
      const onAbort = () => {
        cleanup();
        try {
          this.sendJSON({ type: "cancel", id: newRequestID(), targetId: id });
        } catch {
          /* ignore */
        }
        reject(new TransportError(method, ctx?.signal?.reason ?? "调用者取消"));
      };
      if (ctx?.signal?.aborted) {
        onAbort();
        return;
      }
      ctx?.signal?.addEventListener("abort", onAbort, { once: true });
      timer = setTimeout(() => {
        cleanup();
        try {
          this.sendJSON({ type: "cancel", id: newRequestID(), targetId: id });
        } catch {
          /* ignore */
        }
        reject(new TransportError(method, "调用超时"));
      }, timeoutMs);
      const entry = {
        method,
        resolve: (v: unknown) => {
          cleanup();
          const out: Record<string, Uint8Array> = {};
          for (const [k, val] of binaries) out[k] = val;
          resolve({ result: v, binaries: out });
        },
        reject: (e: unknown) => {
          cleanup();
          reject(e);
        },
        timer,
        binaries,
        expected,
      };
      this.pending.set(id, entry);
      this.inflight += 1;
      try {
        const decls = Object.entries(attachments).map(([name, data]) => ({ id: name, bytes: data.length }));
        this.sendJSON({ type: "request", id, method, params, attachments: decls });
        for (const [name, data] of Object.entries(attachments)) {
          this.sendJSON({ type: "binary-begin", requestId: id, attachmentId: name, declaredBytes: data.length });
          this.sendBinary(encodeAttachment(id, name, data), data.length);
        }
      } catch (e) {
        cleanup();
        reject(e);
      }
    });
  }

  async call(ctx: { signal?: AbortSignal } | undefined, method: string, params?: unknown): Promise<unknown> {
    const cat = (MethodTimeout as Record<string, string>)[method];
    if (cat === undefined) throw new ProtocolError(method, "BAD_REQUEST", "未知方法");
    if (this.closed) throw new TransportError(method, "连接已关闭");
    if (this.inflight >= this.limits.connInflightRequests) {
      throw new ProtocolError(method, "TOO_LARGE", "在途请求过多");
    }
    const id = newRequestID();
    const timeoutMs = timeoutFor(cat);
    return new Promise<unknown>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const cleanup = () => {
        // onMessage 已删除 pending 并扣减 inflight；只有仍存在时才清理，
        // 避免带附件调用在响应路径被重复扣减。
        if (this.pending.delete(id)) this.inflight -= 1;
        if (timer) clearTimeout(timer);
        ctx?.signal?.removeEventListener("abort", onAbort);
      };
      const onAbort = () => {
        cleanup();
        try {
          this.sendJSON({ type: "cancel", id: newRequestID(), targetId: id });
        } catch {
          /* ignore */
        }
        reject(new TransportError(method, ctx?.signal?.reason ?? "调用者取消"));
      };
      if (ctx?.signal?.aborted) {
        onAbort();
        return;
      }
      ctx?.signal?.addEventListener("abort", onAbort, { once: true });
      if (timeoutMs !== null) {
        timer = setTimeout(() => {
          cleanup();
          try {
            this.sendJSON({ type: "cancel", id: newRequestID(), targetId: id });
          } catch {
            /* ignore */
          }
          reject(new TransportError(method, "调用超时"));
        }, timeoutMs);
      }
      this.pending.set(id, {
        method,
        resolve: (v) => {
          cleanup();
          resolve(v);
        },
        reject: (e) => {
          cleanup();
          reject(e);
        },
        timer,
      });
      this.inflight += 1;
      try {
        this.sendJSON({ type: "request", id, method, params });
      } catch (e) {
        cleanup();
        reject(e);
      }
    });
  }

  subscribeEvents(
    topics: string[],
    handlers: { onSnapshot?: (s: SnapshotMsg) => void; onEvent?: (e: EventMsg) => boolean | void; onResync?: () => void },
    opts: { signal?: AbortSignal } = {},
  ): () => void {
    for (const t of topics) {
      if (!(KnownTopics as readonly string[]).includes(t)) {
        throw new ProtocolError("events.subscribe", "BAD_REQUEST", `未知主题: ${t}`);
      }
    }
    const id = newRequestID();
    const sub = { topics, onSnapshot: handlers.onSnapshot, onEvent: handlers.onEvent, onResync: handlers.onResync, lastSeq: new Map<string, number>() };
    this.subs.set(id, sub);
    this.sendJSON({ type: "subscribe", id, topics });
    const cancel = () => {
      try {
        this.sendJSON({ type: "unsubscribe", id: newRequestID(), subscriptionId: id, topics });
      } catch {
        /* ignore */
      }
      this.subs.delete(id);
    };
    opts.signal?.addEventListener("abort", cancel, { once: true });
    return cancel;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    try {
      this.ws?.close(1000, "bye");
    } catch {
      /* ignore */
    }
    this.failAll(new TransportError("close", "连接已关闭"));
  }

  // ---- 类型化业务方法（Web/CLI 共用同一方法表） ----

  async status(): Promise<import("./types.gen.js").Status> {
    return (await this.call(undefined, "status.get")) as import("./types.gen.js").Status;
  }
  async info(): Promise<import("./types.gen.js").Info> {
    return (await this.call(undefined, "info.get")) as import("./types.gen.js").Info;
  }
  async unlock(password: string): Promise<import("./types.gen.js").Ok> {
    return (await this.call(undefined, "wallet.unlock", { password })) as import("./types.gen.js").Ok;
  }
  async listRoots(): Promise<import("./types.gen.js").RootsView> {
    return (await this.call(undefined, "roots.list")) as import("./types.gen.js").RootsView;
  }
  async addRoot(path: string, revision: number): Promise<import("./types.gen.js").AddRootResponse> {
    return (await this.call(undefined, "roots.add", { path, revision })) as import("./types.gen.js").AddRootResponse;
  }
  async removeRoot(id: string, revision: number): Promise<import("./types.gen.js").RevisionedOk> {
    return (await this.call(undefined, "roots.remove", { id, revision })) as import("./types.gen.js").RevisionedOk;
  }
  async setDefaultRoot(id: string, revision: number): Promise<import("./types.gen.js").RevisionedOk> {
    return (await this.call(undefined, "roots.setDefault", { id, revision })) as import("./types.gen.js").RevisionedOk;
  }
  async listFiles(): Promise<import("./types.gen.js").FilesView> {
    return (await this.call(undefined, "files.list")) as import("./types.gen.js").FilesView;
  }
  async browse(path: string): Promise<import("./types.gen.js").BrowseView> {
    return (await this.call(undefined, "files.browse", { path })) as import("./types.gen.js").BrowseView;
  }
  async getSettings(): Promise<import("./types.gen.js").Settings> {
    return (await this.call(undefined, "settings.get")) as import("./types.gen.js").Settings;
  }
  async updateSettings(params: unknown): Promise<import("./types.gen.js").Settings> {
    return (await this.call(undefined, "settings.update", params)) as import("./types.gen.js").Settings;
  }
  async resetSettings(revision: number): Promise<import("./types.gen.js").Settings> {
    return (await this.call(undefined, "settings.reset", { revision })) as import("./types.gen.js").Settings;
  }
  async refreshCatalog(rootId?: string, force?: boolean): Promise<unknown> {
    return this.call(undefined, "catalog.refresh", { rootId, force });
  }
  async collectorStatus(): Promise<import("./types.gen.js").CollectorStatus> {
    return (await this.call(undefined, "collector.status")) as import("./types.gen.js").CollectorStatus;
  }
  async collectorHistory(): Promise<import("./types.gen.js").CollectorHistory> {
    return (await this.call(undefined, "collector.history")) as import("./types.gen.js").CollectorHistory;
  }
  async collectorDescribe(seedHash: string, description: string): Promise<import("./types.gen.js").Ok> {
    return (await this.call(undefined, "collector.describe", { seedHash, description })) as import("./types.gen.js").Ok;
  }
  async collectorFlush(): Promise<import("./types.gen.js").CollectorFlushResponse> {
    return (await this.call(undefined, "collector.flush")) as import("./types.gen.js").CollectorFlushResponse;
  }
  async collectorRetry(): Promise<import("./types.gen.js").CollectorRetryResponse> {
    return (await this.call(undefined, "collector.retry")) as import("./types.gen.js").CollectorRetryResponse;
  }
  async listTasks(): Promise<import("./types.gen.js").TaskList> {
    return (await this.call(undefined, "tasks.list")) as import("./types.gen.js").TaskList;
  }
  async createTask(seedHashHex: string): Promise<import("./types.gen.js").Task> {
    return (await this.call(undefined, "tasks.create", { seedHashHex })) as import("./types.gen.js").Task;
  }
  async cancelTask(seedHashHex: string): Promise<import("./types.gen.js").Ok> {
    return (await this.call(undefined, "tasks.cancel", { seedHashHex })) as import("./types.gen.js").Ok;
  }
  async approvalsList(): Promise<unknown> {
    return this.call(undefined, "connect.approvals.list");
  }
  async approvalsDecide(approvalId: string, decision: string, grantAuto?: boolean): Promise<unknown> {
    return this.call(undefined, "connect.approvals.decide", grantAuto === undefined ? { approvalId, decision } : { approvalId, decision, grantAuto });
  }
  async bindingsList(): Promise<unknown> {
    return this.call(undefined, "connect.bindings.list");
  }
  async authList(): Promise<unknown> {
    return this.call(undefined, "connect.auth.list");
  }
  async authRevoke(origin: string, appPublicKey: string, appId: string): Promise<unknown> {
    return this.call(undefined, "connect.auth.revoke", { origin, appPublicKey, appId });
  }
  async permissionsList(applicationKey?: string): Promise<unknown> {
    return this.call(undefined, "connect.permissions.list", applicationKey ? { applicationKey } : {});
  }
  async permissionsRevoke(applicationKey: string, method: string): Promise<unknown> {
    return this.call(undefined, "connect.permissions.revoke", { applicationKey, method });
  }
  async historyList(applicationKey?: string, limit?: number): Promise<unknown> {
    const params: Record<string, unknown> = {};
    if (applicationKey) params["applicationKey"] = applicationKey;
    if (limit !== undefined) params["limit"] = limit;
    return this.call(undefined, "connect.history.list", params);
  }
  async appsList(): Promise<unknown> {
    return this.call(undefined, "connect.apps.list");
  }
  async connectionPause(authorizationId: string): Promise<unknown> {
    return this.call(undefined, "connect.connection.pause", { authorizationId });
  }
  async connectExec(params: unknown): Promise<unknown> {
    return this.call(undefined, "connect.request.exec", params);
  }
  async walletCreate(password: string, network: string): Promise<unknown> {
    return this.call(undefined, "wallet.create", { password, network });
  }
  async walletImport(password: string, keyMaterial: string): Promise<unknown> {
    return this.call(undefined, "wallet.import", { password, keyMaterial });
  }

  private onMessage(ev: MessageEvent): void {
    const data = ev.data as unknown;
    if (typeof data !== "string") {
      // 二进制帧：按 {4B 头长 + JSON 头 + 原始字节} 解码并路由到对应请求。
      const bytes = data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : data instanceof Uint8Array
          ? data
          : null;
      if (!bytes) return;
      let hdr: { requestId: string; attachmentId: string };
      let body: Uint8Array;
      try {
        const decoded = decodeAttachment(bytes, -1);
        hdr = { requestId: decoded.requestId, attachmentId: decoded.attachmentId };
        body = decoded.data;
      } catch {
        return;
      }
      const p = this.pending.get(hdr.requestId);
      if (!p || !p.binaries) return;
      const want = p.expected?.get(hdr.attachmentId);
      if (want !== undefined && body.length !== want) return;
      p.binaries.set(hdr.attachmentId, body);
      return;
    }
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }
    const t = msg["type"] as string;
    if (t === "binary-begin") {
      // 服务端声明即将发送的附件；记录期望长度用于校验。
      const b = msg as unknown as { requestId?: string; attachmentId?: string; declaredBytes?: number };
      if (typeof b.requestId !== "string" || typeof b.attachmentId !== "string" || typeof b.declaredBytes !== "number") return;
      const p = this.pending.get(b.requestId);
      if (!p) return;
      if (!p.expected) p.expected = new Map<string, number>();
      if (!p.binaries) p.binaries = new Map<string, Uint8Array>();
      if (!p.expected.has(b.attachmentId)) {
        p.expected.set(b.attachmentId, b.declaredBytes);
      }
      return;
    }
    if (t === "response") {
      const id = msg["id"] as string;
      const p = this.pending.get(id);
      if (!p) return;
      this.pending.delete(id);
      this.inflight -= 1;
      if (p.timer) clearTimeout(p.timer);
      const err = msg["error"] as { code: string; message: string; retryable?: boolean } | undefined;
      if (err) p.reject(new ProtocolError(p.method, err.code, err.message, id, err.retryable));
      else p.resolve(msg["result"] ?? null);
    } else if (t === "snapshot") {
      const snap = msg as unknown as SnapshotMsg;
      // 快照只投递给签发它的订阅；服务端未带 id 的旧帧才按主题集合回退匹配。
      const direct = snap.id ? this.subs.get(snap.id) : undefined;
      if (direct) {
        direct.onSnapshot?.(snap);
      } else if (!snap.id) {
        for (const s of this.subs.values()) {
          if (s.topics.length === snap.topics.length && s.topics.every((topic) => snap.topics.includes(topic))) {
            s.onSnapshot?.(snap);
          }
        }
      }
    } else if (t === "event") {
      const e = msg as unknown as EventMsg;
      if (e.instanceId && e.instanceId !== this.instanceId) return;
      for (const s of this.subs.values()) {
        // 空主题集合表示全部主题；server 的 EventMsg.Seq 按主题连续。
        if (s.topics.length > 0 && !s.topics.includes(e.topic)) continue;
        const last = s.lastSeq.get(e.topic) ?? 0;
        if (e.seq <= last) continue;
        if (last !== 0 && e.seq !== last + 1) s.onResync?.();
        // 缺口时仍推进并投递当前事件；状态重同步会补齐缺失内容，不能让
        // 客户端因一次缺口永久卡在同一序号。
        s.lastSeq.set(e.topic, e.seq);
        s.onEvent?.(e);
      }
    } else if (t === "resync") {
      const id = msg["id"] as string | undefined;
      if (id && this.subs.has(id)) {
        this.subs.get(id)!.onResync?.();
      } else {
        for (const s of this.subs.values()) s.onResync?.();
      }
    }
  }

  private failAll(err: unknown): void {
    for (const [, p] of this.pending) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(err);
    }
    this.pending.clear();
    this.inflight = 0;
    this.setStatus('disconnected');
  }
}
