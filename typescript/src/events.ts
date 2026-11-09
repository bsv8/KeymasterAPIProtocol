import { HTTPError, ParseError, TransportError } from "./errors.js";
import type { EventEnvelope, SnapshotEvent } from "./types.gen.js";
import type { FetchFn } from "./client.js";

export const KnownTopics = ["connection", "wallet", "balance", "task", "roots", "settings", "collector"] as const;
export type Topic = (typeof KnownTopics)[number];

export interface SubscribeHandlers {
  onSnapshot?: (snap: SnapshotEvent) => void;
  /** 返回 false 结束订阅。 */
  onChange?: (env: EventEnvelope) => boolean | void;
  /** 返回 false 结束订阅；默认收到 resync 即结束，由调用者重建并重取快照。 */
  onResync?: () => boolean | void;
}

/**
 * 通用事件订阅（GET /api/events，fetch 流式读取）。
 * topics 为空表示全部；未知主题由服务端 400。
 * 只恢复原地址，不执行三级回退。心跳为 SSE 注释，忽略。
 * 初版重连即重取快照，不做 Last-Event-ID 历史重放。
 */
export async function subscribeEvents(
  baseURL: string,
  topics: string[],
  handlers: SubscribeHandlers,
  opts: { fetchFn?: FetchFn; signal?: AbortSignal } = {},
): Promise<void> {
  const base = baseURL.replace(/\/+$/, "");
  const path = topics.length === 0 ? "/api/events" : `/api/events?topics=${encodeURIComponent(topics.join(","))}`;
  const op = `GET ${path}`;
  const fetchFn = opts.fetchFn ?? fetch;
  let resp: Response;
  try {
    resp = await fetchFn(base + path, {
      headers: { Accept: "text/event-stream" },
      signal: opts.signal,
    });
  } catch (e) {
    throw new TransportError(op, e);
  }
  if (!resp.ok) {
    const text = await resp.text();
    throw new HTTPError(op, resp.status, text.trim());
  }
  if (!resp.body) return;
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let eventType = "";
  let data = "";

  const flush = (): boolean => {
    const t = eventType.trim();
    const raw = data.trim();
    eventType = "";
    data = "";
    if (!raw) return true;
    const kind = t === "" ? "change" : t;
    if (kind === "snapshot") {
      let snap: SnapshotEvent;
      try {
        snap = JSON.parse(raw) as SnapshotEvent;
      } catch (e) {
        throw new ParseError(op, e);
      }
      handlers.onSnapshot?.(snap);
      return true;
    }
    if (kind === "change") {
      let env: EventEnvelope;
      try {
        env = JSON.parse(raw) as EventEnvelope;
      } catch (e) {
        throw new ParseError(op, e);
      }
      const r = handlers.onChange?.(env);
      return r === false ? false : true;
    }
    if (kind === "resync") {
      const r = handlers.onResync?.();
      return false && r === true ? true : false;
    }
    return true;
  };

  for (;;) {
    const { done, value } = await reader.read();
    if (value) buf += decoder.decode(value, { stream: !done });
    for (;;) {
      const idx = buf.indexOf("\n");
      if (idx < 0) break;
      const line = buf.slice(0, idx).replace(/\r$/, "");
      buf = buf.slice(idx + 1);
      if (line.startsWith(":")) continue;
      if (line.startsWith("event:")) {
        if (data) {
          if (!flush()) {
            await reader.cancel().catch(() => undefined);
            return;
          }
        }
        eventType = line.slice("event:".length).trim();
        continue;
      }
      if (line.startsWith("data:")) {
        data += (data ? "\n" : "") + line.slice("data:".length).trim();
        continue;
      }
      if (line === "") {
        if (!flush()) {
          await reader.cancel().catch(() => undefined);
          return;
        }
      }
    }
    if (done) return;
    if (opts.signal?.aborted) {
      await reader.cancel().catch(() => undefined);
      return;
    }
  }
}
