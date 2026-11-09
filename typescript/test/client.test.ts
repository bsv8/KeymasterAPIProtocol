import { describe, expect, test, vi } from "vitest";
import { KmClient, checkIdentity } from "../src/client.js";
import { HTTPError, TransportError, isConflict, isLocked, isNotFound, isNotInitialized } from "../src/errors.js";
import { subscribeEvents } from "../src/events.js";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

describe("identity", () => {
  test("product/apiVersion 精确校验", () => {
    expect(() => checkIdentity({ initialized: true, state: "locked", product: "other", apiVersion: "v1" } as never)).toThrow();
    expect(() => checkIdentity({ initialized: true, state: "locked", product: "km", apiVersion: "v1" } as never)).not.toThrow();
  });
});

describe("errors", () => {
  test("状态码分类", () => {
    expect(isLocked(new HTTPError("GET /x", 423, "locked"))).toBe(true);
    expect(isNotInitialized(new HTTPError("GET /x", 412, "no"))).toBe(true);
    expect(isNotFound(new HTTPError("GET /x", 404, "no"))).toBe(true);
    expect(isConflict(new HTTPError("GET /x", 409, "rev"))).toBe(true);
    expect(new TransportError("GET /x", new Error("x"))).toBeInstanceOf(Error);
  });
});

describe("client", () => {
  test("status 映射与未知 api 404", async () => {
    const fetchFn = vi.fn(async (url: string) => {
      if (String(url).endsWith("/api/status")) return jsonResponse({ initialized: true, state: "locked", product: "km", apiVersion: "v1" });
      return new Response("not found", { status: 404 });
    });
    const c = new KmClient({ baseURL: "http://127.0.0.1:8790", fetchFn: fetchFn as typeof fetch });
    const st = await c.status();
    expect(st.product).toBe("km");
    await expect(c.getTask("a".repeat(64))).rejects.toMatchObject({ status: 404 });
  });
});

describe("events", () => {
  test("snapshot/change/resync 与心跳", async () => {
    const sse =
      `event: snapshot\ndata: {"protocolVersion":"v1","instanceId":"i1","topics":["task"]}\n\n` +
      `: heartbeat\n\n` +
      `event: change\ndata: {"protocolVersion":"v1","instanceId":"i1","seq":4,"topic":"task","entityId":"e1"}\n\n` +
      `event: resync\ndata: {}\n\n`;
    const stream = new ReadableStream({
      start(ctrl) {
        ctrl.enqueue(new TextEncoder().encode(sse));
        ctrl.close();
      },
    });
    const fetchFn = vi.fn(async () => new Response(stream, { headers: { "Content-Type": "text/event-stream" } }));
    let snaps = 0;
    let changes = 0;
    await subscribeEvents(
      "http://127.0.0.1:8790",
      [],
      {
        onSnapshot: () => snaps++,
        onChange: () => {
          changes++;
        },
        onResync: () => false,
      },
      { fetchFn: fetchFn as typeof fetch },
    );
    expect(snaps).toBe(1);
    expect(changes).toBe(1);
  });
});
