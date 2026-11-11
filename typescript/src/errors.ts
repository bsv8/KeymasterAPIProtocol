export class TransportError extends Error {
  constructor(
    readonly op: string,
    readonly cause?: unknown,
  ) {
    super(`连接后台失败 (${op}): ${String(cause)}`);
    this.name = "TransportError";
  }
}

/** 已退役：旧 HTTP/SSE 客户端兼容保留，最终删除。新代码使用 ProtocolError。 */
export class HTTPError extends Error {
  constructor(
    readonly op: string,
    readonly status: number,
    readonly body: string,
  ) {
    super(`后台返回 ${status}: ${body}`);
    this.name = "HTTPError";
  }
}

export class ParseError extends Error {
  constructor(
    readonly op: string,
    readonly cause?: unknown,
  ) {
    super(`解析后台响应失败 (${op}): ${String(cause)}`);
    this.name = "ParseError";
  }
}

export class VersionError extends Error {
  constructor(
    readonly op: string,
    readonly field: string,
    readonly got: string,
    readonly want: string,
  ) {
    super(`后台协议不兼容 (${op})：${field}=${JSON.stringify(got)}，需要 ${JSON.stringify(want)}`);
    this.name = "VersionError";
  }
}

export class ProtocolError extends Error {
  constructor(
    readonly op: string,
    readonly code: string,
    readonly message: string,
    readonly id?: string,
    readonly retryable?: boolean,
  ) {
    super(`后台业务错误 ${code} (${op}): ${message}`);
    this.name = "ProtocolError";
  }
}

/** 是否为连接失败。 */
export function isTransport(err: unknown): boolean {
  return err instanceof TransportError;
}

/** HTTP 业务状态码（若为 HTTPError，仅旧客户端兼容）。 */
export function httpStatus(err: unknown): number | null {
  return err instanceof HTTPError ? err.status : null;
}

/** 业务错误码（若为 ProtocolError）。 */
export function errorCode(err: unknown): string | null {
  return err instanceof ProtocolError ? err.code : null;
}

/** LOCKED：需解锁。兼容旧 423。 */
export function isLocked(err: unknown): boolean {
  if (err instanceof ProtocolError) return err.code === "LOCKED";
  return err instanceof HTTPError && err.status === 423;
}

/** NOT_INITIALIZED：需 init。兼容旧 412。 */
export function isNotInitialized(err: unknown): boolean {
  if (err instanceof ProtocolError) return err.code === "NOT_INITIALIZED";
  return err instanceof HTTPError && err.status === 412;
}

/** STOPPING：后台停止中。兼容旧 503。 */
export function isStopping(err: unknown): boolean {
  if (err instanceof ProtocolError) return err.code === "STOPPING";
  return err instanceof HTTPError && err.status === 503;
}

/** NOT_FOUND：不存在。兼容旧 404。 */
export function isNotFound(err: unknown): boolean {
  if (err instanceof ProtocolError) return err.code === "NOT_FOUND";
  return err instanceof HTTPError && err.status === 404;
}

/** CONFLICT/STATE_CONFLICT：版本冲突或审批竞争。兼容旧 409。 */
export function isConflict(err: unknown): boolean {
  if (err instanceof ProtocolError) return err.code === "CONFLICT" || err.code === "STATE_CONFLICT";
  return err instanceof HTTPError && err.status === 409;
}
