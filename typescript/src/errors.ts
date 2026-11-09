export class TransportError extends Error {
  constructor(
    readonly op: string,
    readonly cause?: unknown,
  ) {
    super(`连接后台失败 (${op}): ${String(cause)}`);
    this.name = "TransportError";
  }
}

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

/** 是否为连接失败（未收到 HTTP 响应）。 */
export function isTransport(err: unknown): boolean {
  return err instanceof TransportError;
}

/** HTTP 业务状态码（若为 HTTPError）。 */
export function httpStatus(err: unknown): number | null {
  return err instanceof HTTPError ? err.status : null;
}

/** 423：需解锁，不是自动重试。 */
export function isLocked(err: unknown): boolean {
  return err instanceof HTTPError && err.status === 423;
}

/** 412：需 init。 */
export function isNotInitialized(err: unknown): boolean {
  return err instanceof HTTPError && err.status === 412;
}

/** 503：后台停止中。 */
export function isStopping(err: unknown): boolean {
  return err instanceof HTTPError && err.status === 503;
}

/** 404：不存在。 */
export function isNotFound(err: unknown): boolean {
  return err instanceof HTTPError && err.status === 404;
}

/** 409：revision 过期或业务冲突。 */
export function isConflict(err: unknown): boolean {
  return err instanceof HTTPError && err.status === 409;
}
