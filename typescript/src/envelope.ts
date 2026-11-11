import { Product, APIVersion } from "./methods.gen.js";

export const MsgHello = "hello";
export const MsgWelcome = "welcome";
export const MsgRequest = "request";
export const MsgResponse = "response";
export const MsgCancel = "cancel";
export const MsgSubscribe = "subscribe";
export const MsgUnsubscribe = "unsubscribe";
export const MsgSnapshot = "snapshot";
export const MsgEvent = "event";
export const MsgResync = "resync";
export const MsgAttachBegin = "binary-begin";

export interface ClientInfo {
  name: string;
  version: string;
}

export interface Limits {
  jsonControlMaxBytes: number;
  binarySingleMaxBytes: number;
  connUnackedMaxBytes: number;
  connPendingSendMaxBytes: number;
  connInflightRequests: number;
  attachmentDeadlineSecs: number;
  storagePlainMaxBytes?: number;
  msfileSeedMaxBytes?: number;
  msfileBlockMaxBytes?: number;
}

export interface Hello {
  type: string;
  product: string;
  apiVersion: string;
  client: ClientInfo;
}

export interface Welcome {
  type: string;
  product: string;
  apiVersion: string;
  instanceId: string;
  connectionId: string;
  limits: Limits;
}

export interface RequestMsg {
  type: string;
  id: string;
  method: string;
  params?: unknown;
  /** 随后的二进制附件声明；服务端据此预注册，未声明的附件帧将被忽略。 */
  attachments?: Array<{ id: string; bytes: number }>;
}

export interface ErrorDetail {
  code: string;
  message: string;
  retryable?: boolean;
}

export interface ResponseMsg {
  type: string;
  id: string;
  result?: unknown;
  error?: ErrorDetail;
}

export function defaultLimits(): Limits {
  return {
    jsonControlMaxBytes: 1 << 20,
    binarySingleMaxBytes: 16 << 20,
    connUnackedMaxBytes: 32 << 20,
    connPendingSendMaxBytes: 32 << 20,
    connInflightRequests: 16,
    attachmentDeadlineSecs: 30,
    storagePlainMaxBytes: 16 << 20,
    msfileSeedMaxBytes: 16 << 20,
    msfileBlockMaxBytes: 256 << 10,
  };
}

export function checkHello(product: string, apiVersion: string): void {
  if (product !== Product) throw new Error(`product mismatch: ${product}`);
  if (apiVersion !== APIVersion) throw new Error(`apiVersion mismatch: ${apiVersion}`);
}

export function checkIdentity(status: { product: string; apiVersion: string }): void {
  checkHello(status.product, status.apiVersion);
}

/** 单 WS binary 消息编码：4 字节大端 headerLen + JSON{requestId,attachmentId} + 原始字节。 */
export function encodeAttachment(requestId: string, attachmentId: string, data: Uint8Array): Uint8Array {
  const hdr = new TextEncoder().encode(JSON.stringify({ requestId, attachmentId }));
  const out = new Uint8Array(4 + hdr.length + data.length);
  new DataView(out.buffer).setUint32(0, hdr.length, false);
  out.set(hdr, 4);
  out.set(data, 4 + hdr.length);
  return out;
}

export function decodeAttachment(frame: Uint8Array, declared: number): { requestId: string; attachmentId: string; data: Uint8Array } {
  if (frame.length < 4) throw new Error("附件帧过短");
  const hl = new DataView(frame.buffer, frame.byteOffset, 4).getUint32(0, false);
  if (4 + hl > frame.length) throw new Error("附件头长度非法");
  const hdr = JSON.parse(new TextDecoder().decode(frame.slice(4, 4 + hl)));
  const data = frame.slice(4 + hl);
  if (declared >= 0 && data.length !== declared) throw new Error("声明长度与实际长度不一致");
  return { requestId: hdr.requestId, attachmentId: hdr.attachmentId, data };
}
