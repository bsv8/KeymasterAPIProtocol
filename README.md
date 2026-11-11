# KeymasterAPIProtocol

Keymaster 后台业务 API v1 的独立协议项目：现行消息规范与 Go／TypeScript 双 SDK。不绑定 CLI、数据库或私钥实现；不含后台业务、消息总线实现、Session 与三级回退。

- Go module：`github.com/bsv8/KeymasterAPIProtocol`，包 `kmapi`
  （`go test ./...`）
- npm 包：`keymaster-api-protocol`（`typescript/`，
  `npm run typecheck && npm run build && npm test`）
- 真值：`spec/protocol.yaml`；DTO、方法映射与协议常量由 `tools/gen` 派生
  （`kmapi/types.gen.go`、`kmapi/methods.gen.go`、`typescript/src/types.gen.ts`、`typescript/src/methods.gen.ts`，禁止手工修改）
- 规范正文见 `docs/协议.md`
  （快照+序号+实例 ID，重连即重取快照，不承诺历史重放）。
- 跨语言固定向量在工程仓 `parity/KeymasterAPIProtocol/`，
  命令为 `parity:keymaster-api-protocol`

## 布局

```text
spec/protocol.yaml      契约真值（v1 消息外壳/方法/参数/结果/错误/事件/限制）
docs/协议.md            订阅恢复与传输规范正文
tools/gen/              生成器（spec 子集 → Go + TS 类型/方法/常量，可重复运行）
kmapi/                  Go SDK（生成类型/方法 + 手写传输/编解码/校验/客户端）
typescript/src/         TS SDK（生成类型/方法 + 手写传输/编解码/校验/客户端）
```

## 生成

```sh
cd tools/gen && go run . ../../spec/protocol.yaml ../../kmapi/types.gen.go ../../typescript/src/types.gen.ts
```

## Go

```sh
go test ./...
```

`kmapi.NewWS(url)`：外部后台走 WebSocket（`/api/ws`），内嵌后台由产品注入进程内 transport，共用同一分派器语义。订阅只恢复原地址，不执行三级回退。有副作用的调用发送后失去响应结果未知，SDK 不自动重发。

## TypeScript

```sh
cd typescript && npm install
npm run typecheck && npm run build && npm test
```

TS 经 WebSocket 工作，不依赖 React；单读取循环、串行写入、并发请求关联、超时/取消/关闭清理。

## Go 二进制调用

WS、进程内与 `UnifiedClient` 均提供 `CallWithBinary(ctx, method, params, attachments)`；返回 `CallResult{Result, Attachments}`。WS 连接读取上限按 `BinarySingleMaxBytes + 64 KiB` 设置，控制消息仍逐条受 `JSONControlMaxBytes` 限制。连接关闭信号由连接级共享 channel 一次性关闭，请求等待不创建轮询 goroutine。
