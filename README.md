# KeymasterAPIProtocol

Keymaster 后台业务 API v1 的独立协议项目：机器可读契约（OpenAPI 3.1）与 Go／TypeScript 双 SDK。不绑定 CLI、MyGo、React、数据库或私钥实现；不含后台业务、消息总线实现、Session 与三级回退。

- Go module：`github.com/bsv8/KeymasterAPIProtocol`，包 `kmapi`
  （`go test ./...`）
- npm 包：`keymaster-api-protocol`（`typescript/`，
  `npm run typecheck && npm run build && npm test`）
- 真值：`openapi/openapi.yaml`；DTO 由 `tools/gen` 派生
  （`kmapi/types.gen.go`、`typescript/src/types.gen.ts`，禁止手工修改）
- 事件恢复语义见 `docs/事件协议.md`（快照+序号+实例 ID，
  初版重连即重取快照，不承诺历史重放）
- 跨语言固定向量在工程仓 `parity/KeymasterAPIProtocol/`，
  命令为 `parity:keymaster-api-protocol`

## 布局

```text
openapi/openapi.yaml      契约真值（v1 路径/方法/请求/响应/状态码 + 新增 /api/events）
tools/gen/                生成器（OpenAPI 子集 → Go + TS 类型，可重复运行）
kmapi/                    Go SDK（生成类型 + 手写 client/errors/events）
typescript/src/           TS SDK（生成类型 + 手写 client/errors/events）
docs/事件协议.md           订阅恢复规范正文
```

## 生成

```sh
cd tools/gen && go run . ../../openapi/openapi.yaml ../../kmapi/types.gen.go ../../typescript/src/types.gen.ts
```

## Go

```sh
go test ./...
```

`kmapi.New(baseURL, httpClient)`：`httpClient` 可注入，外部后台用标准
HTTP，内嵌后台由产品注入内存 transport。订阅只恢复原地址，不执行
三级回退。有副作用的 POST 发送后失去响应结果未知，SDK 不自动重发。

## TypeScript

```sh
cd typescript && npm install
npm run typecheck && npm run build && npm test
```

TS 经可注入的 `fetch` 与 `AbortSignal` 工作，不依赖 React；
订阅使用 fetch 流式读取（无需 EventSource 另一实现）。
