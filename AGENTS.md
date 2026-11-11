# KeymasterAPIProtocol 工作约定

- 真值是 `spec/protocol.yaml` 与 `docs/协议.md`；DTO、方法映射与协议常量由
  `tools/gen` 派生（`kmapi/types.gen.go`、`kmapi/methods.gen.go`、
  `typescript/src/types.gen.ts`、`typescript/src/methods.gen.ts`），
  生成文件禁止手工修改，改定义后重新生成。
- 本仓只放公开契约、生成定义、编解码、校验、传输及客户端能力：不放钱包、
  下载、售卖、账本、持久化、消息总线实现、HTTP handler、Session、三级回退、
  GUI 或密钥逻辑；不反向依赖产品、GUI、数据库或私钥实现。
- 仍为 v1，原地替换；不做 v2、不兼容旧协议、不双轨、不保留旧 HTTP/SSE
  客户端或兼容转换层。
- 私钥永不进入协议；解锁密码只出现在请求体。
- 发布（Go tag、npm publish）与远端仓库操作需另行授权。
