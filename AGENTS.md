# KeymasterAPIProtocol 工作约定

- 真值是 `openapi/openapi.yaml` 与 `docs/事件协议.md`；DTO 由
  `tools/gen` 派生（`kmapi/types.gen.go`、`typescript/src/types.gen.ts`），
  生成文件禁止手工修改，改定义后重新生成。
- 本仓只放公开契约与双 SDK：不放钱包、下载、售卖、账本、持久化、
  消息总线实现、HTTP handler、Session、三级回退、GUI 或密钥逻辑；
  不反向依赖产品、GUI、数据库或私钥实现。
- 首轮保持 v1 wire 格式与行为；结构化错误等不兼容变化走明确的
  API 版本升级，不藏在整理中。
- 私钥永不进入协议；解锁密码只出现在请求体。
- 发布（Go tag、npm publish）与远端仓库操作需另行授权。
