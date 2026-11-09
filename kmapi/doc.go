// Package kmapi 是 Keymaster 后台业务 API v1 的 Go SDK。
//
// 真值是 openapi/openapi.yaml；DTO 由 tools/gen 派生（types.gen.go，
// 禁止手工修改）。本包手写客户端只引用生成类型，不反向依赖产品、
// GUI、数据库或私钥实现。解锁密码是敏感请求字段，只出现在请求体，
// 永不写入响应、事件、日志或调试序列化。
package kmapi
