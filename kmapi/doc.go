// Package kmapi 是 Keymaster 后台业务 API v1 的 Go SDK。
//
// 真值是 spec/protocol.yaml；DTO、方法映射与协议常量由 tools/gen 派生
// （types.gen.go、methods.gen.go，禁止手工修改）。本包手写传输、编解码、
// 校验与客户端，只引用生成类型，不含后台业务、授权、数据库或私钥实现。
// 解锁密码是敏感请求字段，只出现在请求体，永不写入响应、事件、日志或调试序列化。
package kmapi
