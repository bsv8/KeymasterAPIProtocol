package kmapi

import (
	"errors"
	"fmt"
	"strings"
)

// TransportError 表示未收到业务响应：连接拒绝、DNS、超时、调用者取消或
// 连接关闭。它永不携带业务响应，调用者不得把它误认为业务错误。
type TransportError struct {
	Op  string
	Err error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("连接后台失败 (%s): %v", e.Op, e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// ProtocolError 表示已收到业务响应但为结构化错误（v1 消息 response.error）。
// 服务器已到达，不得据此重试另一后台；SDK 不自动重试，更不执行换后台。
type ProtocolError struct {
	Op        string
	ID        string
	Code      string
	Message   string
	Retryable bool
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("后台业务错误 %s (%s): %s", e.Code, e.Op, e.Message)
}

// HTTPError 已退役：旧 HTTP/SSE 客户端兼容保留，最终删除。
// 新代码使用 ProtocolError；不得新增 HTTPError 依赖。
type HTTPError struct {
	Op     string
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("后台返回 %d: %s", e.Status, e.Body)
}

// ParseError 表示响应体解析失败（例如把 HTML 当 JSON 解析）。
// 未知 /api/* 路径必须返回 404 而不是 SPA HTML；若收到 HTML，
// 此处明确报错，不静默丢弃。
type ParseError struct {
	Op  string
	Err error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("解析后台响应失败 (%s): %v", e.Op, e.Err)
}

func (e *ParseError) Unwrap() error { return e.Err }

// VersionError 表示协议不兼容：product 不是 "km" 或 apiVersion 不是 "v1"。
type VersionError struct {
	Op    string
	Got   string
	Want  string
	Field string
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("后台协议不兼容 (%s)：%s=%q，需要 %q", e.Op, e.Field, e.Got, e.Want)
}

// IsTransport 报告是否为连接失败（可考虑换级/重试连接，不能换语义）。
func IsTransport(err error) bool {
	var t *TransportError
	return errors.As(err, &t)
}

// IsCallerCancel 报告是否为调用者取消（永不回退、不建内嵌后台）。
func IsCallerCancel(err error) bool {
	return errors.Is(err, errCanceled{})
}

type errCanceled struct{}

func (errCanceled) Error() string { return "调用者取消" }

// HTTPStatus 返回 HTTP 业务状态码（若为 HTTPError，仅旧客户端兼容）。
func HTTPStatus(err error) (int, bool) {
	var h *HTTPError
	if errors.As(err, &h) {
		return h.Status, true
	}
	return 0, false
}

// ErrorCode 返回业务错误码（若为 ProtocolError）。
func ErrorCode(err error) (string, bool) {
	var p *ProtocolError
	if errors.As(err, &p) {
		return p.Code, true
	}
	return "", false
}

// IsLocked 报告 LOCKED（需解锁，不是自动重试）。兼容旧 423。
func IsLocked(err error) bool {
	if c, ok := ErrorCode(err); ok {
		return c == ErrLocked
	}
	s, ok := HTTPStatus(err)
	return ok && s == 423
}

// IsNotInitialized 报告 NOT_INITIALIZED（需 init）。兼容旧 412。
func IsNotInitialized(err error) bool {
	if c, ok := ErrorCode(err); ok {
		return c == ErrNotInitialized
	}
	s, ok := HTTPStatus(err)
	return ok && s == 412
}

// IsStopping 报告 STOPPING（后台停止中）。兼容旧 503。
func IsStopping(err error) bool {
	if c, ok := ErrorCode(err); ok {
		return c == ErrStopping
	}
	s, ok := HTTPStatus(err)
	return ok && s == 503
}

// IsNotFound 报告 NOT_FOUND。兼容旧 404。
func IsNotFound(err error) bool {
	if c, ok := ErrorCode(err); ok {
		return c == ErrNotFound
	}
	s, ok := HTTPStatus(err)
	return ok && s == 404
}

// IsConflict 报告 CONFLICT 或 STATE_CONFLICT（revision 过期、条件写冲突或审批竞争）。兼容旧 409。
func IsConflict(err error) bool {
	if c, ok := ErrorCode(err); ok {
		return c == ErrConflict || c == ErrStateConflict
	}
	s, ok := HTTPStatus(err)
	return ok && s == 409
}

var _ = strings.TrimSpace
