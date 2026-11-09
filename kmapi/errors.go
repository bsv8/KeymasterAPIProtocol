package kmapi

import (
	"errors"
	"fmt"
	"strings"
)

// TransportError 表示未收到 HTTP 响应：连接拒绝、DNS、超时或调用者取消。
// 它永不携带业务响应，调用者不得把它误认为业务错误。
type TransportError struct {
	Op  string
	Err error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("连接后台失败 (%s): %v", e.Op, e.Err)
}

func (e *TransportError) Unwrap() error { return e.Err }

// HTTPError 表示已收到业务响应但状态码非 2xx。服务器已到达，
// 不得据此重试另一后台；可重试性由调用方按状态码自行判断，
// SDK 不自动重试，更不执行换后台。
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

// HTTPStatus 返回 HTTP 业务状态码（若为 HTTPError）。
func HTTPStatus(err error) (int, bool) {
	var h *HTTPError
	if errors.As(err, &h) {
		return h.Status, true
	}
	return 0, false
}

// IsLocked 报告 423（需解锁，不是自动重试）。
func IsLocked(err error) bool {
	s, ok := HTTPStatus(err)
	return ok && s == 423
}

// IsNotInitialized 报告 412（需 init）。
func IsNotInitialized(err error) bool {
	s, ok := HTTPStatus(err)
	return ok && s == 412
}

// IsStopping 报告 503（后台停止中）。
func IsStopping(err error) bool {
	s, ok := HTTPStatus(err)
	return ok && s == 503
}

// IsNotFound 报告 404。
func IsNotFound(err error) bool {
	s, ok := HTTPStatus(err)
	return ok && s == 404
}

// IsConflict 报告 409（revision 过期或业务冲突）。
func IsConflict(err error) bool {
	s, ok := HTTPStatus(err)
	return ok && s == 409
}

var _ = strings.TrimSpace
