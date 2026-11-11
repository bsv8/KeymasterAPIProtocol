package kmapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// Dispatcher 由产品后台实现：同一分派器同时服务 WS 与进程内调用。
// 协议仓只定义接口，不放业务实现。attachments 为已收齐的二进制附件
// （requestId 无关，key 为 attachmentId）；binaries 为待发送的二进制结果。
type Dispatcher interface {
	Dispatch(ctx context.Context, method string, params json.RawMessage, attachments map[string][]byte) (result any, binaries map[string][]byte, errDetail *ErrorDetail)
}

// EventSource 为进程内订阅提供快照与事件流（与 WS 的 subscribe/snapshot/event 对等）。
type EventSource interface {
	SubscribeEvents(topics []string) (SnapshotEvent, <-chan EventEnvelope, func(), error)
}

// CallResult 为一次调用结果：JSON 结果与可选二进制附件。
type CallResult struct {
	Result      json.RawMessage
	Attachments map[string][]byte
}

// Caller 为统一调用接口：WS 与进程内均实现同步/二进制调用、关闭与实例标识。
type Caller interface {
	Call(ctx context.Context, method string, params any) (json.RawMessage, error)
	CallWithBinary(ctx context.Context, method string, params any, attachments map[string][]byte) (CallResult, error)
	Close() error
	InstanceID() string
}

// ValidateMethod 检查方法名是否在规范方法表中。
func ValidateMethod(method string) error {
	if _, ok := MethodTimeout[method]; !ok {
		return &ProtocolError{Op: method, Code: ErrBadRequest, Message: "未知方法"}
	}
	return nil
}

// TimeoutFor 返回方法对应的超时类别。
func TimeoutFor(method string) string {
	if t, ok := MethodTimeout[method]; ok {
		return t
	}
	return "short"
}

// EncodeAttachment 将附件编码为单 WS binary 消息：
// 4 字节大端 headerLen + JSON{requestId,attachmentId} + 原始字节。
func EncodeAttachment(requestID, attachmentID string, data []byte) ([]byte, error) {
	hdr, err := json.Marshal(AttachHeader{RequestID: requestID, AttachmentID: attachmentID})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	var ln [4]byte
	binary.BigEndian.PutUint32(ln[:], uint32(len(hdr)))
	buf.Write(ln[:])
	buf.Write(hdr)
	buf.Write(data)
	return buf.Bytes(), nil
}

// DecodeAttachment 解析单 WS binary 消息，校验声明长度。
func DecodeAttachment(frame []byte, declared int64) (AttachHeader, []byte, error) {
	if len(frame) < 4 {
		return AttachHeader{}, nil, fmt.Errorf("附件帧过短")
	}
	hl := int(binary.BigEndian.Uint32(frame[:4]))
	if hl < 0 || 4+hl > len(frame) {
		return AttachHeader{}, nil, fmt.Errorf("附件头长度非法")
	}
	var hdr AttachHeader
	if err := json.Unmarshal(frame[4:4+hl], &hdr); err != nil {
		return AttachHeader{}, nil, err
	}
	data := frame[4+hl:]
	if declared >= 0 && int64(len(data)) != declared {
		return AttachHeader{}, nil, fmt.Errorf("声明长度与实际长度不一致")
	}
	return hdr, data, nil
}
