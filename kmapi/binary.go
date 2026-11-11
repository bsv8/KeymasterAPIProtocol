package kmapi

import (
	"fmt"
	"sort"
)

// validateAttachments 检查附件正文与累计未确认字节；完整帧的额外头开销由
// 发送端在 EncodeAttachment 后单独检查。
func validateAttachments(attachments map[string][]byte, limits Limits) error {
	var total int64
	for id, data := range attachments {
		if id == "" || len(id) > 128 {
			return &ProtocolError{Op: "send", Code: ErrBadRequest, Message: "附件 ID 非法"}
		}
		if int64(len(data)) > limits.BinarySingleMaxBytes {
			return &ProtocolError{Op: "send", Code: ErrTooLarge, Message: "附件过大"}
		}
		total += int64(len(data))
		if total > limits.ConnUnackedMaxBytes {
			return &ProtocolError{Op: "send", Code: ErrTooLarge, Message: "附件总量过大"}
		}
	}
	return nil
}

func sortedAttachmentIDs(attachments map[string][]byte) []string {
	ids := make([]string, 0, len(attachments))
	for id := range attachments {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func cloneAttachments(src map[string][]byte) map[string][]byte {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(src))
	for k, v := range src {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

func attachmentFrameTooLarge(frame []byte, limits Limits) error {
	if int64(len(frame)) > limits.BinarySingleMaxBytes+BinaryFrameOverheadMaxBytes {
		return &ProtocolError{Op: "send", Code: ErrTooLarge, Message: fmt.Sprintf("附件帧过大: %d", len(frame))}
	}
	return nil
}
