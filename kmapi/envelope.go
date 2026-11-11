package kmapi

// v1 消息外壳。真值见 spec/protocol.yaml；生成文件只含类型/方法/常量，
// 外壳手写以保证编解码与校验一致。

const (
	TopicConnection = "connection"
	TopicWallet     = "wallet"
	TopicBalance    = "balance"
	TopicTask       = "task"
	TopicRoots      = "roots"
	TopicSettings   = "settings"
	TopicCollector  = "collector"
)

const (
	MsgHello       = "hello"
	MsgWelcome     = "welcome"
	MsgRequest     = "request"
	MsgResponse    = "response"
	MsgCancel      = "cancel"
	MsgCancelAck   = "cancel-ack"
	MsgSubscribe   = "subscribe"
	MsgUnsubscribe = "unsubscribe"
	MsgSnapshot    = "snapshot"
	MsgEvent       = "event"
	MsgResync      = "resync"
	MsgAttachBegin = "binary-begin"
)

// Hello 由客户端在 WS 建立后首帧发送。
type Hello struct {
	Type       string     `json:"type"`
	Product    string     `json:"product"`
	APIVersion string     `json:"apiVersion"`
	Client     ClientInfo `json:"client"`
}

// ClientInfo 标识调用方。
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Limits 与 spec/limits 一致，后台可在 welcome 公布更低值。
type Limits struct {
	JSONControlMaxBytes     int64 `json:"jsonControlMaxBytes"`
	BinarySingleMaxBytes    int64 `json:"binarySingleMaxBytes"`
	ConnUnackedMaxBytes     int64 `json:"connUnackedMaxBytes"`
	ConnPendingSendMaxBytes int64 `json:"connPendingSendMaxBytes"`
	ConnInflightRequests    int   `json:"connInflightRequests"`
	AttachmentDeadlineSecs  int   `json:"attachmentDeadlineSecs"`
	StoragePlainMaxBytes    int64 `json:"storagePlainMaxBytes,omitempty"`
	MsfileSeedMaxBytes      int64 `json:"msfileSeedMaxBytes,omitempty"`
	MsfileBlockMaxBytes     int64 `json:"msfileBlockMaxBytes,omitempty"`
}

// Welcome 由后台首帧返回。
type Welcome struct {
	Type         string `json:"type"`
	Product      string `json:"product"`
	APIVersion   string `json:"apiVersion"`
	InstanceID   string `json:"instanceId"`
	ConnectionID string `json:"connectionId"`
	Limits       Limits `json:"limits"`
}

// Request 发起调用。ID 同一连接内不复用；只关联调用，不作持久幂等键。
type Request struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
	// Attachments 声明随后的二进制附件（{id, bytes}）：服务端在处理请求前
	// 同步预注册期望值，未声明的附件帧将被忽略；声明缺失则请求按无附件执行。
	Attachments []AttachmentDecl `json:"attachments,omitempty"`
}

// AttachmentDecl 为单个二进制附件的声明。
type AttachmentDecl struct {
	ID    string `json:"id"`
	Bytes int64  `json:"bytes"`
}

// ErrorDetail 为线上传输的结构化业务错误体。
type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

func (e *ErrorDetail) Error() string {
	if e == nil {
		return "nil error"
	}
	return "后台业务错误 " + e.Code + ": " + e.Message
}

// Response 返回对应请求的结果或结构化错误。
type Response struct {
	Type   string       `json:"type"`
	ID     string       `json:"id"`
	Result any          `json:"result,omitempty"`
	Error  *ErrorDetail `json:"error,omitempty"`
}

// Cancel 尝试取消指定在途调用。ID 为取消操作自身 ID，TargetID 为目标请求 ID。
type Cancel struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	TargetID string `json:"targetId"`
}

// CancelAck 为取消操作的明确结果。
type CancelAck struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	TargetID string `json:"targetId"`
	Status   string `json:"status"`
}

// CancelStatus 取值。
const (
	CancelRequested      = "requested-cancelled"
	CancelFinished       = "already-finished"
	CancelNotCancellable = "not-cancellable"
	CancelNotFound       = "not-found"
)

// Subscribe 建立订阅。
type Subscribe struct {
	Type   string   `json:"type"`
	ID     string   `json:"id"`
	Topics []string `json:"topics"`
}

// Unsubscribe 撤销订阅。
type Unsubscribe struct {
	Type           string   `json:"type"`
	ID             string   `json:"id"`
	SubscriptionID string   `json:"subscriptionId,omitempty"`
	Topics         []string `json:"topics,omitempty"`
}

// SnapshotMsg 承载快照。
type SnapshotMsg struct {
	Type       string         `json:"type"`
	ID         string         `json:"id,omitempty"`
	InstanceID string         `json:"instanceId"`
	Topics     []string       `json:"topics"`
	Revisions  map[string]any `json:"revisions,omitempty"`
	State      map[string]any `json:"state,omitempty"`
}

// EventMsg 承载后续变化。
type EventMsg struct {
	Type       string         `json:"type"`
	InstanceID string         `json:"instanceId"`
	Seq        int64          `json:"seq"`
	Topic      string         `json:"topic"`
	EntityID   *string        `json:"entityId,omitempty"`
	Revision   *int64         `json:"revision,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
}

// ResyncMsg 要求重取快照。
type ResyncMsg struct {
	Type   string `json:"type"`
	Reason string `json:"reason,omitempty"`
}

// AttachBegin 声明二进制附件；随后一帧 WS binary 消息承载完整附件。
// 二进制帧布局：4 字节大端 headerLen + JSON{requestId,attachmentId} + 原始字节。
// 声明长度与实际长度不一致、重复附件、未声明附件均拒绝；未收齐不执行业务。
type AttachBegin struct {
	Type          string `json:"type"`
	RequestID     string `json:"requestId"`
	AttachmentID  string `json:"attachmentId"`
	DeclaredBytes int64  `json:"declaredBytes"`
}

// AttachHeader 为二进制帧内嵌 JSON 头。
type AttachHeader struct {
	RequestID    string `json:"requestId"`
	AttachmentID string `json:"attachmentId"`
}

// BinaryFrameOverheadMaxBytes 是完整二进制帧除正文外允许的协议头开销
// （4 字节头长 + JSON 头）。正文仍受 BinarySingleMaxBytes 限制。
const BinaryFrameOverheadMaxBytes = 64 << 10

// DefaultLimits 返回规范默认值。
func DefaultLimits() Limits {
	return Limits{
		JSONControlMaxBytes:     1 << 20,
		BinarySingleMaxBytes:    16 << 20,
		ConnUnackedMaxBytes:     32 << 20,
		ConnPendingSendMaxBytes: 32 << 20,
		ConnInflightRequests:    16,
		AttachmentDeadlineSecs:  30,
		StoragePlainMaxBytes:    16 << 20,
		MsfileSeedMaxBytes:      16 << 20,
		MsfileBlockMaxBytes:     256 << 10,
	}
}

// CheckHello 精确校验产品与版本。
func CheckHello(product, apiVersion string) *VersionError {
	if product != Product {
		return &VersionError{Op: "hello", Field: "product", Got: product, Want: Product}
	}
	if apiVersion != APIVersion {
		return &VersionError{Op: "hello", Field: "apiVersion", Got: apiVersion, Want: APIVersion}
	}
	return nil
}
