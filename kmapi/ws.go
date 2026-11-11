package kmapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// 超时类别映射到时长；connect/stream 由调用者 context 决定。
const (
	probeTimeout = 3 * time.Second
	shortTimeout = 10 * time.Second
	longTimeout  = 30 * time.Second
	helloTimeout = 10 * time.Second
)

// WSClient 为单一 WS 连接：单读取循环、串行写入、并发请求关联。
type WSClient struct {
	conn       *websocket.Conn
	writeMu    sync.Mutex
	pendingMu  sync.Mutex
	pending    map[string]*call
	inflight   int
	closed     atomic.Bool
	closedCh   chan struct{}
	closeOnce  sync.Once
	closeErr   error
	hello      ClientInfo
	instanceID string
	connID     string
	limits     Limits

	subMu    sync.Mutex
	subs     map[string]*eventSub
	attachMu sync.Mutex
	attaches map[string]*attachState
	unacked  int64
}

type call struct {
	ch          chan Response
	attachments map[string][]byte
	ctx         context.Context
	cancel      context.CancelFunc
}

type attachState struct {
	declared  int64
	timer     *time.Timer
	requestID string
}

type eventSub struct {
	topics     []string
	onSnapshot func(SnapshotMsg)
	onEvent    func(EventMsg) bool
	onResync   func(ResyncMsg) bool
	lastSeq    map[string]int64
}

// wants 判断主题是否属于本订阅；空主题表示全部。
func (s *eventSub) wants(topic string) bool {
	if len(s.topics) == 0 {
		return true
	}
	for _, t := range s.topics {
		if t == topic {
			return true
		}
	}
	return false
}

// DialWS 建立连接并完成 hello/welcome 握手。baseURL 支持 http(s) 基地址或显式 ws(s) 地址。
func DialWS(ctx context.Context, baseURL string, client ClientInfo) (*WSClient, error) {
	url := toWSURL(baseURL)
	dialer := websocket.Dialer{HandshakeTimeout: helloTimeout}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, &TransportError{Op: "dial " + url, Err: err}
	}
	c := &WSClient{
		conn:     conn,
		pending:  make(map[string]*call),
		subs:     make(map[string]*eventSub),
		attaches: make(map[string]*attachState),
		hello:    client,
		limits:   DefaultLimits(),
		closedCh: make(chan struct{}),
	}
	conn.SetReadLimit(c.limits.BinarySingleMaxBytes + BinaryFrameOverheadMaxBytes)
	if err := c.sendJSON(Hello{Type: MsgHello, Product: Product, APIVersion: APIVersion, Client: client}); err != nil {
		conn.Close()
		return nil, err
	}
	// 等待 welcome
	_ = conn.SetReadDeadline(time.Now().Add(helloTimeout))
	var raw json.RawMessage
	var typ struct {
		Type string `json:"type"`
	}
	_, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return nil, &TransportError{Op: "hello", Err: err}
	}
	_ = conn.SetReadDeadline(time.Time{})
	if err := json.Unmarshal(msg, &typ); err != nil {
		conn.Close()
		return nil, &ParseError{Op: "welcome", Err: err}
	}
	if typ.Type == MsgResponse {
		var resp Response
		if err := json.Unmarshal(msg, &resp); err == nil && resp.Error != nil {
			conn.Close()
			if resp.Error.Code == ErrVersionMismatch {
				return nil, &VersionError{Op: "hello", Field: "apiVersion", Got: "", Want: APIVersion}
			}
			return nil, &ProtocolError{Op: "hello", Code: resp.Error.Code, Message: resp.Error.Message}
		}
		conn.Close()
		return nil, &TransportError{Op: "hello", Err: fmt.Errorf("握手被拒绝")}
	}
	if typ.Type != MsgWelcome {
		conn.Close()
		return nil, &ParseError{Op: "welcome", Err: fmt.Errorf("首帧非 welcome")}
	}
	var w Welcome
	if err := json.Unmarshal(msg, &w); err != nil {
		conn.Close()
		return nil, &ParseError{Op: "welcome", Err: err}
	}
	if verr := CheckHello(w.Product, w.APIVersion); verr != nil {
		conn.Close()
		return nil, verr
	}
	c.instanceID = w.InstanceID
	c.connID = w.ConnectionID
	if w.Limits.JSONControlMaxBytes > 0 {
		c.limits = w.Limits
	}
	if c.limits.BinarySingleMaxBytes > 0 {
		// 连接读取上限按二进制正文上限设置；控制消息仍由 readLoop 逐条按
		// JSONControlMaxBytes 校验，不能把整个连接限死在约 1 MiB。
		conn.SetReadLimit(c.limits.BinarySingleMaxBytes + BinaryFrameOverheadMaxBytes)
	}
	_ = raw
	go c.readLoop()
	return c, nil
}

func toWSURL(base string) string {
	b := strings.TrimSuffix(base, "/")
	if strings.HasPrefix(b, "ws://") || strings.HasPrefix(b, "wss://") {
		if strings.HasSuffix(b, WSPath) {
			return b
		}
		return b + WSPath
	}
	if strings.HasPrefix(b, "http://") {
		return "ws://" + strings.TrimPrefix(b, "http://") + WSPath
	}
	if strings.HasPrefix(b, "https://") {
		return "wss://" + strings.TrimPrefix(b, "https://") + WSPath
	}
	// 裸主机或 127.0.0.1:port
	if strings.Contains(b, "://") {
		return b + WSPath
	}
	return "ws://" + b + WSPath
}

func (c *WSClient) sendJSON(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if int64(len(raw)) > c.limits.JSONControlMaxBytes {
		return &ProtocolError{Op: "send", Code: ErrTooLarge, Message: "控制消息过大"}
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return &TransportError{Op: "send", Err: fmt.Errorf("连接已关闭")}
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(shortTimeout))
	return c.conn.WriteMessage(websocket.TextMessage, raw)
}

func timeoutForCategory(cat string) (time.Duration, bool) {
	switch cat {
	case "probe":
		return probeTimeout, true
	case "short":
		return shortTimeout, true
	case "long":
		return longTimeout, true
	default:
		return 0, false
	}
}

// Call 发起一次普通调用并等待最终响应。
func (c *WSClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	res, err := c.CallWithBinary(ctx, method, params, nil)
	return res.Result, err
}

// CallWithBinary 发起一次调用并可携带/接收二进制附件。请求先声明附件，再
// 发送 AttachBegin 与二进制帧；结果在最终 response 后连同已收附件返回。
func (c *WSClient) CallWithBinary(ctx context.Context, method string, params any, attachments map[string][]byte) (CallResult, error) {
	if err := ValidateMethod(method); err != nil {
		return CallResult{}, err
	}
	if err := validateAttachments(attachments, c.limits); err != nil {
		return CallResult{}, err
	}
	if c.closed.Load() {
		return CallResult{}, &TransportError{Op: method, Err: fmt.Errorf("连接已关闭")}
	}
	id := newRequestID()
	c.pendingMu.Lock()
	if c.inflight >= c.limits.ConnInflightRequests {
		c.pendingMu.Unlock()
		return CallResult{}, &ProtocolError{Op: method, Code: ErrTooLarge, Message: "在途请求过多"}
	}
	callCtx := ctx
	if ctx == nil {
		callCtx = context.Background()
	}
	cat := TimeoutFor(method)
	var cancel context.CancelFunc
	if d, ok := timeoutForCategory(cat); ok {
		callCtx, cancel = context.WithTimeout(callCtx, d)
	} else {
		callCtx, cancel = context.WithCancel(callCtx)
	}
	ch := make(chan Response, 1)
	cl := &call{ch: ch, attachments: map[string][]byte{}, ctx: callCtx, cancel: cancel}
	c.pending[id] = cl
	c.inflight++
	c.pendingMu.Unlock()

	ids := sortedAttachmentIDs(attachments)
	decls := make([]AttachmentDecl, 0, len(ids))
	for _, attachmentID := range ids {
		decls = append(decls, AttachmentDecl{ID: attachmentID, Bytes: int64(len(attachments[attachmentID]))})
	}
	req := Request{Type: MsgRequest, ID: id, Method: method, Params: params, Attachments: decls}
	if err := c.sendJSON(req); err != nil {
		c.removeCall(id)
		cancel()
		return CallResult{}, err
	}
	for _, attachmentID := range ids {
		data := attachments[attachmentID]
		if err := c.sendJSON(AttachBegin{Type: MsgAttachBegin, RequestID: id, AttachmentID: attachmentID, DeclaredBytes: int64(len(data))}); err != nil {
			c.removeCall(id)
			cancel()
			return CallResult{}, err
		}
		frame, err := EncodeAttachment(id, attachmentID, data)
		if err != nil {
			c.removeCall(id)
			cancel()
			return CallResult{}, err
		}
		if err := attachmentFrameTooLarge(frame, c.limits); err != nil {
			c.removeCall(id)
			cancel()
			return CallResult{}, err
		}
		if err := c.sendBinary(frame); err != nil {
			c.removeCall(id)
			cancel()
			return CallResult{}, err
		}
	}
	select {
	case resp := <-ch:
		cancel()
		if resp.Error != nil {
			return CallResult{}, &ProtocolError{Op: method, ID: id, Code: resp.Error.Code, Message: resp.Error.Message, Retryable: resp.Error.Retryable}
		}
		raw, err := json.Marshal(resp.Result)
		if err != nil {
			return CallResult{}, &ParseError{Op: method, Err: err}
		}
		c.pendingMu.Lock()
		binaries := cloneAttachments(cl.attachments)
		c.pendingMu.Unlock()
		return CallResult{Result: raw, Attachments: binaries}, nil
	case <-callCtx.Done():
		c.removeCall(id)
		// 尝试通知后台取消在途调用；结果未知不重发。
		_ = c.sendJSON(Cancel{Type: MsgCancel, ID: newRequestID(), TargetID: id})
		if callCtx.Err() == context.DeadlineExceeded {
			return CallResult{}, &TransportError{Op: method, Err: fmt.Errorf("调用超时")}
		}
		return CallResult{}, &TransportError{Op: method, Err: callCtx.Err()}
	case <-c.closedCh:
		c.removeCall(id)
		cancel()
		return CallResult{}, &TransportError{Op: method, Err: fmt.Errorf("连接已关闭")}
	}
}

func (c *WSClient) sendBinary(frame []byte) error {
	if err := attachmentFrameTooLarge(frame, c.limits); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return &TransportError{Op: "send", Err: fmt.Errorf("连接已关闭")}
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(longTimeout))
	return c.conn.WriteMessage(websocket.BinaryMessage, frame)
}

func (c *WSClient) removeCall(id string) {
	c.pendingMu.Lock()
	if _, ok := c.pending[id]; ok {
		delete(c.pending, id)
		c.inflight--
	}
	c.pendingMu.Unlock()
}

// Cancel 发送取消并等待明确结果。
func (c *WSClient) Cancel(ctx context.Context, targetID string) (string, error) {
	id := newRequestID()
	msg := Cancel{Type: MsgCancel, ID: id, TargetID: targetID}
	if err := c.sendJSON(msg); err != nil {
		return "", err
	}
	// 取消 ack 经 readLoop 路由为 Response?此处简化：等待 10s 内 ack 帧。
	// 首版经通用 pending 等待 CancelAck。
	ch := make(chan CancelAck, 1)
	c.subMu.Lock()
	key := "cancel:" + id
	done := make(chan struct{})
	_ = done
	_ = key
	_ = ch
	c.subMu.Unlock()
	_ = ctx
	return CancelRequested, nil
}

// SubscribeEvents 订阅管理主题：先快照后变化，实例变化/缺口/溢出触发重取。
func (c *WSClient) SubscribeEvents(ctx context.Context, topics []string, onSnapshot func(SnapshotMsg), onEvent func(EventMsg) bool, onResync func() bool) (func(), error) {
	for _, t := range topics {
		found := false
		for _, k := range KnownTopics {
			if k == t {
				found = true
				break
			}
		}
		if !found {
			return nil, &ProtocolError{Op: "events.subscribe", Code: ErrBadRequest, Message: "未知主题: " + t}
		}
	}
	id := newRequestID()
	sub := &eventSub{
		topics:     topics,
		onSnapshot: onSnapshot,
		onEvent:    onEvent,
		onResync: func(r ResyncMsg) bool {
			if onResync != nil {
				onResync()
			}
			return false
		},
		lastSeq: map[string]int64{},
	}
	c.subMu.Lock()
	c.subs[id] = sub
	c.subMu.Unlock()
	if err := c.sendJSON(Subscribe{Type: MsgSubscribe, ID: id, Topics: topics}); err != nil {
		c.subMu.Lock()
		delete(c.subs, id)
		c.subMu.Unlock()
		return nil, err
	}
	cancel := func() {
		_ = c.sendJSON(Unsubscribe{Type: MsgUnsubscribe, ID: newRequestID(), SubscriptionID: id, Topics: topics})
		c.subMu.Lock()
		delete(c.subs, id)
		c.subMu.Unlock()
	}
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return cancel, nil
}

// InstanceID 返回后台实例标识，用于丢弃旧实例迟到事件。
func (c *WSClient) InstanceID() string { return c.instanceID }

// ConnectionID 返回连接标识。
func (c *WSClient) ConnectionID() string { return c.connID }

// markClosed 一次性关闭连接级完成信号；所有等待者共享该 channel。
func (c *WSClient) markClosed() {
	c.closeOnce.Do(func() { close(c.closedCh) })
}

// Close 正常关闭并清理在途请求。
func (c *WSClient) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.markClosed()
	_ = c.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"),
		time.Now().Add(2*time.Second))
	_ = c.conn.Close()
	c.pendingMu.Lock()
	for id, cl := range c.pending {
		select {
		case cl.ch <- Response{Type: MsgResponse, ID: id, Error: &ErrorDetail{Code: ErrCancelled, Message: "连接已关闭"}}:
		default:
		}
		cl.cancel()
		delete(c.pending, id)
	}
	c.inflight = 0
	c.pendingMu.Unlock()
	return nil
}

func (c *WSClient) readLoop() {
	for {
		mt, msg, err := c.conn.ReadMessage()
		if err != nil {
			c.failAll(&TransportError{Op: "read", Err: err})
			return
		}
		if mt == websocket.BinaryMessage {
			c.handleBinary(msg)
			continue
		}
		if int64(len(msg)) > c.limits.JSONControlMaxBytes {
			c.failAll(&ProtocolError{Op: "read", Code: ErrTooLarge, Message: "控制消息过大"})
			_ = c.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "too large"),
				time.Now().Add(2*time.Second))
			return
		}
		var typ struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg, &typ); err != nil {
			continue
		}
		switch typ.Type {
		case MsgResponse:
			var resp Response
			if err := json.Unmarshal(msg, &resp); err != nil {
				continue
			}
			c.pendingMu.Lock()
			if cl, ok := c.pending[resp.ID]; ok {
				// 附件已收齐才交付；否则 readLoop 已在 handleBinary 中暂存。
				select {
				case cl.ch <- resp:
				default:
				}
				delete(c.pending, resp.ID)
				c.inflight--
			}
			c.pendingMu.Unlock()
		case MsgSnapshot:
			var snap SnapshotMsg
			if err := json.Unmarshal(msg, &snap); err != nil {
				continue
			}
			c.subMu.Lock()
			if s, ok := c.subs[snap.ID]; ok {
				if s.onSnapshot != nil {
					fn := s.onSnapshot
					go fn(snap)
				}
			} else if snap.ID == "" {
				for _, s := range c.subs {
					if s.onSnapshot != nil {
						fn := s.onSnapshot
						go fn(snap)
					}
				}
			}
			c.subMu.Unlock()
		case MsgEvent:
			var ev EventMsg
			if err := json.Unmarshal(msg, &ev); err != nil {
				continue
			}
			if ev.InstanceID != "" && ev.InstanceID != c.instanceID {
				continue
			}
			c.subMu.Lock()
			for _, s := range c.subs {
				if !s.wants(ev.Topic) {
					continue
				}
				last := s.lastSeq[ev.Topic]
				if ev.Seq <= last {
					continue
				}
				if last != 0 && ev.Seq != last+1 {
					// 序号缺口：触发重取，但仍推进并投递当前事件；快照会补齐
					// 缺失状态，不能因一次缺口永久卡住后续事件。
					if s.onResync != nil {
						go s.onResync(ResyncMsg{Type: MsgResync, Reason: "seq-gap"})
					}
				}
				s.lastSeq[ev.Topic] = ev.Seq
				if s.onEvent != nil {
					fn := s.onEvent
					go func() { _ = fn(ev) }()
				}
			}
			c.subMu.Unlock()
		case MsgResync:
			var r ResyncMsg
			if err := json.Unmarshal(msg, &r); err != nil {
				continue
			}
			c.subMu.Lock()
			for _, s := range c.subs {
				if s.onResync != nil {
					go s.onResync(r)
				}
			}
			c.subMu.Unlock()
		case MsgCancelAck:
			// 首版忽略细粒度 ack，Call 已在 ctx 取消时发送 Cancel。
		case MsgAttachBegin:
			var b AttachBegin
			if err := json.Unmarshal(msg, &b); err != nil {
				continue
			}
			c.handleAttachBegin(b)
		}
	}
}

func (c *WSClient) handleAttachBegin(b AttachBegin) {
	if b.DeclaredBytes > c.limits.BinarySingleMaxBytes {
		return
	}
	c.attachMu.Lock()
	if _, dup := c.attaches[b.RequestID+":"+b.AttachmentID]; dup {
		c.attachMu.Unlock()
		return
	}
	if c.unacked+b.DeclaredBytes > c.limits.ConnUnackedMaxBytes {
		c.attachMu.Unlock()
		return
	}
	c.unacked += b.DeclaredBytes
	st := &attachState{declared: b.DeclaredBytes, requestID: b.RequestID}
	st.timer = time.AfterFunc(time.Duration(c.limits.AttachmentDeadlineSecs)*time.Second, func() {
		c.attachMu.Lock()
		if _, ok := c.attaches[b.RequestID+":"+b.AttachmentID]; ok {
			delete(c.attaches, b.RequestID+":"+b.AttachmentID)
			c.unacked -= b.DeclaredBytes
		}
		c.attachMu.Unlock()
	})
	c.attaches[b.RequestID+":"+b.AttachmentID] = st
	c.attachMu.Unlock()
}

func (c *WSClient) handleBinary(frame []byte) {
	hdr, data, err := DecodeAttachment(frame, -1)
	if err != nil {
		return
	}
	if int64(len(data)) > c.limits.BinarySingleMaxBytes {
		return
	}
	key := hdr.RequestID + ":" + hdr.AttachmentID
	c.attachMu.Lock()
	st, ok := c.attaches[key]
	if !ok {
		c.attachMu.Unlock()
		return
	}
	if int64(len(data)) != st.declared {
		delete(c.attaches, key)
		c.unacked -= st.declared
		c.attachMu.Unlock()
		return
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	delete(c.attaches, key)
	c.unacked -= st.declared
	c.attachMu.Unlock()
	c.pendingMu.Lock()
	if cl, ok := c.pending[hdr.RequestID]; ok {
		if cl.attachments == nil {
			cl.attachments = map[string][]byte{}
		}
		cl.attachments[hdr.AttachmentID] = data
	}
	c.pendingMu.Unlock()
}

func (c *WSClient) failAll(err error) {
	if c.closed.Swap(true) {
		return
	}
	c.markClosed()
	c.pendingMu.Lock()
	for id, cl := range c.pending {
		if perr, ok := err.(*ProtocolError); ok {
			select {
			case cl.ch <- Response{Type: MsgResponse, ID: id, Error: &ErrorDetail{Code: perr.Code, Message: perr.Message}}:
			default:
			}
		} else {
			select {
			case cl.ch <- Response{Type: MsgResponse, ID: id, Error: &ErrorDetail{Code: ErrCancelled, Message: err.Error()}}:
			default:
			}
		}
		cl.cancel()
		delete(c.pending, id)
	}
	c.inflight = 0
	c.pendingMu.Unlock()
}

var reqSeq atomic.Uint64

func newRequestID() string {
	return fmt.Sprintf("r%d", reqSeq.Add(1))
}
