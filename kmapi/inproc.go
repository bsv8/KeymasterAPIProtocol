package kmapi

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// InprocClient 为内嵌后台的进程内调用：经过相同校验和同一分派器，
// 不直接访问业务对象，不监听端口，不经过网络代理。
type InprocClient struct {
	dispatcher Dispatcher
	events     EventSource
	instanceID string
	limits     Limits
	closed     atomic.Bool
	inflightMu sync.Mutex
	inflight   int
	seq        atomic.Uint64
}

// NewInproc 构造进程内客户端。dispatcher 为后台统一分派器。
func NewInproc(dispatcher Dispatcher, instanceID string) *InprocClient {
	return &InprocClient{dispatcher: dispatcher, instanceID: instanceID, limits: DefaultLimits()}
}

// NewInprocWithEvents 构造带订阅能力的进程内客户端。
func NewInprocWithEvents(dispatcher Dispatcher, events EventSource, instanceID string) *InprocClient {
	return &InprocClient{dispatcher: dispatcher, events: events, instanceID: instanceID, limits: DefaultLimits()}
}

// InstanceID 返回后台实例标识。
func (c *InprocClient) InstanceID() string { return c.instanceID }

// Call 经过相同校验调用同一分派器；仅返回 JSON 结果。
func (c *InprocClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	res, err := c.CallWithBinary(ctx, method, params, nil)
	return res.Result, err
}

// CallWithBinary 与 WS 的二进制调用语义一致：附件交给同一分派器，分派器
// 返回的二进制结果原样交给调用者。
func (c *InprocClient) CallWithBinary(ctx context.Context, method string, params any, attachments map[string][]byte) (CallResult, error) {
	if c.closed.Load() {
		return CallResult{}, &TransportError{Op: method, Err: fmt.Errorf("连接已关闭")}
	}
	if err := ValidateMethod(method); err != nil {
		return CallResult{}, err
	}
	if err := validateAttachments(attachments, c.limits); err != nil {
		return CallResult{}, err
	}
	c.inflightMu.Lock()
	if c.inflight >= c.limits.ConnInflightRequests {
		c.inflightMu.Unlock()
		return CallResult{}, &ProtocolError{Op: method, Code: ErrTooLarge, Message: "在途请求过多"}
	}
	c.inflight++
	c.inflightMu.Unlock()
	defer func() {
		c.inflightMu.Lock()
		c.inflight--
		c.inflightMu.Unlock()
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	cat := TimeoutFor(method)
	if d, ok := timeoutForCategory(cat); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	var rawParams json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return CallResult{}, err
		}
		if int64(len(b)) > c.limits.JSONControlMaxBytes {
			return CallResult{}, &ProtocolError{Op: method, Code: ErrTooLarge, Message: "控制消息过大"}
		}
		rawParams = b
	}
	if c.dispatcher == nil {
		return CallResult{}, &TransportError{Op: method, Err: fmt.Errorf("缺少后台分派器")}
	}
	// 取消只取消等待，不证明业务已回滚；分派器自行判断阶段是否可取消。
	type dispatchResult struct {
		v    any
		bins map[string][]byte
		per  *ErrorDetail
	}
	resCh := make(chan dispatchResult, 1)
	go func() {
		v, bins, per := c.dispatcher.Dispatch(ctx, method, rawParams, attachments)
		resCh <- dispatchResult{v: v, bins: bins, per: per}
	}()
	select {
	case r := <-resCh:
		if r.per != nil {
			return CallResult{}, &ProtocolError{Op: method, Code: r.per.Code, Message: r.per.Message, Retryable: r.per.Retryable}
		}
		raw := json.RawMessage(`null`)
		if r.v != nil {
			b, err := json.Marshal(r.v)
			if err != nil {
				return CallResult{}, &ParseError{Op: method, Err: err}
			}
			raw = b
		}
		return CallResult{Result: raw, Attachments: cloneAttachments(r.bins)}, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return CallResult{}, &TransportError{Op: method, Err: fmt.Errorf("调用超时")}
		}
		return CallResult{}, &TransportError{Op: method, Err: ctx.Err()}
	case <-time.After(longTimeout + time.Second):
		return CallResult{}, &TransportError{Op: method, Err: fmt.Errorf("调用超时")}
	}
}

// Close 标记关闭；不影响后台本身。
func (c *InprocClient) Close() error {
	c.closed.Store(true)
	return nil
}

// SubscribeEvents 经进程内事件源订阅：先快照后变化，语义与 WS 一致。
func (c *InprocClient) SubscribeEvents(ctx context.Context, topics []string, onSnapshot func(SnapshotEvent), onChange func(EventEnvelope) bool, onResync func() bool) error {
	if c.closed.Load() {
		return &TransportError{Op: "events.subscribe", Err: fmt.Errorf("连接已关闭")}
	}
	for _, t := range topics {
		found := false
		for _, k := range KnownTopics {
			if k == t {
				found = true
				break
			}
		}
		if !found {
			return &ProtocolError{Op: "events.subscribe", Code: ErrBadRequest, Message: "未知主题: " + t}
		}
	}
	if c.events == nil {
		return &TransportError{Op: "events.subscribe", Err: fmt.Errorf("缺少事件源")}
	}
	snap, ch, unsubscribe, err := c.events.SubscribeEvents(topics)
	if err != nil {
		if detail, ok := err.(*ErrorDetail); ok {
			return &ProtocolError{Op: "events.subscribe", Code: detail.Code, Message: detail.Message}
		}
		return err
	}
	defer unsubscribe()
	if onSnapshot != nil {
		onSnapshot(snap)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case env, ok := <-ch:
			if !ok {
				if onResync != nil && !onResync() {
					return nil
				}
				return nil
			}
			if env.InstanceID != "" && env.InstanceID != c.instanceID {
				continue
			}
			if onChange != nil && !onChange(env) {
				return nil
			}
		}
	}
}
