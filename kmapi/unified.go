package kmapi

import (
	"context"
	"encoding/json"
)

// UnifiedClient 为 CLI 会话的统一类型化客户端：外部后台走 WS，内嵌后台走进程内，
// 共用同一分派器语义。命令层保持原有类型化调用不变。
type UnifiedClient struct {
	ws     *WSClient
	inproc *InprocClient
}

// NewUnifiedWS 构造外部后台客户端。
func NewUnifiedWS(ws *WSClient) *UnifiedClient {
	return &UnifiedClient{ws: ws}
}

// NewUnifiedInproc 构造内嵌后台客户端。
func NewUnifiedInproc(inproc *InprocClient) *UnifiedClient {
	return &UnifiedClient{inproc: inproc}
}

// InstanceID 返回后台实例标识。
func (c *UnifiedClient) InstanceID() string {
	if c.ws != nil {
		return c.ws.InstanceID()
	}
	if c.inproc != nil {
		return c.inproc.InstanceID()
	}
	return ""
}

// Close 关闭连接；外部关闭 WS，内嵌仅标记。
func (c *UnifiedClient) Close() error {
	if c.ws != nil {
		return c.ws.Close()
	}
	if c.inproc != nil {
		return c.inproc.Close()
	}
	return nil
}

// Call 统一普通调用：外部走 WS，内嵌走进程内。
func (c *UnifiedClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	res, err := c.CallWithBinary(ctx, method, params, nil)
	return res.Result, err
}

// CallWithBinary 统一二进制调用；两条传输路径的附件语义一致。
func (c *UnifiedClient) CallWithBinary(ctx context.Context, method string, params any, attachments map[string][]byte) (CallResult, error) {
	if c.ws != nil {
		return c.ws.CallWithBinary(ctx, method, params, attachments)
	}
	if c.inproc != nil {
		return c.inproc.CallWithBinary(ctx, method, params, attachments)
	}
	return CallResult{}, &TransportError{Op: method, Err: errClosed()}
}

func callInto(ctx context.Context, c *UnifiedClient, method string, params any, out any) error {
	raw, err := c.Call(ctx, method, params)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if string(raw) == "null" {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &ParseError{Op: method, Err: err}
	}
	return nil
}

// Status 对应 status.get。
func (c *UnifiedClient) Status(ctx context.Context) (Status, error) {
	var out Status
	if err := callInto(ctx, c, MethodStatusGet, nil, &out); err != nil {
		return Status{}, err
	}
	return out, nil
}

// Info 对应 info.get。
func (c *UnifiedClient) Info(ctx context.Context) (Info, error) {
	var out Info
	if err := callInto(ctx, c, MethodInfoGet, nil, &out); err != nil {
		return Info{}, err
	}
	return out, nil
}

// Unlock 对应 wallet.unlock。
func (c *UnifiedClient) Unlock(ctx context.Context, password string) error {
	var out Ok
	if err := callInto(ctx, c, MethodWalletUnlock, UnlockRequest{Password: password}, &out); err != nil {
		return err
	}
	return nil
}

// CreateTask 对应 tasks.create。
func (c *UnifiedClient) CreateTask(ctx context.Context, seed string) (Task, error) {
	var out Task
	if err := callInto(ctx, c, MethodTasksCreate, CreateTaskRequest{SeedHashHex: seed}, &out); err != nil {
		return Task{}, err
	}
	return out, nil
}

// GetTask 对应 tasks.get。
func (c *UnifiedClient) GetTask(ctx context.Context, seed string) (Task, error) {
	var out Task
	if err := callInto(ctx, c, MethodTasksGet, TaskGetRequest{SeedHashHex: seed}, &out); err != nil {
		return Task{}, err
	}
	return out, nil
}

// CancelTask 对应 tasks.cancel。
func (c *UnifiedClient) CancelTask(ctx context.Context, seed string) error {
	var out Ok
	if err := callInto(ctx, c, MethodTasksCancel, TaskCancelRequest{SeedHashHex: seed}, &out); err != nil {
		return err
	}
	return nil
}

// ListTasks 对应 tasks.list。
func (c *UnifiedClient) ListTasks(ctx context.Context) (TaskList, error) {
	var out TaskList
	if err := callInto(ctx, c, MethodTasksList, nil, &out); err != nil {
		return TaskList{}, err
	}
	return out, nil
}

// SubscribeEvents 统一订阅：先快照后变化，语义与旧客户端一致。
func (c *UnifiedClient) SubscribeEvents(ctx context.Context, topics []string, onSnapshot func(SnapshotEvent), onChange func(EventEnvelope) bool, onResync func() bool) error {
	if c.ws != nil {
		_, err := c.ws.SubscribeEvents(ctx, topics,
			func(snap SnapshotMsg) {
				if onSnapshot != nil {
					onSnapshot(SnapshotEvent{InstanceID: snap.InstanceID, ProtocolVersion: APIVersion, Topics: snap.Topics, Revisions: snap.Revisions, State: snap.State})
				}
			},
			func(ev EventMsg) bool {
				if onChange == nil {
					return true
				}
				return onChange(EventEnvelope{ProtocolVersion: APIVersion, InstanceID: ev.InstanceID, Seq: ev.Seq, Topic: ev.Topic, EntityID: ev.EntityID, Revision: ev.Revision, Data: ev.Data})
			},
			func() bool {
				if onResync == nil {
					return false
				}
				onResync()
				return false
			})
		if err != nil {
			return err
		}
		if ctx == nil {
			select {}
		}
		<-ctx.Done()
		return nil
	}
	if c.inproc != nil {
		return c.inproc.SubscribeEvents(ctx, topics, onSnapshot, onChange, onResync)
	}
	return &TransportError{Op: "events.subscribe", Err: errClosed()}
}

// CheckIdentity 校验 product/apiVersion（精确匹配）。
func CheckIdentity(st Status) error {
	if st.Product != Product {
		return &VersionError{Op: "status.get", Field: "product", Got: st.Product, Want: Product}
	}
	if st.APIVersion != APIVersion {
		return &VersionError{Op: "status.get", Field: "apiVersion", Got: st.APIVersion, Want: APIVersion}
	}
	return nil
}

type closedErr struct{}

func (closedErr) Error() string { return "连接已关闭" }

func errClosed() error { return closedErr{} }
