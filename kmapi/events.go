package kmapi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// 事件主题（与 openapi EventEnvelope.topic 一致）。
const (
	TopicConnection = "connection"
	TopicWallet     = "wallet"
	TopicBalance    = "balance"
	TopicTask       = "task"
	TopicRoots      = "roots"
	TopicSettings   = "settings"
	TopicCollector  = "collector"
)

// KnownTopics 列出全部合法主题；未知主题服务端应 400。
var KnownTopics = []string{TopicConnection, TopicWallet, TopicBalance, TopicTask, TopicRoots, TopicSettings, TopicCollector}

// SubscribeEvents 连接 GET /api/events。topics 为空表示全部；
// 否则按逗号拼接为 ?topics=。只恢复原地址，不执行三级回退。
//
// 事件类型：snapshot（当前快照）、change（后续变化）、resync
// （客户端必须重新同步，即重建本连接并重取快照）。心跳为 SSE
// 注释帧，默认每 15s，忽略不计。初版重连即重取快照，不做
// Last-Event-ID 历史重放。
func (c *Client) SubscribeEvents(ctx context.Context, topics []string, onSnapshot func(SnapshotEvent), onChange func(EventEnvelope) bool, onResync func() bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	path := "/api/events"
	if len(topics) > 0 {
		path += "?topics=" + q(strings.Join(topics, ","))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return transportErr("GET "+path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return &HTTPError{Op: "GET " + path, Status: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	reader := bufio.NewReader(resp.Body)
	var eventType string
	var data strings.Builder
	flush := func() error {
		t := strings.TrimSpace(eventType)
		raw := strings.TrimSpace(data.String())
		eventType = ""
		data.Reset()
		if raw == "" {
			return nil
		}
		if t == "" {
			t = "change"
		}
		switch t {
		case "snapshot":
			var snap SnapshotEvent
			if err := json.Unmarshal([]byte(raw), &snap); err != nil {
				return &ParseError{Op: "GET " + path, Err: err}
			}
			if onSnapshot != nil {
				onSnapshot(snap)
			}
			return nil
		case "change":
			var env EventEnvelope
			if err := json.Unmarshal([]byte(raw), &env); err != nil {
				return &ParseError{Op: "GET " + path, Err: err}
			}
			if onChange != nil && !onChange(env) {
				return io.EOF
			}
			return nil
		case "resync":
			if onResync != nil && !onResync() {
				return io.EOF
			}
			// 默认可观察行为：收到 resync 即结束本连接，由调用者
			// 重建订阅并重取快照。
			return io.EOF
		default:
			return nil
		}
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(trimmed, ":") {
			continue
		}
		if strings.HasPrefix(trimmed, "event:") {
			// 同一事件内的旧 data 先结算，避免串帧。
			if data.Len() > 0 {
				if ferr := flush(); ferr != nil {
					if ferr == io.EOF {
						return nil
					}
					return ferr
				}
			}
			eventType = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			continue
		}
		if strings.HasPrefix(trimmed, "data:") {
			payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
			if data.Len() > 0 {
				data.WriteString("\n")
			}
			data.WriteString(payload)
			continue
		}
		if trimmed == "" {
			if err := flush(); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	}
}
