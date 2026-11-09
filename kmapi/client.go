package kmapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// ProbeTimeout 限定单次状态探测。
	ProbeTimeout = 3 * time.Second
	// ShortTimeout 限定普通短请求。
	ShortTimeout = 10 * time.Second
	// LongTimeout 限定 KDF 与任务提交等长请求。
	LongTimeout = 30 * time.Second
)

// Client 面向一个后台 baseURL。httpClient 可注入：外部后台用标准
// HTTP，内嵌后台由产品注入内存 transport。两者共用 SDK 与订阅解析。
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// New 构造客户端。httpClient 为 nil 时使用无总超时的默认客户端；
// 读写各自由请求 context 限定，SSE 随调用者 context 生命周期。
func New(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &Client{BaseURL: strings.TrimSuffix(baseURL, "/"), HTTP: httpClient}
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, d)
}

func transportErr(op string, err error) error { return &TransportError{Op: op, Err: err} }

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	op := method + " " + path
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return transportErr(op, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return &HTTPError{Op: op, Status: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	if out == nil {
		return nil
	}
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &ParseError{Op: op, Err: err}
	}
	return nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// Status 是 GET /api/status（探测超时）。调用者负责校验 product/apiVersion。
func (c *Client) Status(ctx context.Context) (Status, error) {
	ctx, cancel := withTimeout(ctx, ProbeTimeout)
	defer cancel()
	var out Status
	if err := c.get(ctx, "/api/status", &out); err != nil {
		return Status{}, err
	}
	return out, nil
}

// CheckIdentity 校验 product/apiVersion（精确匹配）。
func CheckIdentity(st Status) error {
	if st.Product != "km" {
		return &VersionError{Op: "GET /api/status", Field: "product", Got: st.Product, Want: "km"}
	}
	if st.APIVersion != "v1" {
		return &VersionError{Op: "GET /api/status", Field: "apiVersion", Got: st.APIVersion, Want: "v1"}
	}
	return nil
}

// Info 是 GET /api/info。
func (c *Client) Info(ctx context.Context) (Info, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Info
	if err := c.get(ctx, "/api/info", &out); err != nil {
		return Info{}, err
	}
	return out, nil
}

// Unlock 提交密码（长超时）。调用者负责清零密码缓冲。
func (c *Client) Unlock(ctx context.Context, password string) error {
	ctx, cancel := withTimeout(ctx, LongTimeout)
	defer cancel()
	return c.do(ctx, http.MethodPost, "/api/unlock", map[string]string{"password": password}, nil)
}

// ListRoots 是 GET /api/roots。
func (c *Client) ListRoots(ctx context.Context) (RootsView, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out RootsView
	if err := c.get(ctx, "/api/roots", &out); err != nil {
		return RootsView{}, err
	}
	return out, nil
}

// AddRoot 是 POST /api/roots。
func (c *Client) AddRoot(ctx context.Context, req AddRootRequest) (AddRootResponse, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out AddRootResponse
	if err := c.do(ctx, http.MethodPost, "/api/roots", req, &out); err != nil {
		return AddRootResponse{}, err
	}
	return out, nil
}

// DeleteRoot 是 DELETE /api/roots?id=&revision=。
func (c *Client) DeleteRoot(ctx context.Context, id string, revision int64) (RevisionedOk, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out RevisionedOk
	path := "/api/roots?id=" + q(id) + "&revision=" + itoa(revision)
	if err := c.do(ctx, http.MethodDelete, path, nil, &out); err != nil {
		return RevisionedOk{}, err
	}
	return out, nil
}

// SetDefaultRoot 是 PUT /api/roots/default。
func (c *Client) SetDefaultRoot(ctx context.Context, req SetDefaultRootRequest) (RevisionedOk, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out RevisionedOk
	if err := c.do(ctx, http.MethodPut, "/api/roots/default", req, &out); err != nil {
		return RevisionedOk{}, err
	}
	return out, nil
}

// ListFiles 是 GET /api/files。
func (c *Client) ListFiles(ctx context.Context) (FilesView, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out FilesView
	if err := c.get(ctx, "/api/files", &out); err != nil {
		return FilesView{}, err
	}
	return out, nil
}

// Browse 是 GET /api/browse?path=。
func (c *Client) Browse(ctx context.Context, path string) (BrowseView, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out BrowseView
	if err := c.get(ctx, "/api/browse?path="+q(path), &out); err != nil {
		return BrowseView{}, err
	}
	return out, nil
}

// GetSettings 是 GET /api/settings。
func (c *Client) GetSettings(ctx context.Context) (Settings, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Settings
	if err := c.get(ctx, "/api/settings", &out); err != nil {
		return Settings{}, err
	}
	return out, nil
}

// UpdateSettings 是 PUT /api/settings（需携带 revision）。
func (c *Client) UpdateSettings(ctx context.Context, req SettingsUpdate) (Settings, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Settings
	if err := c.do(ctx, http.MethodPut, "/api/settings", req, &out); err != nil {
		return Settings{}, err
	}
	return out, nil
}

// ResetSettings 是 POST /api/settings/reset。
func (c *Client) ResetSettings(ctx context.Context, revision int64) (Settings, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Settings
	if err := c.do(ctx, http.MethodPost, "/api/settings/reset", RevisionRequest{Revision: revision}, &out); err != nil {
		return Settings{}, err
	}
	return out, nil
}

// RefreshCatalog 是 POST /api/catalog/refresh。
// v1 单根返回原生报告对象（产品内部直通），多根返回 refreshed 数组；
// 为忠实 v1，本方法返回原始 JSON，由产品按需解释。
func (c *Client) RefreshCatalog(ctx context.Context, req CatalogRefreshRequest) (json.RawMessage, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out json.RawMessage
	if err := c.do(ctx, http.MethodPost, "/api/catalog/refresh", req, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// CollectorStatus 是 GET /api/collector/status。
func (c *Client) CollectorStatus(ctx context.Context) (CollectorStatus, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out CollectorStatus
	if err := c.get(ctx, "/api/collector/status", &out); err != nil {
		return CollectorStatus{}, err
	}
	return out, nil
}

// CollectorHistory 是 GET /api/collector/history。
func (c *Client) CollectorHistory(ctx context.Context) (CollectorHistory, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out CollectorHistory
	if err := c.get(ctx, "/api/collector/history", &out); err != nil {
		return CollectorHistory{}, err
	}
	return out, nil
}

// CollectorDescribe 是 POST /api/collector/description。
func (c *Client) CollectorDescribe(ctx context.Context, req CollectorDescriptionRequest) error {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Ok
	return c.do(ctx, http.MethodPost, "/api/collector/description", req, &out)
}

// CollectorConfig 是 POST /api/collector/config。
func (c *Client) CollectorConfig(ctx context.Context, req CollectorConfigRequest) error {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Ok
	return c.do(ctx, http.MethodPost, "/api/collector/config", req, &out)
}

// CollectorFlush 是 POST /api/collector/flush。
func (c *Client) CollectorFlush(ctx context.Context) (CollectorFlushResponse, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out CollectorFlushResponse
	if err := c.do(ctx, http.MethodPost, "/api/collector/flush", nil, &out); err != nil {
		return CollectorFlushResponse{}, err
	}
	return out, nil
}

// CollectorRetry 是 POST /api/collector/retry。
func (c *Client) CollectorRetry(ctx context.Context) (CollectorRetryResponse, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out CollectorRetryResponse
	if err := c.do(ctx, http.MethodPost, "/api/collector/retry", nil, &out); err != nil {
		return CollectorRetryResponse{}, err
	}
	return out, nil
}

// SetPricingDefaults 是 PUT /api/pricing/defaults。
func (c *Client) SetPricingDefaults(ctx context.Context, req PricingDefaultsRequest) (PricingDefaultsResponse, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out PricingDefaultsResponse
	if err := c.do(ctx, http.MethodPut, "/api/pricing/defaults", req, &out); err != nil {
		return PricingDefaultsResponse{}, err
	}
	return out, nil
}

// PricingBatch 是 POST /api/pricing/batch。
func (c *Client) PricingBatch(ctx context.Context, req PricingBatchRequest) (PricingBatchResponse, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out PricingBatchResponse
	if err := c.do(ctx, http.MethodPost, "/api/pricing/batch", req, &out); err != nil {
		return PricingBatchResponse{}, err
	}
	return out, nil
}

// SetPricingFile 是 PUT /api/pricing/file。
func (c *Client) SetPricingFile(ctx context.Context, req PricingFileRequest) (PricingFileResponse, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out PricingFileResponse
	if err := c.do(ctx, http.MethodPut, "/api/pricing/file", req, &out); err != nil {
		return PricingFileResponse{}, err
	}
	return out, nil
}

// ClearPricingFile 是 DELETE /api/pricing/file?rootId=&path=。
func (c *Client) ClearPricingFile(ctx context.Context, rootID, path string) (PricingClearResponse, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out PricingClearResponse
	p := "/api/pricing/file?rootId=" + q(rootID) + "&path=" + q(path)
	if err := c.do(ctx, http.MethodDelete, p, nil, &out); err != nil {
		return PricingClearResponse{}, err
	}
	return out, nil
}

// ListTasks 是 GET /api/tasks。
func (c *Client) ListTasks(ctx context.Context) (TaskList, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out TaskList
	if err := c.get(ctx, "/api/tasks", &out); err != nil {
		return TaskList{}, err
	}
	return out, nil
}

// CreateTask 是 POST /api/tasks（按 seed 幂等）。请求已发送后失去响应
// 结果未知：调用者应查询原任务，不得自动重发 POST。
func (c *Client) CreateTask(ctx context.Context, seed string) (Task, error) {
	ctx, cancel := withTimeout(ctx, LongTimeout)
	defer cancel()
	var out Task
	if err := c.do(ctx, http.MethodPost, "/api/tasks", CreateTaskRequest{SeedHashHex: seed}, &out); err != nil {
		return Task{}, err
	}
	return out, nil
}

// GetTask 是 GET /api/tasks/{seed}。
func (c *Client) GetTask(ctx context.Context, seed string) (Task, error) {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Task
	if err := c.get(ctx, "/api/tasks/"+seed, &out); err != nil {
		return Task{}, err
	}
	return out, nil
}

// CancelTask 是 POST /api/tasks/{seed}/cancel。
func (c *Client) CancelTask(ctx context.Context, seed string) error {
	ctx, cancel := withTimeout(ctx, ShortTimeout)
	defer cancel()
	var out Ok
	return c.do(ctx, http.MethodPost, "/api/tasks/"+seed+"/cancel", nil, &out)
}

// TaskEvents 流式读取 /api/tasks/{seed}/events，直到 ctx 结束、
// 服务端关闭或 fn 返回 false。终态由服务端关闭流，无固定总超时。
func (c *Client) TaskEvents(ctx context.Context, seed string, fn func(Task) bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return fmt.Errorf("TaskEvents 需要回调函数")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/tasks/"+seed+"/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return transportErr("GET /api/tasks/"+seed+"/events", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return &HTTPError{Op: "GET /api/tasks/" + seed + "/events", Status: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	reader := bufio.NewReader(resp.Body)
	var data strings.Builder
	flush := func() error {
		if data.Len() == 0 {
			return nil
		}
		raw := strings.TrimSpace(data.String())
		data.Reset()
		if raw == "" {
			return nil
		}
		var task Task
		if err := json.Unmarshal([]byte(raw), &task); err != nil {
			return &ParseError{Op: "GET /api/tasks/" + seed + "/events", Err: err}
		}
		if !fn(task) {
			return io.EOF
		}
		return nil
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF && data.Len() == 0 {
				return nil
			}
			if data.Len() > 0 {
				if ferr := flush(); ferr != nil {
					if ferr == io.EOF {
						return nil
					}
					return ferr
				}
			}
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

func q(s string) string {
	// 查询参数最小编码：保留与现有 CLI 行为一致的直通语义由 net/url 完成。
	// 为避免引入额外依赖，此处做保守转义。
	r := strings.ReplaceAll(s, "%", "%25")
	r = strings.ReplaceAll(r, " ", "%20")
	r = strings.ReplaceAll(r, "&", "%26")
	r = strings.ReplaceAll(r, "?", "%3F")
	r = strings.ReplaceAll(r, "#", "%23")
	r = strings.ReplaceAll(r, "+", "%2B")
	return r
}

func itoa(n int64) string { return fmt.Sprintf("%d", n) }
