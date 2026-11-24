package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	tinfoil "github.com/tinfoilsh/tinfoil-go"
)

// Models 汇总 harness 所服务模型的相关状态：自动模型、catalog 缓存、模型 Offer、按角色索引的服务映射，以及 controlplane 地址。
type Models struct {
	Auto         *Model
	Catalog      CatalogCache
	Offer        Offer
	Services     map[string]*Service
	Controlplane string
}

// A model the harness serves through the router, which verifies the replica
// it relays each request to.
// 由 harness 通过 router 提供服务的模型，router 会验证它转发的每个请求所对应的 replica。
type Model struct {
	Endpoint string
	Name     string
	Vision   bool

	Client *http.Client // pinned to the attested router
	// Client 固定指向已 attest 的 router。
}

// chatCompletionsPath is where the router takes model turns.
// chatCompletionsPath 是 router 处理模型 Turn 的路径。
const chatCompletionsPath = "/v1/chat/completions"

// Requests without a caller's own cache secret fall back to this one, so the
// SDK never tries to persist a secret under the read-only home directory.
// 没有调用者自己的 cache secret 的请求会回退到这个值，这样 SDK 就不会尝试在只读的主目录下持久化 secret。
var cacheFallback = tinfoil.WithUserCacheSecret(api.Token())

// attestedTLS pins the TLS connection to the key an enclave's attestation
// names, so headers, status codes and bodiless requests, which EHBP leaves
// open, reach no one else. Every enclave the harness dials uses it.
// attestedTLS 将 TLS 连接固定到 enclave 的 attestation 所声明的 key 上，这样 header、状态码和无请求体的请求（EHBP 会让其保持打开）就不会到达其他任何人。harness 拨号访问的每个 enclave 都使用它。
var attestedTLS = tinfoil.WithTransport(tinfoil.TransportTLS)

// Attest verifies an enclave the harness dials directly.
// Attest 验证 harness 直接拨号访问的 enclave。
func Attest(ctx context.Context, enclave, repo, usageSecret string) (*http.Client, error) {
	return startupAttestation.run(ctx, func() (*http.Client, error) {
		verified, err := tinfoil.NewClientWithOptions(tinfoil.WithEnclave(enclave), tinfoil.WithRepo(repo), attestedTLS, cacheFallback)
		if err != nil {
			return nil, err
		}
		client := verified.HTTPClient()
		client.Transport = &auth.CallerTransport{Inner: client.Transport, UsageSecret: usageSecret}
		return client, nil
	})
}

// Offer holds the models the harness serves now.
// Offer 保存 harness 当前所服务的模型。
type Offer struct {
	mu     sync.RWMutex
	models []*Model
}

// Get 返回当前服务的模型列表。
func (c *Offer) Get() []*Model { c.mu.RLock(); defer c.mu.RUnlock(); return c.models }

// Set 设置当前服务的模型列表。
func (c *Offer) Set(models []*Model) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.models = models
}

// Served 返回模型名称的逗号分隔列表。
func Served(models []*Model) string {
	names := make([]string, len(models))
	for i, m := range models {
		names[i] = m.Name
	}
	return strings.Join(names, ", ")
}

// Service 是经由 URL 和 HTTP 客户端访问的一项服务。
type Service struct {
	URL    string
	Client *http.Client
}

// Boot verifies the router and reads the catalog of models the harness
// serves, which it keeps refreshing.
// Boot 验证 router 并读取 harness 所服务模型的 catalog，且会持续刷新它。
func (inf *Models) Boot(ctx context.Context, usageSecret string) error {
	host := os.Getenv("TINFOIL_ROUTER_ENCLAVE")
	if host == "" {
		return errors.New("TINFOIL_ROUTER_ENCLAVE is required")
	}
	client, err := Attest(ctx, host, "tinfoilsh/confidential-model-router", usageSecret)
	if err != nil {
		return fmt.Errorf("verify required router enclave %s: %w", host, err)
	}
	router := &Service{URL: "https://" + host, Client: client}
	inf.Services = map[string]*Service{"router": router}
	inf.Auto = &Model{Name: "auto", Client: router.Client, Endpoint: router.URL + chatCompletionsPath}
	if err := inf.RefreshCatalog(ctx); err != nil {
		return err
	}
	go func() {
		defer func() { api.Rescued("catalog maintenance", recover()) }()
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		refresh := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				refresh++
				// Every minute until the controlplane first lists a model, then every five.
				// 在 controlplane 首次列出模型之前每分钟刷新一次，之后每五分钟一次。
				if refresh%5 == 0 || len(inf.Offer.Get()) == 0 {
					if err := inf.RefreshCatalog(ctx); err != nil {
						slog.Warn("catalog refresh refused", "error", err)
					}
				}
			}
		}
	}()
	return nil
}

// ServiceReady 报告指定角色的服务是否就绪。
func (inf *Models) ServiceReady(role string) bool {
	service := inf.Services[role]
	return service != nil && service.Client != nil
}

type usageSinkKey struct{}

// WithUsageSink has the completions made under ctx tell sink of their usage.
// WithUsageSink 让在 ctx 下完成的 completion 向 sink 报告其用量。
func WithUsageSink(ctx context.Context, sink UsageSink) context.Context {
	return context.WithValue(ctx, usageSinkKey{}, sink)
}

// UsageSink is told of a completion the model accepted and the usage it
// reports.
// UsageSink 会被告知模型已接受的 completion 及其报告的用量。
type UsageSink interface {
	// Accepted 表示模型已接受该 completion。
	Accepted()
	// Meter 上报该 completion 的用量。
	Meter(usage json.RawMessage)
}

// Completion 向模型发送一次补全请求并返回其文本回复。
func (inf *Models) Completion(ctx context.Context, m *Model, payload api.Object) (string, error) {
	payload["stream"] = false
	// Without a scope of its own, a request would share the process-wide
	// cache secret with everyone holding the same credential.
	// 如果没有自己的作用域，请求就会与持有同一凭据的所有人共享进程级 cache secret。
	if _, scoped := payload["user_cache_secret"]; !scoped {
		payload["user_cache_secret"] = api.Token()
	}
	req, err := http.NewRequestWithContext(ctx, "POST", m.Endpoint, bytes.NewReader(api.Raw(payload)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", api.WithRetryAfter(api.DownstreamError(resp.StatusCode, b), resp.Header)
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(UsageSink); ok {
		if usage, ok := auth.Usage(ctx); ok && usage.BillCustomerRequest {
			sink.Accepted()
		}
	}
	var result struct {
		Usage   json.RawMessage `json:"usage"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, domain.MaxShownOutput)).Decode(&result); err != nil {
		return "", err
	}
	if len(result.Choices) == 0 {
		return "", api.Err(502, "UPSTREAM_REFUSED", "The completion is empty")
	}
	if sink, ok := ctx.Value(usageSinkKey{}).(UsageSink); ok && len(result.Usage) > 0 {
		sink.Meter(result.Usage)
	}
	return result.Choices[0].Message.Content, nil
}
