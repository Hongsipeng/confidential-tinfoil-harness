package inference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// CatalogCache 是 catalog 的并发安全缓存。
type CatalogCache struct {
	mu    sync.RWMutex
	value *domain.Catalog
}

// Get 返回缓存的 catalog。
func (c *CatalogCache) Get() *domain.Catalog { c.mu.RLock(); defer c.mu.RUnlock(); return c.value }

// Set 设置缓存的 catalog。
func (c *CatalogCache) Set(value *domain.Catalog) { c.mu.Lock(); defer c.mu.Unlock(); c.value = value }

func fetchConfig(ctx context.Context, client *http.Client, base, path string, out any) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", base+path, nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("config %s answered %d", path, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(out)
}

// RefreshCatalog 从 controlplane 重新拉取模型 catalog 并更新 Offer 与缓存。
func (inf *Models) RefreshCatalog(ctx context.Context) error {
	client := &http.Client{Timeout: 15 * time.Second}
	var config []api.Object
	if err := fetchConfig(ctx, client, inf.Controlplane, "/api/config/models", &config); err != nil {
		return err
	}
	c, models := buildCatalog(config, inf.Services["router"])
	// The first catalog may offer only Auto; a later one without models keeps the last.
	// 首次 catalog 可能只提供 Auto；之后若某次 catalog 没有模型，则保留上一次的结果。
	if len(c.Models) == 0 && inf.Catalog.Get() != nil {
		return errors.New("the catalog lists no chat model with a context window")
	}
	inf.Offer.Set(models)
	inf.Catalog.Set(c)
	return nil
}

// buildCatalog lists the chat models the controlplane configures, each served
// through the router.
// buildCatalog 列出 controlplane 所配置的 chat 模型，每个模型都通过 router 提供服务。
func buildCatalog(config []api.Object, router *Service) (*domain.Catalog, []*Model) {
	prompt := domain.BuiltInPrompt()
	c := &domain.Catalog{Prompt: prompt.SystemPrompt, Rules: prompt.Rules, WidgetHeader: prompt.GenUI.Header, EnabledWidgets: slices.Clone(prompt.GenUI.EnabledWidgets), Models: []api.Object{}, Presets: domain.BuiltInPresets()}
	var unsized []string
	var models []*Model
	for _, definition := range config {
		if !api.Bool(definition["chat"], false) || api.Str(definition["type"]) != "chat" {
			continue
		}
		name := api.Str(definition["modelName"])
		c.Listed = append(c.Listed, name)
		if api.Bool(definition["deprecated"], false) {
			c.Deprecated = append(c.Deprecated, definition)
			continue
		}
		// The configured window is the only prompt budget a model has.
		// 配置的上下文窗口是模型唯一拥有的 prompt 预算。
		configured := api.Int(api.Obj(definition["chatConfig"])["contextWindowTokens"])
		if configured <= 0 {
			unsized = append(unsized, name)
			continue
		}
		def := maps.Clone(definition)
		def["budgetWindow"] = configured
		c.Models = append(c.Models, def)
		models = append(models, &Model{Name: name, Vision: api.Bool(definition["multimodal"], false), Endpoint: router.URL + chatCompletionsPath, Client: router.Client})
	}
	if len(unsized) > 0 {
		slog.Warn("skipping controlplane chat models without a context window", "models", strings.Join(unsized, ", "))
	}
	return c, models
}
