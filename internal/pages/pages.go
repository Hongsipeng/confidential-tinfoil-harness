// Package pages reads what a link preview shows of a web page, through
// context.dev, and a site's favicon, through DuckDuckGo, so the harness never
// contacts a page's host. context.dev receives the page's URL, with zero data
// retention and no shared cache; DuckDuckGo receives only its host. Nothing is
// cached, and no URL is logged.
// Package pages 通过 context.dev 读取链接预览所展示的网页内容，通过 DuckDuckGo 读取
// 站点 favicon，因此 harness 绝不联系页面主机。context.dev 仅收到页面 URL（零数据
// 保留、无共享缓存）；DuckDuckGo 仅收到其主机名。不缓存任何内容，也不记录任何 URL。
package pages

import (
	"context"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/context-dot-dev/context-go-sdk/option"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

const (
	fetchTimeout               = 15 * time.Second
	maxPageBytes         int64 = 5 << 20
	maxFaviconBytes      int64 = 256 << 10
	duckDuckGoIcons            = "https://icons.duckduckgo.com"
	maxServerErrorStatus       = 599
)

// Status of a favicon the reader found, or found the site has none of.
// Status 是 reader 找到的 favicon 状态，或发现该站点没有 favicon 的状态。
const (
	FaviconFound   = "found"
	FaviconMissing = "missing"
)

// Reader reads pages and favicons for the harness's callers.
// Reader 为 harness 的调用方读取页面与 favicon。
type Reader struct {
	contextDev *contextDev
	icons      *http.Client
	iconsURL   string
}

// New reads pages with the harness's context.dev key.
// New 使用 harness 的 context.dev key 读取页面。
func New(apiKey string) *Reader {
	return newReader(apiKey, duckDuckGoIcons)
}

func newReader(apiKey, iconsURL string, contextDevOptions ...option.RequestOption) *Reader {
	return &Reader{
		contextDev: newContextDev(apiKey, fetchTimeout, contextDevOptions...),
		icons: &http.Client{
			Timeout: fetchTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		iconsURL: iconsURL,
	}
}

// Details is what a link preview shows of a page. A detail the page lacks
// is nil.
// Details 是链接预览所展示的页面信息。页面缺少的某一项为 nil。
type Details struct {
	// URL is where the page ended up, after its redirects.
	// URL 是页面在经历重定向后的最终地址。
	URL         string
	Title       *string
	Description *string
	SiteName    *string
	Image       *string
}

// Favicon is a site's favicon, as the harness's protocol sends it: Bytes
// and ContentType are empty when the site has none.
// Favicon 是站点的 favicon，按 harness 协议发送：当站点没有 favicon 时，Bytes 与
// ContentType 为空。
type Favicon struct {
	Status      string `json:"status"`
	Bytes       []byte `json:"favicon_bytes"`
	ContentType string `json:"favicon_content_type"`
}

func refused(err error) error {
	return api.Err(400, "BAD_REQUEST", "The page's URL is not allowed: "+err.Error())
}

// Details reads a page's details.
// Details 读取页面的详细信息。
func (r *Reader) Details(ctx context.Context, rawURL string) (*Details, error) {
	target, err := NormalizeURL(rawURL)
	if err != nil {
		return nil, refused(err)
	}
	unreadable := api.Err(502, "UPSTREAM_REFUSED", "The page's details could not be read")
	page, err := r.contextDev.scrape(ctx, target, maxPageBytes)
	if err != nil {
		return nil, unreadable
	}
	contentType, _, err := mime.ParseMediaType(page.ContentType)
	if err != nil || !strings.EqualFold(contentType, "text/html") {
		return nil, unreadable
	}
	pageURL := page.URL
	if pageURL == "" {
		pageURL = target
	}
	if pageURL, err = NormalizeURL(pageURL); err != nil {
		return nil, unreadable
	}
	base, err := url.Parse(pageURL)
	if err != nil {
		return nil, unreadable
	}
	details := &Details{URL: pageURL, Title: present(page.Title), Description: present(page.Description), SiteName: present(page.SiteName)}
	if image := strings.TrimSpace(page.Image); image != "" {
		if ref, err := url.Parse(image); err == nil {
			if resolved, err := NormalizeURL(base.ResolveReference(ref).String()); err == nil {
				details.Image = &resolved
			}
		}
	}
	return details, nil
}

func present(value string) *string {
	if value = strings.TrimSpace(value); value == "" {
		return nil
	}
	return &value
}

// Favicon reads the favicon of a page's site. A favicon DuckDuckGo cannot
// serve for now is refused with when to ask again.
// Favicon 读取页面所在站点的 favicon。DuckDuckGo 暂时无法提供的 favicon 会连同
// 重试时间一并被拒绝。
func (r *Reader) Favicon(ctx context.Context, rawURL string) (*Favicon, error) {
	target, err := NormalizeURL(rawURL)
	if err != nil {
		return nil, refused(err)
	}
	parsed, err := url.Parse(target)
	if err != nil {
		return nil, refused(err)
	}
	unavailable := api.Err(502, "UPSTREAM_REFUSED", "The site's favicon is unavailable")
	malformed := api.Err(502, "UPSTREAM_REFUSED", "The site's favicon is not an image")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.iconsURL+"/ip3/"+url.PathEscape(parsed.Hostname())+".ico", nil)
	if err != nil {
		return nil, malformed
	}
	resp, err := r.icons.Do(req)
	if err != nil {
		return nil, unavailable
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
		return &Favicon{Status: FaviconMissing, Bytes: []byte{}}, nil
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode >= http.StatusInternalServerError && resp.StatusCode <= maxServerErrorStatus:
		return nil, api.WithRetryAfter(unavailable, resp.Header)
	case resp.StatusCode != http.StatusOK:
		return nil, malformed
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFaviconBytes+1))
	if err != nil {
		return nil, unavailable
	}
	if len(body) == 0 || int64(len(body)) > maxFaviconBytes {
		return nil, malformed
	}
	contentType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(contentType, "image/") {
		if contentType = http.DetectContentType(body); !strings.HasPrefix(contentType, "image/") {
			return nil, malformed
		}
	}
	return &Favicon{Status: FaviconFound, Bytes: body, ContentType: contentType}, nil
}
