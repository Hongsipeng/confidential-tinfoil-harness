package pages

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	sdk "github.com/context-dot-dev/context-go-sdk"
	"github.com/context-dot-dev/context-go-sdk/option"
)

const (
	apiResponseSizeMultiplier = 2
	maxAPIResponseOverhead    = 64 * 1024
	maxSupportedBodyBytes     = (math.MaxInt64 - maxAPIResponseOverhead - 1) / apiResponseSizeMultiplier
)

// contextDev reads pages through the context.dev web scraping API
// (https://docs.context.dev/api-reference/web-scraping/html), which extracts
// a page's metadata, so the harness never contacts the page's host and
// never parses its HTML. Every context.dev call goes through it.
// contextDev 通过 context.dev 的网页抓取 API
// （https://docs.context.dev/api-reference/web-scraping/html）读取页面，该 API 会
// 提取页面元数据，因此 harness 既不会联系页面主机，也不会解析其 HTML。所有
// context.dev 调用都经由它。
type contextDev struct {
	sdk     sdk.Client
	timeout time.Duration
}

// scraped is a page's final URL and the metadata context.dev extracted.
// scraped 是页面的最终 URL 以及 context.dev 提取的元数据。
type scraped struct {
	URL         string
	ContentType string
	Title       string
	Description string
	SiteName    string
	Image       string
}

func newContextDev(apiKey string, timeout time.Duration, opts ...option.RequestOption) *contextDev {
	return &contextDev{
		sdk:     sdk.NewClient(append([]option.RequestOption{option.WithAPIKey(apiKey)}, opts...)...),
		timeout: timeout,
	}
}

// scrape reads targetURL with zero data retention enforced: zdr=enabled
// keeps the request out of shared caches and retained usage logs, and
// maxAgeMs=0 asks for a fresh scrape. Requests fail with ZDR_NOT_ENABLED
// until zero data retention is enabled for the organization.
// scrape 以强制零数据保留的方式读取 targetURL：zdr=enabled 使请求不进入共享缓存与
// 保留的使用日志，maxAgeMs=0 要求一次全新的抓取。在组织启用零数据保留之前，请求会
// 因 ZDR_NOT_ENABLED 而失败。
func (c *contextDev) scrape(ctx context.Context, targetURL string, maxBodyBytes int64) (*scraped, error) {
	if maxBodyBytes <= 0 || maxBodyBytes > maxSupportedBodyBytes {
		return nil, errors.New("invalid maximum body size")
	}
	maxAPIResponseBytes := maxBodyBytes*apiResponseSizeMultiplier + maxAPIResponseOverhead
	page, err := c.sdk.Web.WebScrapeHTML(ctx, sdk.WebWebScrapeHTMLParams{
		URL:      targetURL,
		MaxAgeMs: sdk.Int(0),
	},
		// The pinned SDK has no typed zdr parameter.
		// 当前固定的 SDK 没有类型化的 zdr 参数。
		option.WithQuery("zdr", "enabled"),
		option.WithHTTPClient(&limitedHTTPClient{
			inner:    &http.Client{Timeout: c.timeout},
			maxBytes: maxAPIResponseBytes,
		}),
	)
	if err != nil {
		var apiErr *sdk.Error
		if errors.As(err, &apiErr) {
			// apiErr's message holds the request's URL, which carries the
			// page's, and the raw response; neither may reach a log.
			// apiErr 的消息包含请求的 URL（进而包含页面 URL）与原始响应；两者都不得进入日志。
			return nil, fmt.Errorf("context.dev request failed with status %d", apiErr.StatusCode)
		}
		// A transport error's message holds the request's URL too.
		// 传输错误的消息同样包含请求的 URL。
		return nil, errors.New("context.dev request failed")
	}
	finalURL := page.Metadata.FinalURL
	if finalURL == "" {
		finalURL = page.URL
	}
	return &scraped{
		URL:         finalURL,
		ContentType: contentTypeFor(page.Type),
		Title:       page.Metadata.Title,
		Description: page.Metadata.Description,
		SiteName:    page.Metadata.SiteName,
		Image:       page.Metadata.Image,
	}, nil
}

// contentTypeFor maps the content type context.dev detected to a MIME type.
// contentTypeFor 将 context.dev 检测到的内容类型映射为 MIME 类型。
func contentTypeFor(detected sdk.WebWebScrapeHTMLResponseType) string {
	switch detected {
	case sdk.WebWebScrapeHTMLResponseTypeHTML:
		return "text/html"
	case sdk.WebWebScrapeHTMLResponseTypeXml:
		return "application/xml"
	case sdk.WebWebScrapeHTMLResponseTypeJson:
		return "application/json"
	case sdk.WebWebScrapeHTMLResponseTypeText:
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}

// limitedHTTPClient bounds how many response bytes the SDK reads, so a
// misbehaving upstream cannot exhaust the enclave's memory.
// limitedHTTPClient 限制 SDK 读取的响应字节数，使行为异常的 upstream 无法耗尽
// enclave 的内存。
type limitedHTTPClient struct {
	inner    *http.Client
	maxBytes int64
}

func (c *limitedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.inner.Do(req)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	// One extra byte lets a response of exactly maxBytes reach EOF while
	// anything larger fails on the next read.
	// 额外的一个字节使恰好为 maxBytes 的响应能读到 EOF，而更大的响应会在下一次读取时失败。
	resp.Body = &limitedReadCloser{inner: resp.Body, remaining: c.maxBytes + 1}
	return resp, nil
}

type limitedReadCloser struct {
	inner     io.ReadCloser
	remaining int64
}

func (l *limitedReadCloser) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, errors.New("context.dev response too large")
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.inner.Read(p)
	l.remaining -= int64(n)
	if l.remaining <= 0 {
		// The extra byte was read, so the response exceeds maxBytes even
		// when this read also reached EOF.
		// 已读到额外字节，说明响应超过 maxBytes，即使本次读取也到达了 EOF。
		return n, errors.New("context.dev response too large")
	}
	return n, err
}

func (l *limitedReadCloser) Close() error { return l.inner.Close() }
