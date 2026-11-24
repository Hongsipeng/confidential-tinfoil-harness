// Package remoteimage fetches public images without forwarding caller metadata.
// Package remoteimage 抓取公开图片，而不转发调用方的元数据。
package remoteimage

import (
	"bytes"
	"context"
	"errors"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"code.dny.dev/ssrf"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/pages"
	_ "golang.org/x/image/webp"
)

const (
	maxURLBytes    = 8192
	maxImageBytes  = 8 << 20
	maxImagePixels = 20_000_000
	maxImageSide   = 16_384
	// Decoding and orientation can each hold a full-size raster.
	// 解码与方向校正各需要持有一份全尺寸位图。
	maxConcurrentFetches = 8
	maxRedirects         = 3
	busyRetrySeconds     = 5
	fetchTimeout         = 15 * time.Second
	idleTimeout          = time.Minute
	maxHeaderBytes       = 32 << 10
	httpsPort            = 443
	imageContentType     = "image/png"
	imageAccept          = "image/png, image/jpeg, image/webp, image/gif"
	imageUserAgent       = "TinfoilImageFetcher/1.0"
)

// Image 是一张图片的字节内容及其内容类型。
type Image struct {
	Bytes       []byte `json:"image_bytes"`
	ContentType string `json:"image_content_type"`
}

// Client 通过受控的 HTTPS 抓取并规范化远程图片。
type Client struct {
	http  *http.Client
	slots chan struct{}
}

// New 构造一个对出站连接施加 SSRF 防护的图片抓取客户端。
func New() *Client {
	guard := ssrf.New(ssrf.WithPorts(httpsPort))
	dialer := &net.Dialer{Timeout: fetchTimeout, Control: guard.Safe}
	return &Client{
		http: &http.Client{
			Timeout:       fetchTimeout,
			CheckRedirect: checkRedirect,
			Transport: &http.Transport{
				DialContext:            dialer.DialContext,
				ForceAttemptHTTP2:      true,
				DisableCompression:     true,
				MaxConnsPerHost:        maxConcurrentFetches,
				MaxIdleConns:           maxConcurrentFetches,
				MaxIdleConnsPerHost:    maxConcurrentFetches,
				IdleConnTimeout:        idleTimeout,
				MaxResponseHeaderBytes: maxHeaderBytes,
			},
		},
		slots: make(chan struct{}, maxConcurrentFetches),
	}
}

func imageURL(raw string) (*url.URL, error) {
	if len(raw) > maxURLBytes {
		return nil, errors.New("the image URL is too long")
	}
	normalized, err := pages.NormalizeURL(raw)
	if err != nil {
		return nil, err
	}
	target, err := url.Parse(normalized)
	if err != nil || target.Scheme != "https" || target.Port() != "" && target.Port() != strconv.Itoa(httpsPort) {
		return nil, errors.New("images require HTTPS on its standard port")
	}
	return target, nil
}

func imageHeaders() http.Header {
	return http.Header{"Accept": {imageAccept}, "User-Agent": {imageUserAgent}}
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > maxRedirects {
		return errors.New("too many image redirects")
	}
	target, err := imageURL(req.URL.String())
	if err != nil {
		return err
	}
	req.URL, req.Header = target, imageHeaders()
	return nil
}

// Fetch returns a bounded PNG copy. Decoding drops active formats and metadata;
// an animated image contributes only its first frame.
// Fetch 返回一张有界大小的 PNG 副本。解码会丢弃有源格式与元数据；动图只保留首帧。
func (c *Client) Fetch(ctx context.Context, rawURL string) (*Image, error) {
	target, err := imageURL(rawURL)
	if err != nil {
		return nil, api.Err(400, "BAD_REQUEST", "The image URL is not allowed")
	}
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	default:
		e := api.Err(503, "UPSTREAM_REFUSED", "Image loading is at capacity; retry shortly")
		e.RetryAfter = busyRetrySeconds
		return nil, e
	}
	unavailable := api.Err(502, "UPSTREAM_REFUSED", "The image could not be loaded")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, unavailable
	}
	request.Header = imageHeaders()
	response, err := c.http.Do(request)
	if err != nil {
		return nil, unavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ContentLength > maxImageBytes {
		return nil, unavailable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxImageBytes+1))
	defer clear(body)
	if err != nil || len(body) == 0 || len(body) > maxImageBytes {
		return nil, unavailable
	}
	switch http.DetectContentType(body) {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
	default:
		return nil, unavailable
	}
	config, _, err := image.DecodeConfig(bytes.NewReader(body))
	if err != nil || config.Width <= 0 || config.Height <= 0 ||
		config.Width > maxImageSide || config.Height > maxImageSide || config.Width > maxImagePixels/config.Height {
		return nil, unavailable
	}
	decoded, format, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		return nil, unavailable
	}
	if format == "jpeg" {
		decoded = orientJPEG(decoded, body)
	}
	var encoded boundedImage
	if err := png.Encode(&encoded, decoded); err != nil {
		return nil, unavailable
	}
	return &Image{Bytes: encoded.Bytes(), ContentType: imageContentType}, nil
}

type boundedImage struct{ buffer bytes.Buffer }

func (b *boundedImage) Bytes() []byte { return b.buffer.Bytes() }

func (b *boundedImage) Len() int { return b.buffer.Len() }

func (b *boundedImage) Write(p []byte) (int, error) {
	if len(p) > maxImageBytes-b.Len() {
		return 0, errors.New("the image is too large")
	}
	return b.buffer.Write(p)
}
