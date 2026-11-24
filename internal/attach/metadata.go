package attach

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/pages"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/remoteimage"
)

// Pages reads what a link preview shows of a page, and a site's favicon.
// Pages 读取链接预览所展示的页面内容，以及站点的 favicon。
type Pages interface {
	Details(ctx context.Context, url string) (*pages.Details, error)
	Favicon(ctx context.Context, url string) (*pages.Favicon, error)
}

// Images 按 URL 抓取远程图像。
type Images interface {
	Fetch(context.Context, string) (*remoteimage.Image, error)
}

// Image 抓取指定 URL 的远程图像。
func (at *Service) Image(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *PageRequest) (any, error) {
	if _, err := at.Auth.Inference(ctx, p); err != nil {
		return nil, err
	}
	return at.Images.Fetch(ctx, in.URL)
}

// Favicon reads a site's favicon for a caller with chat access.
// Favicon 为具有 chat 访问权限的调用方读取站点的 favicon。
func (at *Service) Favicon(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *PageRequest) (any, error) {
	if _, err := at.Auth.Inference(ctx, p); err != nil {
		return nil, err
	}
	return at.Pages.Favicon(ctx, in.URL)
}

// LinkPreview reads a page's details for a link preview that holds none, as
// the previous web client's did, for a caller with chat access.
// LinkPreview 为具有 chat 访问权限的调用方读取页面详情，用于没有任何内容的链接预览，与旧 Web 客户端的做法一致。
func (at *Service) LinkPreview(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *PageRequest) (any, error) {
	if _, err := at.Auth.Inference(ctx, p); err != nil {
		return nil, err
	}
	return at.LinkDetails(ctx, in.URL)
}

// LinkDetails are a page's details, as a link preview shows them.
// LinkDetails 是页面的详情，即链接预览所展示的内容。
func (at *Service) LinkDetails(ctx context.Context, url string) (any, error) {
	details, err := at.Pages.Details(ctx, url)
	if err != nil {
		return nil, err
	}
	return api.Object{"url": url, "title": details.Title, "description": details.Description, "siteName": details.SiteName, "image": details.Image}, nil
}
