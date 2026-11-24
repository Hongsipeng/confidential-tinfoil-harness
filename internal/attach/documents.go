package attach

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A stored document's blob holds its extracted content as JSON, and the
// chat row keeps only metadata and the blob key. The control plane deletes
// any blob a chat stops referencing, so every attachment a row names must be
// stored under that row's chat. Every client reads and writes this format.
// 已存储文档的 blob 以 JSON 形式保存其提取出的内容，chat 行只保留元数据和 blob key。控制面会删除 chat 不再引用的任何 blob，因此某行所指的每个 attachment 都必须存储在该行的 chat 之下。所有客户端都读写此格式。

// DocumentBlob is a document's extracted content, as its blob holds it.
// DocumentBlob 是文档提取出的内容，如其在 blob 中所保存的那样。
type DocumentBlob struct {
	Pages       []domain.DocumentPage `json:"pages,omitempty"`
	TextContent string                `json:"textContent,omitempty"`
}

// content is the extracted content an attachment holds inline.
// content 是 attachment 内联保存的提取内容。
func Content(a *domain.Attachment) DocumentBlob {
	return DocumentBlob{TextContent: a.TextContent, Pages: a.Pages}
}

func (d DocumentBlob) empty() bool { return d.TextContent == "" && len(d.Pages) == 0 }

func encodeDocument(a *domain.Attachment) ([]byte, error) {
	payload := DocumentBlob{TextContent: a.TextContent}
	for _, page := range a.Pages {
		image := page.Image
		if strings.HasPrefix(image, "data:") {
			_, image, _ = strings.Cut(image, ",")
		}
		payload.Pages = append(payload.Pages, domain.DocumentPage{Page: max(0, page.Page), Text: page.Text, Image: image, IsScanned: page.IsScanned})
	}
	if payload.empty() {
		return nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The document has no extracted content")
	}
	return api.Raw(payload), nil
}

// DecodeDocument 从 blob 中解码出文档的提取内容。
func DecodeDocument(data []byte) (DocumentBlob, error) {
	if trimmed := bytes.TrimSpace(data); len(trimmed) > 0 && trimmed[0] == '{' {
		var payload struct {
			TextContent *string                `json:"textContent"`
			Pages       *[]domain.DocumentPage `json:"pages"`
		}
		if json.Unmarshal(trimmed, &payload) == nil && (payload.TextContent != nil || payload.Pages != nil) {
			return DocumentBlob{TextContent: domain.ValueOr(payload.TextContent, ""), Pages: domain.ValueOr(payload.Pages, []domain.DocumentPage{})}, nil
		}
	}
	return DocumentBlob{}, api.Err(422, "ATTACHMENT_UNSUPPORTED", "The stored document cannot be read")
}

// DocumentText 返回文档 blob 的纯文本形式。
func DocumentText(content DocumentBlob) string {
	if content.TextContent != "" {
		return content.TextContent
	}
	parts := []string{}
	for _, page := range content.Pages {
		if page.Text != "" {
			parts = append(parts, page.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}

// Unreachable is whether an attachment's content is gone or unusable: its
// blob is not held, its key does not open it, or what it holds cannot be
// read as its kind.
// Unreachable 表示 attachment 的内容是否已丢失或不可用：其 blob 未被持有、key 无法打开它，或其内容无法按其类型读取。
func Unreachable(err error) bool {
	e := api.AsError(err)
	return e.HTTP == 404 || e.Code == "ATTACHMENT_UNSUPPORTED"
}

// PlaceAttachments stores every attachment of a thread under the thread, in
// the protocol 3 format, before the thread is pushed. It keeps the content
// inline in the given copy, which is the one a prompt is built from.
// PlaceAttachments 在 thread 被 push 之前，以 protocol 3 格式把该 thread 的每个 attachment 存到该 thread 之下。它在给定的副本中内联保留内容，该副本正是构建 prompt 所用的那份。
func (at *Service) PlaceAttachments(ctx context.Context, p *auth.Principal, key auth.ContentKey, threadID string, t *domain.Thread) error {
	defer clear(key[:])
	return t.EachAttachment(func(a *domain.Attachment) error { return at.placeAttachment(ctx, p, key, threadID, a) })
}

func (at *Service) placeAttachment(ctx context.Context, p *auth.Principal, key auth.ContentKey, threadID string, a *domain.Attachment) error {
	defer clear(key[:])
	keyed := a.StorageKey() != ""
	elsewhere := !domain.EnclaveAttachmentID.MatchString(a.ID) || cmp.Or(a.StorageThreadID, threadID) != threadID
	var data []byte
	switch {
	case !a.IsImage() && a.HasContent():
		// A new upload, or a row the web or iOS app wrote before it stored
		// documents as blobs, keeps the content inline.
		// 新上传，或 Web/iOS 应用在把文档存为 blob 之前写入的行，会内联保留内容。
		payload, err := encodeDocument(a)
		if err != nil {
			return err
		}
		data = payload
	case a.IsImage() && a.Base64 != "" && (!keyed || elsewhere):
		decoded, err := base64.StdEncoding.DecodeString(a.Base64)
		if err != nil {
			return api.Err(400, "ATTACHMENT_UNSUPPORTED", "An image is not valid base64")
		}
		data = decoded
	case keyed && elsewhere:
		stored, err := at.Bytes(ctx, p, a)
		if err == nil && !a.IsImage() {
			var content DocumentBlob
			content, err = DecodeDocument(stored)
			clear(stored)
			if err == nil {
				stored, err = encodeDocument(&domain.Attachment{TextContent: content.TextContent, Pages: content.Pages})
			}
		}
		if Unreachable(err) {
			// The control plane rejects a push naming an id that is not an enclave id, so
			// such a reference is retired now. Any other unavailable one fails
			// the push as missing, and RepairAttachments then retires it.
			// 控制面会拒绝指向非 enclave id 的 push，因此这类引用现在就被退役。其他不可用的引用会使 push 因缺失而失败，随后由 RepairAttachments 将其退役。
			if !domain.EnclaveAttachmentID.MatchString(a.ID) {
				retireAttachment(a)
			}
			return nil
		}
		if err != nil {
			return err
		}
		data = stored
	default:
		return nil
	}
	defer clear(data)
	storedID, attachmentKey, err := at.Store.PutAttachment(ctx, p, key, threadID, a.ID, data)
	if err != nil {
		return err
	}
	a.ID, a.EncryptionKey, a.StorageThreadID, a.Key = storedID, attachmentKey, threadID, ""
	return nil
}

// UnavailableDocument 返回表示某文档已不可用的提示文本。
func UnavailableDocument(a *domain.Attachment) string {
	return "[The document " + cmp.Or(a.FileName, "attachment") + " is no longer available.]"
}

// HydrateDocuments reads offloaded documents into a working copy of a thread.
// A document whose blob is gone is described as unavailable rather than
// failing every later turn in its chat.
// HydrateDocuments 把已外置的文档读入 thread 的工作副本。blob 已丢失的文档会被描述为不可用，而不是使其 chat 中后续的每个 Turn 都失败。
func (at *Service) HydrateDocuments(ctx context.Context, p *auth.Principal, t *domain.Thread) error {
	return t.EachAttachment(func(a *domain.Attachment) error {
		if a.IsImage() || a.HasContent() {
			return nil
		}
		if a.StorageKey() == "" {
			a.TextContent = UnavailableDocument(a)
			return nil
		}
		if err := at.InflateDocument(ctx, p, a); err != nil {
			if !Unreachable(err) {
				return err
			}
			a.TextContent = UnavailableDocument(a)
		}
		return nil
	})
}

// InflateDocument reads a document's content from its blob into it.
// InflateDocument 从文档的 blob 中读取其内容并写入文档本身。
func (at *Service) InflateDocument(ctx context.Context, p *auth.Principal, a *domain.Attachment) error {
	data, err := at.Bytes(ctx, p, a)
	if err != nil {
		return err
	}
	defer clear(data)
	content, err := DecodeDocument(data)
	if err != nil {
		return err
	}
	a.TextContent, a.Pages = content.TextContent, content.Pages
	return nil
}

// RepairAttachments makes a row pushable when the control plane names
// references it does not hold under the row's chat: a row the web or iOS app wrote before the store
// checked references may point at blobs filed under another chat or deleted
// since.
// RepairAttachments 在控制面指出某行引用了该行 chat 之下并未持有的引用时，使其可被 push：在 store 校验引用之前由 Web/iOS 应用写入的行，可能指向存于其他 chat 之下或已被删除的 blob。
func (at *Service) RepairAttachments(ctx context.Context, p *auth.Principal, key auth.ContentKey, t *domain.Thread, missing []string) error {
	defer clear(key[:])
	return t.EachAttachment(func(a *domain.Attachment) error {
		if !slices.Contains(missing, a.ID) {
			return nil
		}
		a.StorageThreadID = "elsewhere"
		if err := at.placeAttachment(ctx, p, key, t.ID, a); err != nil {
			return err
		}
		if a.StorageThreadID != t.ID {
			retireAttachment(a)
		}
		return nil
	})
}

// An attachment without its blob keeps its metadata, and an image its
// thumbnail.
// 没有 blob 的 attachment 保留其元数据，图像则保留其缩略图。
func retireAttachment(a *domain.Attachment) {
	a.EncryptionKey, a.Key, a.StorageThreadID = "", "", ""
}

// StripInlineAttachments keeps only metadata and keys in a thread that is
// about to be stored. Content stays inline only for attachments without a
// blob.
// StripInlineAttachments 在 thread 即将存储时只保留元数据和 key。只有没有 blob 的 attachment 才会内联保留内容。
func StripInlineAttachments(t *domain.Thread) {
	t.EachAttachment(func(a *domain.Attachment) error {
		a.InferenceMimeType = ""
		if a.StorageKey() == "" {
			return nil
		}
		a.Base64 = ""
		if !a.IsImage() {
			a.TextContent, a.Pages = "", nil
		}
		return nil
	})
}
