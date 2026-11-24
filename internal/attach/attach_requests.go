package attach

import (
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// PageRequest names a page whose details the metadata service reads.
// PageRequest 用于指定要由 metadata 服务读取详情的页面。
type PageRequest struct {
	URL string `json:"url"`
}

// Validate 校验请求字段。
func (in *PageRequest) Validate() error {
	if strings.TrimSpace(in.URL) == "" {
		return api.Err(400, "BAD_REQUEST", "A page URL is required")
	}
	return nil
}

// speechRequest names a message to read aloud: one of a stored chat, named
// with the content key, or of a device chat its client sends. Text a client
// selected, or a shared chat's message, is sent as it is. A device chat may
// leave its id out, as clients released before it was asked for do.
// speechRequest 指定要朗读的消息：可以是存储的 chat 中、用 content key 指名的消息，也可以是客户端发来的设备 chat 消息。客户端选中的文本或共享 chat 的消息会原样发送。设备 chat 可以省略其 id，因为在要求提供 id 之前发布的客户端就是这么做的。
type speechRequest struct {
	domain.ThreadSource
	api.KeyRequest
	ThreadID  string   `json:"threadId"`
	MessageID string   `json:"messageId"`
	Text      *string  `json:"text"`
	Speed     *float64 `json:"speed"`
}

// Validate 校验请求字段。
func (in *speechRequest) Validate() error {
	if speed := domain.ValueOr(in.Speed, domain.VoiceSpeed); speed < domain.MinSpeechSpeed || speed > domain.MaxSpeechSpeed {
		return api.Err(400, "BAD_REQUEST", "speed must be between 0.5 and 2")
	}
	if in.MessageID == "" {
		if strings.TrimSpace(domain.ValueOr(in.Text, "")) == "" {
			return api.Err(400, "BAD_REQUEST", "A message or text is required")
		}
		return nil
	}
	if !in.Local || in.ThreadID != "" {
		if err := api.RequireID("threadId", in.ThreadID); err != nil {
			return err
		}
	}
	if err := api.RequireID("messageId", in.MessageID); err != nil {
		return err
	}
	return in.ThreadSource.Validate()
}

// attachmentRequest downloads an attachment of a stored chat, or one a share
// names by the key its link carries.
// attachmentRequest 下载存储的 chat 中的 attachment，或由 share 用其链接所携带的 key 指名的 attachment。
type attachmentRequest struct {
	api.KeyRequest
	ID          string                `json:"id"`
	ThreadID    string                `json:"threadId"`
	AttKey      string                `json:"attKey"`
	Type        string                `json:"type"`
	Kind        string                `json:"kind"`
	FileName    string                `json:"fileName"`
	MimeType    string                `json:"mimeType"`
	TextContent string                `json:"textContent"`
	Pages       []domain.DocumentPage `json:"pages"`
}

// Validate 校验请求字段。
func (in *attachmentRequest) Validate() error {
	if err := api.RequireID("id", in.ID); err != nil {
		return err
	}
	if in.ThreadID != "" {
		return api.RequireID("threadId", in.ThreadID)
	}
	return nil
}

// Sent is an upload's descriptor as a turn sends it back, or the
// id alone of an attachment the thread already holds.
// Sent 是一个 Turn 回传的上传 descriptor，或该 thread 已持有的 attachment 的 id。
type Sent struct {
	ID          string                `json:"id"`
	Kind        string                `json:"kind,omitempty"`
	FileName    string                `json:"fileName,omitempty"`
	MimeType    string                `json:"mimeType,omitempty"`
	Size        *int64                `json:"size,omitempty"`
	Thumbnail   *string               `json:"thumbnail,omitempty"`
	TextContent string                `json:"textContent,omitempty"`
	Description string                `json:"description,omitempty"`
	Pages       []domain.DocumentPage `json:"pages,omitempty"`
	Base64      string                `json:"base64,omitempty"`
}

// Validate 校验请求字段。
func (a *Sent) Validate() error {
	if err := api.RequireID("id", a.ID); err != nil {
		return err
	}
	if a.Kind != "" && !slices.Contains(domain.AttachmentKinds, a.Kind) {
		return api.Err(400, "BAD_REQUEST", "An attachment kind must be image, document, or audio")
	}
	if a.whole() {
		return a.fitsUpload()
	}
	return nil
}

// whole reports whether the turn sends the attachment's descriptor rather
// than naming one its thread holds.
// whole 报告该 Turn 发送的是 attachment 的 descriptor，而不是仅指名其 thread 已持有的某个 attachment。
func (a *Sent) whole() bool { return a.Kind != "" }
