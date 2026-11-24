// 本文件定义 API 返回的 thread 视图与分享、运行状态结构。
package domain

import (
	"encoding/json"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// What the API returns of a thread and its parts.
// API 返回的 thread 及其各部分的视图。

// ThreadSummary is a chat as its list shows it.
// ThreadSummary 是聊天在其列表中的展示形式。
type ThreadSummary struct {
	ID               string        `json:"id"`
	Title            string        `json:"title"`
	TitleState       string        `json:"titleState"`
	CreatedAt        *string       `json:"createdAt"`
	UpdatedAt        *string       `json:"updatedAt"`
	ActivityAt       string        `json:"activityAt"`
	ProjectID        *string       `json:"projectId"`
	Model            string        `json:"model"`
	WebSearchEnabled bool          `json:"webSearchEnabled"`
	Pinned           bool          `json:"pinned"`
	ContextUsage     *ContextUsage `json:"contextUsage"`
	ActiveRun        *ActiveRun    `json:"activeRun"`
	MessageCount     int           `json:"messageCount"`
	Flagged          bool          `json:"flagged,omitempty"`
}

// Summarize 把 thread 简化为列表视图。
func Summarize(t *Thread) ThreadSummary {
	s := ThreadSummary{ID: t.ID, Title: t.Title, TitleState: t.TitleState, CreatedAt: Present(t.CreatedAt), UpdatedAt: Present(t.UpdatedAt), ProjectID: t.ProjectID, Model: t.Model,
		WebSearchEnabled: t.WebSearchEnabled, Pinned: t.Pinned, ContextUsage: t.ContextUsage, MessageCount: t.MessageCount(), Flagged: t.Flagged}
	s.ActivityAt = t.activityAt()
	// One naming no instance was stored by a process from before runs
	// named theirs, whose runs ended when it did.
	// 未命名 instance 的标记，是由 runs 开始命名自己的 instance 之前的进程存储的，其 run
	// 随进程结束而结束。
	if run := t.ActiveRun; run != nil && run.Instance != "" {
		s.ActiveRun = &ActiveRun{RunID: run.RunID, ExpiresAt: run.ExpiresAt, Instance: run.Instance}
	}
	return s.Current()
}

// Current is the summary as it reads now: its run hint lapses with the
// run's deadline.
// Current 返回当前时刻读取的摘要：其 run 提示会随 run 的截止时间而过期。
func (s ThreadSummary) Current() ThreadSummary {
	if run := s.ActiveRun; run != nil {
		if expires, err := time.Parse(time.RFC3339Nano, run.ExpiresAt); err != nil || !api.Now().Before(expires) {
			s.ActiveRun = nil
		}
	}
	return s
}

// MessageCount is how many messages the chat shows. A compaction marker is
// shown, but is not one of its messages.
// MessageCount 是聊天展示的消息数量。compaction 标记会被展示，但不计入消息数。
func (t *Thread) MessageCount() int {
	count := 0
	for _, m := range t.Messages[min(len(t.Messages), t.HiddenCount):] {
		if m.Role != CompactionRole {
			count++
		}
	}
	return count
}

func (t *Thread) activityAt() string {
	latest, _ := time.Parse(time.RFC3339Nano, t.CreatedAt)
	for _, message := range t.Messages {
		if at, err := time.Parse(time.RFC3339Nano, message.CreatedAt); err == nil && at.After(latest) {
			latest = at
		}
	}
	if latest.IsZero() {
		return t.CreatedAt
	}
	return latest.Format(time.RFC3339Nano)
}

// Present is a text field a view shows as null when it is empty.
// Present 把空文本转为 nil，使视图以 null 展示该字段。
func Present(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ThreadWindow is a chat shown from its latest messages back.
// ThreadWindow 是从最新消息开始向前展示的聊天窗口。
type ThreadWindow struct {
	ThreadSummary
	Revision string        `json:"revision"`
	PresetID *string       `json:"presetId"`
	Messages []MessageView `json:"messages"`
	HasOlder bool          `json:"hasOlder"`
}

// A chat is shown in windows of its latest messages.
// 聊天以最新消息的窗口形式展示。
const (
	MessageWindow    = 50
	MaxMessageWindow = 200
)

// Window shows the messages before the one named, or the latest.
// Window 展示指定消息之前的消息；未指定则展示最新消息。
func Window(row *ThreadRow, before string, limit int) ThreadWindow {
	t := &row.Thread
	all := t.Messages[min(len(t.Messages), t.HiddenCount):]
	end := len(all)
	if before != "" {
		for i, m := range all {
			if m.ID == before {
				end = i
				break
			}
		}
	}
	if limit <= 0 || limit > MaxMessageWindow {
		limit = MessageWindow
	}
	start := max(0, end-limit)
	messages := []MessageView{}
	for i := range all[start:end] {
		messages = append(messages, ViewMessage(&all[start+i]))
	}
	return ThreadWindow{ThreadSummary: Summarize(t), Revision: row.ETag, PresetID: t.PresetID, Messages: messages, HasOlder: start > 0}
}

// MessageView is a message as a client shows it: a question's text or a
// reply's timeline, and its attachments without their content.
// MessageView 是客户端展示消息的形式：提问的文本或回复的 timeline，以及不含内容的附件。
type MessageView struct {
	ID               string          `json:"id"`
	Role             string          `json:"role"`
	CreatedAt        string          `json:"createdAt"`
	Quote            string          `json:"quote,omitempty"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	IsInterrupted    *bool           `json:"isInterrupted,omitempty"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	Annotations      []Annotation    `json:"annotations,omitzero"`
	Timeline         []Block         `json:"timeline,omitzero"`
	Content          *string         `json:"content,omitempty"`
	Attachments      []Attachment    `json:"attachments,omitempty"`
	Answers          []WidgetAnswers `json:"answers,omitzero"`
	Compaction       *Compaction     `json:"compaction,omitempty"`
	Research         *Research       `json:"research,omitempty"`
}

// ViewMessage 把消息转为客户端视图形式。
func ViewMessage(m *Message) MessageView {
	view := MessageView{ID: m.ID, Role: m.Role, CreatedAt: m.CreatedAt, Quote: m.Quote, Model: m.Model, ModelDisplayName: m.ModelDisplayName,
		IsError: m.IsError, IsInterrupted: m.IsInterrupted, Usage: m.Usage, Annotations: m.Annotations, Answers: m.Answers, Compaction: m.Compaction, Research: m.Research}
	if m.Role == "assistant" {
		view.Timeline = CheckedWidgets(m.Timeline)
		if view.Timeline == nil {
			view.Timeline = []Block{}
		}
	} else {
		view.Content = &m.Content
	}
	for i := range m.Attachments {
		view.Attachments = append(view.Attachments, ViewAttachment(&m.Attachments[i]))
	}
	return view
}

// ViewAttachment is an attachment as a client lists it: its metadata,
// thumbnail and any text it holds inline, without its blob's key or its
// bytes. A text-only model reads an image through its description.
// ViewAttachment 是客户端列出的附件：其元数据、缩略图以及内联的文本，但不含其 blob 的
// key 或字节。纯文本模型通过描述读取图像。
func ViewAttachment(a *Attachment) Attachment {
	return Attachment{ID: a.ID, Kind: a.Kind, FileName: a.FileName, MimeType: a.MimeType, Size: a.Size, Thumbnail: a.Thumbnail, TextContent: a.TextContent, Description: a.Description}
}

// DeviceTranscript is a stored chat as its device keeps it, without the
// fields the harness owns.
// DeviceTranscript 是设备所保存的已存储聊天，不含 harness 自有的字段。
type DeviceTranscript struct {
	Thread
	ActiveRun    *ActiveRun    `json:"activeRun,omitempty"`
	ProjectID    *string       `json:"projectId,omitempty"`
	ContextUsage *ContextUsage `json:"contextUsage,omitempty"`
	HiddenCount  int           `json:"harnessHiddenCount,omitempty"`
	ImportedFrom *ImportOrigin `json:"_restore,omitempty"`
}

// SharePayload is what a share link opens, sealed under its key: each
// attachment's thumbnail or text, and the key of its blob, which buckets keeps
// for the share.
// SharePayload 是分享链接打开的内容，以该分享的 key 密封：每个附件的缩略图或文本，以及
// 其 blob 的 key（由 buckets 为该分享保管）。
type SharePayload struct {
	V         int            `json:"v"`
	Title     string         `json:"title"`
	Messages  []ShareMessage `json:"messages"`
	CreatedAt int64          `json:"createdAt"`
}

type ShareMessage struct {
	Role             string          `json:"role"`
	Content          string          `json:"content"`
	Timestamp        int64           `json:"timestamp"`
	Timeline         []Block         `json:"timeline"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	Quote            string          `json:"quote,omitempty"`
	Annotations      []Annotation    `json:"annotations,omitempty"`
	Attachments      []Attachment    `json:"attachments,omitempty"`
	Answers          []WidgetAnswers `json:"answers,omitempty"`
}

// SharedThread is an opened share, whose attachments name the key that
// reads each blob.
// SharedThread 是已打开的分享，其附件指明读取每个 blob 所用的 key。
type SharedThread struct {
	Title     string          `json:"title"`
	CreatedAt *string         `json:"createdAt"`
	Messages  []SharedMessage `json:"messages"`
}

type SharedMessage struct {
	MessageView
	Attachments []SharedAttachment `json:"attachments,omitempty"`
}

type SharedAttachment struct {
	Attachment
	AttKey *string `json:"attKey"`
}

// StatePatch is one change a run makes to its client's state.
// StatePatch 是 run 对其客户端状态所做的一处变更。
type StatePatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// RunMetadata is what a run's last event reports.
// RunMetadata 是 run 最后一个事件所报告的内容。
type RunMetadata struct {
	Messages  []CommittedReply `json:"messages,omitzero"`
	Thread    *Thread          `json:"thread,omitempty"`
	Usage     *api.Usage       `json:"usage,omitempty"`
	RateLimit *api.RateLimit   `json:"rateLimit,omitempty"`
	Cancelled *bool            `json:"cancelled,omitempty"`
	// Truncated is set when the model stopped the reply because it reached
	// its length limit (finish_reason length), so the run committed the reply
	// interrupted rather than finished. A cancelled run never sets it.
	// Truncated 在模型因达到长度上限（finish_reason length）而停止回复时设置，此时 run
	// 提交的是被中断的回复而非完成的回复。被取消的 run 不会设置它。
	Truncated bool `json:"truncated,omitempty"`
}

// CommittedReply is what a run commits to a reply beyond its streamed
// content, so a client that stores the thread itself stores the same.
// CommittedReply 是 run 在流式内容之外提交给回复的内容，这样自行存储 thread 的客户端能
// 存储相同的内容。
type CommittedReply struct {
	ID               string          `json:"id"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	IsInterrupted    *bool           `json:"isInterrupted,omitempty"`
	Annotations      []Annotation    `json:"annotations,omitempty"`
	Research         *Research       `json:"research,omitempty"`
}

// QueueItem is a turn waiting behind its chat's running reply.
// QueueItem 是排在其聊天正在运行的回复之后等待的 turn。
type QueueItem struct {
	QueueID     string             `json:"queueId"`
	Kind        string             `json:"kind"`
	Content     *string            `json:"content"`
	Quote       *string            `json:"quote"`
	Attachments []QueuedAttachment `json:"attachments"`
	CreatedAt   string             `json:"createdAt"`
}

type QueuedAttachment struct {
	ID        string  `json:"id,omitempty"`
	Kind      string  `json:"kind,omitempty"`
	FileName  string  `json:"fileName,omitempty"`
	MimeType  string  `json:"mimeType,omitempty"`
	Size      *int64  `json:"size,omitempty"`
	Thumbnail *string `json:"thumbnail,omitempty"`
}

// StateSnapshot is a run's chat, its queue, and the caller's quota.
// StateSnapshot 是 run 的聊天、其队列以及调用方的 quota。
type StateSnapshot struct {
	Thread    ThreadSummary `json:"thread"`
	Queue     []QueueItem   `json:"queue"`
	RateLimit api.RateLimit `json:"rateLimit"`
}
