// 本文件定义 thread（聊天）的存储结构：消息、block、附件与 compaction 标记。
package domain

import (
	"cmp"
	"encoding/json"
	"slices"
)

// RecordVersion is the format of the rows and device records this harness
// writes. A change to what they hold changes it.
// RecordVersion 是本 harness 写入的 row 与设备记录的格式。它们所持有的内容一旦变化，
// 该值也随之变化。
const RecordVersion = 2

// readVersions are the formats a record is read in as written: version 1,
// and version 2, which adds compaction markers to chats and is otherwise the
// same.
// readVersions 是读取记录时按其写入格式识别的版本：版本 1，以及版本 2——版本 2 为聊天
// 增加了 compaction 标记，其余部分相同。
var readVersions = []int{1, 2}

// Thread is a chat in the thread format: what the harness stores in the cloud,
// what a device keeps, and what a device chat's run returns.
// Thread 是 thread 格式的聊天：本 harness 在云端存储的内容、设备保留的内容，以及设备
// 聊天 run 返回的内容。
type Thread struct {
	ID               string        `json:"id"`
	Title            string        `json:"title"`
	TitleState       string        `json:"titleState"`
	CreatedAt        string        `json:"createdAt,omitempty"`
	UpdatedAt        string        `json:"updatedAt,omitempty"`
	Model            string        `json:"model"`
	ProjectID        *string       `json:"projectId"`
	PresetID         *string       `json:"presetId"`
	Pinned           bool          `json:"pinned"`
	WebSearchEnabled bool          `json:"webSearchEnabled"`
	Messages         []Message     `json:"messages"`
	LastRequestID    string        `json:"lastRequestId,omitempty"`
	LastRequestKind  string        `json:"lastRequestKind,omitempty"` // what that request's turn did, which a retry repeats；该请求的 turn 做了什么，重试时会重复。
	ActiveRun        *ActiveRun    `json:"activeRun"`
	ContextUsage     *ContextUsage `json:"contextUsage,omitempty"`
	Flagged          bool          `json:"flagged,omitempty"` // the safeguards flagged the chat, which takes no more turns；safeguards 标记了该聊天，它不再接收更多 turn。
	// HiddenCount is how many leading messages an ask's thread holds as
	// context without showing them.
	// HiddenCount 是 ask 的 thread 作为上下文持有、但不展示的前置消息数量。
	HiddenCount   int `json:"harnessHiddenCount,omitempty"`
	RecordVersion int `json:"recordVersion,omitempty"`
	// ImportedFrom is set on a stored chat an import made, so the same
	// import run again finds it. No client is sent it.
	// ImportedFrom 设置在导入生成的已存储聊天上，以便再次导入时能找到它。不会发送给任何
	// 客户端。
	ImportedFrom *ImportOrigin `json:"_restore,omitempty"`
}

// Message is one turn of a chat. Its pointer fields are absent unless set.
// Message 是聊天的一个 turn。其指针字段除非已设置，否则不出现。
type Message struct {
	ID               string          `json:"id"`
	Role             string          `json:"role"`
	CreatedAt        string          `json:"createdAt"`
	Content          string          `json:"content"`
	Quote            string          `json:"quote,omitempty"`
	Model            string          `json:"model,omitempty"`
	ModelDisplayName string          `json:"modelDisplayName,omitempty"`
	Timeline         []Block         `json:"timeline,omitzero"`
	Attachments      []Attachment    `json:"attachments,omitzero"`
	Annotations      []Annotation    `json:"annotations,omitzero"`
	Usage            json.RawMessage `json:"usage,omitempty"`
	IsError          *bool           `json:"isError,omitempty"`
	IsInterrupted    *bool           `json:"isInterrupted,omitempty"`
	// Answers are what the user had answered of each questions widget whose
	// answers changed since an earlier message, as this message sent them.
	// Answers 是用户对每个 questions widget 所做的回答（自早先消息以来答案有变化的那些），
	// 即本条消息发送时的答案。
	Answers []WidgetAnswers `json:"answers,omitzero"`
	// Compaction is set on a compaction marker, whose content is its summary.
	// Compaction 设置在 compaction 标记上，其内容即摘要。
	Compaction *Compaction `json:"compaction,omitempty"`
	// Research is set on a question that asks for deep research, and on a
	// reply that researched its question, whose content is its report.
	// Research 设置在请求深度研究的提问上，也设置在对其提问做过研究的回复上，其内容即报告。
	Research *Research `json:"research,omitempty"`
}

// Research is what a question that asks for deep research, or a reply that
// researched its question, records of it. The model reads such a reply back
// as its report alone: what it searched and read stays in the reply's
// timeline for the user, not in later prompts.
// Research 是请求深度研究的提问，或对其提问做过研究的回复，所记录的研究信息。模型会把
// 这类回复仅读作其报告：它搜索与阅读的内容留在回复的 timeline 中供用户查看，而不会进入
// 后续的 prompt。
type Research struct {
	// Version is the version of the instructions it researched under.
	// Version 是它做研究时所依据的指令版本。
	Version int `json:"version"`
}

// CompactionRole is the role of a compaction marker: a message whose content
// summarizes every message before it, which the model reads in their place.
// CompactionRole 是 compaction 标记的角色：其内容概括之前每条消息的消息，模型会以它替代
// 那些消息来读取。
const CompactionRole = "compaction"

// Compaction is what a compaction marker records of its summary.
// Compaction 是 compaction 标记对其摘要的记录。
type Compaction struct {
	// Messages is how many messages the summary covers, those an earlier
	// marker covered included.
	// Messages 是摘要所覆盖的消息数量，包括更早的标记已覆盖的那些。
	Messages int `json:"messages"`
	// StartedAt and EndedAt are when the summary was written, in epoch
	// milliseconds.
	// StartedAt 与 EndedAt 是摘要写入的时间，以 Unix 毫秒计。
	StartedAt float64 `json:"startedAt"`
	EndedAt   float64 `json:"endedAt"`
	// Model wrote the summary under version Version of its instructions.
	// Model 在其指令的 Version 版本下写下了该摘要。
	Model   string `json:"model"`
	Version int    `json:"version"`
}

// LatestCompaction is the index of the last compaction marker among
// messages, or -1 when there is none.
// LatestCompaction 返回 messages 中最后一个 compaction 标记的下标；没有时为 -1。
func LatestCompaction(messages []Message) int {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == CompactionRole {
			return i
		}
	}
	return -1
}

// DropCompactionsAfter removes every compaction marker after the message at
// index, each of which summarizes that message as it was.
// DropCompactionsAfter 删除 index 处消息之后的每个 compaction 标记，这些标记各自概括了
// 那条消息当时的样子。
func (t *Thread) DropCompactionsAfter(index int) {
	messages := slices.Clone(t.Messages[:index+1])
	for _, m := range t.Messages[index+1:] {
		if m.Role != CompactionRole {
			messages = append(messages, m)
		}
	}
	t.Messages = messages
}

// Block is one step of a reply: text, reasoning, or a tool call. What tools
// return is kept as it came.
// Block 是回复的一步：文本、推理或工具调用。工具返回的内容按原样保留。
type Block struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Content    string            `json:"content,omitempty"`
	ToolCallID string            `json:"toolCallId,omitempty"`
	Name       string            `json:"name,omitempty"`
	Arguments  string            `json:"arguments,omitempty"`
	Complete   bool              `json:"complete,omitempty"`
	Result     json.RawMessage   `json:"result,omitempty"`
	Metadata   json.RawMessage   `json:"metadata,omitempty"`
	Progress   []json.RawMessage `json:"progress,omitzero"`
	Activity   json.RawMessage   `json:"activity,omitempty"`
	Display    *ToolDisplay      `json:"display,omitempty"`
	// StartedAt and EndedAt are when a reasoning or tool step ran, in epoch
	// milliseconds, and Duration is how long a reasoning step thought, in
	// seconds, as the apps have stored them.
	// StartedAt 与 EndedAt 是推理或工具步骤运行的时间（Unix 毫秒），Duration 是推理步骤
	// 思考的时长（秒），与各应用存储的一致。
	StartedAt float64 `json:"startedAt,omitempty"`
	EndedAt   float64 `json:"endedAt,omitempty"`
	Duration  float64 `json:"duration,omitempty"`
	// Answers are the options the user picked in a questions widget, by
	// question id. A question absent from them is unanswered. They are
	// replaced whole, never changed in place, so copies of a block share them.
	// Answers 是用户在 questions widget 中按题目 id 选中的选项。未出现在其中的题目即未答。
	// 它们会被整体替换、绝不原地修改，因此 block 的多个副本共享同一份。
	Answers map[string][]string `json:"answers,omitempty"`
	// InvalidArguments marks a widget call whose arguments the widget's
	// current declaration refuses, so its client offers to regenerate them.
	// It is worked out again for whatever clients are sent, never taken
	// from a row.
	// InvalidArguments 标记参数被 widget 当前声明拒绝的 widget 调用，以便客户端提议重新生成
	// 这些参数。它会针对发送给每个客户端的内容重新计算，绝不取自 row。
	InvalidArguments bool `json:"invalidArguments,omitempty"`
}

// ToolDisplay is what a web search or page fetch shows its client, kept
// current while the call runs.
// ToolDisplay 是网络搜索或页面抓取向客户端展示的内容，并在调用运行期间保持最新。
type ToolDisplay struct {
	Query   string          `json:"query,omitempty"`
	URL     string          `json:"url,omitempty"`
	Status  string          `json:"status"`
	Sources []DisplaySource `json:"sources"`
}

type DisplaySource struct {
	URL     string `json:"url"`
	Title   string `json:"title"`
	Snippet string `json:"snippet,omitempty"`
}

// SearchDisplay is what a web search or page fetch shows, read from its
// arguments and the result its client was sent: a call without a result is
// running, and one whose result is an error failed. Other calls show none.
// SearchDisplay 是网络搜索或页面抓取所展示的内容，从其参数与发送给客户端的结果读出：没有
// 结果的调用仍在运行，结果为错误的调用则失败。其他调用不展示。
func SearchDisplay(name, args string, result json.RawMessage) *ToolDisplay {
	if name != "web_search" && name != "web_fetch" {
		return nil
	}
	var arguments struct {
		Query string   `json:"query"`
		URL   string   `json:"url"`
		URLs  []string `json:"urls"`
	}
	readLenient([]byte(HistoryArgs(args)), &arguments)
	var shown struct {
		Query   string          `json:"query"`
		URL     string          `json:"url"`
		Error   json.RawMessage `json:"error"`
		Results []DisplaySource `json:"results"`
		Pages   []DisplaySource `json:"pages"`
	}
	readLenient(result, &shown)
	display := &ToolDisplay{Status: "completed", Sources: []DisplaySource{}}
	switch {
	case len(result) == 0 || string(result) == "null":
		display.Status = "running"
	case shown.Error != nil:
		display.Status = "failed"
	}
	if name == "web_search" {
		display.Query = cmp.Or(shown.Query, arguments.Query)
		display.Sources = append(display.Sources, shown.Results...)
		return display
	}
	display.URL = cmp.Or(shown.URL, arguments.URL)
	if display.URL == "" && len(arguments.URLs) > 0 {
		display.URL = arguments.URLs[0]
	}
	for _, page := range shown.Pages {
		display.Sources = append(display.Sources, DisplaySource{URL: page.URL, Title: page.Title})
	}
	return display
}

// Attachment is a file a message carries, in the one form an upload returns
// it, a turn sends it, a chat stores it and a view shows it. A stored chat
// keeps the key of its blob, with its content inline only while no blob
// holds it; a chat on a device keeps its content.
// Attachment 是消息携带的文件，采用上传返回、turn 发送、聊天存储、视图展示时共用的单一
// 形式。已存储的聊天保留其 blob 的 key，仅在无 blob 持有时才内联内容；设备上的聊天保留
// 其内容。
type Attachment struct {
	ID          string         `json:"id"`
	Kind        string         `json:"kind"` // image, document or audio；图像、文档或音频
	FileName    string         `json:"fileName"`
	MimeType    string         `json:"mimeType"`
	Size        *int64         `json:"size,omitempty"`
	Thumbnail   string         `json:"thumbnail,omitempty"`
	TextContent string         `json:"textContent,omitempty"`
	Description string         `json:"description,omitempty"`
	Pages       []DocumentPage `json:"pages,omitempty"`
	// Base64 is an image's copy that models read.
	// Base64 是模型读取的图像副本。
	Base64 string `json:"base64,omitempty"`
	// EncryptionKey opens the attachment's blob, as every client names it.
	// EncryptionKey 打开附件的 blob，各客户端都以该名称指代它。
	EncryptionKey string `json:"encryptionKey,omitempty"`
	// Key is the control plane key of an image stored before buckets existed.
	// Key 是 buckets 出现之前存储的图像的控制平面 key。
	Key             string `json:"key,omitempty"`
	StorageThreadID string `json:"storageThreadId,omitempty"`

	// inferenceMimeType is the type of the copy a model reads, set while a
	// turn's prompt is built.
	// inferenceMimeType 是模型读取的副本类型，在构建 turn 的 prompt 时设置。
	InferenceMimeType string `json:"-"`
}

// DocumentPage is a converted document page. Its fields are in the order
// a blob has always held them.
// DocumentPage 是转换后的文档页。其字段顺序与 blob 一直以来的保存顺序一致。
type DocumentPage struct {
	Image     string `json:"image"`
	IsScanned bool   `json:"is_scanned"`
	Page      int    `json:"page"`
	Text      string `json:"text"`
}

// Annotation marks a span of a reply as citing a page.
// Annotation 把回复中的一段标记为引用某个页面。
type Annotation struct {
	Type        string       `json:"type"`
	URLCitation *URLCitation `json:"url_citation,omitempty"`
}

type URLCitation struct {
	URL        string `json:"url"`
	Title      string `json:"title"`
	StartIndex int    `json:"start_index"`
	EndIndex   int    `json:"end_index"`
}

// ActiveRun tells clients a reply may be streaming, until the run's
// deadline. It never locks the chat: a run whose process died leaves it.
// ActiveRun 告知客户端回复可能仍在流式输出，直到该 run 的截止时间。它绝不会锁定聊天：
// 进程已死亡的 run 会留下该标记。
type ActiveRun struct {
	RunID     string `json:"runId"`
	ExpiresAt string `json:"expiresAt"`
	Instance  string `json:"instance"` // the process holding the run；持有该 run 的进程
}

// ContextUsage is how much of the model's window the chat's last prompt filled.
// ContextUsage 是聊天最后一个 prompt 占用的模型窗口大小。
type ContextUsage struct {
	UsedTokens  int     `json:"usedTokens"`
	LimitTokens int     `json:"limitTokens"`
	Percentage  float64 `json:"percentage"`
}

// MessageUsage is what a reply cost.
// MessageUsage 是回复的消耗。
type MessageUsage struct {
	PromptTokens     int64 `json:"promptTokens"`
	CompletionTokens int64 `json:"completionTokens"`
}

// ThreadRow is a stored chat and the revision a write must match; a chat
// not yet stored has none.
// ThreadRow 是已存储的聊天以及写入必须匹配的修订号；尚未存储的聊天没有修订号。
type ThreadRow struct {
	Thread
	ETag string `json:"-"`
}

// Clone copies a thread deeply enough to change it apart from the original.
// Raw tool output is never changed in place, so copies share it.
// Clone 深拷贝 thread，使其可与原对象独立修改。原始工具输出绝不原地修改，因此副本会共享它。
func (t *Thread) Clone() Thread {
	copy := *t
	copy.Messages = make([]Message, len(t.Messages))
	for i := range t.Messages {
		copy.Messages[i] = t.Messages[i].clone()
	}
	if t.ActiveRun != nil {
		run := *t.ActiveRun
		copy.ActiveRun = &run
	}
	if t.ContextUsage != nil {
		usage := *t.ContextUsage
		copy.ContextUsage = &usage
	}
	return copy
}

func (m *Message) clone() Message {
	copy := *m
	copy.Timeline = clonedSlice(m.Timeline)
	for i := range copy.Timeline {
		copy.Timeline[i].Progress = clonedSlice(copy.Timeline[i].Progress)
	}
	copy.Attachments = clonedSlice(m.Attachments)
	for i := range copy.Attachments {
		copy.Attachments[i].Pages = clonedSlice(copy.Attachments[i].Pages)
	}
	copy.Annotations = clonedSlice(m.Annotations)
	if m.Compaction != nil {
		compaction := *m.Compaction
		copy.Compaction = &compaction
	}
	if m.Research != nil {
		research := *m.Research
		copy.Research = &research
	}
	return copy
}

// clonedSlice copies a slice, keeping nil and empty apart.
// clonedSlice 拷贝切片，并区分 nil 与空切片。
func clonedSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	return append(make([]T, 0, len(s)), s...)
}

// MessageIndex 返回给定 id 的消息在 thread 中的下标，找不到时为 -1。
func (t *Thread) MessageIndex(id string) int {
	return slices.IndexFunc(t.Messages, func(m Message) bool { return m.ID == id })
}

// FindAttachment finds an attachment of the thread by id.
// FindAttachment 按 id 查找 thread 中的附件。
func (t *Thread) FindAttachment(id string) *Attachment {
	for i := range t.Messages {
		for j := range t.Messages[i].Attachments {
			if t.Messages[i].Attachments[j].ID == id {
				return &t.Messages[i].Attachments[j]
			}
		}
	}
	return nil
}

// EachAttachment visits every attachment of the thread in place.
// EachAttachment 原地访问 thread 中的每个附件。
func (t *Thread) EachAttachment(visit func(*Attachment) error) error {
	for i := range t.Messages {
		for j := range t.Messages[i].Attachments {
			if err := visit(&t.Messages[i].Attachments[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// CallID is the id a tool call block answers to.
// CallID 是工具调用 block 所应答的 id。
func (b *Block) CallID() string {
	if b.ToolCallID != "" {
		return b.ToolCallID
	}
	return b.ID
}

// IsImage 报告附件是否为图像。
func (a *Attachment) IsImage() bool { return a.Kind == "image" }

// StorageKey opens the attachment's blob: a buckets key, or a control plane
// key from before buckets.
// StorageKey 打开附件的 blob：buckets 的 key，或 buckets 出现之前的控制平面 key。
func (a *Attachment) StorageKey() string {
	if a.EncryptionKey != "" {
		return a.EncryptionKey
	}
	return a.Key
}

// HasContent 报告附件是否含有可展示的内容。
func (a *Attachment) HasContent() bool { return a.TextContent != "" || len(a.Pages) > 0 }

// ValueOr is what p points at, or fallback when p is nil.
// ValueOr 返回 p 指向的值；p 为 nil 时返回 fallback。
func ValueOr[T any](p *T, fallback T) T {
	if p == nil {
		return fallback
	}
	return *p
}

// AttachmentKinds are the kinds of attachment a message carries.
// AttachmentKinds 是消息可携带的附件类型。
var AttachmentKinds = []string{"image", "document", "audio"}
