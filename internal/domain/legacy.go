// 本文件定义旧格式记录与 legacy 读取器，用于把历史聊天与设置读入当前格式。
package domain

import (
	"bytes"
	"cmp"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// A row or device record names the format it is in. One in a format of
// readVersions is read as written. One that names no format was written before
// formats were named, by the web and iOS apps, the old web client, or a
// client's blank record of a new chat, and the legacy reader reads it into
// the current format. One in any other format is refused.
//
// The legacy reader reads rows of any age: chats the apps stored in the cloud,
// device chats the old web client kept, old shares, and profiles and
// projects old clients wrote. A field of the wrong type reads as absent, so
// one odd field never hides a chat.
// 一条 row 或设备记录会标明其所在格式。处于 readVersions 中某种格式的记录按原样读取。
// 未标明格式的记录，是在格式被命名之前由 web 与 iOS 应用、旧 web 客户端，或客户端为
// 新聊天写入的空白记录，由 legacy 读取器读入当前格式。任何其他格式的记录都会被拒绝。
//
// legacy 读取器可读取任意年代的 row：应用存入云端的聊天、旧 web 客户端保留的设备聊天、
// 旧分享，以及旧客户端写入的 profile 与 project。类型错误的字段会按缺失处理，因此单个
// 异常字段不会掩盖整个聊天。

// errRecordUnsupported refuses a record in a format this harness does not
// read, as a later harness writes, or that breaks the format it names.
// errRecordUnsupported 拒绝本 harness 无法读取格式的记录（如更新版 harness 所写），
// 或与其所声明格式不符的记录。
var errRecordUnsupported = api.Err(409, "RECORD_UNSUPPORTED", "The record is in a format this harness cannot read")

// currentFormat reports whether a record is in a format of readVersions,
// as opposed to one written before formats were named.
// currentFormat 报告记录是否处于 readVersions 中的某种格式，而非格式被命名之前写入的
// 那种。
func currentFormat(data []byte) (bool, error) {
	var marker struct {
		RecordVersion int `json:"recordVersion"`
	}
	if json.Unmarshal(data, &marker) != nil || marker.RecordVersion != 0 && !slices.Contains(readVersions, marker.RecordVersion) {
		return false, errRecordUnsupported
	}
	return marker.RecordVersion != 0, nil
}

// readCurrent reads a record in the current format into v, refusing one
// that leaves out a member the format requires or gives a member a value of
// another type. A member the format does not name is read past: the other
// clients of the same rows write members of their own, and keep a record's
// format marker when they write it back.
// readCurrent 将当前格式的记录读入 v；若记录缺少该格式必需的成员，或某成员的值类型不符，
// 则拒绝。该格式未命名的成员会被略过：同一批 row 的其他客户端会写入自己的成员，并在
// 写回时保留记录的格式标记。
func readCurrent(data []byte, v any, required ...string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(v) != nil || decoder.More() || !holds(data, required) {
		return errRecordUnsupported
	}
	return nil
}

// holds reports whether a JSON object has every one of the members, each
// set to a value other than null unless the format allows null for it.
// holds 报告 JSON 对象是否包含全部成员，且每个成员都非 null（除非该格式允许其为 null）。
func holds(data []byte, members []string) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil {
		return false
	}
	for _, member := range members {
		value, ok := object[member]
		if !ok || string(value) == "null" && !slices.Contains(nullableMembers, member) {
			return false
		}
	}
	return true
}

// The members a record in the current format requires, as
// protocol/openapi.yaml names them. The control plane files a chat's project apart from
// it, and leaves projectId out of a fork outside a project.
// 当前格式记录所要求的成员，名称与 protocol/openapi.yaml 一致。控制平面将聊天的 project
// 与聊天分开归档，对于不在 project 中的 fork 则不写 projectId。
var (
	currentChat     = []string{"id", "title", "titleState", "model", "presetId", "pinned", "webSearchEnabled", "messages"}
	currentMessage  = []string{"id", "role", "createdAt", "content"}
	currentProject  = []string{"id", "name", "description", "systemInstructions", "color"}
	currentDocument = []string{"id", "projectId", "filename", "contentType", "sizeBytes"}
	titleStates     = []string{"placeholder", "generated", "manual"}
	nullableMembers = []string{"presetId", "color"}
)

// currentCompaction are the members a compaction marker's record requires:
// what its summary covers, and what wrote it.
// currentCompaction 是 compaction 标记记录所要求的成员：其摘要所覆盖的内容，以及写它的是
// 什么。
var currentCompaction = []string{"messages", "startedAt", "endedAt", "model", "version"}

// readCurrentThread reads a chat in the current format into v, whose
// thread is t, refusing one whose messages or values that format does not
// allow.
// readCurrentThread 将当前格式的聊天读入 v（其 thread 为 t），并拒绝消息或值不符合该格式
// 的记录。
func readCurrentThread(data []byte, v any, t *Thread) error {
	if err := readCurrent(data, v, currentChat...); err != nil {
		return err
	}
	var shape struct {
		Messages []json.RawMessage `json:"messages"`
	}
	json.Unmarshal(data, &shape)
	for _, message := range shape.Messages {
		var marker struct {
			Role       string          `json:"role"`
			Compaction json.RawMessage `json:"compaction"`
		}
		json.Unmarshal(message, &marker)
		if !holds(message, currentMessage) || marker.Role == CompactionRole && !holds(marker.Compaction, currentCompaction) {
			return errRecordUnsupported
		}
	}
	if !slices.Contains(titleStates, t.TitleState) || slices.ContainsFunc(t.Messages, func(m Message) bool { return !slices.Contains(MessageRoles, m.Role) }) {
		return errRecordUnsupported
	}
	return nil
}

// readLenient decodes data into v, leaving out each field of the wrong
// type. Rows reach it as valid JSON objects.
// readLenient 将 data 解码进 v，并忽略每个类型错误的字段。row 到达这里时都是合法的 JSON
// 对象。
func readLenient(data []byte, v any) { json.Unmarshal(data, v) }

// LooseTime is a time old clients stored as RFC 3339 text or as Unix
// milliseconds, read as RFC 3339.
// LooseTime 是旧客户端以 RFC 3339 文本或 Unix 毫秒存储的时间，读取时统一为 RFC 3339。
type LooseTime string

func (t *LooseTime) UnmarshalJSON(b []byte) error {
	var value any
	json.Unmarshal(b, &value)
	*t = LooseTime(ISOTime(value))
	return nil
}

// ISOTime 将各种旧客户端写入的时间值统一为 RFC 3339 文本。
func ISOTime(value any) string {
	switch v := value.(type) {
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return parsed.UTC().Format(time.RFC3339Nano)
		}
		return v
	case float64:
		if v > 0 {
			return time.UnixMilli(int64(v)).UTC().Format(time.RFC3339Nano)
		}
	}
	return ""
}

// legacyThread is a chat row as any client stored it.
// legacyThread 是任意客户端存储的聊天 row。
type legacyThread struct {
	Thread
	CreatedAt        LooseTime       `json:"createdAt"`
	UpdatedAt        LooseTime       `json:"updatedAt"`
	WebSearchEnabled *bool           `json:"webSearchEnabled"`
	Messages         []legacyMessage `json:"messages"`
}

// legacyMessage is a message with the flat fields the apps stored before
// replies had timelines and attachments had their own records.
// legacyMessage 是带有扁平字段的消息，这些字段是应用在回复拥有 timeline、附件拥有独立
// 记录之前存储的。
type legacyMessage struct {
	Message
	Timestamp               LooseTime          `json:"timestamp"`
	Timeline                []legacyBlock      `json:"timeline"`
	Thoughts                string             `json:"thoughts"`
	ThinkingDuration        float64            `json:"thinkingDuration"`
	WebSearch               *legacySearch      `json:"webSearch"`
	WebSearchBeforeThinking bool               `json:"webSearchBeforeThinking"`
	URLFetches              []legacyFetch      `json:"urlFetches"`
	CodeExecCalls           []legacyCodeCall   `json:"codeExecCalls"`
	ToolCalls               []Block            `json:"toolCalls"`
	DocumentContent         string             `json:"documentContent"`
	MultimodalText          string             `json:"multimodalText"`
	Documents               []legacyFileName   `json:"documents"`
	ImageData               []legacyImage      `json:"imageData"`
	Attachments             []legacyAttachment `json:"attachments"`
}

// legacyFileName names one file of a message's flat fields.
// legacyFileName 是消息扁平字段中的一个文件名。
type legacyFileName struct {
	Name string `json:"name"`
}

// legacyAttachment is an attachment under the names the web and iOS apps
// stored it with: its kind as type, and its size and thumbnail. In a backup,
// an image and a page's image name the image the backup holds.
// legacyAttachment 是 web 与 iOS 应用存储附件时所用的字段：其类型记为 type，并含大小与
// 缩略图。在备份中，图像与页面图像会指向备份所持有的图像。
type legacyAttachment struct {
	Attachment
	Type            string       `json:"type"`
	FileSize        *int64       `json:"fileSize"`
	ThumbnailBase64 string       `json:"thumbnailBase64"`
	ImageID         string       `json:"imageId"`
	Pages           []legacyPage `json:"pages"`
}

type legacyPage struct {
	DocumentPage
	ImageID string `json:"imageId"`
}

// BackupImages are the images a backup holds for one chat, by the id its
// messages name each with.
// BackupImages 是备份为单个聊天持有的图像，以消息为每张图指定的 id 为键。
type BackupImages map[string]HeldImage

// HeldImage is an image a backup holds, its bytes base64 encoded.
// HeldImage 是备份持有的一张图像，其字节为 base64 编码。
type HeldImage struct {
	FileName, MimeType, Description string
	Size                            int64
	Base64                          string
}

// upgrade is the attachment in the current form. Old rows name only images
// and documents, by type; a row the harness wrote names audio by kind too.
// upgrade 返回当前形式的附件。旧 row 仅按 type 区分 image 与 document；harness 写入的
// row 还会按 kind 记录 audio。
func (a legacyAttachment) upgrade(images BackupImages) Attachment {
	upgraded := a.Attachment
	switch {
	case slices.Contains(AttachmentKinds, a.Kind):
	case a.Type == "image":
		upgraded.Kind = "image"
	default:
		upgraded.Kind = "document"
	}
	if upgraded.Size == nil {
		upgraded.Size = a.FileSize
	}
	upgraded.Thumbnail = cmp.Or(upgraded.Thumbnail, a.ThumbnailBase64)
	if a.Pages != nil {
		upgraded.Pages = make([]DocumentPage, len(a.Pages))
		for i, page := range a.Pages {
			if held, ok := images[page.ImageID]; ok && page.Image == "" {
				page.Image = held.Base64
			}
			upgraded.Pages[i] = page.DocumentPage
		}
	}
	if held, ok := images[a.ImageID]; ok && upgraded.Base64 == "" {
		upgraded.Base64, upgraded.Size = held.Base64, &held.Size
		upgraded.FileName = cmp.Or(upgraded.FileName, held.FileName)
		upgraded.MimeType = cmp.Or(held.MimeType, upgraded.MimeType)
		upgraded.Description = cmp.Or(upgraded.Description, held.Description)
	}
	return upgraded
}

// legacyBlock is a timeline block, including the kinds the web client wrote
// before every tool step was a tool call.
// legacyBlock 是 timeline 块，包含 web 客户端在每个工具步骤都成为工具调用之前所写的
// 各种类型。
type legacyBlock struct {
	Block
	State   *legacySearch    `json:"state"`
	Fetches []legacyFetch    `json:"fetches"`
	Calls   []legacyCodeCall `json:"calls"`
}

type legacySearch struct {
	Query   string `json:"query"`
	Status  string `json:"status"`
	Sources []struct {
		URL         string `json:"url"`
		Title       string `json:"title"`
		Snippet     string `json:"snippet"`
		Description string `json:"description"`
	} `json:"sources"`
}

type legacyFetch struct {
	ID        string  `json:"id"`
	URL       string  `json:"url"`
	Status    string  `json:"status"`
	Title     string  `json:"title"`
	Text      string  `json:"text"`
	StartedAt float64 `json:"startedAt"`
	EndedAt   float64 `json:"endedAt"`
}

// legacyFailed is whether an old client recorded a search or page read as
// not completed: failed, blocked, or cut off while it ran. The iOS app
// wrote a step without a status as completed.
// legacyFailed 报告旧客户端是否把搜索或页面读取记录为未完成：失败、被阻止，或运行中被
// 截断。iOS 应用把没有状态的步骤记为已完成。
func legacyFailed(status string) bool { return status != "" && status != "completed" }

type legacyCodeCall struct {
	ID        string          `json:"id"`
	ToolName  string          `json:"toolName"`
	Arguments json.RawMessage `json:"arguments"`
	Output    string          `json:"output"`
}

type legacyImage struct {
	Base64   string `json:"base64"`
	MimeType string `json:"mimeType"`
	ImageID  string `json:"imageId"`
}

// ReadThread reads a chat row into the thread format.
// ReadThread 将聊天 row 读入 thread 格式。
func ReadThread(id string, data []byte) (Thread, error) {
	switch current, err := currentFormat(data); {
	case err != nil:
		return Thread{}, err
	case !current:
		return upgradeThread(id, data, ""), nil
	}
	// Older clients marked a chat as kept in the cloud. The mark is theirs,
	// and the harness leaves it out.
	// 旧客户端会标记聊天保存在云端。该标记属于它们，harness 会将其略去。
	var stored struct {
		Thread
		IsLocalOnly *bool `json:"isLocalOnly"`
	}
	if err := readCurrentThread(data, &stored, &stored.Thread); err != nil {
		return Thread{}, err
	}
	t := stored.Thread
	t.ID = id
	return t, nil
}

// upgradeThread reads a chat, dated created when it holds no date.
// upgradeThread 读取聊天，并在其没有日期时使用 created 作为日期。
func upgradeThread(id string, data []byte, created string) Thread {
	return readThreadWith(id, data, created, nil)
}

// ReadBackupThread reads a chat of a backup, whose images the backup holds
// apart from it.
// ReadBackupThread 读取备份中的聊天，其图像由备份另行持有。
func ReadBackupThread(id string, data []byte, images BackupImages) Thread {
	return readThreadWith(id, data, api.Timestamp(), images)
}

func readThreadWith(id string, data []byte, created string, images BackupImages) Thread {
	var stored legacyThread
	readLenient(data, &stored)
	t := stored.Thread
	t.ID, t.CreatedAt, t.UpdatedAt = id, cmp.Or(string(stored.CreatedAt), created), string(stored.UpdatedAt)
	t.Title = cmp.Or(t.Title, "New Chat")
	if t.TitleState == "" {
		t.TitleState = "generated"
		if t.Title == "New Chat" {
			t.TitleState = "placeholder"
		}
	}
	t.Model = normalizeModel(t.Model)
	t.WebSearchEnabled = ValueOr(stored.WebSearchEnabled, true)
	t.Messages = make([]Message, len(stored.Messages))
	for i, item := range stored.Messages {
		m := item.Message
		m.ID = cmp.Or(m.ID, id+"-message-"+strconv.Itoa(i))
		m.CreatedAt = cmp.Or(m.CreatedAt, string(item.Timestamp), t.CreatedAt)
		m.Timeline = item.timeline(m)
		m.Attachments = item.attachments(m.ID, images)
		t.Messages[i] = m
	}
	return t
}

// ReadThreadRow reads a stored chat. The control plane files a chat under its project
// apart from the sealed row, and that filing is the chat's project.
// ReadThreadRow 读取已存储的聊天。控制平面将聊天归档到其 project 下（与密封 row 分开），
// 该归档即聊天的 project。
func ReadThreadRow(row *StoredRow) (*ThreadRow, error) {
	t, err := ReadThread(row.ID, row.Data)
	if err != nil {
		return nil, err
	}
	if row.ProjectSet {
		t.ProjectID = row.Project
	}
	return &ThreadRow{Thread: t, ETag: row.ETag}, nil
}

// ReadDeviceThread reads a thread a client keeps on its device. The fields
// the harness owns are never taken from a client's copy.
// ReadDeviceThread 读取客户端保存在设备上的 thread。harness 自有的字段绝不会取自客户端
// 的副本。
func ReadDeviceThread(id string, data []byte) (Thread, error) {
	var t Thread
	switch current, err := currentFormat(data); {
	case err != nil:
		return Thread{}, err
	case !current:
		t = upgradeThread(id, data, api.Timestamp())
	default:
		if err := readCurrentThread(data, &t, &t); err != nil {
			return Thread{}, err
		}
		t.ID = id
	}
	t.ActiveRun, t.HiddenCount, t.ProjectID, t.ContextUsage, t.ImportedFrom = nil, 0, nil, nil, nil
	return t, nil
}

// timeline is a reply's timeline, built from its old flat fields when it
// has none and with every old block kind made a tool call.
// timeline 是回复的 timeline：当回复没有 timeline 时，从其旧的扁平字段构建，并把每种旧
// 块类型都做成工具调用。
func (m *legacyMessage) timeline(message Message) []Block {
	if message.Role != "assistant" {
		if m.Timeline == nil {
			return nil
		}
		blocks := make([]Block, len(m.Timeline))
		for i, b := range m.Timeline {
			blocks[i] = b.Block
		}
		return blocks
	}
	id, source := message.ID, m.Timeline
	if len(source) == 0 {
		addSearch := func() {
			if m.WebSearch != nil {
				source = append(source, legacyBlock{Block: Block{Type: "web_search", ID: id + "-search"}, State: m.WebSearch})
			}
		}
		if m.WebSearchBeforeThinking {
			addSearch()
		}
		if m.Thoughts != "" {
			source = append(source, legacyBlock{Block: Block{Type: "thinking", ID: id + "-thinking", Content: m.Thoughts, Duration: m.ThinkingDuration}})
		}
		if !m.WebSearchBeforeThinking {
			addSearch()
		}
		if len(m.URLFetches) > 0 {
			source = append(source, legacyBlock{Block: Block{Type: "url_fetches", ID: id + "-fetches"}, Fetches: m.URLFetches})
		}
		if len(m.CodeExecCalls) > 0 {
			source = append(source, legacyBlock{Block: Block{Type: "code_exec", ID: id + "-code"}, Calls: m.CodeExecCalls})
		}
		for _, call := range m.ToolCalls {
			call.Type, call.ToolCallID = "tool_call", call.ID
			source = append(source, legacyBlock{Block: call})
		}
		if message.Content != "" {
			source = append(source, legacyBlock{Block: Block{Type: "content", ID: id + "-content", Content: message.Content}})
		}
	}
	blocks := []Block{}
	// A call keeps when the step it came from ran. One that did not complete
	// holds the result a failed call holds.
	// 调用保留其来源步骤运行的时间。未完成的调用则保留失败调用所持有的结果。
	call := func(startedAt, endedAt float64, id, name, args string, result any, status string) {
		if legacyFailed(status) {
			result = json.RawMessage(ToolError)
		}
		blocks = append(blocks, Block{Type: "tool_call", ID: id, ToolCallID: id, Name: name, Arguments: args, Result: api.Raw(result), Complete: true, StartedAt: startedAt, EndedAt: endedAt})
	}
	for index, block := range source {
		blockID := cmp.Or(block.ID, id+"-block-"+strconv.Itoa(index))
		switch block.Type {
		case "web_search":
			state := legacySearch{}
			if block.State != nil {
				state = *block.State
			}
			results := []map[string]string{}
			for _, source := range state.Sources {
				results = append(results, map[string]string{"url": source.URL, "title": source.Title, "snippet": cmp.Or(source.Snippet, source.Description)})
			}
			call(block.StartedAt, block.EndedAt, blockID, "web_search", string(api.Raw(map[string]string{"query": state.Query})), map[string]any{"query": state.Query, "results": results}, state.Status)
		case "url_fetches":
			for i, fetch := range block.Fetches {
				call(cmp.Or(fetch.StartedAt, block.StartedAt), cmp.Or(fetch.EndedAt, block.EndedAt), cmp.Or(fetch.ID, blockID+"-"+strconv.Itoa(i)), "web_fetch", string(api.Raw(map[string]string{"url": fetch.URL})), map[string]string{"url": fetch.URL, "title": fetch.Title, "text": fetch.Text}, fetch.Status)
			}
		case "code_exec":
			for i, c := range block.Calls {
				args := c.arguments()
				call(block.StartedAt, block.EndedAt, cmp.Or(c.ID, blockID+"-"+strconv.Itoa(i)), c.ToolName, args, NormalizeToolResult(c.ToolName, args, c.Output), "")
			}
		default:
			block.ID = blockID
			blocks = append(blocks, block.Block)
		}
	}
	// A search keeps what the harness showed of it; one without shows what
	// its arguments and result say.
	// 搜索保留 harness 展示给它的内容；没有的则展示其参数与结果所示的内容。
	for i := range blocks {
		if b := &blocks[i]; b.Type == "tool_call" && b.Display == nil {
			b.Display = SearchDisplay(b.Name, b.Arguments, b.Result)
		}
	}
	return blocks
}

// arguments is a code call's arguments as JSON text. The apps stored them
// as text or as an object, which is written out with sorted keys.
// arguments 返回代码调用的参数（JSON 文本）。应用曾以文本或对象形式存储它们，对象会按
// 排序后的键写出。
func (c legacyCodeCall) arguments() string {
	var text string
	if json.Unmarshal(c.Arguments, &text) == nil && text != "" {
		return text
	}
	fields := map[string]any{}
	json.Unmarshal(c.Arguments, &fields)
	return string(api.Raw(fields))
}

// attachments are a message's attachments, made from the document text and
// image bytes the apps once kept on the message itself. Each has an id.
// attachments 返回消息的附件，由应用过去保存在消息本身的文档文本与图像字节构建。每个附件
// 都有 id。
func (m *legacyMessage) attachments(messageID string, images BackupImages) []Attachment {
	var attachments []Attachment
	if m.Attachments != nil {
		attachments = make([]Attachment, len(m.Attachments))
		for i, a := range m.Attachments {
			attachments[i] = a.upgrade(images)
		}
	}
	if len(attachments) == 0 {
		attachments = m.flatAttachments(images)
	}
	for i := range attachments {
		attachments[i].ID = cmp.Or(attachments[i].ID, messageID+"-attachment-"+strconv.Itoa(i))
	}
	return attachments
}

// flatAttachments are the files a message kept in its flat fields, its
// documents and then its images. documents names every file, and each
// file's text is its section of the message's combined text: a document's
// contents, or an image's description. The images are the files the
// descriptions name, in the order of imageData; before the web app kept
// descriptions apart from the documents' contents, they are the first
// files, as the apps read them. Text no file has a section of is the only
// document's, or else one unnamed document's.
// flatAttachments 返回消息保存在扁平字段中的文件：先是文档，后是图像。documents 列出
// 每个文件，每个文件的文本是消息合并文本中属于它的那一段：文档的内容，或图像的描述。
// images 是描述所指的文件，按 imageData 的顺序排列；在 web 应用把描述与文档内容分开
// 存储之前，它们是应用所读取的最前面的文件。没有文件拥有对应段落的文本，属于唯一的文档，
// 否则属于某个未命名的文档。
func (m *legacyMessage) flatAttachments(images BackupImages) []Attachment {
	claimed := false
	contents := func(name string) string {
		claimed = claimed || strings.Contains(m.DocumentContent, documentHeading(name))
		return legacySection(m.DocumentContent, documentHeading(name), "\nDocument title: ")
	}
	described := slices.ContainsFunc(m.Documents, func(f legacyFileName) bool { return strings.Contains(m.MultimodalText, imageHeading(f.Name)) })
	var documents, shown []Attachment
	var imageNames []string
	for i, file := range m.Documents {
		if described && strings.Contains(m.MultimodalText, imageHeading(file.Name)) || !described && i < len(m.ImageData) {
			imageNames = append(imageNames, file.Name)
		} else {
			documents = append(documents, Attachment{Kind: "document", FileName: file.Name, TextContent: contents(file.Name)})
		}
	}
	for i, image := range m.ImageData {
		if held, ok := images[image.ImageID]; ok && image.Base64 == "" {
			image.Base64, image.MimeType = held.Base64, cmp.Or(held.MimeType, image.MimeType)
		}
		if image.Base64 == "" {
			continue
		}
		a := Attachment{Kind: "image", FileName: "image-" + strconv.Itoa(i), MimeType: cmp.Or(image.MimeType, "image/png"), Base64: image.Base64}
		if i < len(imageNames) {
			name := imageNames[i]
			a.FileName = cmp.Or(name, a.FileName)
			a.Description = cmp.Or(legacySection(m.MultimodalText, imageHeading(name), "\n\nImage: "), contents(name))
		}
		shown = append(shown, a)
	}
	var unclaimed string
	if !claimed {
		unclaimed = m.DocumentContent
	}
	if !described {
		unclaimed = cmp.Or(unclaimed, m.MultimodalText)
	}
	if unclaimed != "" {
		if len(documents) == 1 && documents[0].TextContent == "" {
			documents[0].TextContent = unclaimed
		} else {
			documents = append(documents, Attachment{Kind: "document", FileName: "Attached document", TextContent: unclaimed})
		}
	}
	return append(documents, shown...)
}

func documentHeading(name string) string { return "Document title: " + name + "\nDocument contents:\n" }
func imageHeading(name string) string    { return "Image: " + name + "\nDescription:\n" }

// legacySection is the text after heading in combined, up to the next
// section's start, as the apps cut it.
// legacySection 返回 combined 中 heading 之后、到下一段开始之前的文本，与应用切割的
// 一致。
func legacySection(combined, heading, next string) string {
	_, rest, found := strings.Cut(combined, heading)
	if !found {
		return ""
	}
	section, _, _ := strings.Cut(rest, next)
	return strings.TrimSpace(section)
}

// ReadProfile reads a profile row.
// ReadProfile 读取 profile row。
func ReadProfile(data []byte) (StoredProfile, error) {
	switch current, err := currentFormat(data); {
	case err != nil:
		return StoredProfile{}, err
	case current:
		profile := StoredProfile{Others: otherMembers(data)}
		err := readCurrent(data, &profile)
		return profile, err
	}
	var stored struct {
		StoredProfile
		UpdatedAt LooseTime `json:"updatedAt"`
	}
	readLenient(data, &stored)
	profile := stored.StoredProfile
	profile.UpdatedAt = string(stored.UpdatedAt)
	profile.Others = otherMembers(data)
	// The migrated prompt is dated by the profile, or by undatedProfile for a
	// profile without a date, so each read of a profile not yet stored again
	// makes the same one.
	// 迁移后的 prompt 以 profile 的日期为准；无日期的 profile 则用 undatedProfile，这样
	// 尚未重新存储的 profile 每次读取都会得到相同结果。
	written, err := time.Parse(time.RFC3339Nano, profile.UpdatedAt)
	if err != nil {
		written = undatedProfile
	}
	upgradeProfile(&profile.Profile, written)
	return profile, nil
}

// undatedProfile is the date a profile without one is read as written.
// undatedProfile 是无日期 profile 被当作写入日期使用的日期。
var undatedProfile = time.Unix(0, 0).UTC()

// MigratedPromptPresetID names the saved prompt an old custom default
// prompt becomes, as the iOS app names it.
// MigratedPromptPresetID 是旧的自定义默认 prompt 迁移后所得的已保存 prompt 名称，
// 与 iOS 应用命名一致。
const MigratedPromptPresetID = "user:migrated-custom-prompt"

// systemTags are the tags old clients wrapped a custom prompt in.
// systemTags 是旧客户端包裹自定义 prompt 所用的标签。
var systemTags = regexp.MustCompile(`^<system>\s*\n?|\n?</system>\s*$`)

// upgradeProfile reads settings old clients wrote as current ones: Auto's
// earlier tiers are Auto, and an enabled custom default prompt becomes a
// saved prompt that new chats start with, unless another default was
// chosen. The old setting is turned off either way.
// upgradeProfile 把旧客户端写入的设置读成当前设置：Auto 的早期档位一律视为 Auto；已启用
// 的自定义默认 prompt 变成新聊天起始所用的已保存 prompt（除非已另选默认）。旧设置在两种
// 情况下都会被关闭。
func upgradeProfile(p *Profile, now time.Time) {
	if p.SelectedModel != nil {
		p.SelectedModel = new(normalizeModel(*p.SelectedModel))
	}
	p.CustomPromptPresets = slices.Clone(p.CustomPromptPresets)
	for i := range p.CustomPromptPresets {
		if model := p.CustomPromptPresets[i].Model; model != nil {
			p.CustomPromptPresets[i].Model = new(normalizeModel(*model))
		}
	}
	if !ValueOr(p.IsUsingCustomPrompt, false) {
		return
	}
	prompt := ValueOr(p.CustomSystemPrompt, "")
	p.IsUsingCustomPrompt, p.CustomSystemPrompt = new(false), new("")
	if strings.TrimSpace(systemTags.ReplaceAllString(prompt, "")) == "" || ValueOr(p.DefaultPromptPresetID, "") != "" {
		return
	}
	if p.Preset(MigratedPromptPresetID) == nil {
		date := api.Raw(now.UnixMilli())
		p.CustomPromptPresets = append(p.CustomPromptPresets, CustomPreset{ID: MigratedPromptPresetID, Name: "My default prompt", Description: new("Migrated from the custom default prompt setting"),
			SystemPrompt: &prompt, CreatedAt: date, UpdatedAt: date})
	}
	p.DefaultPromptPresetID = new(MigratedPromptPresetID)
}

// ReadProject reads a project row.
// ReadProject 读取 project row。
func ReadProject(row *StoredRow) (Project, error) {
	switch current, err := currentFormat(row.Data); {
	case err != nil:
		return Project{}, err
	case current:
		var stored StoredProject
		if err := readCurrent(row.Data, &stored, currentProject...); err != nil {
			return Project{}, err
		}
		stored.Project.ID = row.ID
		return stored.Project, nil
	}
	var stored struct {
		Project
		CreatedAt LooseTime `json:"createdAt"`
		UpdatedAt LooseTime `json:"updatedAt"`
	}
	readLenient(row.Data, &stored)
	project := stored.Project
	project.ID, project.CreatedAt, project.UpdatedAt = row.ID, string(stored.CreatedAt), cmp.Or(string(stored.UpdatedAt), row.rowTime())
	return project, nil
}

// ReadProjectDocument reads a project document row, which is filed under
// the project's id.
// ReadProjectDocument 读取 project 文档 row，该 row 归档在 project 的 id 之下。
func ReadProjectDocument(row *StoredRow, projectID string) (ProjectDocument, error) {
	var doc ProjectDocument
	switch current, err := currentFormat(row.Data); {
	case err != nil:
		return ProjectDocument{}, err
	case current:
		var stored StoredDocument
		if err := readCurrent(row.Data, &stored, currentDocument...); err != nil {
			return ProjectDocument{}, err
		}
		doc = stored.ProjectDocument
	default:
		var stored struct {
			ProjectDocument
			CreatedAt LooseTime `json:"createdAt"`
			UpdatedAt LooseTime `json:"updatedAt"`
		}
		readLenient(row.Data, &stored)
		doc = stored.ProjectDocument
		doc.CreatedAt, doc.UpdatedAt = string(stored.CreatedAt), cmp.Or(string(stored.UpdatedAt), row.rowTime())
	}
	doc.ID, doc.ProjectID = strings.TrimPrefix(row.ID, projectID+"/"), projectID
	return doc, nil
}
