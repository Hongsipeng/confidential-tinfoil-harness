// 本文件把一次 run 的事件流组装成回复的消息与 timeline。
package domain

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// MessageAssembler builds a reply's messages from the events of its run.
// Apply runs under stream.mu, so tool goroutines and model deltas use one order.
// MessageAssembler 从 run 的事件流构建回复的消息。Apply 在 stream.mu 锁下运行，因此工具
// goroutine 与模型 delta 使用同一顺序。
type MessageAssembler struct {
	ordered []*Message
	byID    map[string]*Message
	calls   map[string]blockRef
	sealed  map[string]int // leading blocks of a continued reply that new text starts after；续写回复中新文本从其之后开始的前置 block
}

// blockRef is a tool call's block, which stays at its index as a reply grows.
// blockRef 是工具调用的 block，在回复增长时它始终保持在同一下标。
type blockRef struct {
	message *Message
	index   int
}

func (r blockRef) block() *Block { return &r.message.Timeline[r.index] }

// NewMessageAssembler 返回一个新的消息组装器。
func NewMessageAssembler() *MessageAssembler {
	return &MessageAssembler{byID: map[string]*Message{}, calls: map[string]blockRef{}, sealed: map[string]int{}}
}

// Seed starts from a reply that a continuation extends. As on the web
// client, only a trailing content block takes the new text.
// Seed 从续写所要延伸的回复开始。与 web 客户端一致，只有末尾的 content block 会承接新
// 文本。
func (a *MessageAssembler) Seed(reply Message) {
	m := reply.clone()
	if m.Timeline == nil {
		m.Timeline = []Block{}
	}
	sealed := len(m.Timeline)
	if sealed > 0 && m.Timeline[sealed-1].Type == "content" {
		sealed--
	}
	a.byID[m.ID], a.sealed[m.ID] = &m, sealed
	a.ordered = append(a.ordered, &m)
}
func (a *MessageAssembler) message(id string) *Message {
	if id == "" {
		id = "msg_orphan"
	}
	if m := a.byID[id]; m != nil {
		return m
	}
	m := &Message{ID: id, Role: "assistant", CreatedAt: api.Timestamp(), Timeline: []Block{}}
	a.byID[id] = m
	a.ordered = append(a.ordered, m)
	return m
}

// stamp dates a reply by its first event; a continued reply keeps its date.
// stamp 以回复的第一个事件为其标注日期；续写的回复保留原日期。
func (a *MessageAssembler) stamp(id string, m *Message, at string) {
	if _, seeded := a.sealed[id]; at != "" && !seeded && len(m.Timeline) == 0 {
		m.CreatedAt = at
	}
}

// streamed adds text or reasoning a reply streamed to its timeline.
// streamed 把回复流式输出的文本或推理添加到其 timeline。
func (a *MessageAssembler) streamed(id, kind, delta, emitted string) {
	m := a.message(id)
	a.stamp(id, m, emitted)
	at := float64(Millis(emitted))
	blocks := m.Timeline
	if kind == "thinking" {
		// Reasoning that arrives after the reply's text began is the tail
		// of the reasoning before that text, as on the web client.
		// 在回复文本开始之后到达的推理，是该文本之前那段推理的尾部，与 web 客户端一致。
		if n := len(blocks); n-2 >= a.sealed[id] && blocks[n-1].Type == "content" && blocks[n-2].Type == "thinking" {
			blocks[n-2].Content += delta
			return
		}
	}
	if len(blocks) <= a.sealed[id] || blocks[len(blocks)-1].Type != kind {
		block := Block{ID: fmt.Sprintf("%s_%s_%d", id, kind, len(blocks)), Type: kind}
		if kind == "thinking" {
			block.StartedAt = at
		}
		m.Timeline = append(blocks, block)
	}
	last := &m.Timeline[len(m.Timeline)-1]
	last.Content += delta
	if kind == "content" {
		m.Content += delta
	} else if last.StartedAt > 0 && at >= last.StartedAt {
		last.EndedAt, last.Duration = at, (at-last.StartedAt)/1000
	}
}

// Apply 把单个事件应用到正在组装的回复。
func (a *MessageAssembler) Apply(e Event, at string) {
	switch e := e.(type) {
	case TextChunk:
		a.streamed(e.MessageID, "content", e.Delta, at)
	case ReasoningChunk:
		a.streamed(strings.TrimSuffix(e.MessageID, ReasoningSuffix), "thinking", e.Delta, at)
	case ToolCallStart:
		parent := e.ParentMessageID
		if parent == "" && len(a.ordered) > 0 {
			parent = a.ordered[len(a.ordered)-1].ID
		}
		if parent == "" {
			parent = "msg_" + e.ToolCallID
		}
		m := a.message(parent)
		a.stamp(parent, m, at)
		a.calls[e.ToolCallID] = blockRef{m, len(m.Timeline)}
		m.Timeline = append(m.Timeline, Block{ID: e.ToolCallID, Type: "tool_call", ToolCallID: e.ToolCallID, Name: e.ToolCallName, Progress: []json.RawMessage{}, StartedAt: float64(Millis(at))})
	case ToolCallArgs:
		if call, ok := a.calls[e.ToolCallID]; ok {
			call.block().Arguments += e.Delta
		}
	case ToolCallEnd:
		if call, ok := a.calls[e.ToolCallID]; ok {
			call.block().Complete = true
		}
	case ToolCallResult:
		if call, ok := a.calls[e.ToolCallID]; ok {
			b := call.block()
			b.Result = e.Content
			if at := float64(Millis(at)); at >= b.StartedAt {
				b.EndedAt = at
			}
			if len(e.Metadata) > 0 {
				b.Metadata = e.Metadata
			}
			if e.Display != nil {
				b.Display = e.Display
			}
		}
	case ActivitySnapshot:
		if call, ok := a.calls[strings.TrimPrefix(e.MessageID, ActivityPrefix)]; ok && e.Display != nil {
			call.block().Display = e.Display
		}
	case ActivityDelta:
		if call, ok := a.calls[strings.TrimPrefix(e.MessageID, ActivityPrefix)]; ok {
			for _, op := range e.Patch {
				if op.Path == "/output/-" {
					b := call.block()
					b.Progress = append(b.Progress, api.Raw(op.Value))
				}
			}
		}
	}
}

// ReplaceReplyText rewrites a reply's text as the web client does: its
// content blocks collapse into one holding the text, where the first was.
// ReplaceReplyText 按 web 客户端的方式重写回复文本：其 content block 合并为一个持有该
// 文本的块，位于第一个 content block 原来的位置。
func ReplaceReplyText(message *Message, text string) {
	timeline := message.Timeline
	first := slices.IndexFunc(timeline, func(block Block) bool { return block.Type == "content" })
	edited := Block{Type: "content", ID: "edited-content", Content: text}
	if first < 0 {
		first = len(timeline)
	} else {
		edited.ID = timeline[first].ID
	}
	blocks := []Block{}
	for i := range len(timeline) + 1 {
		if i == first && text != "" {
			blocks = append(blocks, edited)
		}
		if i < len(timeline) && timeline[i].Type != "content" {
			blocks = append(blocks, timeline[i])
		}
	}
	message.Content = text
	message.Timeline = nil
	if len(blocks) > 0 {
		message.Timeline = blocks
	}
}

// LastBlock is the block a reply streams into now, if any.
// LastBlock 是回复当前正在流式写入的 block；没有则为 nil。
func (a *MessageAssembler) LastBlock(id string) *Block {
	m := a.byID[id]
	if m == nil || len(m.Timeline) == 0 {
		return nil
	}
	return &m.Timeline[len(m.Timeline)-1]
}

// Messages 返回组装结果中每个消息的副本。
func (a *MessageAssembler) Messages() []Message {
	out := []Message{}
	for _, m := range a.ordered {
		out = append(out, m.clone())
	}
	return out
}

// Millis 把 RFC 3339 时间值解析为 Unix 毫秒；解析失败时返回 0。
func Millis(value string) int64 {
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t.UnixMilli()
	}
	return 0
}

// ChatURL is where a share link opens.
// ChatURL 是分享链接打开的位置。
const ChatURL = "https://chat.tinfoil.sh"

// A signed-in client can also share a chat it keeps itself by sending the
// transcript, with no content key. As from the web client, that share holds
// image thumbnails and document text, so the transcript may omit image bytes.
// No attachment is uploaded for it: the cloud holds no chat to keep one under.
// 已登录客户端也可以通过发送 transcript（不带 content key）来分享它自己保存的聊天。与
// web 客户端一致，该分享持有图像缩略图与文档文本，因此 transcript 可省略图像字节。它不会
// 上传任何附件：云端没有聊天可用来存放附件。
