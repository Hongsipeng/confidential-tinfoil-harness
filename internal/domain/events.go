// 本文件定义一次 run 流式输出的事件及其编码。
package domain

import (
	"bytes"
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Event is one event of a run, each type as protocol/openapi.yaml names it.
// Event 是一次 run 的单个事件，每种类型以 protocol/openapi.yaml 中的名称命名。
type Event interface{ eventType() string }

// RunStarted 表示一次 run 的开始。
type RunStarted struct {
	ThreadID  string `json:"threadId"`
	RunID     string `json:"runId"`
	Instance  string `json:"instance"`
	MessageID string `json:"messageId,omitempty"` // the reply a continuation streams into
	// 续写流入的回复。
}

// MessagesSnapshot 是消息的快照。
type MessagesSnapshot struct {
	Messages []MessageView `json:"messages"`
	HasOlder bool          `json:"hasOlder"`
}

// StateSnapshotEvent 携带一次状态快照。
type StateSnapshotEvent struct {
	Snapshot StateSnapshot `json:"snapshot"`
}

// StateDelta changes a run's state. One naming a run is about a turn queued
// behind the running reply.
// StateDelta 变更一次 run 的状态。其中指名 run 的那一条，描述排在运行中回复之后、正在
// 排队的 turn。
type StateDelta struct {
	ThreadID string       `json:"threadId,omitempty"`
	RunID    string       `json:"runId,omitempty"`
	Delta    []StatePatch `json:"delta"`
}

// TextChunk 是回复文本的一段增量。
type TextChunk struct {
	MessageID string `json:"messageId"`
	Role      string `json:"role"`
	Delta     string `json:"delta"`
}

// ReasoningChunk streams a reply's reasoning, under the reply's id with
// ReasoningSuffix after it.
// ReasoningChunk 流式输出回复的推理内容，其 id 为回复 id 加上 ReasoningSuffix 后缀。
type ReasoningChunk struct {
	MessageID string `json:"messageId"`
	Delta     string `json:"delta"`
}

// ReasoningSuffix 是推理消息 id 的后缀。
const ReasoningSuffix = "-reasoning"

// ReasoningSummary 是回复推理内容的摘要。
type ReasoningSummary struct {
	MessageID string `json:"messageId"`
	BlockID   string `json:"blockId"`
	Content   string `json:"content"`
}

// ToolCallStart 表示一次工具调用的开始。
type ToolCallStart struct {
	ToolCallID      string `json:"toolCallId"`
	ToolCallName    string `json:"toolCallName"`
	ParentMessageID string `json:"parentMessageId,omitempty"`
}

// ToolCallArgs 是工具调用参数的一段增量。
type ToolCallArgs struct {
	ToolCallID string `json:"toolCallId"`
	Delta      string `json:"delta"`
}

// ToolCallEnd 表示一次工具调用的结束。
type ToolCallEnd struct {
	ToolCallID string `json:"toolCallId"`
}

// ToolCallResult 是一次工具调用的结果。
type ToolCallResult struct {
	MessageID  string          `json:"messageId"`
	ToolCallID string          `json:"toolCallId"`
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	Display    *ToolDisplay    `json:"display,omitempty"`
}

// ActivitySnapshot opens a tool's output while it runs, under the call's id
// with activityPrefix before it.
// ActivitySnapshot 在工具运行时开启其输出，其 id 为调用 id 前加 activityPrefix 前缀。
type ActivitySnapshot struct {
	MessageID    string       `json:"messageId"`
	ActivityType string       `json:"activityType"`
	Content      ToolActivity `json:"content"`
	Display      *ToolDisplay `json:"display,omitempty"`
}

// ActivityDelta 是工具活动输出的增量补丁。
type ActivityDelta struct {
	MessageID    string          `json:"messageId"`
	ActivityType string          `json:"activityType"`
	Patch        []ActivityPatch `json:"patch"`
	Display      *ToolDisplay    `json:"display,omitempty"`
}

// ActivityPrefix 是工具活动消息 id 的前缀。
const ActivityPrefix = "act_"

// ToolActivity is a tool's output as it runs: its progress, and the lines
// it printed.
// ToolActivity 是工具运行时的输出：其进度，以及它打印的各行。
type ToolActivity struct {
	ToolCallID string   `json:"toolCallId"`
	Tool       string   `json:"tool"`
	Progress   float64  `json:"progress"`
	Output     []string `json:"output"`
}

// ActivityPatch 是活动输出的单处变更。
type ActivityPatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// CompactionStart opens a compaction marker, placed before the question it
// precedes, or at the end of the chat when it names none, while its summary
// is written.
// CompactionStart 开启一个 compaction 标记：放置在其所前置的问题之前；若未指名问题，则
// 放在聊天末尾，在其摘要写出时。
type CompactionStart struct {
	MessageID       string `json:"messageId"`
	BeforeMessageID string `json:"beforeMessageId"`
}

// CompactionEnd closes a compaction marker, where its CompactionStart placed
// it: completed with the marker, or failed, when it is not kept.
// CompactionEnd 关闭由对应 CompactionStart 放置的 compaction 标记：随标记一起完成；或
// 在标记不被保留时失败。
type CompactionEnd struct {
	MessageID       string       `json:"messageId"`
	BeforeMessageID string       `json:"beforeMessageId"`
	Outcome         string       `json:"outcome"`
	Marker          *MessageView `json:"marker,omitempty"`
}

// The outcomes a CompactionEnd closes its marker with.
// CompactionEnd 关闭其标记时的两种结果。
const (
	CompactionCompleted = "completed"
	CompactionFailed    = "failed"
)

// RunFinished 表示一次 run 完成。
type RunFinished struct {
	ThreadID string      `json:"threadId"`
	RunID    string      `json:"runId"`
	Metadata RunMetadata `json:"metadata"`
}

// RunError ends a failed run with the members a refusal has. One a stream
// ends with after its run was lost names neither the chat nor the run.
// RunError 以一次拒绝所包含的成员结束失败的 run。若某流在其 run 丢失后结束，则既不指名
// 聊天也不指名 run。
type RunError struct {
	ThreadID string `json:"threadId,omitempty"`
	RunID    string `json:"runId,omitempty"`
	*api.Error
	Metadata *RunMetadata `json:"metadata,omitempty"`
}

// FailedRun 构造一次失败的 run 结束事件。
func FailedRun(threadID, runID string, e *api.Error, metadata *RunMetadata) RunError {
	return RunError{ThreadID: threadID, RunID: runID, Error: e, Metadata: metadata}
}

func (RunStarted) eventType() string         { return "RUN_STARTED" }
func (MessagesSnapshot) eventType() string   { return "MESSAGES_SNAPSHOT" }
func (StateSnapshotEvent) eventType() string { return "STATE_SNAPSHOT" }
func (StateDelta) eventType() string         { return "STATE_DELTA" }
func (TextChunk) eventType() string          { return "TEXT_MESSAGE_CHUNK" }
func (ReasoningChunk) eventType() string     { return "REASONING_MESSAGE_CHUNK" }
func (ReasoningSummary) eventType() string   { return "REASONING_SUMMARY" }
func (ToolCallStart) eventType() string      { return "TOOL_CALL_START" }
func (ToolCallArgs) eventType() string       { return "TOOL_CALL_ARGS" }
func (ToolCallEnd) eventType() string        { return "TOOL_CALL_END" }
func (ToolCallResult) eventType() string     { return "TOOL_CALL_RESULT" }
func (ActivitySnapshot) eventType() string   { return "ACTIVITY_SNAPSHOT" }
func (ActivityDelta) eventType() string      { return "ACTIVITY_DELTA" }
func (CompactionStart) eventType() string    { return "COMPACTION_START" }
func (CompactionEnd) eventType() string      { return "COMPACTION_END" }
func (RunFinished) eventType() string        { return "RUN_FINISHED" }
func (RunError) eventType() string           { return "RUN_ERROR" }

// TextDelta is an event that extends the text that earlier events of its
// type with the same target began: a message's content or reasoning, or a
// call's arguments. A client appends its delta, so two in turn apply as one
// whose delta is both, which is how a run sends deltas that follow each
// other closely and how its checkpoint sends them; the protocol's
// description of checkpoints names each such event.
// TextDelta 是一种扩展文本的事件，延续同类型、同目标的前序事件所开始的文本：消息的正文
// 或推理内容，或调用的参数。客户端会追加其 delta，因此连续的两条等价于 delta 为二者之和
// 的一条，这正是 run 发送紧密相连的 delta 以及 checkpoint 发送它们的方式；协议对
// checkpoint 的描述点名了每一类这样的事件。
type TextDelta interface {
	Event
	Target() string
	// Text is the text the event appends.
	// Text 是该事件所追加的文本。
	Text() string
	// WithText is the event appending text instead.
	// WithText 是改为追加 text 的事件。
	WithText(text string) TextDelta
}

// Target 返回该事件所追加文本的目标 id。
func (c TextChunk) Target() string      { return c.MessageID }
func (c ReasoningChunk) Target() string { return c.MessageID }
func (a ToolCallArgs) Target() string   { return a.ToolCallID }

// Text 返回该事件追加的文本。
func (c TextChunk) Text() string      { return c.Delta }
func (c ReasoningChunk) Text() string { return c.Delta }
func (a ToolCallArgs) Text() string   { return a.Delta }

// WithText 返回改为追加给定文本的事件。
func (c TextChunk) WithText(text string) TextDelta      { c.Delta = text; return c }
func (c ReasoningChunk) WithText(text string) TextDelta { c.Delta = text; return c }
func (a ToolCallArgs) WithText(text string) TextDelta   { a.Delta = text; return a }

// EncodeEvent is an event's frame: its type, the time it was emitted unless
// none is given, then its own members.
// EncodeEvent 是一个事件的帧：其类型、发出时间（如未给出则省略），随后是其自身成员。
func EncodeEvent(e Event, at string) ([]byte, error) {
	members, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	header, err := json.Marshal(struct {
		Type      string `json:"type"`
		Timestamp string `json:"timestamp,omitempty"`
	}{e.eventType(), at})
	if err != nil {
		return nil, err
	}
	if bytes.Equal(members, []byte("{}")) {
		return header, nil
	}
	return append(append(header[:len(header)-1], ','), members[1:]...), nil
}
