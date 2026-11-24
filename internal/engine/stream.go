package engine

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Stream 是 run 的输出流：它将模型回复、工具活动与推理等事件成帧写入 run 的日志。
type Stream struct {
	Run            *Log
	concreteModel  string
	modelAccepted  bool
	replyID        string
	turnReasoning  string
	reasoningField string // which of reasoning_content and reasoning the last turn streamed
	truncated      bool   // the model filled the room its window left the reply
	// musing shows what the model writes as its reasoning, as a research's
	// notes between its calls are, which are not its reply.
	// musing 显示模型写下的推理内容，就像 research 在其调用之间的笔记那样，它们并不是回复。
	musing bool

	// mu serializes writes against the tool goroutines.
	// mu 将写入与工具 goroutine 串行化。
	mu       sync.Mutex
	spend    api.Usage
	Observe  func(e domain.Event, at string)
	thoughts *thoughtSummaries // nil unless a summarizer summarizes the run's reasoning
	sources  []citationSource  // pages the run's tools showed the model

	// folding is set while the reply streams, when a log past its budget is
	// folded into a checkpoint; not before the reply starts or once it ends.
	// folding 在回复流式输出期间、当日志超过预算而被折叠为 checkpoint 时置位；
	// 在回复开始前或结束后不会。
	folding bool
	// activities is each tool's activity as its events have left it, which
	// a checkpoint sends in their place.
	// activities 是每个工具活动在其事件离开后的状态，checkpoint 会在其位置发送它。
	activities []*domain.ActivitySnapshot
	// pending is the deltas held back to leave as one frame; lastDelta is
	// what the log's last frame extends, when it is a delta, and lastAt
	// when it was framed.
	// pending 是为作为一个帧发出而被暂扣的 delta；lastDelta 是日志最后一帧所扩展的目标
	// （当它是 delta 时），lastAt 是它成帧的时间。
	pending   *pendingDelta
	lastDelta string
	lastAt    time.Time
}

// A delta that continues the frame before it within deltaWindow is held
// back until the window ends, and the deltas to its target that follow join
// it, so a fast model's reply grows by a frame a window rather than a frame
// a token, which a slow client falls behind on. A delta after anything else
// leaves at once, and anything else sends the held deltas before it.
// 在 deltaWindow 内继续其前一帧的 delta 会被暂扣，直到窗口结束，随后针对同一目标的 delta
// 会加入它，这样快速模型的回复是按每个窗口一帧而非每个 token 一帧增长，慢客户端才不会落后。
// 其他事件之后的 delta 会立即发出，而其他事件会先发送暂扣的 delta。
const deltaWindow = 50 * time.Millisecond

// pendingDelta is deltas to one target held back until their window ends.
// pendingDelta 是针对同一目标、暂扣到其窗口结束的 delta。
type pendingDelta struct {
	first domain.TextDelta
	key   string
	text  strings.Builder
	// at is when the last of them was emitted, which their frame keeps, as
	// a client times reasoning by when it last grew.
	// at 是其中最后一个被发出的时间，其帧会保留它，因为客户端按推理最后一次增长的时间来计时。
	at    string
	timer *time.Timer
}

// track keeps each tool's activity as its events leave it: the snapshot a
// call opens with, then the progress and lines it reports.
// track 在其事件离开时保存每个工具的活动：先是调用开启时的快照，然后是它所报告的进度和输出行。
func (s *Stream) track(e domain.Event) {
	switch e := e.(type) {
	case domain.ActivitySnapshot:
		e.Content.Output = slices.Clone(e.Content.Output)
		if i := slices.IndexFunc(s.activities, func(a *domain.ActivitySnapshot) bool { return a.MessageID == e.MessageID }); i >= 0 {
			s.activities[i] = &e
		} else {
			s.activities = append(s.activities, &e)
		}
	case domain.ActivityDelta:
		i := slices.IndexFunc(s.activities, func(a *domain.ActivitySnapshot) bool { return a.MessageID == e.MessageID })
		if i < 0 {
			return
		}
		a := s.activities[i]
		for _, op := range e.Patch {
			switch value := op.Value.(type) {
			case float64:
				if op.Path == "/progress" {
					a.Content.Progress = value
				}
			case string:
				if op.Path == "/output/-" {
					a.Content.Output = append(a.Content.Output, value)
				}
			}
		}
		if e.Display != nil {
			a.Display = e.Display
		}
	}
}

// activityFrame is the frame of a tool's activity as its events have left
// it, or nil for one the stream does not follow.
// activityFrame 是工具活动在其事件离开后的帧；对于流未跟随的活动则返回 nil。
func (s *Stream) activityFrame(id string) []byte {
	i := slices.IndexFunc(s.activities, func(a *domain.ActivitySnapshot) bool { return a.MessageID == id })
	if i < 0 {
		return nil
	}
	encoded, err := domain.EncodeEvent(*s.activities[i], api.Timestamp())
	if err != nil {
		return nil
	}
	return encoded
}

// merged is one frame for a run of frames of domain.TextDelta events to the
// same target: the first's members, with each frame's delta in turn.
// merged 是将针对同一目标的连续 domain.TextDelta 事件帧合并为一个帧：取首帧的成员，
// 依次拼接各帧的 delta。
func merged(deltas [][]byte) []byte {
	var first map[string]json.RawMessage
	json.Unmarshal(deltas[0], &first)
	var text strings.Builder
	for _, frame := range deltas {
		var d struct {
			Delta string `json:"delta"`
		}
		json.Unmarshal(frame, &d)
		text.WriteString(d.Delta)
	}
	first["delta"], _ = json.Marshal(text.String())
	encoded, _ := json.Marshal(first)
	return encoded
}

func (s *Stream) cite(sources []citationSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources = append(s.sources, sources...)
}

func (s *Stream) emit(e domain.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frame(e, api.Timestamp())
}

func (s *Stream) text(message, delta string) {
	s.mu.Lock()
	musing := s.musing
	s.mu.Unlock()
	if musing {
		s.reasoning(message, delta)
		return
	}
	s.emit(domain.TextChunk{MessageID: message, Role: "assistant", Delta: delta})
}

// muse sets whether what the model writes is shown as its reasoning.
// muse 设置模型写下的内容是否作为其推理显示。
func (s *Stream) muse(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.musing = on
}

func (s *Stream) reasoning(message, delta string) {
	s.emit(domain.ReasoningChunk{MessageID: message + domain.ReasoningSuffix, Delta: delta})
}

func (s *Stream) callStart(id, name, parent string) {
	s.emit(domain.ToolCallStart{ToolCallID: id, ToolCallName: name, ParentMessageID: parent})
}

func (s *Stream) callArgs(id, delta string) {
	s.emit(domain.ToolCallArgs{ToolCallID: id, Delta: delta})
}

func (s *Stream) callEnd(id string) {
	s.emit(domain.ToolCallEnd{ToolCallID: id})
}

// activity opens one call's output under an id derived from the call itself.
// activity 以由调用本身派生出的 id 开启一次调用的输出。
func (s *Stream) activity(call ToolCall) {
	s.emit(domain.ActivitySnapshot{MessageID: domain.ActivityPrefix + call.ID, ActivityType: activityTool, Content: domain.ToolActivity{ToolCallID: call.ID, Tool: call.Name, Output: []string{}}, Display: domain.SearchDisplay(call.Name, call.Args, nil)})
}

// output is a list because RFC 6902 cannot grow a string.
// output 是列表，因为 RFC 6902 无法扩展字符串。
func (s *Stream) progress(call, note string, done float64) {
	patch := []domain.ActivityPatch{{Op: "replace", Path: "/progress", Value: done}}
	if note != "" {
		patch = append(patch, domain.ActivityPatch{Op: "add", Path: "/output/-", Value: note})
	}
	s.emit(domain.ActivityDelta{MessageID: domain.ActivityPrefix + call, ActivityType: activityTool, Patch: patch})
}

const activityTool = "TOOL"

// Accepted marks the model as having accepted a request of the run.
// Accepted 标记模型已接受 run 的请求。
func (s *Stream) Accepted() {
	s.mu.Lock()
	s.modelAccepted = true
	s.mu.Unlock()
}

func (s *Stream) Meter(raw json.RawMessage) {
	var turn api.Usage
	if json.Unmarshal(raw, &turn) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spend.Prompt += turn.Prompt
	s.spend.Completion += turn.Completion
	s.spend.Total += turn.Total
}

// frame frames e, unless it is a delta held back to join others.
// frame 为 e 成帧，除非它是被暂扣以合并其他 delta 的 delta。
func (s *Stream) frame(e domain.Event, at string) {
	kind, key := kindOf(e)
	if kind != frameDelta {
		s.framePending()
		s.write(e, kind, key, at)
		return
	}
	d := e.(domain.TextDelta)
	if p := s.pending; p != nil && p.key == key && p.text.Len()+len(d.Text()) <= mergedFrameBytes {
		p.text.WriteString(d.Text())
		p.at = at
		return
	}
	s.framePending()
	if wait := deltaWindow - time.Since(s.lastAt); key == s.lastDelta && wait > 0 {
		p := &pendingDelta{first: d, key: key, at: at}
		p.text.WriteString(d.Text())
		p.timer = time.AfterFunc(wait, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.pending == p {
				s.framePending()
			}
		})
		s.pending = p
		return
	}
	s.write(e, kind, key, at)
}

// framePending frames the deltas held back as one, if there are any.
// framePending 若有暂扣的 delta，则将其作为一个帧发出。
func (s *Stream) framePending() {
	p := s.pending
	if p == nil {
		return
	}
	s.pending = nil
	p.timer.Stop()
	s.write(p.first.WithText(p.text.String()), frameDelta, p.key, p.at)
}

func (s *Stream) write(e domain.Event, kind frameKind, key, at string) {
	encoded, err := domain.EncodeEvent(e, at)
	if err != nil {
		return
	}
	full := s.Run.Frames.grow(encoded, kind, key)
	s.lastDelta = ""
	if kind == frameDelta {
		s.lastDelta, s.lastAt = key, time.Now()
	}
	s.track(e)
	if s.Observe != nil {
		s.Observe(e, at)
		if s.thoughts != nil {
			s.thoughts.follow(e)
		}
	}
	// The activities are tracked up to this frame, under the lock that
	// orders frames, so they stand for every frame folded.
	// 活动会一直跟踪到这个帧，在给帧排序的锁保护下进行，因此它们能代表每个被折叠的帧。
	if full && s.folding {
		s.Run.Frames.fold(merged, s.activityFrame)
	}
}

// kindOf is what a checkpoint does with an event, and what the event extends
// or reports on. A checkpoint merges text deltas and keeps each tool's
// activity as it stands; every other event, the run's snapshots and the
// calls and their results among them, it sends as it was.
// kindOf 表示 checkpoint 对某个事件所做的处理，以及该事件所扩展或报告的目标。
// checkpoint 会合并文本 delta 并按当前状态保存每个工具的活动；其他所有事件——
// 包括 run 的快照、调用及其结果——则按原样发送。
func kindOf(e domain.Event) (frameKind, string) {
	switch e := e.(type) {
	case domain.TextDelta:
		return frameDelta, fmt.Sprintf("%T:%s", e, e.Target())
	case domain.ActivitySnapshot:
		return frameActivity, e.MessageID
	case domain.ActivityDelta:
		return frameActivity, e.MessageID
	}
	return frameKept, ""
}

const (
	// maxSnapshotBytes bounds a MESSAGES_SNAPSHOT, so a run starts by sending
	// no more of a long chat than this; a client loads older messages as it
	// shows them.
	// maxSnapshotBytes 限制 MESSAGES_SNAPSHOT 的大小，这样 run 开始时发送的长聊天内容
	// 就不会超过这个量；客户端会在展示时按需加载更早的消息。
	maxSnapshotBytes = 8 << 20
	// snapshotEnvelope is room for what a snapshot's event holds besides
	// its messages.
	// snapshotEnvelope 是快照事件除其消息之外所持有内容的空间。
	snapshotEnvelope = 4 << 10
)

// fittedSnapshot is a snapshot of messages that fits in maxSnapshotBytes:
// the oldest that do not fit are left out, as older messages, and the latest
// is always sent. A latest message too large alone, as one carrying large
// documents, is sent with its attachments but not their text, which clients
// read on demand.
// fittedSnapshot 是能放入 maxSnapshotBytes 的消息快照：放不下的最老消息会被略去，
// 标记为更早的消息，而最新消息总是会发送。单条就过大的最新消息（如携带大文档的消息）
// 会连同其附件发送，但不含附件文本，客户端会按需读取。
func fittedSnapshot(messages []domain.MessageView, hasOlder bool) domain.MessagesSnapshot {
	sizes, total := make([]int, len(messages)), 0
	for i := range messages {
		encoded, _ := json.Marshal(messages[i])
		sizes[i] = len(encoded) + 1
		total += sizes[i]
	}
	start := 0
	for start < len(messages)-1 && total > maxSnapshotBytes-snapshotEnvelope {
		total -= sizes[start]
		start++
	}
	fitted := messages[start:]
	if len(fitted) == 1 && total > maxSnapshotBytes-snapshotEnvelope {
		latest := fitted[0]
		latest.Attachments = slices.Clone(latest.Attachments)
		for i := range latest.Attachments {
			latest.Attachments[i].TextContent = ""
		}
		fitted = []domain.MessageView{latest}
	}
	return domain.MessagesSnapshot{Messages: fitted, HasOlder: hasOlder || start > 0}
}

// Follow streams a run's log from the frame from, and tails it until the
// run ends. Only a checkpoint's last event names a cursor, the one before the
// first frame after it, so a follower that misses part of a checkpoint is
// sent all of it again.
// It reports whether it sent the run to its end.
// Follow 从帧 from 开始流式发送 run 的日志，并一直跟踪到 run 结束。只有 checkpoint 的
// 最后一个事件会给出游标（即其后首帧之前的一个），这样错过了 checkpoint 一部分的跟随者
// 会被重新发送整个 checkpoint。
// 它报告是否将 run 发送到了末尾。
func Follow(w http.ResponseWriter, r *http.Request, rn *Log, from int) bool {
	flush := http.NewResponseController(w)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	opened := false
	open := func() {
		if opened {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		opened = true
	}
	send := func(checkpoint, frames [][]byte, first int) error {
		for i, frame := range checkpoint {
			open()
			var err error
			if i == len(checkpoint)-1 {
				_, err = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", first-1, frame)
			} else {
				_, err = fmt.Fprintf(w, "data: %s\n\n", frame)
			}
			if err != nil {
				return err
			}
		}
		for i, frame := range frames {
			open()
			if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", first+i, frame); err != nil {
				return err
			}
		}
		return nil
	}
	for {
		checkpoint, frames, first, wake, closed, done := rn.Frames.Read(from)
		err := send(checkpoint, frames, first)
		done()
		if err != nil {
			return false
		}
		from = first + len(frames)
		if len(checkpoint)+len(frames) > 0 {
			if err := flush.Flush(); err != nil {
				return false
			}
		}
		if closed != nil {
			open()
			return true
		}
		select {
		case <-wake:
		case <-heartbeat.C:
			open()
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return false
			}
			if err := flush.Flush(); err != nil {
				return false
			}
		case <-r.Context().Done():
			return false
		}
	}
}
