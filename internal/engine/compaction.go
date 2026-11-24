package engine

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// A turn whose prompt would not fit the window compacts its chat: the
// messages from the chat's latest compaction marker on, up to the latest
// exchanges before the turn's question, are summarized into a new marker
// before the first question it keeps. The model reads the summary in their
// place, and the exchanges after it and the question as they were. A run
// compacts once, before its first model turn or between two of them, and
// its marker is kept with the turn even when the reply then fails. A compact
// turn compacts its chat the same way on request, its last question being
// the turn's, and asks the model nothing. One that compacts the latest
// exchange too summarizes every message up to the chat's last reply, and
// its marker sits after that reply, at the end of the chat unless a question
// without a reply follows.
// 当 turn 的 prompt 无法放入窗口时会对其聊天做 compaction：从聊天最近一次 compaction
// 标记开始、直到该 turn 的问题之前最近的若干 exchange，这些消息被汇总为一个新标记，
// 放在其保留的第一个问题之前。模型在该位置读取摘要，而其后的 exchange 以及该问题则按原样读取。
// 每个 run 只做一次 compaction，发生在其第一个模型 turn 之前或两个 turn 之间，且即使回复随后
// 失败，标记也会随该 turn 保留。compact turn 会按同样方式在请求时对聊天做 compaction，
// 其最后一个问题就是该 turn 的问题，并且不向模型提问。若同时压缩最近的 exchange，
// 则会汇总直到聊天最后一条回复的所有消息，其标记位于该回复之后、聊天末尾，
// 除非其后还跟着一个没有回复的问题。

// recentTokens is how much of the chat before a compacted turn's question
// the model may go on reading as it was, so a follow-up on what was just
// said does not rest on the summary.
// recentTokens 是紧凑后的 turn 问题之前、模型仍可按原样继续读取的聊天内容大小，
// 这样对刚说过内容的追问就不必依赖摘要。
const recentTokens = 20000

// recentLimitShareTenths is the share of the prompt budget the kept
// exchanges may fill instead, when that is more, so a model with a larger
// window keeps more of them.
// recentLimitShareTenths 是在保留的 exchange 可占用更多空间时，它们可占据 prompt 预算的比例
// （以十分之一为单位），这样窗口更大的模型就能保留更多的 exchange。
const recentLimitShareTenths = 1

// recentShareTenths is the share of the room left beside the summary that
// the kept exchanges may fill, so the turns after a compaction have room
// before the next one, and a summary longer than its reserve still fits.
// recentShareTenths 是摘要之外剩余空间中保留 exchange 可占用的比例（以十分之一为单位），
// 这样 compaction 之后的 turn 在下一次 compaction 前仍有空间，且比其预留更长的摘要也放得下。
const recentShareTenths = 5

// compaction is what a run's compaction leaves: its marker, the message it
// sits before, or else the one it sits after at the end of the chat, and the
// context the turn's prompt fills after it.
// compaction 是 run 的 compaction 所留下的内容：其标记、它位于其前的消息，或在聊天末尾时
// 位于其后的消息，以及该 turn 的 prompt 在其后所填充的上下文。
type compaction struct {
	marker   domain.Message
	beforeID string
	afterID  string
	usage    *domain.ContextUsage
}

// at is where the marker goes among messages: before the message it sits
// before, or after the one it sits after, or -1 once that message is gone.
// at 返回标记在消息中的位置：位于其前的那条消息之前，或位于其后的那条消息之后；
// 一旦该消息消失则返回 -1。
func (c *compaction) at(t *domain.Thread) int {
	if c.beforeID != "" {
		return t.MessageIndex(c.beforeID)
	}
	if after := t.MessageIndex(c.afterID); after >= 0 {
		return after + 1
	}
	return -1
}

// errNothingToCompact is a prompt too large whose chat holds no message
// before its question that a summary could replace.
// errNothingToCompact 表示 prompt 过大，但聊天在其问题之前没有可被摘要替换的消息。
var errNothingToCompact = errors.New("engine: nothing before the question to compact")

// errCompactionFailed refuses a turn whose chat could not be compacted.
// errCompactionFailed 拒绝那些聊天无法被 compact 的 turn。
func errCompactionFailed() error {
	return api.Err(502, "COMPACTION_FAILED", "The chat could not be compacted to fit the context window")
}

// errNothingToCompactNow refuses a compact turn whose chat holds no message
// after its latest marker that a summary could replace.
// errNothingToCompactNow 拒绝那些在其最新标记之后没有可被摘要替换的消息的 compact turn。
func errNothingToCompactNow() error {
	return api.Err(400, "NOTHING_TO_COMPACT", "The chat holds nothing to compact")
}

// lastOf is the index of the last message of role among messages, or -1.
// lastOf 返回 messages 中最后一个角色为 role 的消息下标，没有则返回 -1。
func lastOf(messages []domain.Message, role string) int {
	last := len(messages) - 1
	for last >= 0 && messages[last].Role != role {
		last--
	}
	return last
}

// lastQuestion is the index of the last question among messages, or -1.
// lastQuestion 返回 messages 中最后一个问题（user 消息）的下标，没有则返回 -1。
func lastQuestion(messages []domain.Message) int {
	return lastOf(messages, "user")
}

// compactable reports whether a compact turn of messages has something to
// summarize after the latest marker: a message before the last question,
// or a reply when the turn compacts the latest exchange too.
// compactable 报告这些消息的 compact turn 在最新标记之后是否有可汇总的内容：
// 最后一个问题之前的消息，或在 turn 也压缩最新 exchange 时的一条回复。
func compactable(messages []domain.Message, latestToo bool) bool {
	if latestToo {
		return lastOf(messages, "assistant") > domain.LatestCompaction(messages)
	}
	return lastQuestion(messages) > domain.LatestCompaction(messages)+1
}

// compactChat carries out a compact turn: up to the chat's last reply when
// it compacts the latest exchange too, or else as a turn whose prompt fills
// what a turn's would besides the chat.
// compactChat 执行 compact turn：若它也压缩最新 exchange，则一直到聊天的最后一条回复；
// 否则按一个 prompt 除聊天外所填充内容的 turn 来处理。
func (eng *Engine) compactChat(ctx context.Context, r *Run, req *Request) error {
	if r.turn.CompactLatest {
		return eng.compactBefore(ctx, r, req, lastOf(req.messages, "assistant")+1)
	}
	return eng.compact(ctx, r, req, fixedCost(req, req.webSearch, api.Raw(req.rendered)))
}

// compact summarizes the messages of req before its latest exchanges into a
// marker, and reads the chat into req from that marker on. reserved is what
// the turn's prompt fills besides the chat: its system prompt, tools and
// reminder, and the run's exchange so far.
// compact 将 req 中最新若干 exchange 之前的消息汇总为一个标记，并从该标记起将聊天读入 req。
// reserved 是除聊天外该 turn 的 prompt 所填充的内容：其 system prompt、工具与提醒，
// 以及 run 到目前为止的 exchange。
func (eng *Engine) compact(ctx context.Context, r *Run, req *Request, reserved int) error {
	messages := req.messages
	question := lastQuestion(messages)
	latest := domain.LatestCompaction(messages)
	if question <= latest+1 {
		return errNothingToCompact
	}
	room := req.limit() - reserved - summaryReserve
	for i := question; i < len(messages); i++ {
		room -= messageCost(req, &messages[i])
	}
	shown := slices.IndexFunc(messages, func(m domain.Message) bool { return m.ID == r.firstShown })
	if shown < 0 {
		shown = question
	}
	kept := keptFrom(req, latest, question, shown, min(max(recentTokens, req.limit()*recentLimitShareTenths/10), room*recentShareTenths/10))
	return eng.compactBefore(ctx, r, req, kept)
}

// compactBefore summarizes the messages of req from its latest marker on,
// up to the one at kept, into a marker there, and reads the chat into req
// from that marker on. A marker with no message after it ends the chat.
// compactBefore 将 req 中从其最新标记起、直到 kept 处的消息汇总为该处的标记，
// 并从该标记起将聊天读入 req。之后没有消息的标记意味着聊天的结束。
func (eng *Engine) compactBefore(ctx context.Context, r *Run, req *Request, kept int) error {
	messages := req.messages
	latest := domain.LatestCompaction(messages)
	earlier := ""
	if latest >= 0 {
		earlier = messages[latest].Content
	}
	marker := domain.Message{ID: "msg_" + api.Token(), Role: domain.CompactionRole}
	before := ""
	if kept < len(messages) {
		before = messages[kept].ID
	}
	r.out.emit(domain.CompactionStart{MessageID: marker.ID, BeforeMessageID: before})
	started := api.Now()
	var result summarize.Result
	err := errCompactionFailed()
	if eng.Summaries != nil {
		input := summarize.Input{Earlier: earlier, Documents: transcript(messages[latest+1:kept], req.loc)}
		result, err = eng.Summaries.Summarize(ctx, summarize.Compaction, input)
		// The caller pays for the summary, so a run whose chat access could
		// not be renewed ends saying so, as a model turn's does.
		// 调用方要为摘要付费，因此若 run 的聊天访问权限无法续期，它会像模型 turn 一样
		// 在结束时说明原因。
		var refused *api.Error
		if errors.As(err, &refused) {
			err = refused
		} else if err != nil {
			err = errCompactionFailed()
		}
	}
	if err != nil {
		r.out.emit(domain.CompactionEnd{MessageID: marker.ID, BeforeMessageID: before, Outcome: domain.CompactionFailed})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	// A device may send its chat from a compaction marker on, whose summary
	// covers messages the harness was not sent.
	// 设备可能从其某个 compaction 标记开始发送聊天，该标记的摘要覆盖了 harness 未收到的消息。
	covered := 0
	if first := messages[0]; first.Role == domain.CompactionRole && first.Compaction != nil {
		covered = first.Compaction.Messages
	}
	for _, m := range messages[:kept] {
		if m.Role != domain.CompactionRole {
			covered++
		}
	}
	marker.CreatedAt, marker.Content = api.Timestamp(), result.Text
	marker.Compaction = &domain.Compaction{Messages: covered, StartedAt: float64(started.UnixMilli()), EndedAt: float64(api.Now().UnixMilli()), Model: result.Model, Version: summarize.Compaction.Version}
	req.messages = slices.Insert(slices.Clone(messages), kept, marker)
	req.render()
	req.remind()
	r.compacted = &compaction{marker: marker, beforeID: before, afterID: messages[kept-1].ID, usage: req.usage}
	view := domain.ViewMessage(&marker)
	r.out.emit(domain.CompactionEnd{MessageID: marker.ID, BeforeMessageID: before, Outcome: domain.CompactionCompleted, Marker: &view})
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	summary := domain.Summarize(&r.row.Thread)
	summary.ContextUsage = r.compacted.usage
	r.out.emit(domain.StateSnapshotEvent{Snapshot: domain.StateSnapshot{Thread: summary, Queue: queueItems(r.Thread), RateLimit: r.credential.Limit}})
	return nil
}

// keptFrom is the index of the first message of req a compaction keeps: the
// question that opens the latest whole exchanges before the turn's question
// that fill no more than budget, or else the turn's question. It leaves at
// least one message after the chat's latest marker for the summary, and
// keeps only messages from shown on, which the run's MESSAGES_SNAPSHOT
// showed, so a client can place the marker.
// keptFrom 是 compaction 保留的 req 中第一条消息的下标：即在该 turn 的问题之前、占用不超过
// budget 的最近若干完整 exchange 的开头问题，否则就是该 turn 的问题本身。它会在聊天的最新标记
// 之后为摘要至少保留一条消息，并且只保留 shown 及其后的消息（这些是 run 的 MESSAGES_SNAPSHOT
// 所展示的），以便客户端能定位该标记。
func keptFrom(req *Request, latest, question, shown, budget int) int {
	kept, cost := question, 0
	for i := question - 1; i >= max(latest+2, shown); i-- {
		if cost += messageCost(req, &req.messages[i]); cost > budget {
			break
		}
		if req.messages[i].Role == "user" {
			kept = i
		}
	}
	return kept
}

// messageCost is what a message of req fills of a prompt.
// messageCost 是 req 的一条消息在 prompt 中所占用的空间。
func messageCost(req *Request, m *domain.Message) int {
	cost := 0
	for _, message := range RenderMessage(m, req.vision, req.policy, req.loc) {
		cost += messageTokens(message)
	}
	return cost
}

// transcript is what a compaction reads of messages: each question and
// reply with when it was sent, the text of each file attached to a
// question, and what each tool call of a reply did. What a search or page
// returned is left out: the reply says what it drew from them.
// transcript 是 compaction 所读取的消息内容：每个问题与回复及其发送时间、问题所附带的
// 每个文件的文本，以及回复中每个工具调用做了什么。搜索或页面返回的内容会被略去：
// 回复本身会说明它从这些结果中得出了什么。
func transcript(messages []domain.Message, loc *time.Location) []summarize.Document {
	var docs []summarize.Document
	for i := range messages {
		m := &messages[i]
		attrs := []summarize.Attr{{Name: "from", Value: m.Role}}
		if sent, err := time.Parse(time.RFC3339Nano, m.CreatedAt); err == nil {
			attrs = append(attrs, summarize.Attr{Name: "sent", Value: englishTime(sent.In(loc), loc.String())})
		}
		switch m.Role {
		case "user":
			docs = append(docs, summarize.Document{Kind: "message", Attrs: attrs, Text: questionText(m)})
			for j := range m.Attachments {
				docs = append(docs, attachmentDocument(&m.Attachments[j]))
			}
		case "assistant":
			if domain.ValueOr(m.IsError, false) {
				attrs = append(attrs, summarize.Attr{Name: "state", Value: "failed"})
			} else if domain.ValueOr(m.IsInterrupted, false) {
				attrs = append(attrs, summarize.Attr{Name: "state", Value: "stopped"})
			}
			docs = append(docs, summarize.Document{Kind: "message", Attrs: attrs, Text: replyText(m)})
		}
	}
	return docs
}

func questionText(m *domain.Message) string {
	parts := []string{}
	if len(m.Answers) > 0 {
		parts = append(parts, domain.AnswersPrompt(m.Answers))
	}
	if m.Quote != "" {
		parts = append(parts, "In reply to:\n> "+strings.ReplaceAll(m.Quote, "\n", "\n> "))
	}
	if m.Content != "" {
		parts = append(parts, m.Content)
	}
	return strings.Join(parts, "\n\n")
}

// replyText is a reply's text with a line for each tool call where it ran.
// A research's reply is its report alone, as the model reads it, with the
// artifact it may have written the report in.
// replyText 是回复的文本，并为每个执行过的工具调用附一行说明。research 的回复只是其报告本身
// （即模型所读到的内容），并可能包含写报告所用的 artifact。
func replyText(m *domain.Message) string {
	var b strings.Builder
	wrote := false
	for _, block := range m.Timeline {
		switch block.Type {
		case "content":
			b.WriteString(block.Content)
			wrote = wrote || block.Content != ""
		case "tool_call":
			if m.Research == nil || reportBlock(block) {
				b.WriteString("\n[" + callLine(&block) + "]\n")
			}
		}
	}
	if !wrote {
		return strings.TrimSpace(m.Content + "\n" + b.String())
	}
	return strings.TrimSpace(b.String())
}

// callLine says what a tool call did: what a search looked for and found,
// which page was read, or what a widget showed the user. A search or read
// that found nothing says so, so its sources are not taken as read.
// callLine 说明一次工具调用做了什么：搜索查询了什么并找到了什么、读取了哪个页面，
// 或 widget 向用户展示了什么。一无所获的搜索或读取会明确说明，这样其来源就不会被当作已读。
func callLine(b *domain.Block) string {
	display := b.Display
	if display == nil {
		display = domain.SearchDisplay(b.Name, b.Arguments, b.Result)
	}
	sources := func() string {
		named := []string{}
		for _, s := range display.Sources {
			named = append(named, cmp.Or(s.Title, s.URL)+" <"+s.URL+">")
		}
		return strings.Join(named, "; ")
	}
	switch {
	case b.Name == "web_search" && display != nil && display.Status != "completed":
		return `Tried to search the web for "` + display.Query + `", but ` + unfinished(display)
	case b.Name == "web_search" && display != nil:
		return `Searched the web for "` + display.Query + `". Results: ` + cmp.Or(sources(), "none")
	case b.Name == "web_fetch" && display != nil && display.Status != "completed":
		return "Tried to read " + display.URL + ", but " + unfinished(display)
	case b.Name == "web_fetch" && display != nil:
		return "Read " + cmp.Or(sources(), display.URL)
	case strings.HasPrefix(b.Name, "render_"):
		return "Showed the user the " + b.Name + " widget with " + domain.HistoryArgs(cmp.Or(b.Arguments, "{}"))
	}
	return "Called " + b.Name
}

// unfinished is how a search or read that returned nothing ended.
// unfinished 说明一无所获的搜索或读取是如何结束的。
func unfinished(display *domain.ToolDisplay) string {
	if display.Status == "failed" {
		return "it failed"
	}
	return "it was stopped before it finished"
}

// attachmentDocument is a file attached to a question, as a compaction reads
// it: a document's text, or an image's description.
// attachmentDocument 是问题所附带的文件，如 compaction 所读取的那样：
// 文档的文本，或图片的描述。
func attachmentDocument(a *domain.Attachment) summarize.Document {
	text := attach.DocumentText(attach.Content(a))
	if a.IsImage() {
		text = cmp.Or(a.Description, "[An image with no description.]")
	}
	return summarize.Document{Kind: "attachment", Attrs: []summarize.Attr{{Name: "name", Value: a.FileName}, {Name: "kind", Value: a.Kind}},
		Text: cmp.Or(text, "[No text was read from this file.]")}
}
