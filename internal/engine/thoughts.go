package engine

import (
	"context"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// A reply's reasoning is summarized while it thinks, as the web client did:
// once its trace has enough words to say something, from the trace's tail,
// at most once per interval, and never after the trace closed.
// 回复的推理会在它思考时被汇总，正如 web 客户端所做的那样：一旦其 trace 有足够多的词可说明
// 内容，就从 trace 的尾部汇总，每个时间间隔最多一次，且绝不会在 trace 关闭之后。
const (
	minSummaryWords  = 20
	summaryTailWords = 200
	summaryInterval  = 3 * time.Second
)

// thoughtSummaries follows the thinking block a reply streams into. The
// stream's mutex guards every field.
// thoughtSummaries 跟随回复流式写入的 thinking 块。流的互斥锁保护每个字段。
type thoughtSummaries struct {
	ctx       context.Context
	out       *Stream
	assembler *domain.MessageAssembler
	summarize func(context.Context, string) (string, error)

	message, block, trace string
	// epoch changes whenever the block it follows closes or another opens,
	// so a summary of an earlier trace is dropped.
	// epoch 在其跟随的块关闭或另一个块打开时都会变化，这样对更早 trace 的摘要就会被丢弃。
	epoch    int
	inFlight bool
	last     time.Time
	timer    *time.Timer
}

// thoughtSummaries starts following a run's reasoning, unless the harness
// has no summarizer.
// thoughtSummaries 开始跟随 run 的推理，除非 harness 没有 summarizer。
func (eng *Engine) thoughtSummaries(ctx context.Context, out *Stream, assembler *domain.MessageAssembler) *thoughtSummaries {
	summaries := eng.Summaries
	if summaries == nil {
		return nil
	}
	return &thoughtSummaries{ctx: ctx, out: out, assembler: assembler, summarize: func(ctx context.Context, text string) (string, error) {
		summary, err := summaries.Summarize(ctx, summarize.Thoughts, summarize.Input{Documents: []summarize.Document{{Text: text}}})
		return summary.Text, err
	}}
}

// Follow reads a frame the run's reply was built from.
// Follow 读取用于构建 run 回复的一个帧。
func (s *thoughtSummaries) follow(e domain.Event) {
	switch e := e.(type) {
	case domain.ReasoningChunk:
		message := strings.TrimSuffix(e.MessageID, domain.ReasoningSuffix)
		// A late tail of reasoning the reply's text closed is not followed.
		// 回复文本已关闭后迟到的推理尾部不会被跟随。
		block := s.assembler.LastBlock(message)
		if block == nil || block.Type != "thinking" {
			return
		}
		if block.ID != s.block {
			s.close()
			s.message, s.block = message, block.ID
		}
		s.trace = block.Content
		s.consider()
	case domain.TextChunk, domain.ToolCallStart:
		s.close()
	}
}

// close stops following a trace the reply moved on from.
// close 停止跟随回复已离开的 trace。
func (s *thoughtSummaries) close() {
	s.epoch++
	s.message, s.block, s.trace, s.inFlight = "", "", "", false
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

func (s *thoughtSummaries) consider() {
	if s.inFlight || s.timer != nil || len(strings.Fields(s.trace)) < minSummaryWords {
		return
	}
	wait := summaryInterval - time.Since(s.last)
	if wait <= 0 {
		s.fire()
		return
	}
	epoch := s.epoch
	s.timer = time.AfterFunc(wait, func() {
		s.out.mu.Lock()
		defer s.out.mu.Unlock()
		if s.epoch == epoch {
			s.timer = nil
			s.fire()
		}
	})
}

// fire summarizes the trace's tail and frames the summary if its trace is
// still open when it arrives.
// fire 汇总 trace 的尾部，并在摘要到达时若其 trace 仍打开则把摘要成帧。
func (s *thoughtSummaries) fire() {
	s.inFlight, s.last = true, time.Now()
	tail := s.trace
	if words := strings.Fields(tail); len(words) > summaryTailWords {
		tail = strings.Join(words[len(words)-summaryTailWords:], " ")
	}
	epoch, message, block := s.epoch, s.message, s.block
	go func() {
		defer func() { api.Rescued("reasoning summary", recover()) }()
		summary, err := s.summarize(s.ctx, tail)
		s.out.mu.Lock()
		defer s.out.mu.Unlock()
		if s.epoch != epoch {
			return
		}
		s.inFlight = false
		if summary = strings.TrimSpace(summary); err == nil && summary != "" {
			s.out.frame(domain.ReasoningSummary{MessageID: message, BlockID: block, Content: summary}, api.Timestamp())
		}
	}()
}
