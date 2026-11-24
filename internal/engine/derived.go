package engine

import (
	"context"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// titleSource is the text a title summarizes: the first user message, or the
// content of its documents. It is read from the prompt's copy of the thread,
// since a stored row keeps no document content.
// titleSource 是标题所汇总的文本：第一条用户消息，或其文档的内容。
// 它从 prompt 的 thread 副本中读取，因为存储的行不保留文档内容。
func titleSource(t *domain.Thread) string {
	for _, m := range t.Messages {
		if m.Role != "user" {
			continue
		}
		text := m.Content
		if text == "" {
			for i := range m.Attachments {
				// A document whose blob is gone must not name the chat.
				// blob 已丢失的文档不能用来命名聊天。
				if a := &m.Attachments[i]; attach.DocumentText(attach.Content(a)) != attach.UnavailableDocument(a) {
					text += attach.DocumentText(attach.Content(a)) + " "
				}
			}
		}
		return text
	}
	return ""
}

func (eng *Engine) generateTitle(ctx context.Context, r *Run, p *auth.Principal, titleState string, text string) {
	if eng.Summaries == nil || titleState != "placeholder" {
		return
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return
	}
	text = strings.Join(words[:min(len(words), 100)], " ")
	summary, err := eng.Summaries.Summarize(ctx, summarize.Title, summarize.Input{Documents: []summarize.Document{{Text: text}}})
	if err != nil {
		return
	}
	title := strings.Trim(summary.Text, "\"'")
	if title == "" {
		return
	}
	name := func(t *domain.Thread) error {
		if t.TitleState == "placeholder" {
			t.Title, t.TitleState = title, "generated"
		}
		return nil
	}
	// A local thread's row lives only in this run, and its client stores the
	// title it reads from the event below.
	// 本地 thread 的行只存在于这个 run 中，其客户端保存它从下面事件中读到的标题。
	if r.Thread.local {
		eng.Live.mu.Lock()
		defer eng.Live.mu.Unlock()
		name(&r.row.Thread)
		r.out.emit(domain.StateDelta{Delta: titleDelta(&r.row.Thread)})
		return
	}
	updated, err := eng.Store.MutateThread(ctx, p, r.key, r.Thread.ID, name)
	if err != nil {
		return
	}
	r.out.emit(domain.StateDelta{Delta: titleDelta(&updated.Thread)})
}

func titleDelta(t *domain.Thread) []domain.StatePatch {
	return []domain.StatePatch{{Op: "replace", Path: "/thread/title", Value: t.Title}, {Op: "replace", Path: "/thread/titleState", Value: t.TitleState}}
}
