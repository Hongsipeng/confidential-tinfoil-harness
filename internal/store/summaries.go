package store

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A chat list shows a summary of each chat, which only the sealed chat
// holds. So that a listing opens only the chats that changed, the store
// keeps each chat's summary as last read or written, bound to the caller,
// the key that opened it and the revision it was read at. A summary is
// served only while the control plane lists the chat at that revision.
//
// Summaries (titles, counts and times, never messages or key material)
// stay in the same attested memory that holds chat plaintext during a
// request, for at most summaryLifetime, and are gone when the process ends.
// chat 列表会展示每个 chat 的 summary，而它只存在于封存的 chat 中。为使列表只打开发生变化的 chat，
// store 会按最近一次读取或写入保存每个 chat 的 summary，并绑定到调用者、打开它的 key
// 以及读取时的修订版本。只有在 control plane 仍以该修订版本列出该 chat 时，summary 才会被提供。
//
// summary（标题、计数与时间，绝不包含消息或 key 材料）保存在与请求期间 chat 明文相同的
// attested 内存中，最多保留 summaryLifetime，进程结束后即消失。
const (
	// maxSummaries bounds the summaries kept, across every caller.
	// maxSummaries 限制跨所有调用者保留的 summary 数量。
	maxSummaries = 100_000
	// summaryLifetime is how long a summary is kept after it was read.
	// summaryLifetime 是 summary 被读取后保留的时长。
	summaryLifetime = time.Hour
)

// summaryKey is whose a kept summary is: the caller, the key that opened
// the chat, and the chat.
// summaryKey 标识一个已保留 summary 的归属：调用者、打开该 chat 的 key 以及该 chat。
type summaryKey struct {
	owner, keyID, id string
}

// keptSummary is a chat's summary and the revision it was read at.
// keptSummary 是一个 chat 的 summary 及其读取时的修订版本。
type keptSummary struct {
	etag    string
	summary domain.ThreadSummary
}

func (s *Store) keptSummaries() *expirable.LRU[summaryKey, keptSummary] {
	s.summariesOnce.Do(func() {
		s.summaries = expirable.NewLRU[summaryKey, keptSummary](maxSummaries, nil, summaryLifetime)
	})
	return s.summaries
}

// keepSummary keeps a chat's summary at the revision row holds, and is it.
// keepSummary 以 row 所持有的修订版本保留 chat 的 summary，并返回它。
func (s *Store) keepSummary(p *auth.Principal, kid string, row *domain.ThreadRow) domain.ThreadSummary {
	summary := domain.Summarize(&row.Thread)
	s.keptSummaries().Add(summaryKey{p.Scope(), kid, row.ID}, keptSummary{row.ETag, summary})
	return summary
}

// ThreadSummaryBatch is the chats among ids as their list shows them,
// opened now and kept for listings. A chat ThreadBatch leaves out is left
// out.
// ThreadSummaryBatch 返回 ids 中各 chat 在列表中所展示的 summary，现已打开并保留以供列表使用。
// ThreadBatch 略过的 chat 这里同样略过。
func (s *Store) ThreadSummaryBatch(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]domain.ThreadSummary, error) {
	rows, err := s.ThreadBatch(ctx, p, key, ids)
	if err != nil {
		return nil, err
	}
	kid := keyID(key)
	summaries := make([]domain.ThreadSummary, 0, len(rows))
	for _, row := range rows {
		summaries = append(summaries, s.keepSummary(p, kid, row))
	}
	return summaries, nil
}

// ThreadSummaries is one page of a user's chats as their list shows them,
// newest first, or of a project's. It opens only the chats whose summary
// under this key is not kept at their listed revision. A chat this
// harness cannot read is left out, as one the key cannot open is.
// ThreadSummaries 返回用户（或某个 project）的 chats 列表所展示的一页（最新优先）。
// 它只打开那些在此 key 下、其 summary 未按所列修订版本保留的 chat。
// 本 harness 无法读取的 chat 会被略过，正如 key 无法打开的一样。
func (s *Store) ThreadSummaries(ctx context.Context, p *auth.Principal, key auth.ContentKey, cursor, project string, limit int) ([]domain.ThreadSummary, string, error) {
	listed, next, err := s.Rows.Revisions(ctx, p, "chat", cursor, project, limit)
	if err != nil {
		return nil, "", err
	}
	kid, kept := keyID(key), s.keptSummaries()
	found := make(map[string]domain.ThreadSummary, len(listed))
	stale := []string{}
	for _, row := range listed {
		if summary, ok := kept.Get(summaryKey{p.Scope(), kid, row.ID}); ok && summary.etag == row.ETag {
			// The control plane can move a chat out of a deleted project
			// without a new revision, so its listed project is current.
			// control plane 可以在不产生新修订版本的情况下把 chat 移出已删除的 project，
			// 因此其列表中的 project 是当前的。
			summary.summary.ProjectID = row.Project
			found[row.ID] = summary.summary
		} else {
			stale = append(stale, row.ID)
		}
	}
	rows, _, err := s.Rows.Pull(ctx, p, key, "chat", stale)
	if err != nil {
		return nil, "", err
	}
	for i := range rows {
		if row, err := domain.ReadThreadRow(&rows[i]); err == nil {
			found[row.ID] = s.keepSummary(p, kid, row)
		}
	}
	summaries := make([]domain.ThreadSummary, 0, len(found))
	for _, row := range listed {
		if summary, ok := found[row.ID]; ok {
			summaries = append(summaries, summary.Current())
			delete(found, row.ID)
		}
	}
	return summaries, next, nil
}
