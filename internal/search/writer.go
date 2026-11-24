// 本文件实现每个调用方的 writer：写入落地后暂存其索引更新，按批应用并一次发布，
// 使写入轮流、而非争抢地进入索引，也让查询能看到调用方自己的写入。
package search

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// A chat's write reaches the caller's index after the write has returned.
// Its update waits with the caller's writer, one per caller and key,
// which applies every update waiting in one publication, then those that
// arrived meanwhile in the next: a caller's writes take turns for the
// index rather than race for it, and a burst of them is published once.
// The writer holds the key the index is sealed with, and nothing else of
// the caller's, until no update waits.
// 聊天写入在写入返回之后才进入调用方的索引。其更新在调用方的 writer 处等待——每个调用方与
// 密钥一个——writer 把当前等待的所有更新放进一次 publication 中应用，随后到达的则放在下一次：
// 于是调用方的各次写入轮流、而非争抢地进入索引，一批突发写入也只发布一次。writer 持有用于
// 封存索引的密钥，而不持有调用方的其他任何东西，直到没有更新再等待。

// updateTimeout bounds how long after a write its update may still be
// applied, and so how long a writer holds the index key after the
// caller's last write; an update not applied by then is left to a rebuild.
// updateTimeout 限定一次写入之后其更新最迟仍可被应用的时长，从而限定调用方最后一次写入之后
// writer 持有索引密钥的时长；到那时仍未应用的更新则留给 rebuild。
const updateTimeout = time.Minute

type writerKey struct{ owner, kid string }

// writer is one caller's updates under one key: the latest update of each
// chat, waiting to be applied together.
// writer 是单个调用方在一把密钥下的更新：每个聊天的最新更新，等待被一起应用。
type writer struct {
	c       caller
	pending map[string]chatUpdate
	// deadline is when the oldest pending update is applied by.
	// deadline 是最早的待处理更新被应用的截止时间。
	deadline time.Time
	// waiting is closed once the pending updates are applied, applied
	// once the updates last taken are, and ended once the writer is.
	// waiting 在待处理更新被应用后关闭；applied 在最近一次接收的更新被应用后关闭；
	// ended 在 writer 结束时关闭。
	waiting, applied, ended chan struct{}
}

// enqueue leaves an update with the caller's writer, starting one when
// they have none.
// enqueue 把一次更新交给调用方的 writer，若其没有 writer 则先启动一个。
func (s *Service) enqueue(c caller, u chatUpdate) {
	name := writerKey{c.owner(), c.kid}
	s.writersMu.Lock()
	defer s.writersMu.Unlock()
	w := s.writers[name]
	if w == nil {
		// The writer needs the index key, not the content key.
		// writer 需要的是索引密钥，而不是 content key。
		w = &writer{c: c.detached(auth.ContentKey{}), pending: map[string]chatUpdate{}, waiting: make(chan struct{}), ended: make(chan struct{})}
		s.writers[name] = w
		go s.runWriter(name, w)
	}
	if len(w.pending) == 0 {
		w.deadline = time.Now().Add(updateTimeout)
	}
	w.add(u)
}

// add keeps one update per chat: the later revision's, or the later to
// arrive when either has none, which stands for the other's writes too.
// A removal without a revision, of a row already gone when it was read,
// does not take the place of a write with one, which the index takes only
// while it is the chat's latest.
// add 每个聊天只保留一次更新：取修订更晚者，两者都没有修订时取更晚到达者，后者也代表对方
// 的写入。没有修订的删除，若其行在读取时已不存在，则不会取代带修订的写入，因为索引只在
// 该写入仍是聊天最新时才接收它。
func (w *writer) add(u chatUpdate) {
	if prev, ok := w.pending[u.id]; ok {
		if prev.revision > u.revision && (u.revision > 0 || u.removed) {
			prev, u = u, prev
		}
		u.replaced = append(u.replaced, prev.revisions()...)
	}
	w.pending[u.id] = u
}

// runWriter applies the writer's updates a batch at a time until none
// waits, then ends.
// runWriter 一次一批地应用 writer 的更新，直到没有更新等待，然后结束。
func (s *Service) runWriter(name writerKey, w *writer) {
	defer close(w.ended)
	defer w.c.zero()
	for {
		s.writersMu.Lock()
		if len(w.pending) == 0 {
			delete(s.writers, name)
			s.writersMu.Unlock()
			return
		}
		updates := slices.SortedFunc(maps.Values(w.pending), func(a, b chatUpdate) int {
			return cmp.Or(cmp.Compare(a.revision, b.revision), cmp.Compare(a.id, b.id))
		})
		deadline, applied := w.deadline, w.waiting
		w.pending, w.waiting, w.applied = map[string]chatUpdate{}, make(chan struct{}), applied
		s.writersMu.Unlock()
		behind := s.applyBy(deadline, w.c, updates)
		s.writersMu.Lock()
		w.fold(behind)
		s.writersMu.Unlock()
		close(applied)
	}
}

// applyBy applies a writer's updates by deadline, and is the revisions
// of those a later write of their chat superseded. A failure, or a
// panic, leaves the updates to the rebuild a search starts on finding
// the index short of the writes.
// applyBy 在 deadline 前应用 writer 的更新，返回被其聊天更晚写入所取代的更新修订。失败或
// panic 会把更新留给 rebuild——搜索在发现索引落后于写入时启动 rebuild。
func (s *Service) applyBy(deadline time.Time, c caller, updates []chatUpdate) (behind map[string][]int64) {
	defer func() { api.Rescued("search index update", recover()) }()
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	behind, _ = s.apply(ctx, c, updates)
	return behind
}

// fold hands the revisions of superseded updates to the update of the
// later write of their chat that waits here, which then stands for them.
// fold 把被取代更新的修订交给此处等待的、该聊天更晚写入的更新，后者随后代表它们。
func (w *writer) fold(behind map[string][]int64) {
	for id, revisions := range behind {
		if u, ok := w.pending[id]; ok && u.revision > slices.Max(revisions) {
			u.replaced = append(u.replaced, revisions...)
			w.pending[id] = u
		}
	}
}

// settled waits until the updates this process holds for the caller's
// index are applied, or ctx ends, so a query sees the caller's own writes
// rather than an index short of them.
// settled 等待本进程为调用方索引持有的更新被应用，或 ctx 结束，使查询能看到调用方自己的
// 写入，而不是落后于它们的索引。
func (s *Service) settled(ctx context.Context, c caller) {
	s.writersMu.Lock()
	var applied chan struct{}
	if w := s.writers[writerKey{c.owner(), c.kid}]; w != nil {
		applied = w.applied
		if len(w.pending) > 0 {
			applied = w.waiting
		}
	}
	s.writersMu.Unlock()
	if applied == nil {
		return
	}
	select {
	case <-applied:
	case <-ctx.Done():
	}
}

// Flush waits until every update this process holds for any caller's
// index is applied, or ctx ends, and is whether they were.
// Flush 等待本进程为任何调用方索引持有的每个更新都被应用，或 ctx 结束，并返回它们是否都已应用。
func (s *Service) Flush(ctx context.Context) bool {
	for {
		s.writersMu.Lock()
		ended := make([]chan struct{}, 0, len(s.writers))
		for _, w := range s.writers {
			ended = append(ended, w.ended)
		}
		s.writersMu.Unlock()
		if len(ended) == 0 {
			return true
		}
		for _, done := range ended {
			select {
			case <-done:
			case <-ctx.Done():
				return false
			}
		}
	}
}
