// 本文件定义聊天写入进入索引的更新与应用：ChatStored/ChatRemoved 将写入送入 writer，
// apply 把更新合并进索引并一次发布，storeEntry/removeEntry 处理条目，snapshotState 判断
// 聊天是否仍处于索引条目所依据的修订。
package search

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// ChatStored takes a chat just written into the caller's index, at the
// revision its write returned and the source revision it left. The write
// has landed and does not wait for the index: the chat's text is read
// now, while the harness holds it, and the caller's writer applies it
// after. Nothing here may fail the write: a failure degrades search until
// a rebuild, and a panic is contained the same way.
// ChatStored 把刚写入的聊天纳入调用方的索引，位置在其写入返回的修订及留下的 source 修订处。
// 写入已经落地，不等待索引：聊天文本此刻在 harness 仍持有它时被读取，之后由调用方的 writer
// 应用。这里的任何操作都不得使写入失败：失败只会在 rebuild 之前降低搜索质量，panic 也以同样
// 方式被遏制。
func (s *Service) ChatStored(p *auth.Principal, key auth.ContentKey, id string, plaintext []byte, etag string, at time.Time, revision int64) {
	defer clear(key[:])
	defer func() { _ = recover() }()
	if !s.configured() {
		return
	}
	c, err := s.callerOf(p, key)
	if err != nil {
		return
	}
	defer c.zero()
	s.enqueue(c, storedUpdate(id, plaintext, etag, at, revision))
}

// ChatRemoved drops a deleted chat from the caller's index, through the
// caller's writer as ChatStored does. A stale entry only means a deleted
// chat can still come up until the next rebuild, so a failure is let go,
// as is a panic.
// ChatRemoved 从调用方的索引中移除一条已删除的聊天，与 ChatStored 一样经由其 writer。陈旧
// 条目最多意味着已删除的聊天在下次 rebuild 前仍可能被检索到，因此失败与 panic 都予以放过。
func (s *Service) ChatRemoved(p *auth.Principal, key auth.ContentKey, id string, revision int64) {
	defer clear(key[:])
	defer func() { _ = recover() }()
	if !s.configured() {
		return
	}
	c, err := s.callerOf(p, key)
	if err != nil {
		return
	}
	defer c.zero()
	s.enqueue(c, chatUpdate{id: id, removed: true, revision: revision})
}

// chatUpdate is what one chat's write changes in the index: the chat as
// the write left it, its text cut into chunks and tokens, or its removal.
// at is when the write landed, which orders the entry against other
// writes of the chat well before the embedding round trip can reorder
// their completion. replaced are the source revisions of the chat's
// earlier writes whose updates this one took the place of while waiting.
// chatUpdate 表示一次聊天写入对索引造成的变更：写入后留下的聊天、其文本切成的 chunk 与
// token，或它的删除。at 是写入落地的时间，用于将条目相对该聊天的其他写入排序，且远在
// embedding 往返可能打乱完成顺序之前。replaced 是在等待期间被本次更新所取代的、该聊天
// 更早写入的 source 修订。
type chatUpdate struct {
	id       string
	removed  bool
	etag     string
	at       time.Time
	revision int64
	replaced []int64
	chunks   []string
	tokens   []string
}

// storedUpdate is the update a chat's write leaves for its index.
// storedUpdate 是一次聊天写入留给其索引的更新。
func storedUpdate(id string, plaintext []byte, etag string, at time.Time, revision int64) chatUpdate {
	chunks := chatChunks(plaintext)
	return chatUpdate{id: id, etag: etag, at: at, revision: revision, chunks: chunks, tokens: searchindex.Tokenize(strings.Join(chunks, "\n"))}
}

// revisions are the source revisions of the writes an update stands for.
// revisions 是一次更新所代表的那些写入的 source 修订。
func (u *chatUpdate) revisions() []int64 {
	if u.revision <= 0 {
		return u.replaced
	}
	return append([]int64{u.revision}, u.replaced...)
}

// outcome is what became of an update in a publication: taken when the
// index reflects its writes, superseded when a later write of the chat
// replaced them before the index took them, and missed otherwise.
// outcome 表示一次更新在一次 publication 中的结局：索引已反映其写入时为 taken；在索引接收
// 之前被该聊天更晚的写入取代时为 superseded；其余为 missed。
type outcome int

const (
	missed outcome = iota
	taken
	superseded
)

// apply merges a caller's updates into their index in one publication,
// and is the revisions of the updates a later write of their chat
// superseded, by chat, which that write's update then stands for. The
// publication covers the writes it took up to the first gap, as complete
// when the index it changed was complete and readable.
// apply 把调用方的更新合并进其索引并一次性发布，返回被这些聊天更晚写入所取代的更新的修订
// （按聊天分组），那些更晚写入的更新随后会代表它们。publication 覆盖其接收的写入，直到第一个
// 缺口；当它所修改的索引是完整且可读时，标为 complete。
func (s *Service) apply(ctx context.Context, c caller, updates []chatUpdate) (map[string][]int64, error) {
	owner := c.owner()
	inHand, embed := map[int64]bool{}, true
	entries := make([]searchindex.Entry, len(updates))
	for i := range updates {
		u := &updates[i]
		for _, revision := range u.revisions() {
			inHand[revision] = true
		}
		if u.removed {
			continue
		}
		entries[i] = searchindex.Entry{ETag: u.etag, SourceRevision: u.revision, UpdatedAt: u.at.UTC().Format(time.RFC3339Nano)}
		if len(u.chunks) > 0 {
			// Without vectors the chat is still found by keyword, and a
			// later query repairs the embedding. One embedding refused
			// or failed, the rest of the batch goes without.
			// 没有向量时聊天仍可凭关键词检索到，之后的查询会修复 embedding。一旦某次 embedding
			// 被拒绝或失败，本批次其余条目就都不带向量。
			entries[i].EmbeddingPending, entries[i].EmbeddingKeyID = true, c.kid
			if embed {
				vectors, err := s.embedChunks(ctx, owner, docPrefix, u.chunks, false)
				if embed = err == nil; embed {
					entries[i].Vectors, entries[i].EmbeddingPending, entries[i].EmbeddingKeyID = quantizeAll(vectors), false, ""
				}
			}
		}
	}

	unlock := s.lock(owner)
	defer unlock()
	// A removal leaves the entry of a chat that may have been made again
	// under its id since.
	// 删除会留下一个可能此后又以同一 id 重新创建的聊天的条目。
	remade := map[string]bool{}
	for _, u := range updates {
		if u.removed {
			_, err := s.Controlplane.GetBlob(ctx, "chat", u.id, string(c.p.JWT), owner)
			remade[u.id] = !controlplaneStatus(err, http.StatusNotFound)
		}
	}
	for attempt := 0; attempt < publishAttempts; attempt++ {
		publication, objectKey, err := s.publication(ctx, c)
		if err != nil {
			return nil, err
		}
		ix, state, err := s.load(ctx, owner, objectKey, c.indexKey)
		if err != nil {
			return nil, err
		}
		if publication.ObjectKey != "" && objectKey == "" {
			state = loadUnreadable
		}
		changed, took, behind := false, map[int64]bool{}, map[string][]int64{}
		for i := range updates {
			u := &updates[i]
			var mutated bool
			var result outcome
			if u.removed {
				mutated, result = removeEntry(ix, state, u, remade[u.id])
			} else {
				mutated, result = s.storeEntry(ctx, c, ix, publication, inHand, u, entries[i])
			}
			changed = changed || mutated
			switch revisions := u.revisions(); {
			case result == taken:
				for _, revision := range revisions {
					took[revision] = true
				}
			case result == superseded && len(revisions) > 0:
				behind[u.id] = revisions
			}
		}
		covered := publication.PublishedSourceRevision
		for took[covered+1] {
			covered++
		}
		if !changed && covered == publication.PublishedSourceRevision {
			return behind, nil
		}
		readable := publication.ObjectKey == "" || state == loadOK
		incomplete := !readable || !s.completeThrough(c, publication, objectKey)
		ix.Incomplete = incomplete
		if _, err := s.publish(ctx, c, publication, covered, incomplete, ix); err != nil {
			if isConflict(err) {
				continue
			}
			return nil, err
		}
		return behind, nil
	}
	return nil, errIndexContended
}

// storeEntry applies a chat's write to the index, and is whether it
// changed the index and what became of the write. The write is trusted
// as the chat's live revision while every chat write the control plane
// has counted since is another update in hand; only a gap costs a read
// of the chat's metadata, which says whether the chat was overwritten or
// deleted since.
// storeEntry 把一次聊天写入应用到索引，返回它是否改变了索引以及该写入的结局。当控制平面计数
// 到的此后每次聊天写入都是手里另一个更新时，该写入被信任为聊天的活跃修订；只有出现缺口时
// 才去读聊天元数据，以判断聊天此后是否被覆盖或删除。
func (s *Service) storeEntry(ctx context.Context, c caller, ix *searchindex.Index, publication *controlplane.SearchIndexState, inHand map[int64]bool, u *chatUpdate, entry searchindex.Entry) (bool, outcome) {
	if !liveSince(u.revision, publication.SourceRevision, inHand) {
		switch state, err := s.snapshotState(ctx, c, u.id, u.etag); {
		case err != nil:
			return false, missed
		case state == snapshotStale:
			return false, superseded
		case state == snapshotDeleted:
			_, indexed := ix.Chats[u.id]
			ix.Remove(u.id)
			return indexed, superseded
		}
	}
	// A slower write must not replace the entry a newer write, or a
	// rebuild, already made for this chat.
	// 较慢的写入不得覆盖更晚的写入或 rebuild 已经为该聊天建立的条目。
	if existing, ok := ix.Chats[u.id]; ok && (entrySupersedes(existing, u.etag, u.revision, u.at) || existing.SourceRevision == u.revision && existing.ETag == u.etag) {
		return false, taken
	}
	if err := ix.Upsert(u.id, entry, u.tokens); err != nil {
		return false, missed
	}
	return true, taken
}

// liveSince is whether every chat write after revision, up to source, is
// one in hand, none of which is another write of the same chat.
// liveSince 表示 revision 之后直到 source 为止的每次聊天写入是否都在手，且其中没有该聊天的
// 另一次写入。
func liveSince(revision, source int64, inHand map[int64]bool) bool {
	if revision <= 0 {
		return false
	}
	for later := revision + 1; later <= source; later++ {
		if !inHand[later] {
			return false
		}
	}
	return true
}

// removeEntry applies a chat's removal to the index, and is whether it
// dropped the chat's entry and what became of the removal.
// removeEntry 把聊天的删除应用到索引，返回它是否丢弃了该聊天的条目以及删除的结局。
func removeEntry(ix *searchindex.Index, state loadState, u *chatUpdate, remade bool) (bool, outcome) {
	if remade {
		return false, superseded
	}
	existing, exists := ix.Chats[u.id]
	switch {
	case !exists && state == loadOK:
		return false, taken
	case exists && u.revision > 0 && existing.SourceRevision > u.revision:
		// The entry is from a write after this deletion: a chat made
		// again under the id, which stays.
		// 条目来自此删除之后的一次写入：以该 id 重新创建的聊天，予以保留。
		return false, taken
	}
	ix.Remove(u.id)
	return true, taken
}

// snapshotState is whether a chat is still at the revision an index entry
// was built from, has changed since, or is gone.
// snapshotState 表示一个聊天是否仍处于索引条目所依据的修订、此后已变更，或已消失。
type snapshotState int

const (
	snapshotCurrent snapshotState = iota
	snapshotStale
	snapshotDeleted
)

// snapshotState compares a chat's stored revision with etag.
// snapshotState 将聊天当前存储的修订与 etag 比较。
func (s *Service) snapshotState(ctx context.Context, c caller, id, etag string) (snapshotState, error) {
	current, exists, err := s.currentETag(ctx, c, id)
	switch {
	case err != nil:
		return snapshotStale, err
	case !exists:
		return snapshotDeleted, nil
	case current != etag:
		return snapshotStale, nil
	}
	return snapshotCurrent, nil
}

// currentETag is a chat's stored revision; a deleted chat has none. It
// runs under the owner's lock, so it reads the chat's metadata rather
// than its bytes. A failed metadata read is ambiguous, though: a control
// plane that does not route it answers not found for live chats, and
// taking that for deleted would silently drop the chat from the index,
// so the chat itself is read before a miss counts.
// currentETag 返回聊天当前存储的修订；已删除的聊天没有。它在 owner 锁下运行，因此只读聊天
// 的元数据而非其字节。不过元数据读取失败存在歧义：不路由该请求的控制平面会对存活聊天回答
// not found，若把这种情况当成已删除就会悄悄把聊天从索引中丢弃，因此只有先读取聊天本身之后
// 才把未命中当回事。
func (s *Service) currentETag(ctx context.Context, c caller, id string) (etag string, exists bool, err error) {
	owner := c.owner()
	if meta, err := s.Controlplane.HeadBlob(ctx, "chat", id, string(c.p.JWT), owner); err == nil {
		return meta.ETag, true, nil
	}
	blob, err := s.Controlplane.GetBlob(ctx, "chat", id, string(c.p.JWT), owner)
	if err != nil {
		if controlplaneStatus(err, http.StatusNotFound) {
			return "", false, nil
		}
		return "", false, store.FromControlplane(err)
	}
	return blob.ETag, true, nil
}

func controlplaneStatus(err error, status int) bool {
	var cpe *controlplane.Error
	return errors.As(err, &cpe) && cpe.StatusCode == status
}

// etagGeneration reads a revision as the control plane's per-row counter;
// ok is false for any other shape, which is ordered by time instead.
// etagGeneration 把修订读作控制平面的逐行计数器；其他任何形式都返回 ok=false，此时改按时间
// 排序。
func etagGeneration(etag string) (int64, bool) {
	generation, err := strconv.ParseInt(etag, 10, 64)
	return generation, err == nil
}

// entrySupersedes is whether an index entry is from a strictly newer
// revision of its chat than a write at etag. The source revision orders
// writes across replicas; the per-row revision, then the write time,
// order entries without one.
// entrySupersedes 表示一个索引条目是否来自比 etag 处写入严格更新的聊天修订。source 修订用于
// 跨 replica 排序写入；没有 source 修订的条目则按逐行修订、再按写入时间排序。
func entrySupersedes(existing searchindex.Entry, etag string, revision int64, at time.Time) bool {
	if existing.SourceRevision > 0 && revision > 0 {
		return existing.SourceRevision > revision
	}
	if eg, ok := etagGeneration(existing.ETag); ok {
		if cg, ok := etagGeneration(etag); ok {
			return eg > cg
		}
	}
	return entryTime(existing.UpdatedAt).After(at)
}
