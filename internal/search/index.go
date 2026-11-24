// 本文件定义索引对象的存储格式与读写：gzip 编解码、对象键生成、加载、保存与发布。
package search

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// The index object: the encoded index, gzipped before the bucket seals it
// (sealed bytes do not compress), under a key that names the fence it
// was published under and a random suffix, so a publication never
// overwrites the object another replica reads.
// 索引对象：编码后的索引，在 bucket 封存前先做 gzip（封存的字节无法再压缩），对象键命名
// 发布时所处的 fence 并带一个随机后缀，因此一次 publication 绝不会覆盖另一个 replica
// 正在读取的对象。

const (
	objectKeyPrefix = "search-index-v1"
	// objectKeySuffixBytes is the random part of an object key.
	// objectKeySuffixBytes 是对象键中的随机部分。
	objectKeySuffixBytes = 16

	// maxDecodedBytes caps the inflated index, so a corrupted or hostile
	// object cannot balloon memory: searchindex.MaxChats entries at their
	// worst footprint, with ample margin.
	// maxDecodedBytes 限制解压后的索引大小，使损坏或恶意的对象无法撑爆内存：
	// searchindex.MaxChats 条记录在最坏占用下的规模，并留出充足余量。
	maxDecodedBytes = 512 << 20
	// maxStoredBytes caps the object read from the bucket before it is
	// inflated: the decoded cap plus gzip framing.
	// maxStoredBytes 限制从 bucket 读出的对象（解压前）的大小：解码上限加上 gzip 帧开销。
	maxStoredBytes = maxDecodedBytes + (1 << 20)

	// A publication that lost to another replica's is tried again, from
	// that replica's object, up to publishAttempts times.
	// 输给另一个 replica 的 publication 会从其对象重新尝试，最多 publishAttempts 次。
	publishAttempts = 3
	// An object left behind by a publication is deleted within
	// orphanCleanupTimeout; a publication whose answer was lost is
	// reconciled against the control plane within reconcileTimeout.
	// 由一次 publication 遗留的对象会在 orphanCleanupTimeout 内删除；应答丢失的 publication
	// 会在 reconcileTimeout 内与控制平面对账。
	orphanCleanupTimeout = 5 * time.Second
	reconcileTimeout     = 5 * time.Second
)

var (
	errIndexUnreadable = api.Err(502, "UPSTREAM_REFUSED", "The search index could not be read")
	errIndexNotStored  = api.Err(502, "UPSTREAM_REFUSED", "The search index could not be stored")
	errIndexContended  = api.Err(503, "SEARCH_INDEX_FAILED", "The search index changed during every publication attempt")
)

// loadState is how an index was obtained. Missing and unreadable both
// give a fresh empty index, but say different things about coverage: a
// missing object may be a caller who never had one, while an unreadable
// one existed and its contents are lost (another key, corruption, a
// model change), so anything built on it is known to be incomplete.
// loadState 表示索引是如何获取的。缺失与不可读都会得到一个全新的空索引，但关于覆盖范围
// 含义不同：缺失的对象可能来自从未有过索引的调用方，而不可读的对象曾经存在、其内容已丢失
// （换钥、损坏、模型变更），因此在其上构建的任何内容都视为不完整。
type loadState int

const (
	loadOK loadState = iota
	loadMissing
	loadUnreadable
)

// maxStoredIndexBytes is what load reads at most; a test lowers it.
// maxStoredIndexBytes 是 load 最多读取的字节数；测试会调低它。
var maxStoredIndexBytes int64 = maxStoredBytes

// load is the caller's decoded index, from the cache when it has it and
// from the bucket otherwise. A missing object, an index sealed under
// another key, an unknown format or a model change give a fresh empty
// index with a degraded state, which is never cached, so callers can
// report a rebuild rather than fail. Callers hold the owner's lock,
// shared or exclusive, since a cached index is shared across requests.
// load 返回调用方已解码的索引：缓存中有则取缓存，否则从 bucket 读取。对象缺失、索引用另一把
// 密钥封存、格式未知或模型变更时，会得到一个带降级状态的全新空索引，且从不缓存，以便调用方
// 报告需要 rebuild 而非直接失败。由于缓存的索引在请求间共享，调用方持有 owner 的共享或排它锁。
func (s *Service) load(ctx context.Context, owner, objectKey string, indexKey []byte) (*searchindex.Index, loadState, error) {
	model := s.Embedder.Model()
	if objectKey == "" {
		return searchindex.New(model), loadMissing, nil
	}
	key := cacheKey(indexKey, objectKey)
	if ix, ok := s.cache.get(owner, key); ok {
		if ix.Compatible(model) {
			return ix, loadOK, nil
		}
		s.cache.drop(owner)
	}
	raw, err := s.Buckets.GetLimited(ctx, owner, objectKey, indexKey, maxStoredIndexBytes)
	switch {
	case errors.Is(err, buckets.ErrNotFound):
		return searchindex.New(model), loadMissing, nil
	case errors.Is(err, buckets.ErrForbidden), errors.Is(err, buckets.ErrTooLarge):
		return searchindex.New(model), loadUnreadable, nil
	case err != nil:
		return nil, loadUnreadable, errIndexUnreadable
	}
	encoded, err := gunzipIndex(raw)
	if err != nil {
		return searchindex.New(model), loadUnreadable, nil
	}
	ix, err := searchindex.Decode(encoded)
	if err != nil || !ix.Compatible(model) {
		return searchindex.New(model), loadUnreadable, nil
	}
	s.cache.put(owner, key, ix, len(encoded))
	return ix, loadOK, nil
}

func gunzipIndex(compressed []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	out, err := io.ReadAll(io.LimitReader(zr, maxDecodedBytes+1))
	if err != nil {
		return nil, err
	}
	if len(out) > maxDecodedBytes {
		return nil, errors.New("search: the index exceeds the decoded size limit")
	}
	return out, nil
}

// save writes the index, gzipped, to the bucket under objectKey. A saved
// index is cached; a failed save drops the cache, so the next operation
// reloads the last stored state rather than serve a change that was
// never stored.
// save 将 gzip 后的索引写入 objectKey 所指向的 bucket。保存成功的索引会被缓存；保存失败则
// 丢弃缓存，使下一次操作重新加载最后已存储的状态，而不是提供从未落盘的变更。
func (s *Service) save(ctx context.Context, owner, objectKey string, indexKey []byte, ix *searchindex.Index) error {
	encoded, err := ix.Encode()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(encoded); err != nil {
		_ = zw.Close()
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if err := s.Buckets.Put(ctx, owner, objectKey, buf.Bytes(), indexKey); err != nil {
		s.cache.drop(owner)
		return errIndexNotStored
	}
	s.cache.put(owner, cacheKey(indexKey, objectKey), ix, len(encoded))
	return nil
}

// deleteObject drops an index object no publication names, best effort
// and detached from the request: one left behind is also swept later.
// deleteObject 删除一个不再被任何 publication 命名的索引对象，尽力而为并与请求解耦：
// 遗留的对象稍后也会被清扫。
func (s *Service) deleteObject(ctx context.Context, owner, objectKey string) {
	if objectKey == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), orphanCleanupTimeout)
	defer cancel()
	_ = s.Buckets.Delete(ctx, owner, objectKey)
}

func newObjectKey(fence int64) (string, error) {
	var suffix [objectKeySuffixBytes]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d-%s", objectKeyPrefix, fence, hex.EncodeToString(suffix[:])), nil
}

// publish stores the index as a new object and names it in the control
// plane's publication record at the generation state was read at, as
// covering the source through covered. The object the record named
// before is dropped. A publication another replica's got ahead of is a
// *controlplane.SearchIndexConflictError, with this one's object dropped;
// one whose answer was lost is read back, so a publication that landed
// is not taken for a failure.
// publish 把索引作为新对象存储，并在控制平面的 publication 记录中于 state 所读取的 generation 处
// 命名它，表示覆盖到 covered 为止的源。记录之前命名的对象会被丢弃。输给另一个 replica 的
// publication 表现为 *controlplane.SearchIndexConflictError，其对象一并丢弃；应答丢失的会回读，
// 以免把已经落地的 publication 误判为失败。
func (s *Service) publish(ctx context.Context, c caller, state *controlplane.SearchIndexState, covered int64, incomplete bool, ix *searchindex.Index) (*controlplane.SearchIndexState, error) {
	owner := c.owner()
	objectKey, err := newObjectKey(state.FenceGeneration)
	if err != nil {
		return nil, err
	}
	if err := s.save(ctx, owner, objectKey, c.indexKey, ix); err != nil {
		return nil, err
	}
	published, err := s.Controlplane.PublishSearchIndex(ctx, controlplane.PublishSearchIndexRequest{
		JWT:                   string(c.p.JWT),
		ClerkUserID:           owner,
		ExpectedGeneration:    state.PublicationGeneration,
		ExpectedFence:         state.FenceGeneration,
		CoveredSourceRevision: covered,
		ObjectKey:             objectKey,
		KeyID:                 c.kid,
		Model:                 s.Embedder.Model(),
		Incomplete:            incomplete,
	})
	if err != nil {
		s.cache.drop(owner)
		var conflict *controlplane.SearchIndexConflictError
		if errors.As(err, &conflict) {
			s.deleteObject(ctx, owner, objectKey)
			return nil, err
		}
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileTimeout)
		defer cancel()
		if current, reconcileErr := s.Controlplane.GetSearchIndexState(reconcileCtx, string(c.p.JWT), owner); reconcileErr == nil {
			if current.ObjectKey == objectKey {
				s.deleteObject(ctx, owner, state.ObjectKey)
				return current, nil
			}
			s.deleteObject(ctx, owner, objectKey)
		}
		return nil, store.FromControlplane(err)
	}
	if !incomplete {
		s.cache.publishedComplete(owner, cacheKey(c.indexKey, objectKey), published.PublicationGeneration)
	}
	s.deleteObject(ctx, owner, state.ObjectKey)
	return published, nil
}

// completeThrough is whether the published index covers every write up to
// the revision it was published at: the control plane says so, or this
// process published it as complete and only a write that landed meanwhile
// made the control plane say otherwise.
// completeThrough 表示已发布的索引是否覆盖了发布时修订为止的全部写入：要么控制平面如此声明，
// 要么本进程曾以 complete 状态发布它、只是中途又有写入落地才使控制平面改口为不完整。
func (s *Service) completeThrough(c caller, publication *controlplane.SearchIndexState, objectKey string) bool {
	return !publication.PublicationIncomplete || s.cache.completeAt(c.owner(), cacheKey(c.indexKey, objectKey), publication.PublicationGeneration)
}

// isConflict is whether a publication lost to another replica's, which
// a caller tries again from that replica's state.
// isConflict 表示一次 publication 是否输给了另一个 replica，遇到这种情况调用方会从该 replica 的
// 状态重新尝试。
func isConflict(err error) bool {
	var conflict *controlplane.SearchIndexConflictError
	return errors.As(err, &conflict)
}

// publication is the control plane's record of the caller's index, and
// which object it names for this caller's key and model: none when the
// record names an index under another key or model, which this caller
// cannot read.
// publication 返回控制平面关于调用方索引的记录，以及它针对该调用方的密钥与模型所命名的对象；
// 当记录命名的索引使用另一把密钥或另一个模型、本调用方无法读取时，对象为空。
func (s *Service) publication(ctx context.Context, c caller) (*controlplane.SearchIndexState, string, error) {
	state, err := s.Controlplane.GetSearchIndexState(ctx, string(c.p.JWT), c.owner())
	if err != nil {
		return nil, "", store.FromControlplane(err)
	}
	return state, s.readableObject(state, c), nil
}

// readableObject is the published object when it is sealed under the
// caller's key and built with the current model.
// readableObject 返回已发布对象，前提是它用调用方密钥封存且由当前模型构建。
func (s *Service) readableObject(state *controlplane.SearchIndexState, c caller) string {
	if state.ObjectKey != "" && (state.KeyID != c.kid || state.Model != s.Embedder.Model()) {
		return ""
	}
	return state.ObjectKey
}
