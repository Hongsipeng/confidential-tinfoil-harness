// Package search keeps each caller's encrypted chat index in step with
// their stored chats and answers queries from it. The index is one sealed
// object per caller in the search bucket, under a key derived from the
// caller's content key, and the control plane's publication record names
// the object every replica reads. Chats are read for the index as they
// are stored, while the harness holds their plaintext and key, and taken
// into it just after; a query is embedded the same way and ranked here,
// so no chat text leaves the enclave.
// search 包让每个调用方的加密聊天索引与其已存储聊天保持同步，并据此应答查询。索引是搜索
// bucket 中每个调用方一个封存对象，密钥派生自调用方的 content key，控制平面的 publication
// 记录命名每个 replica 都会读取的那个对象。聊天在存储时、harness 仍持有其明文与密钥之际被
// 读取用于索引，随即纳入其中；查询以同样方式嵌入并在本处排序，因此任何聊天文本都不会离开 enclave。
package search

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// Embedder turns text into vectors through the attested inference service.
// Embedder 通过 attested 推理服务将文本转换为向量。
type Embedder interface {
	Configured() bool
	Model() string
	Embed(ctx context.Context, inputs []string) ([][]float32, error)
}

// Service keeps each caller's search index: one sealed object per caller
// in the search bucket, under a key derived from the caller's content key,
// named by the control plane's publication record so every replica reads
// the same one. Chats are read for the index as they are stored, while
// the harness holds their plaintext and key, and taken into it just
// after; queries are embedded the same way and ranked in the enclave.
// Service 维护每个调用方的搜索索引：搜索 bucket 中每个调用方一个封存对象，密钥派生自调用方的
// content key，由控制平面的 publication 记录命名，使每个 replica 读取同一个对象。聊天在存储时、
// harness 仍持有其明文与密钥之际被读取用于索引，随即纳入其中；查询以同样方式嵌入并在 enclave 内排序。
type Service struct {
	Controlplane *controlplane.Client
	Buckets      *buckets.Client // the search index bucket；搜索索引 bucket。
	Embedder     Embedder
	Rows         store.RowStore // what a rebuild reads；rebuild 读取的来源。
	Rebuilds     *jobs.Runner[RebuildReport]

	// Decoded indexes are kept between requests; the object a caller's
	// index is read from is rewritten whole, so one caller's reads and
	// writes take turns on their lock.
	// 已解码的索引在请求间保留；调用方索引所读取的对象是整体重写的，因此单个调用方的读与写
	// 会在其锁上交替进行。
	cache *indexCache
	locks sync.Map // owner to *sync.RWMutex；owner 到 *sync.RWMutex 的映射。
	// The inference service is shared by every caller, so embedding is
	// rationed per caller and overall.
	// 推理服务为所有调用方共享，因此 embedding 按调用方及整体进行配给。
	gate     *inferenceGate
	timeouts inferenceTimeouts
	// started is what each caller's last rebuild was for, so a request
	// for another kind of rebuild replaces it rather than joins it.
	// started 记录每个调用方最后一次 rebuild 的目的，因此请求另一种 rebuild 时会替换它而不是加入它。
	mu      sync.Mutex
	started map[string]startedRebuild
	// writers hold each caller's chat writes until their index takes them.
	// writers 暂存每个调用方的聊天写入，直到其索引接收这些写入。
	writersMu sync.Mutex
	writers   map[writerKey]*writer
}

func New(cp *controlplane.Client, bk *buckets.Client, embedder Embedder, rows store.RowStore) *Service {
	return &Service{
		Controlplane: cp,
		Buckets:      bk,
		Embedder:     embedder,
		Rows:         rows,
		Rebuilds:     jobs.New[RebuildReport](RebuildBudget, RebuildRetention),
		cache:        newIndexCache(cacheBudgetBytes),
		gate:         newInferenceGate(defaultInferenceLimits),
		timeouts:     defaultInferenceTimeouts,
		started:      map[string]startedRebuild{},
		writers:      map[writerKey]*writer{},
	}
}

// errUnavailable refuses a search while the index bucket or the embedding
// service is not configured; chats are then stored without being indexed.
// errUnavailable 在索引 bucket 或 embedding 服务未配置时拒绝搜索；此时聊天只存储而不被索引。
var errUnavailable = api.Err(503, "SEARCH_INDEX_FAILED", "Search is not configured")

func (s *Service) configured() bool {
	return s != nil && s.Buckets != nil && s.Buckets.Configured() && s.Embedder != nil && s.Embedder.Configured()
}

// caller is who an index belongs to, with the keys it is kept under: the
// id of their content key, which the publication names, and the key the
// index object is sealed with, derived from it. A rebuild's caller also
// holds the content key, which opens the chats it reads.
// caller 表示索引所属者及其所保留的密钥：其 content key 的 id（publication 据此命名），以及
// 派生自它、用于封存索引对象的密钥。rebuild 的 caller 还持有 content key，用于打开它读取的聊天。
type caller struct {
	p        *auth.Principal
	kid      string
	indexKey []byte
	key      auth.ContentKey
}

func (s *Service) callerOf(p *auth.Principal, key auth.ContentKey) (caller, error) {
	indexKey, err := seal.DeriveSearchIndexKey(key[:])
	if err != nil {
		return caller{}, api.Err(400, "BAD_REQUEST", "invalid content key")
	}
	return caller{p: p, kid: keyID(key), indexKey: indexKey}, nil
}

// detached is the caller as a background job keeps them: who they are and
// their credential, without the request's grant, with their own copies
// of the keys.
// detached 是后台 job 所持有的 caller：身份与凭据，但不含请求的授权，并持有密钥的独立副本。
func (c caller) detached(key auth.ContentKey) caller {
	return caller{p: &auth.Principal{ID: c.p.ID, JWT: c.p.JWT}, kid: c.kid, indexKey: bytes.Clone(c.indexKey), key: key}
}

func (c caller) owner() string { return c.p.ID }

func (c *caller) zero() {
	seal.Zero(c.indexKey)
	clear(c.key[:])
}

// keyID is the hex id a content key is registered and sealed under.
// keyID 是 content key 注册与封存所用的十六进制 id。
func keyID(key auth.ContentKey) string {
	id, _ := seal.DeriveKeyID(key[:])
	return seal.KeyIDHex(id)
}

// lock serializes the read-modify-write cycles on one caller's index
// within this process: the bucket has no compare-and-swap, so two writes
// that read the same generation would lose one entry. Readers take the
// shared side, since a cached index is changed in place by writers.
// lock 在本进程内串行化单个调用方索引的读-改-写循环：bucket 没有 compare-and-swap，因此读取
// 同一 generation 的两次写入会丢失其中一次。读者取共享侧，因为缓存的索引会被写者原地修改。
func (s *Service) lock(owner string) func() {
	mu := s.lockOf(owner)
	mu.Lock()
	return mu.Unlock
}

func (s *Service) rlock(owner string) func() {
	mu := s.lockOf(owner)
	mu.RLock()
	return mu.RUnlock
}

func (s *Service) lockOf(owner string) *sync.RWMutex {
	mu, _ := s.locks.LoadOrStore(owner, &sync.RWMutex{})
	return mu.(*sync.RWMutex)
}

// Forget drops what this process holds for a caller whose index the
// control plane fenced off (a start-fresh key), and the fenced object.
// Forget 丢弃本进程为索引被控制平面 fence 掉的调用方（start-fresh 密钥）所持有的内容，以及
// 被 fence 的对象。
func (s *Service) Forget(ctx context.Context, owner, objectKey string) {
	if !s.configured() {
		return
	}
	s.forgetRebuild(owner)
	unlock := s.lock(owner)
	defer unlock()
	s.cache.drop(owner)
	s.deleteObject(ctx, owner, objectKey)
}

// entryTime is when an entry was written, for ordering; an entry without
// a time, or with one that cannot be read, is older than any other.
// entryTime 返回条目写入的时间，用于排序；没有时间、或时间无法解析的条目比任何其他条目都旧。
func entryTime(updatedAt string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return time.Time{}
	}
	return t
}
