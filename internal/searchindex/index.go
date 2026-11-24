// Package searchindex implements the per-user chat search index the
// harness maintains, modeled on what E2EE messengers build
// client-side (SQLite FTS5 style): an inverted index from token to
// posting list for exact keyword search, plus a handful of quantized
// chunk-embedding vectors per chat for semantic ranking as a bonus
// signal.
//
// The whole index is one object fetched into memory from the buckets
// sidecar, so the format is optimized to stay small: each token
// string is stored once (as a postings key) no matter how many chats
// contain it, posting lists hold small integer slots instead of chat
// ids, and vectors are int8-quantized. Queries resolve keywords by
// O(query tokens) posting-list lookups; only the semantic pass scans
// per-chat vectors, which stays cheap at per-user corpus sizes.
//
// The index is serialized to JSON, gzipped, and stored sealed under a
// key derived from the user's CEK (crypto.DeriveSearchIndexKey).
// Plaintext index contents exist only inside the enclave for the
// lifetime of a request.
// searchindex 包实现 harness 维护的、每个用户的聊天搜索索引，仿照 E2EE 通讯工具在客户端
// 构建的方式（SQLite FTS5 风格）：从 token 到 posting list 的倒排索引用于精确关键词搜索，
// 另加每个聊天若干量化 chunk embedding 向量作为语义排序的加分信号。
//
// 整个索引是从 buckets sidecar 读入内存的单个对象，因此格式尽量精简：每个 token 字符串
// 只存储一次（作为 postings 的键），不论多少个聊天包含它；posting list 存放小整数 slot 而非
// 聊天 id；向量做 int8 量化。查询通过 O(查询 token 数) 次 posting-list 查找解析关键词；只有
// 语义阶段会扫描逐聊天向量，在单用户语料规模下开销很低。
//
// 索引序列化为 JSON，经 gzip 压缩后，用派生自用户 CEK 的密钥
// （crypto.DeriveSearchIndexKey）封存存储。明文的索引内容只在 enclave 内、请求存续期间存在。
package searchindex

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	// FormatVersion tags the serialized index shape. Decode rejects
	// anything else so an older enclave never silently corrupts an
	// index written by a newer one; a version bump simply forces a
	// client-driven reindex.
	// FormatVersion 标记序列化索引的结构。Decode 拒绝其他任何版本，以免旧版 enclave 悄悄
	// 破坏新版写出的索引；版本升级只需强制一次客户端驱动的 reindex。
	FormatVersion = 1

	// MaxChats bounds how many entries one index holds, which in turn
	// bounds the object size and per-query cost.
	// MaxChats 限定单个索引能容纳的条目数，进而限定对象大小与每次查询的开销。
	MaxChats = 50000

	// MaxTokensPerChat caps how many tokens one chat contributes to
	// the postings.
	// MaxTokensPerChat 限定单个聊天向 postings 贡献的 token 数量上限。
	MaxTokensPerChat = 512

	// MaxChunksPerChat caps how many embedding vectors one chat may
	// store. Chunked embeddings keep long chats searchable (one
	// vector over a whole conversation dilutes everything), while the
	// cap bounds the dominant per-chat size cost; typical short chats
	// carry a single chunk.
	// MaxChunksPerChat 限定单个聊天可存储的 embedding 向量数量上限。分块 embedding 使长聊天
	// 仍可搜索（对整个会话只用一个向量会把一切都稀释），而上限约束了单聊天主要的体积开销；
	// 典型短聊天只带一个 chunk。
	MaxChunksPerChat = 8

	// minTokenLen drops single-character noise tokens.
	// minTokenLen 丢弃单字符噪声 token。
	minTokenLen = 2

	// maxTokenLen drops pathological fragment tokens (base64 blobs,
	// hashes) that bloat the index without helping keyword search.
	// maxTokenLen 丢弃病态的分片 token（base64 blob、哈希），它们只会撑大索引而对关键词
	// 搜索毫无帮助。
	maxTokenLen = 40

	// maxIdentifierLen is the longer cap for whole identifiers
	// (emails, URLs) kept intact for exact matching.
	// maxIdentifierLen 是完整标识符（邮箱、URL）的更宽松上限，这些标识符保留原样用于精确匹配。
	maxIdentifierLen = 64

	// compactionMinDead avoids rewriting postings for trivial amounts
	// of garbage; compaction only fires once at least this many dead
	// slots exist and they outnumber the live entries.
	// compactionMinDead 避免为微不足道的垃圾量重写 postings；只有当死 slot 至少达到这个数量
	// 且超过存活条目时，compaction 才会触发。
	compactionMinDead = 64

	// rrfK is the standard reciprocal-rank-fusion smoothing constant.
	// rrfK 是标准的倒数排名融合平滑常数。
	rrfK = 60

	// rrfCandidateDepth truncates each ranked list before fusion so a
	// huge corpus doesn't drag thousands of near-zero-signal entries
	// into the fused scoring.
	// rrfCandidateDepth 在融合前截断每个排名列表，以免庞大语料把成千上万近乎零信号的条目
	// 拖进融合打分。
	rrfCandidateDepth = 128

	// maxReindexCursorLength bounds the opaque controlplane cursor
	// persisted in a partial index between resumable rebuild runs.
	// maxReindexCursorLength 限定在可续传 rebuild 各次运行之间、持久化于部分索引中的不透明
	// controlplane cursor 的长度。
	maxReindexCursorLength = 4096

	// maxSerializedSlots admits the tombstones Upsert can leave
	// between compactions while rejecting slot tables this package
	// could never produce.
	// maxSerializedSlots 允许 Upsert 在两次 compaction 之间留下的 tombstone，同时拒绝本包
	// 绝不可能产生的 slot 表。
	maxSerializedSlots = MaxChats * 2

	// lexicalRRFWeight / semanticRRFWeight bias fusion toward keyword
	// hits: exact keyword search is the reliable tier, semantic
	// similarity the bonus tier.
	// lexicalRRFWeight / semanticRRFWeight 使融合偏向关键词命中：精确关键词搜索是可靠层，
	// 语义相似度是加分层。
	lexicalRRFWeight  = 1.0
	semanticRRFWeight = 0.5
)

// ErrFormat 表示不支持的索引格式。
// ErrTooLarge 表示索引已满。
var (
	ErrFormat   = errors.New("searchindex: unsupported index format")
	ErrTooLarge = errors.New("searchindex: index is full")
)

// Vector is an int8-quantized embedding vector that serializes as
// base64, keeping a 768-dim vector at ~1 KiB on the wire instead of
// the ~15 KiB a float JSON array would take. Cosine similarity is
// invariant under per-vector positive scaling, and cosine is the only
// operation performed on stored vectors, so symmetric max-abs
// quantization needs no stored scale factor and costs ranking quality
// only through the (small) rounding error.
// Vector 是 int8 量化的 embedding 向量，序列化为 base64，使 768 维向量在线上仅约 1 KiB，
// 而非 float JSON 数组所需的约 15 KiB。余弦相似度在逐向量正缩放变换下不变，而余弦是唯一对
// 已存向量执行的操作，因此对称 max-abs 量化无需存储缩放因子，仅因（很小的）舍入误差损失
// 排序质量。
type Vector []int8

// Quantize maps a float vector onto the int8 range via symmetric
// max-abs scaling.
// Quantize 通过对称 max-abs 缩放把浮点向量映射到 int8 区间。
func Quantize(v []float32) Vector {
	if len(v) == 0 {
		return nil
	}
	var maxAbs float64
	for _, f := range v {
		if a := math.Abs(float64(f)); a > maxAbs {
			maxAbs = a
		}
	}
	out := make(Vector, len(v))
	// NaN/Inf cannot arrive via well-formed JSON embeddings, but a
	// non-finite value here would make the int8 conversion below
	// implementation-defined; a zero vector is the safe degradation.
	// NaN/Inf 不会来自格式良好的 JSON embedding，但此处若出现非有限值，会让下面的 int8 转换
	// 变成实现定义行为；零向量是安全的降级方案。
	if maxAbs == 0 || math.IsInf(maxAbs, 0) || math.IsNaN(maxAbs) {
		return out
	}
	scale := 127 / maxAbs
	for i, f := range v {
		q := math.Round(float64(f) * scale)
		if math.IsNaN(q) {
			continue
		}
		out[i] = int8(q)
	}
	return out
}

// MarshalJSON 把向量编码为 base64 字符串。
func (v Vector) MarshalJSON() ([]byte, error) {
	buf := make([]byte, len(v))
	for i, q := range v {
		buf[i] = byte(q)
	}
	return []byte(`"` + base64.StdEncoding.EncodeToString(buf) + `"`), nil
}

// UnmarshalJSON 从 base64 字符串解码向量。
func (v *Vector) UnmarshalJSON(data []byte) error {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return errors.New("searchindex: vector must be a base64 string")
	}
	raw, err := base64.StdEncoding.DecodeString(string(data[1 : len(data)-1]))
	if err != nil {
		return fmt.Errorf("searchindex: decode vector: %w", err)
	}
	out := make(Vector, len(raw))
	for i, b := range raw {
		out[i] = int8(b)
	}
	*v = out
	return nil
}

// Entry is one chat's index record. Its tokens live in the shared
// postings, referenced by Slot, not on the entry itself.
// Entry 是一个聊天的索引记录。它的 token 存于共享的 postings 中，通过 Slot 引用，而非存于
// 条目本身。
type Entry struct {
	// Slot is the chat's position in the Slots table; posting lists
	// reference chats by slot. Assigned by Upsert.
	// Slot 是该聊天在 Slots 表中的位置；posting list 通过 slot 引用聊天，由 Upsert 分配。
	Slot int `json:"slot"`
	// ETag is the stored row's etag the entry was built from, so callers
	// can skip re-embedding chats that have not changed.
	// ETag 是构建该条目所依据的已存行 etag，使调用方可以跳过对未变更聊天的重复 embedding。
	ETag           string `json:"etag,omitempty"`
	SourceRevision int64  `json:"source_revision,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
	// Vectors holds one quantized embedding per text chunk; the
	// semantic score for the chat is the max similarity over them.
	// Vectors 为每个文本 chunk 存一个量化 embedding；聊天的语义得分是它们之上的最大相似度。
	Vectors          []Vector `json:"vectors,omitempty"`
	EmbeddingPending bool     `json:"embedding_pending,omitempty"`
	EmbeddingKeyID   string   `json:"embedding_key_id,omitempty"`
}

// ReindexProgress checkpoints a clean page boundary for a partial
// rebuild. It lives inside the encrypted index object so a later job
// can resume after an enclave restart without retaining key material
// or progress in a separate plaintext store.
// ReindexProgress 为部分 rebuild 记录一个干净的页边界检查点。它存放在加密的索引对象内部，
// 使之后的 job 能在 enclave 重启后继续，而无需在单独的明文存储中保留密钥材料或进度。
type ReindexProgress struct {
	NextCursor           string `json:"next_cursor"`
	TargetSourceRevision int64  `json:"target_source_revision"`
	StartedAt            string `json:"started_at"`
}

// Index is the serialized per-user search index.
// Index 是序列化的每用户搜索索引。
type Index struct {
	Version int `json:"version"`
	// Model and Dims pin the embedding space. Vectors from different
	// models are not comparable, so a model change forces a rebuild.
	// Model 与 Dims 固定 embedding 空间。不同模型的向量不可比较，因此模型变更会强制 rebuild。
	Model string `json:"model,omitempty"`
	Dims  int    `json:"dims,omitempty"`
	// Incomplete marks an index known not to cover the full corpus:
	// set when an inline push had to start over on top of an
	// unreadable previous index (wrong key, corruption, model change)
	// and while a rebuild is mid-flight. Queries surface it as
	// needs_reindex; a completed rebuild clears it.
	// Incomplete 标记一个已知未覆盖全语料的索引：在内联 push 不得不基于一个不可读的旧索引
	// （错钥、损坏、模型变更）重新开始，以及 rebuild 进行期间置位。查询将其呈现为
	// needs_reindex；完成的 rebuild 会清除它。
	Incomplete bool `json:"incomplete,omitempty"`
	// Reindex is present only after a clean, non-final rebuild page.
	// Failures clear it so the next run starts over and retries gaps.
	// Reindex 仅在一次干净、非最终的 rebuild 页之后存在。失败会清除它，使下一次运行重新开始
	// 并重试缺口。
	Reindex *ReindexProgress `json:"reindex,omitempty"`
	// Slots maps slot -> chat id. An updated or removed chat leaves a
	// tombstone ("" at its old slot) instead of eagerly rewriting
	// every posting list; queries skip dead slots and compaction
	// reclaims them once they outnumber live entries.
	// Slots 是 slot -> 聊天 id 的映射。被更新或删除的聊天会留下 tombstone（旧 slot 处置 ""），
	// 而不是急于重写每条 posting list；查询跳过死 slot，待其数量超过存活条目后由 compaction 回收。
	Slots []string `json:"slots,omitempty"`
	// Chats maps chat id -> entry.
	// Chats 是聊天 id -> 条目的映射。
	Chats map[string]Entry `json:"chats"`
	// Postings is the inverted index: token -> ascending slot list.
	// Postings 是倒排索引：token -> 升序 slot 列表。
	Postings map[string][]int `json:"postings,omitempty"`

	// dead counts tombstoned slots; recomputed on decode, maintained
	// on mutation, never serialized.
	// dead 统计 tombstone 的 slot 数量；解码时重新计算、变更时维护、从不序列化。
	dead int
}

// New 创建一个空的、绑定到给定模型的索引。
func New(model string) *Index {
	return &Index{
		Version:  FormatVersion,
		Model:    model,
		Chats:    map[string]Entry{},
		Postings: map[string][]int{},
	}
}

// Decode parses a serialized index and validates its shape. Slot
// references are checked so a corrupted object surfaces as a decode
// error (which callers treat as "rebuild") instead of a panic later.
// Decode 解析序列化的索引并校验其结构。slot 引用会被检查，使损坏的对象表现为解码错误
// （调用方将其视为「需要 rebuild」），而非之后的 panic。
func Decode(data []byte) (*Index, error) {
	var ix Index
	if err := json.Unmarshal(data, &ix); err != nil {
		return nil, fmt.Errorf("searchindex: decode: %w", err)
	}
	if ix.Version != FormatVersion {
		return nil, ErrFormat
	}
	if ix.Chats == nil {
		ix.Chats = map[string]Entry{}
	}
	if ix.Postings == nil {
		ix.Postings = map[string][]int{}
	}
	if len(ix.Slots) > maxSerializedSlots {
		return nil, fmt.Errorf("searchindex: %d slots exceeds limit %d", len(ix.Slots), maxSerializedSlots)
	}
	live := 0
	for _, id := range ix.Slots {
		if id == "" {
			ix.dead++
			continue
		}
		live++
		e, ok := ix.Chats[id]
		if !ok || e.Slot < 0 || e.Slot >= len(ix.Slots) || id != ix.Slots[e.Slot] {
			return nil, errors.New("searchindex: slot table does not match entries")
		}
	}
	if live != len(ix.Chats) {
		return nil, errors.New("searchindex: slot table does not match entries")
	}
	if ix.dead >= compactionMinDead && ix.dead > live {
		return nil, errors.New("searchindex: tombstones exceed compaction invariant")
	}
	// Enforce the same bounds Upsert does, so a corrupt stored object
	// cannot smuggle in an index far more expensive to search than
	// anything this code could have built. Rejecting it makes the
	// load path treat it as unreadable, which routes the client to a
	// rebuild instead of serving the oversized index.
	// 执行与 Upsert 相同的上限，使损坏的已存对象无法塞入一个搜索开销远超本代码所能构建的索引。
	// 拒绝它会让加载路径将其视为不可读，从而把客户端导向 rebuild，而不是提供超大索引。
	if len(ix.Chats) > MaxChats {
		return nil, fmt.Errorf("searchindex: %d chats exceeds limit %d", len(ix.Chats), MaxChats)
	}
	if ix.Reindex != nil {
		if !ix.Incomplete {
			return nil, errors.New("searchindex: complete index has reindex progress")
		}
		if ix.Reindex.NextCursor == "" || len(ix.Reindex.NextCursor) > maxReindexCursorLength {
			return nil, errors.New("searchindex: reindex cursor is invalid")
		}
		if ix.Reindex.TargetSourceRevision < 0 {
			return nil, errors.New("searchindex: reindex source revision is negative")
		}
		if _, err := time.Parse(time.RFC3339Nano, ix.Reindex.StartedAt); err != nil {
			return nil, errors.New("searchindex: reindex start time is invalid")
		}
	}
	for _, e := range ix.Chats {
		if e.SourceRevision < 0 {
			return nil, errors.New("searchindex: entry source revision is negative")
		}
		if !validEmbeddingKeyID(e.EmbeddingKeyID) {
			return nil, errors.New("searchindex: entry embedding key id is invalid")
		}
		if !e.EmbeddingPending && e.EmbeddingKeyID != "" {
			return nil, errors.New("searchindex: entry embedding key id requires pending embedding")
		}
		if len(e.Vectors) > MaxChunksPerChat {
			return nil, fmt.Errorf("searchindex: %d chunk vectors exceeds limit %d", len(e.Vectors), MaxChunksPerChat)
		}
		for _, v := range e.Vectors {
			if len(v) == 0 || len(v) != ix.Dims {
				return nil, errors.New("searchindex: entry vector does not match index dims")
			}
		}
	}
	maxPostings := len(ix.Slots) * MaxTokensPerChat
	totalPostings := 0
	postedSlots := make([]bool, len(ix.Slots))
	for tok, slots := range ix.Postings {
		if !validPostingToken(tok) {
			return nil, errors.New("searchindex: posting token is invalid")
		}
		if len(slots) == 0 {
			return nil, errors.New("searchindex: posting list is empty")
		}
		if len(slots) > len(ix.Slots) {
			return nil, errors.New("searchindex: posting list exceeds slot table")
		}
		prev := -1
		for _, s := range slots {
			if s < 0 || s >= len(ix.Slots) {
				return nil, errors.New("searchindex: posting references unknown slot")
			}
			if s <= prev {
				return nil, errors.New("searchindex: posting list is not strictly ordered")
			}
			prev = s
			postedSlots[s] = true
			totalPostings++
			if totalPostings > maxPostings {
				return nil, errors.New("searchindex: postings exceed token limit")
			}
		}
	}
	for id, entry := range ix.Chats {
		if len(entry.Vectors) == 0 && postedSlots[entry.Slot] {
			entry.EmbeddingPending = true
			ix.Chats[id] = entry
		}
	}
	return &ix, nil
}

// Encode 把索引序列化为 JSON。
func (ix *Index) Encode() ([]byte, error) {
	return json.Marshal(ix)
}

func validEmbeddingKeyID(keyID string) bool {
	if keyID == "" {
		return true
	}
	if len(keyID) != 32 {
		return false
	}
	for _, c := range keyID {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func validPostingToken(tok string) bool {
	return len(tok) >= minTokenLen && len(tok) <= maxIdentifierLen
}

// Upsert records or replaces one chat's entry and its postings. The
// previous generation of an updated chat is tombstoned rather than
// scrubbed from every posting list; compaction reclaims tombstones in
// bulk. The first vector to land in an empty index pins Dims; later
// vectors must match.
// Upsert 记录或替换一个聊天的条目及其 postings。被更新聊天的上一代会被 tombstone 掉，而非从
// 每条 posting list 中擦除；compaction 批量回收 tombstone。首个落入空索引的向量固定 Dims；
// 之后的向量必须与其一致。
func (ix *Index) Upsert(id string, e Entry, tokens []string) error {
	if id == "" {
		return errors.New("searchindex: id is required")
	}
	if e.SourceRevision < 0 {
		return errors.New("searchindex: entry source revision is negative")
	}
	if !validEmbeddingKeyID(e.EmbeddingKeyID) {
		return errors.New("searchindex: entry embedding key id is invalid")
	}
	if !e.EmbeddingPending && e.EmbeddingKeyID != "" {
		return errors.New("searchindex: embedding key id requires pending embedding")
	}
	dims, err := validatedVectorDims(ix.Dims, e.Vectors)
	if err != nil {
		return err
	}
	if _, exists := ix.Chats[id]; !exists && len(ix.Chats) >= MaxChats {
		return ErrTooLarge
	}
	ix.Dims = dims
	if old, exists := ix.Chats[id]; exists {
		ix.Slots[old.Slot] = ""
		ix.dead++
	}
	e.Slot = len(ix.Slots)
	ix.Slots = append(ix.Slots, id)
	ix.Chats[id] = e
	// Tokenize already dedupes, but Upsert must not trust its caller:
	// a duplicated token would double-count the chat in that token's
	// IDF and score.
	// Tokenize 已做去重，但 Upsert 不能信任其调用方：重复的 token 会在该 token 的 IDF 与
	// 得分中把聊天重复计数。
	seen := make(map[string]struct{}, len(tokens))
	for _, tok := range tokens {
		if !validPostingToken(tok) {
			continue
		}
		if _, dup := seen[tok]; dup {
			continue
		}
		seen[tok] = struct{}{}
		ix.Postings[tok] = append(ix.Postings[tok], e.Slot)
		if len(seen) >= MaxTokensPerChat {
			break
		}
	}
	ix.maybeCompact()
	return nil
}

// RepairEmbedding replaces only an existing entry's semantic vectors,
// leaving its lexical postings and corpus metadata unchanged.
// RepairEmbedding 仅替换已有条目的语义向量，保持其词法 postings 与语料元数据不变。
func (ix *Index) RepairEmbedding(id string, vectors []Vector) error {
	entry, ok := ix.Chats[id]
	if !ok {
		return errors.New("searchindex: chat does not exist")
	}
	dims, err := validatedVectorDims(ix.Dims, vectors)
	if err != nil {
		return err
	}
	entry.Vectors = vectors
	entry.EmbeddingPending = false
	entry.EmbeddingKeyID = ""
	ix.Chats[id] = entry
	ix.Dims = dims
	return nil
}

// MarkEmbeddingKey records which CEK is required to repair a pending
// entry without changing its lexical data.
// MarkEmbeddingKey 记录修复一个 pending 条目所需的 CEK，而不改变其词法数据。
func (ix *Index) MarkEmbeddingKey(id, keyID string) bool {
	entry, ok := ix.Chats[id]
	if !ok || !entry.EmbeddingPending || keyID == "" || !validEmbeddingKeyID(keyID) || entry.EmbeddingKeyID == keyID {
		return false
	}
	entry.EmbeddingKeyID = keyID
	ix.Chats[id] = entry
	return true
}

func validatedVectorDims(indexDims int, vectors []Vector) (int, error) {
	if len(vectors) > MaxChunksPerChat {
		return 0, fmt.Errorf("searchindex: %d chunk vectors exceeds limit %d", len(vectors), MaxChunksPerChat)
	}
	dims := indexDims
	for _, v := range vectors {
		if len(v) == 0 {
			return 0, errors.New("searchindex: empty chunk vector")
		}
		if dims == 0 {
			dims = len(v)
		} else if len(v) != dims {
			return 0, fmt.Errorf("searchindex: vector has %d dims, index has %d", len(v), dims)
		}
	}
	return dims, nil
}

// Remove drops one chat, leaving a tombstone slot behind.
// Remove 删除一个聊天，留下一个 tombstone slot。
func (ix *Index) Remove(id string) {
	e, ok := ix.Chats[id]
	if !ok {
		return
	}
	ix.Slots[e.Slot] = ""
	ix.dead++
	delete(ix.Chats, id)
	ix.maybeCompact()
}

// maybeCompact rewrites the slot table and postings once tombstones
// outnumber live entries. Amortized this keeps mutations O(tokens per
// chat) while bounding the garbage a churn-heavy user accumulates.
// maybeCompact 在 tombstone 数量超过存活条目时重写 slot 表与 postings。均摊下来这使变更保持
// O(每聊天 token 数)，同时约束高频变更用户积累的垃圾量。
func (ix *Index) maybeCompact() {
	if ix.dead < compactionMinDead || ix.dead <= len(ix.Chats) {
		return
	}
	remap := make(map[int]int, len(ix.Chats))
	newSlots := make([]string, 0, len(ix.Chats))
	for oldSlot, id := range ix.Slots {
		if id == "" {
			continue
		}
		remap[oldSlot] = len(newSlots)
		newSlots = append(newSlots, id)
	}
	for id, e := range ix.Chats {
		e.Slot = remap[e.Slot]
		ix.Chats[id] = e
	}
	for tok, slots := range ix.Postings {
		kept := slots[:0]
		for _, s := range slots {
			if n, ok := remap[s]; ok {
				kept = append(kept, n)
			}
		}
		if len(kept) == 0 {
			delete(ix.Postings, tok)
			continue
		}
		sort.Ints(kept)
		ix.Postings[tok] = kept
	}
	ix.Slots = newSlots
	ix.dead = 0
}

// Compatible reports whether the index can serve queries embedded
// with the given model.
// Compatible 报告该索引能否服务用给定模型嵌入的查询。
func (ix *Index) Compatible(model string) bool {
	return ix.Model == model
}

// NeedsEmbeddingRepair reports whether any lexically indexed entry is
// still waiting for semantic vectors.
// NeedsEmbeddingRepair 报告是否存在任何仅按词法建索引的条目仍在等待语义向量。
func (ix *Index) NeedsEmbeddingRepair() bool {
	for _, entry := range ix.Chats {
		if entry.EmbeddingPending {
			return true
		}
	}
	return false
}

// NeedsEmbeddingRepairFor reports whether the supplied CEK can repair
// at least one pending entry. An empty recorded key means the index
// predates key-aware repair and should be tried once.
// NeedsEmbeddingRepairFor 报告所提供的 CEK 能否修复至少一个 pending 条目。记录为空表示该
// 索引早于按密钥感知的修复机制，应尝试一次。
func (ix *Index) NeedsEmbeddingRepairFor(keyID string) bool {
	for _, entry := range ix.Chats {
		if entry.EmbeddingPending && (entry.EmbeddingKeyID == "" || entry.EmbeddingKeyID == keyID) {
			return true
		}
	}
	return false
}

// identifierPattern matches whole emails and URLs in lowercased text.
// These are kept intact as single tokens (alongside their fragments)
// so a query for an exact identifier like "sacha@gmail.com" matches
// the whole token decisively instead of relying on fragment
// coincidence.
// identifierPattern 匹配小写文本中的完整邮箱与 URL。它们作为单个 token（连同其分片）原样保留，
// 使对精确标识符（如 "sacha@gmail.com"）的查询能决定性地命中整个 token，而非依赖分片的偶然重合。
var identifierPattern = regexp.MustCompile(`https?://[^\s<>"']+|www\.[^\s<>"']+|[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`)

// Tokenize lowercases text and returns its deduplicated token set:
// whole email/URL identifiers first (so the cap can never evict
// them), then fragments split on non-letter/non-digit runes.
// Tokenize 将文本转为小写并返回去重后的 token 集合：完整的邮箱/URL 标识符在前（使上限永远
// 无法淘汰它们），随后是按非字母/非数字字符切分的分片。
func Tokenize(text string) []string {
	lower := strings.ToLower(text)
	seen := make(map[string]struct{})
	var out []string
	add := func(tok string, maxLen int) bool {
		if len(tok) < minTokenLen || len(tok) > maxLen {
			return true
		}
		if _, dup := seen[tok]; dup {
			return true
		}
		seen[tok] = struct{}{}
		out = append(out, tok)
		return len(out) < MaxTokensPerChat
	}
	for _, ident := range identifierPattern.FindAllString(lower, -1) {
		if !add(strings.Trim(ident, ".,;:!?"), maxIdentifierLen) {
			return out
		}
	}
	fields := strings.FieldsFunc(lower, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	for _, f := range fields {
		if !add(f, maxTokenLen) {
			return out
		}
	}
	return out
}

// Result is one ranked search hit.
// Result 是一个已排序的搜索命中。
type Result struct {
	ID    string
	Score float64
}

// Search answers a query in two strict tiers:
//
//   - Lexical: posting-list lookups scored by summed IDF, so rare
//     query tokens ("sacha") dominate ubiquitous ones ("com"). This
//     is the reliable tier and every hit ranks before semantic-only
//     results.
//   - Semantic: cosine over per-chat quantized vectors, the bonus
//     tier that refines lexical ordering and then surfaces
//     "animal" -> duck/dog chats without keyword matches.
//
// Weighted reciprocal-rank fusion orders results within those tiers
// without comparing incompatible cosine and IDF scales. Returned
// scores are meaningful for ordering, not calibrated for anything
// else.
// Search 以两个严格层级应答查询：
//
//   - 词法：posting-list 查找，按 IDF 求和打分，使稀有查询 token（"sacha"）压过常见 token
//     （"com"）。这是可靠层，每个命中都排在纯语义结果之前。
//   - 语义：对逐聊天量化向量做余弦相似度，是精炼词法排序、并浮现无关键词匹配的
//     "animal" -> duck/dog 聊天的加分层。
//
// 加权倒数排名融合在这两层内排序结果，而不必比较不可公度的余弦与 IDF 尺度。返回的分数仅
// 用于排序，未针对任何其他用途校准。
func (ix *Index) Search(queryVec []float32, queryTokens []string, limit int) []Result {
	if limit <= 0 || len(ix.Chats) == 0 {
		return nil
	}
	fused := map[int]float64{}
	lexical := map[int]struct{}{}
	for rank, slot := range ix.lexicalRanked(queryTokens) {
		lexical[slot] = struct{}{}
		fused[slot] += lexicalRRFWeight / float64(rrfK+rank+1)
	}
	for rank, slot := range ix.semanticRanked(queryVec) {
		fused[slot] += semanticRRFWeight / float64(rrfK+rank+1)
	}
	results := make([]Result, 0, len(fused))
	for slot, score := range fused {
		results = append(results, Result{ID: ix.Slots[slot], Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		_, iLexical := lexical[ix.Chats[results[i].ID].Slot]
		_, jLexical := lexical[ix.Chats[results[j].ID].Slot]
		if iLexical != jLexical {
			return iLexical
		}
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})
	if len(results) > limit {
		results = results[:limit]
	}
	return results
}

// lexicalRanked returns live slots ordered by summed IDF over the
// matched query tokens, truncated to rrfCandidateDepth.
// lexicalRanked 返回按匹配查询 token 的 IDF 之和排序的存活 slot，截断到 rrfCandidateDepth。
func (ix *Index) lexicalRanked(queryTokens []string) []int {
	n := len(ix.Chats)
	scores := map[int]float64{}
	seen := map[string]struct{}{}
	for _, tok := range queryTokens {
		if _, dup := seen[tok]; dup {
			continue
		}
		seen[tok] = struct{}{}
		live := ix.livePostings(tok)
		if len(live) == 0 {
			continue
		}
		// BM25-style IDF: a token in every chat scores ~0, a token in
		// one chat scores ~log(N).
		// BM25 风格的 IDF：出现在每个聊天中的 token 得分约 0，只出现在一个聊天中的 token
		// 得分约 log(N)。
		idf := math.Log(1 + (float64(n)-float64(len(live))+0.5)/(float64(len(live))+0.5))
		for _, slot := range live {
			scores[slot] += idf
		}
	}
	return rankSlots(scores, func(a, b int) bool { return ix.Slots[a] < ix.Slots[b] })
}

// semanticRanked returns live slots ordered by the best cosine
// similarity between the query vector and any of the chat's chunk
// vectors, truncated to rrfCandidateDepth. Max-sim over chunks means
// a long chat matches on its most relevant passage instead of an
// average washed out by the rest of the conversation.
// semanticRanked 返回按查询向量与任一 chunk 向量的最佳余弦相似度排序的存活 slot，截断到
// rrfCandidateDepth。对 chunk 取 max-sim 意味着长聊天按其最相关的段落匹配，而非被会话其余
// 部分稀释的平均值。
func (ix *Index) semanticRanked(queryVec []float32) []int {
	if len(queryVec) == 0 {
		return nil
	}
	scores := map[int]float64{}
	for _, e := range ix.Chats {
		best := math.Inf(-1)
		for _, v := range e.Vectors {
			if len(v) != len(queryVec) {
				continue
			}
			if c := cosine(queryVec, v); c > best {
				best = c
			}
		}
		if !math.IsInf(best, -1) {
			scores[e.Slot] = best
		}
	}
	return rankSlots(scores, func(a, b int) bool { return ix.Slots[a] < ix.Slots[b] })
}

// livePostings returns the token's posting list with tombstoned and
// superseded slots filtered out.
// livePostings 返回该 token 的 posting list，过滤掉 tombstone 与被取代的 slot。
func (ix *Index) livePostings(tok string) []int {
	slots := ix.Postings[tok]
	live := make([]int, 0, len(slots))
	for _, s := range slots {
		if ix.Slots[s] == "" {
			continue
		}
		live = append(live, s)
	}
	return live
}

func rankSlots(scores map[int]float64, tieBreak func(a, b int) bool) []int {
	ranked := make([]int, 0, len(scores))
	for slot := range scores {
		ranked = append(ranked, slot)
	}
	sort.Slice(ranked, func(i, j int) bool {
		si, sj := scores[ranked[i]], scores[ranked[j]]
		if si != sj {
			return si > sj
		}
		return tieBreak(ranked[i], ranked[j])
	})
	if len(ranked) > rrfCandidateDepth {
		ranked = ranked[:rrfCandidateDepth]
	}
	return ranked
}

func cosine(a []float32, b Vector) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
