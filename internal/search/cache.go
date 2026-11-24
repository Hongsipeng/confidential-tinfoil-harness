// 本文件实现搜索索引的进程内缓存：在请求之间保留调用方已解码的索引，按字节预算做最近最少使用淘汰。
package search

import (
	"container/list"
	"crypto/sha256"
	"sync"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

// cacheBudgetBytes bounds the decoded indexes kept in memory, counted by
// the encoded size of each, which the decoded structures exceed by a
// small factor. An index is a few megabytes, so every active caller
// stays cached and eviction only sets in under extreme aggregate load.
// cacheBudgetBytes 限制内存中保留的已解码索引总量，按每个索引的编码大小计，解码后的结构
// 会比编码略大一些。一个索引只有几兆字节，因此每个活跃调用方都能一直留在缓存里，
// 只有极端聚合负载下才会触发淘汰。
const cacheBudgetBytes = 4 << 30

// indexCache keeps decoded indexes between requests, so the hot path
// skips the bucket read, the decompression and the decode, evicting the
// least recently used under a byte budget.
// indexCache 在请求之间保留已解码的索引，让热路径跳过 bucket 读取、解压与解码，
// 在字节预算内按最近最少使用淘汰。
//
// Index contents (tokens and quantized vectors derived from chat text,
// never the text or key material) stay in the same attested memory that
// holds chat plaintext during a request, capped by the budget and gone
// when the process ends. Storage stays the source of truth: every change
// is stored before it is served.
// 索引内容（从聊天文本派生的 token 与量化向量，绝不是文本或密钥材料）保存在请求期间
// 存放聊天明文的同一块 attested 内存中，受预算上限约束，进程结束时即消失。存储始终是
// 事实来源：任何变更都是先存储、后对外提供。
//
// An entry is bound to a digest of the object key the publication names
// and the key it is sealed with, so another replica's publication or a
// key change misses and reloads from storage.
// 每个缓存项绑定到 publication 所命名对象键与封存密钥的摘要，因此另一个 replica 的
// publication 或密钥变更都会导致缓存未命中，并重新从存储加载。
//
// An entry this process published as complete keeps the generation it
// was published at: the control plane marks a publication incomplete
// when another write lands while it is made, and the update that brings
// that write from here then knows the index was complete before it.
// 本进程以 complete 状态发布过的缓存项会保留其发布时的 generation：控制平面会在发布进行期间
// 又有新的写入落地时将该 publication 标记为 incomplete，于是从这里带来该写入的更新
// 便知道索引此前是完整的。
type indexCache struct {
	mu      sync.Mutex
	budget  int
	total   int
	lru     *list.List // front is the most recently used；front 是最近使用过的元素。
	byOwner map[string]*list.Element
}

type cacheEntry struct {
	owner string
	ix    *searchindex.Index
	key   [sha256.Size]byte
	size  int
	// completeAt is the generation this process published the object at
	// as complete, or 0.
	// completeAt 是本进程以 complete 状态发布该对象时的 generation，未发布过则为 0。
	completeAt int64
}

func newIndexCache(budget int) *indexCache {
	return &indexCache{budget: budget, lru: list.New(), byOwner: map[string]*list.Element{}}
}

// cacheKey is what a cached index is bound to: the key it is sealed with
// and the object it was read from.
// cacheKey 是缓存索引所绑定的标识：它被用来封存的密钥，以及读取它所依据的对象。
func cacheKey(indexKey []byte, objectKey string) [sha256.Size]byte {
	digest := sha256.New()
	digest.Write(indexKey)
	digest.Write([]byte{0})
	digest.Write([]byte(objectKey))
	var out [sha256.Size]byte
	copy(out[:], digest.Sum(nil))
	return out
}

// get is the owner's cached index when it is bound to key; one bound to
// another key is dropped.
// get 返回绑定到 key 的 owner 缓存索引；若绑定到其他密钥则将其丢弃。
func (c *indexCache) get(owner string, key [sha256.Size]byte) (*searchindex.Index, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byOwner[owner]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*cacheEntry)
	if entry.key != key {
		c.removeLocked(el)
		return nil, false
	}
	c.lru.MoveToFront(el)
	return entry.ix, true
}

// put caches the owner's index as read from, or written to, the object
// key names; one larger than the whole budget is not kept.
// put 缓存 owner 的索引，无论它是从对象键所命名的对象读出还是写入；超过整个预算的索引不予保留。
func (c *indexCache) put(owner string, key [sha256.Size]byte, ix *searchindex.Index, size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byOwner[owner]; ok {
		c.removeLocked(el)
	}
	if size > c.budget {
		return
	}
	c.byOwner[owner] = c.lru.PushFront(&cacheEntry{owner: owner, ix: ix, key: key, size: size})
	c.total += size
	for c.total > c.budget {
		c.removeLocked(c.lru.Back())
	}
}

// publishedComplete records that this process published the owner's
// cached object, bound to key, as complete at generation.
// publishedComplete 记录本进程已将绑定到 key 的 owner 缓存对象，在 generation 处以 complete 状态发布。
func (c *indexCache) publishedComplete(owner string, key [sha256.Size]byte, generation int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byOwner[owner]; ok && el.Value.(*cacheEntry).key == key {
		el.Value.(*cacheEntry).completeAt = generation
	}
}

// completeAt is whether this process published the owner's cached object,
// bound to key, as complete at generation.
// completeAt 表示本进程是否在 generation 处以 complete 状态发布了绑定到 key 的 owner 缓存对象。
func (c *indexCache) completeAt(owner string, key [sha256.Size]byte, generation int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byOwner[owner]
	return ok && generation > 0 && el.Value.(*cacheEntry).key == key && el.Value.(*cacheEntry).completeAt == generation
}

func (c *indexCache) drop(owner string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byOwner[owner]; ok {
		c.removeLocked(el)
	}
}

func (c *indexCache) removeLocked(el *list.Element) {
	entry := c.lru.Remove(el).(*cacheEntry)
	delete(c.byOwner, entry.owner)
	c.total -= entry.size
}
