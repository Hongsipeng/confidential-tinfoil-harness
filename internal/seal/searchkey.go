package seal

import (
	"crypto/sha256"
	"errors"
	"io"

	"golang.org/x/crypto/hkdf"
)

// Search-index key derivation.
// Search index（搜索索引）key 的派生方式。
//
// The per-user search index is stored through the buckets sidecar,
// which seals every object under a caller-supplied AES-256 key. The
// enclave derives that key from the user's CEK so the index inherits
// the CEK's confidentiality without persisting any new key material:
//
//	K_search = HKDF-SHA-256(IKM=CEK, salt="", info="tinfoil-search-index-v1", L=32)
// 每用户的 search index 通过 buckets sidecar 存储，该 sidecar 在每个对象下都
// 以调用方提供的 AES-256 key 密封。enclave 从用户的 CEK 派生该 key，使索引
// 继承 CEK 的机密性，而无需持久化任何新的 key 材料：
//
//	K_search = HKDF-SHA-256(IKM=CEK, salt="", info="tinfoil-search-index-v1", L=32)

const (
	searchIndexKeyInfo = "tinfoil-search-index-v1"

	searchIndexKeySize = 32
)

// ErrSearchKeyMaterial 表示用于派生 search-index subkey 的 CEK 长度不是恰好 32 字节。
var ErrSearchKeyMaterial = errors.New("search-key: CEK must be exactly 32 bytes")

// DeriveSearchIndexKey derives the search-index subkey from the
// user's CEK. Callers MUST zero the returned slice when done.
// DeriveSearchIndexKey 从用户 CEK 派生 search-index subkey。调用方在完成后
// 必须将返回的切片清零。
func DeriveSearchIndexKey(cek []byte) ([]byte, error) {
	if len(cek) != KeySize {
		return nil, ErrSearchKeyMaterial
	}
	out := make([]byte, searchIndexKeySize)
	r := hkdf.New(sha256.New, cek, nil, []byte(searchIndexKeyInfo))
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}
