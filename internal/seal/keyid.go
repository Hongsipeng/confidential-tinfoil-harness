package seal

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"golang.org/x/crypto/hkdf"
)

const (
	// 以下常量定义 key 与 key id 的字节长度，以及 HKDF 的 info 字符串。
	KeySize    = 32
	KeyIDSize  = 16
	keyIDInfo  = "tinfoil-key-id-v1"
	bundleInfo = "tinfoil-key-bundle-v2"
)

var (
	// 以下错误变量描述 key 与 key id 的长度不符合预期。
	ErrKeySize   = errors.New("key must be exactly 32 bytes")
	ErrKeyIDSize = errors.New("key_id must be exactly 16 bytes")
)

// DeriveKeyID 通过 HKDF-SHA-256 从给定 key 派生出 key id。
func DeriveKeyID(key []byte) ([KeyIDSize]byte, error) {
	var out [KeyIDSize]byte
	if len(key) != KeySize {
		return out, ErrKeySize
	}
	r := hkdf.New(sha256.New, key, nil, []byte(keyIDInfo))
	if _, err := io.ReadFull(r, out[:]); err != nil {
		return out, err
	}
	return out, nil
}

// KeyIDHex 将 key id 编码为小写十六进制字符串。
func KeyIDHex(id [KeyIDSize]byte) string {
	return hex.EncodeToString(id[:])
}

// ParseKeyIDHex 将小写十六进制字符串解析为 key id。
func ParseKeyIDHex(s string) ([KeyIDSize]byte, error) {
	var out [KeyIDSize]byte
	if len(s) != KeyIDSize*2 {
		return out, ErrKeyIDSize
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

// Zero 将字节切片清零，用于在不再需要时擦除敏感数据。
func Zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
