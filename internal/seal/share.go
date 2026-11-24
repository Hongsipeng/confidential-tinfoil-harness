package seal

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"errors"
	"io"
)

// A shared chat is sealed under a key its link carries, with no account
// involved: gzip, then AES-GCM under a fresh key with no AAD, the nonce
// before the ciphertext.
// shared chat 在其链接所携带的 key 下密封，不涉及任何账户：先 gzip，然后在一个
// 无 AAD 的新 key 下做 AES-GCM，nonce 位于密文之前。

// MaxSharePlaintextBytes bounds what a share holds: before compression
// when sealed, after decompression when opened. MaxShareBytes bounds the
// sealed share, which is what is stored and fetched; compression can
// leave a share larger than its plaintext.
// MaxSharePlaintextBytes 限制 share 所承载的内容：密封时为压缩前的大小，
// 打开时为解压后的大小。MaxShareBytes 限制密封后的 share（即被存储和获取的
// 内容）；压缩可能使 share 比其明文更大。
const (
	MaxSharePlaintextBytes = 32 << 20
	MaxShareBytes          = MaxSharePlaintextBytes + 1<<20
)

var (
	// ErrShareTooLarge 表示 share 超过了大小限制。
	ErrShareTooLarge = errors.New("seal: the share exceeds the size limit")
	// ErrShareInvalid is a share the key does not open, whatever the
	// reason: to its reader a wrong key and a damaged share are the same.
	// ErrShareInvalid 表示该 share 无法用 key 打开，无论原因如何：对读取者而言，
	// 错误的 key 与损坏的 share 并无区别。
	ErrShareInvalid = errors.New("seal: the share cannot be opened")
)

// SealShare seals plaintext under a fresh key, returned as hex for the
// link's fragment.
// SealShare 在一个新的 key 下密封明文，并返回十六进制形式的 key 作为链接的
// fragment。
func SealShare(plaintext []byte) ([]byte, string, error) {
	if len(plaintext) > MaxSharePlaintextBytes {
		return nil, "", ErrShareTooLarge
	}
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(plaintext); err != nil {
		return nil, "", err
	}
	if err := zw.Close(); err != nil {
		return nil, "", err
	}
	defer Zero(compressed.Bytes())
	key, err := RandomKey()
	if err != nil {
		return nil, "", err
	}
	defer Zero(key)
	nonce, ciphertext, err := GCMSeal(key, compressed.Bytes(), nil)
	if err != nil {
		return nil, "", err
	}
	sealed := append(nonce, ciphertext...)
	if len(sealed) > MaxShareBytes {
		return nil, "", ErrShareTooLarge
	}
	return sealed, hex.EncodeToString(key), nil
}

// OpenShare is the plaintext of a share sealed under key.
// OpenShare 返回在给定 key 下密封的 share 的明文。
func OpenShare(key string, ciphertext []byte) ([]byte, error) {
	if len(key) != KeySize*2 || len(ciphertext) < NonceSize+TagSize || len(ciphertext) > MaxShareBytes {
		return nil, ErrShareInvalid
	}
	raw, err := hex.DecodeString(key)
	if err != nil {
		return nil, ErrShareInvalid
	}
	defer Zero(raw)
	compressed, err := GCMOpen(raw, ciphertext[:NonceSize], ciphertext[NonceSize:], nil)
	if err != nil {
		return nil, ErrShareInvalid
	}
	defer Zero(compressed)
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, ErrShareInvalid
	}
	defer zr.Close()
	plaintext, err := io.ReadAll(io.LimitReader(zr, MaxSharePlaintextBytes+1))
	if err != nil || len(plaintext) > MaxSharePlaintextBytes {
		Zero(plaintext)
		return nil, ErrShareInvalid
	}
	return plaintext, nil
}
