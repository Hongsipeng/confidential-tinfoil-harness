package seal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
)

const (
	// 以下常量定义 AES-GCM 的 nonce 与认证标签长度。
	NonceSize = 12
	TagSize   = 16
)

// ErrCipher 表示 AES-GCM 加解密操作失败。
var ErrCipher = errors.New("aes-gcm: cipher operation failed")

// RandomKey returns a fresh cryptographically-random 32-byte key. It is
// used to mint per-message data keys (DEKs) that the envelope layer seals
// the payload under before wrapping the DEK with the user's CEK.
// RandomKey 返回一个新的密码学安全随机 32 字节 key。它用于生成每条消息的
// data key（DEK），envelope 层先用该 DEK 密封 payload，再用用户的 CEK 包装
// 该 DEK。
func RandomKey() ([]byte, error) {
	k := make([]byte, KeySize)
	if _, err := rand.Read(k); err != nil {
		return nil, err
	}
	return k, nil
}

// GCMSeal 使用 AES-256-GCM 以给定 key 加密 plaintext，并返回 nonce 与密文。
func GCMSeal(key, plaintext, aad []byte) (nonce, ciphertext []byte, err error) {
	if len(key) != KeySize {
		return nil, nil, ErrKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce = make([]byte, NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	ciphertext = aead.Seal(nil, nonce, plaintext, aad)
	return nonce, ciphertext, nil
}

// GCMOpen 使用 AES-256-GCM 以给定 key 解密 ciphertext，并返回明文。
func GCMOpen(key, nonce, ciphertext, aad []byte) ([]byte, error) {
	if len(key) != KeySize {
		return nil, ErrKeySize
	}
	if len(nonce) != NonceSize {
		return nil, ErrCipher
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return aead.Open(nil, nonce, ciphertext, aad)
}
