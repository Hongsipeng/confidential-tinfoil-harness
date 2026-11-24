package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"log/slog"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// ContentKey is request-scoped. It is never included in response models.
// ContentKey 是请求作用域内的密钥。它绝不会出现在 response model 中。
type ContentKey [32]byte

// LogValue 返回脱敏后的日志值，避免泄露 content key 内容。
func (ContentKey) LogValue() slog.Value { return slog.StringValue("REDACTED") }

// DecodeContentKey 校验原始字节并将其解码为 32 字节的 ContentKey。
func DecodeContentKey(value []byte) (ContentKey, error) {
	var key ContentKey
	if len(value) == 0 {
		return key, api.Err(401, "KEY_REQUIRED", "A content key is required")
	}
	if len(value) != len(key) {
		return key, api.Err(400, "BAD_REQUEST", "key must be base64 encoding of 32 bytes")
	}
	copy(key[:], value)
	return key, nil
}

// DecodeBase64Key 先将 base64 解码，再调用 DecodeContentKey 得到 ContentKey。
func DecodeBase64Key(value []byte) (ContentKey, error) {
	b := make([]byte, base64.StdEncoding.DecodedLen(len(value)))
	defer clear(b)
	n, err := base64.StdEncoding.Strict().Decode(b, value)
	if err != nil {
		return ContentKey{}, api.Err(400, "BAD_REQUEST", "key must be base64 encoding of 32 bytes")
	}
	return DecodeContentKey(b[:n])
}

// Fingerprint 返回 content key 的 SHA-256 指纹的十六进制表示。
func (k ContentKey) Fingerprint() string { b := sha256.Sum256(k[:]); return hex.EncodeToString(b[:]) }
