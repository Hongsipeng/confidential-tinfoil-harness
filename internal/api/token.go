package api

import (
	"crypto/rand"
	"encoding/hex"
)

// Token is a random 128-bit hex string.
// Token 是一个随机的 128 位十六进制字符串。
func Token() string {
	var raw [16]byte
	rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}
