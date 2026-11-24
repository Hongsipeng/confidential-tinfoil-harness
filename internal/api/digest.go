package api

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashHex is the hex SHA-256 of data.
// HashHex 返回 data 的十六进制 SHA-256。
func HashHex(data []byte) string { d := sha256.Sum256(data); return hex.EncodeToString(d[:]) }
