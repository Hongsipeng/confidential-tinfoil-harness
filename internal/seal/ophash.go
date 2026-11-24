package seal

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"

	"golang.org/x/crypto/hkdf"
)

// Operation-hash construction.
// Operation-hash（操作哈希）的构造方式。
//
// X-Operation-Hash is a keyed MAC, not a plaintext digest. The
// controlplane stores the value in sync_idempotency_keys to detect
// retries and replays, and it sits inside the threat boundary: a plain
// SHA-256(plaintext) would be brute-forceable against low-entropy
// payloads (profile flags, short prompts, common templates). We
// therefore key the MAC on a subkey derived from the user's CEK that
// the controlplane never sees.
// X-Operation-Hash 是一个带密钥的 MAC，而不是明文摘要。controlplane 将该值
// 存储在 sync_idempotency_keys 中以检测重试和重放，且它位于威胁边界之内：
// 若直接使用 SHA-256(plaintext)，攻击者可以对低熵 payload（profile flags、
// 短 prompt、常见模板）进行暴力破解。因此我们使用一个从用户 CEK 派生、
// controlplane 永远看不到的 subkey 来为 MAC 加密钥。
//
//	K_op = HKDF-SHA-256(IKM=CEK, salt="", info="tinfoil-op-hash-v1", L=32)
//	X-Operation-Hash = hex(HMAC-SHA-256(K_op, canonical))
//
// The canonical input is a length-prefixed concatenation of the
// auth-relevant fields. Lengths are big-endian uint32. See
// AppendCanonical below for the exact byte layout.
// 规范输入是与认证相关字段的长度前缀拼接，长度字段为大端序 uint32。具体字节
// 布局参见下方 AppendCanonical。

const (
	// opHashInfo is the HKDF info string used to derive the operation
	// hash subkey from the user's CEK. Version-tagged so future
	// protocol changes can be introduced without an ambiguous overlap.
	// opHashInfo 是用于从用户 CEK 派生 operation hash subkey 的 HKDF info
	// 字符串。它带有版本标记，以便未来引入协议变更时不会产生含糊的重叠。
	opHashInfo = "tinfoil-op-hash-v1"

	// opHashKeySize is the length of the derived subkey, in bytes.
	// opHashKeySize 是派生出的 subkey 的字节长度。
	opHashKeySize = 32
)

// ErrOpHashKeyMaterial 表示用于派生 operation hash subkey 的 CEK 长度不是恰好 32 字节。
var ErrOpHashKeyMaterial = errors.New("op-hash: CEK must be exactly 32 bytes")

// DeriveOpHashKey derives the operation-hash subkey from the user's
// CEK. Callers MUST zero the returned slice when done.
// DeriveOpHashKey 从用户 CEK 派生 operation-hash subkey。调用方在完成后必须
// 将返回的切片清零。
func DeriveOpHashKey(cek []byte) ([]byte, error) {
	if len(cek) != KeySize {
		return nil, ErrOpHashKeyMaterial
	}
	out := make([]byte, opHashKeySize)
	r := hkdf.New(sha256.New, cek, nil, []byte(opHashInfo))
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

// CanonicalInput describes the fields that go into the canonical tuple
// the MAC is computed over:
//
//	Method:         "PUT" | "POST" | "DELETE" (uppercase ASCII)
//	Path:           request path incl. query, exactly as sent on the wire
//	KeyIDHex:       32-char lowercase hex key_id
//	IfMatch:        decimal ASCII etag, or the literal header value
//	                ("0" for create, "*" or a hex key for register-key)
//	IdempotencyKey: client-chosen UUID/ULID, ASCII
//	Body:           stable logical body bytes (nil-safe for DELETEs;
//	                plaintext for blob writes so randomized envelopes
//	                do not break idempotent retries)
//	AAD:            canonical AAD bytes used during seal; nil for
//	                non-blob operations (key register / rewrap-by-meta /
//	                share / attachment)
//	Envelope:       optional v2 envelope bytes for callers that need
//	                to bind a specific ciphertext generation
//	ProfileSyncProtocol:
//	                optional profile merge protocol version forwarded
//	                to the controlplane
//
// Blob writes pass plaintext in Body and canonical AAD in AAD. The
// plaintext is protected by a keyed MAC under K_op before it ever
// reaches the controlplane, so low-entropy payloads remain outside
// the controlplane's brute-force boundary while retries stay stable
// across AES-GCM nonce changes.
// CanonicalInput 描述参与 MAC 计算的规范元组字段：Method（"PUT" | "POST" |
// "DELETE"）、Path（含 query 的请求路径）、KeyIDHex（32 位小写十六进制
// key_id）、IfMatch（十进制 ASCII etag 或字面 header 值）、IdempotencyKey
// （client 选择的 UUID/ULID）、Body（稳定的逻辑 body 字节）、AAD（seal 时使用
// 的规范 AAD 字节）、Envelope（可选的 v2 envelope 字节）以及
// ProfileSyncProtocol（转发给 controlplane 的可选 profile merge 协议版本）。
// Blob 写入在 Body 中传明文、在 AAD 中传规范 AAD；明文在到达 controlplane 之前
// 即由 K_op 下的带密钥 MAC 保护，因此低熵 payload 始终位于 controlplane 的暴力
// 破解边界之外，同时重试在 AES-GCM nonce 变化时仍保持稳定。
type CanonicalInput struct {
	Method              string
	Path                string
	KeyIDHex            string
	IfMatch             string
	IdempotencyKey      string
	Body                []byte
	AAD                 []byte
	Envelope            []byte
	ProfileSyncProtocol string
}

// AppendCanonical writes the canonical encoding of the tuple to dst and
// returns the extended slice. Each field is preceded by its length as a
// big-endian uint32. This is the exact byte string the MAC is computed
// over.
//
// AAD and Envelope are only appended when at least one is present or
// ProfileSyncProtocol is set.
// Body-only operations (RegisterKey, AddBundle, rewrap, etc.) keep
// the historical encoding so cached sync_idempotency_keys entries
// continue to verify after this rollout. Blob mutations carry AAD so
// they unambiguously land on the extended encoding.
// AppendCanonical 将元组的规范编码写入 dst 并返回扩展后的切片。每个字段之前
// 都带有其长度（大端序 uint32）。这正是 MAC 所计算的精确字节串。
//
// 仅当 AAD、Envelope 至少一个存在，或设置了 ProfileSyncProtocol 时，才会追加
// AAD 和 Envelope。仅含 Body 的操作（RegisterKey、AddBundle、rewrap 等）保留
// 历史编码，使缓存中的 sync_idempotency_keys 条目在本次发布后仍能继续校验。
// Blob 变更会携带 AAD，从而明确落在扩展编码上。
func AppendCanonical(dst []byte, in CanonicalInput) []byte {
	dst = appendLenPrefixed(dst, []byte(in.Method))
	dst = appendLenPrefixed(dst, []byte(in.Path))
	dst = appendLenPrefixed(dst, []byte(in.KeyIDHex))
	dst = appendLenPrefixed(dst, []byte(in.IfMatch))
	dst = appendLenPrefixed(dst, []byte(in.IdempotencyKey))
	dst = appendLenPrefixed(dst, in.Body)
	if in.AAD != nil || in.Envelope != nil || in.ProfileSyncProtocol != "" {
		dst = appendLenPrefixed(dst, in.AAD)
		dst = appendLenPrefixed(dst, in.Envelope)
	}
	if in.ProfileSyncProtocol != "" {
		dst = appendLenPrefixed(dst, []byte(in.ProfileSyncProtocol))
	}
	return dst
}

func appendLenPrefixed(dst, b []byte) []byte {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	dst = append(dst, lenBuf[:]...)
	dst = append(dst, b...)
	return dst
}

// ComputeOperationHash returns the hex-encoded HMAC for the given
// canonical input, using a subkey already derived by DeriveOpHashKey.
// ComputeOperationHash 使用由 DeriveOpHashKey 派生的 subkey，返回给定规范
// 输入的十六进制编码 HMAC。
func ComputeOperationHash(opKey []byte, in CanonicalInput) string {
	mac := hmac.New(sha256.New, opKey)
	mac.Write(AppendCanonical(nil, in))
	sum := mac.Sum(nil)
	return hex.EncodeToString(sum)
}

// VerifyOperationHash returns true if `provided` is the correct
// hex-encoded HMAC for `in` under the subkey derived from `cek`.
// Comparison is constant-time.
// VerifyOperationHash 在 `provided` 是 `in` 在由 `cek` 派生的 subkey 下正确的
// 十六进制编码 HMAC 时返回 true。比较在常数时间内完成。
func VerifyOperationHash(cek []byte, provided string, in CanonicalInput) (bool, error) {
	opKey, err := DeriveOpHashKey(cek)
	if err != nil {
		return false, err
	}
	defer Zero(opKey)

	want, err := hex.DecodeString(provided)
	if err != nil || len(want) != sha256.Size {
		return false, nil
	}
	mac := hmac.New(sha256.New, opKey)
	mac.Write(AppendCanonical(nil, in))
	return hmac.Equal(mac.Sum(nil), want), nil
}
