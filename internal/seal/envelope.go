package seal

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Version 标识 envelope 的格式版本。
type Version int

const (
	// 以下常量标识 envelope 的格式版本。
	VersionUnknown Version = 0
	VersionV0      Version = -1 // legacy JSON {iv, data}（旧版 JSON 格式 {iv, data}）
	VersionV1      Version = -2 // legacy binary gzip(AES-GCM(...))（旧版二进制格式 gzip(AES-GCM(...))）
	VersionV2      Version = 2
)

// V2 是 v2 envelope 的 JSON 表示。
type V2 struct {
	V   int    `json:"v"`
	Alg string `json:"alg"`
	// KID identifies the CEK that wrapped the data key (WDEK), not the
	// key the payload is sealed under. It selects which key unwraps the
	// data key on read and is the single field a rewrap rewrites.
	// KID 标识包装了 data key（WDEK）的 CEK，而不是 payload 被密封所用的 key。
	// 它决定读取时用哪个 key 解包 data key，也是 rewrap 唯一会重写的字段。
	KID string `json:"kid"`
	// WDEK is the per-message data key sealed under the CEK
	// (AES-256-GCM ciphertext+tag, base64). WIV is its 12-byte nonce, hex.
	// WDEK 是在 CEK 下密封的每条消息 data key（AES-256-GCM 的密文+标签，
	// base64 编码）。WIV 是其 12 字节的 nonce，十六进制编码。
	WDEK string `json:"wdek"`
	WIV  string `json:"wiv"`
	// IV / CT are the payload nonce (hex) and the gzipped plaintext
	// sealed under the data key (AES-256-GCM ciphertext+tag, base64).
	// IV 与 CT 分别是 payload 的 nonce（十六进制）和在 data key 下密封的 gzip
	// 明文（AES-256-GCM 的密文+标签，base64 编码）。
	IV string `json:"iv"`
	CT string `json:"ct"`
}

type legacyV0 struct {
	IV   string `json:"iv"`
	Data string `json:"data"`
}

const (
	// MaxPlaintextBytes is the upper bound on decompressed plaintext.
	// Applies to v1 legacy blobs (the original cap) and v2 envelopes
	// now that v2 carries gzipped plaintext as well. Exported so the
	// HTTP layer can report the limit alongside a rejection.
	// MaxPlaintextBytes 是解压后明文的上限。既然 v2 也携带 gzip 明文，该上限
	// 同时适用于 v1 旧版 blob（最初的上限）与 v2 envelope。导出它是为了让
	// HTTP 层能够在拒绝请求的同时报告该限制。
	MaxPlaintextBytes = 32 * 1024 * 1024

	maxDecompressedBytes = MaxPlaintextBytes
)

var (
	// 以下错误变量描述 envelope 解析/解密各阶段的失败。
	ErrUnknownFormat   = errors.New("envelope: unknown format")
	ErrV2Malformed     = errors.New("envelope: malformed v2")
	ErrV0Malformed     = errors.New("envelope: malformed legacy v0")
	ErrV1Malformed     = errors.New("envelope: malformed legacy v1")
	ErrNoMatchingKey   = errors.New("envelope: no key matches kid")
	ErrLegacyDecrypt   = errors.New("envelope: no provided key decrypted legacy blob")
	ErrUnsupportedAlg  = errors.New("envelope: unsupported algorithm")
	ErrInvalidEnvelope = errors.New("envelope: invalid envelope")
	// ErrPlaintextTooLarge wraps ErrInvalidEnvelope so existing
	// errors.Is(err, ErrInvalidEnvelope) checks keep matching while
	// callers that care can distinguish an oversize input from a
	// malformed one and surface a client-actionable status.
	// ErrPlaintextTooLarge 包装 ErrInvalidEnvelope，使现有的
	// errors.Is(err, ErrInvalidEnvelope) 检查依然匹配，同时让需要区分的调用方
	// 能够把过大的输入与格式错误区分开，并返回一个可供 client 处理的错误状态。
	ErrPlaintextTooLarge = fmt.Errorf("%w: plaintext exceeds %d bytes", ErrInvalidEnvelope, MaxPlaintextBytes)
)

// Detect classifies a ciphertext blob by attempting strict parses
// against each known envelope shape in turn. There is intentionally
// no byte-prefix heuristic: any prefix-based discriminator is
// ambiguous because v1 ciphertext bytes are uniformly random and can
// imitate the opening of a JSON object. Instead we ask the harder
// question "does this parse as a fully-formed v2 / v0 envelope?" and
// fall back to v1 only when both strict parses fail.
//
//  1. Try parsing as a v2 envelope: a JSON object with `"v":2`. A
//     real v2 blob always succeeds here; a v1 binary blob has
//     effectively zero probability of being a complete valid JSON
//     document with `"v":2`. The fields the envelope needs are
//     checked when it is opened.
//  2. Try parsing as a v0 envelope. v0 requires `iv` and `data`
//     fields and explicitly forbids a `v` field (otherwise we'd
//     accept v2 as v0 too). Same probabilistic argument as v2.
//  3. Otherwise, if the blob is at least IV+TAG bytes long, treat
//     it as v1. v1 has no version tag on the wire — that's why we
//     cannot do better — but the strict-parse cascade above means
//     we never accidentally route a real v0/v2 blob here.
//
// A truncated or otherwise-malformed v0/v2 envelope fails both
// strict parses and falls through to v1; DecryptLegacy then fails
// to decrypt the malformed bytes and the caller sees a decrypt
// error. That is an unavoidable consequence of v1's lack of a
// wire-level marker; the alternative (returning Unknown on parse
// failure) silently loses real v1 blobs whose random nonce happens
// to start with `{`. We prefer "always recover real v1 data" over
// "produce a slightly more specific error on corruption".
// Detect 通过依次尝试对每种已知 envelope 形状进行严格解析来分类密文 blob。
// 这里有意不使用字节前缀启发式：任何基于前缀的判别器都是含糊的，因为 v1 密文
// 字节均匀随机，可能模仿 JSON 对象的开头。相反，我们问更难的问题——“这是否
// 能解析为一个完整的 v2/v0 envelope？”，只有在两次严格解析都失败时才回退到
// v1。流程为：1) 尝试按 v2 envelope 解析（带 "v":2 的 JSON 对象）；2) 尝试按
// v0 envelope 解析（要求 iv 和 data 字段且禁止 v 字段）；3) 否则若 blob 长度
// 至少为 IV+TAG 字节，则按 v1 处理。被截断或损坏的 v0/v2 envelope 会回退到
// v1，随后解密失败并报错——这是 v1 缺少线上版本标记的必然代价；我们宁愿“总是
// 能恢复真实的 v1 数据”，也不愿“在损坏时给出略具体的错误”。
func Detect(blob []byte) Version {
	if len(blob) == 0 {
		return VersionUnknown
	}
	if looksLikeV2(blob) {
		return VersionV2
	}
	if looksLikeV0(blob) {
		return VersionV0
	}
	if len(blob) >= NonceSize+TagSize {
		return VersionV1
	}
	return VersionUnknown
}

// looksLikeV2 returns true when blob fully parses as JSON and carries
// the v2 version tag. Field validity (non-empty kid/iv/ct, supported
// alg) is enforced by DecryptV2; Detect's only job is shape
// classification. A partial / truncated JSON document fails the
// strict Unmarshal and is therefore not v2.
//
// The probe struct only captures the `v` field; decoding the full
// V2 struct here would copy the (potentially megabyte-sized) `ct`
// base64 string into a fresh allocation on every Detect call, which
// is wasted work because Detect never inspects it.
// looksLikeV2 在 blob 能完整解析为 JSON 且带有 v2 版本标记时返回 true。字段
// 有效性（kid/iv/ct 非空、alg 受支持）由 DecryptV2 强制校验；Detect 只负责
// 形状分类。不完整或被截断的 JSON 文档无法通过严格 Unmarshal，因此不是 v2。
//
// 探测结构体只捕获 v 字段；若在此解码完整的 V2 结构体，会在每次 Detect 调用时
// 把（可能达兆字节的）ct base64 字符串复制到一块新分配中，而 Detect 从不检查它，
// 这纯属浪费。
func looksLikeV2(blob []byte) bool {
	var probe struct {
		V int `json:"v"`
	}
	if err := json.Unmarshal(blob, &probe); err != nil {
		return false
	}
	return probe.V == int(VersionV2)
}

// looksLikeV0 returns true when blob fully parses as JSON in the v0
// legacy shape: `iv` and `data` fields are both present, and the
// `v` field is absent (so v2 envelopes never alias to v0). Empty
// strings are still considered v0-shaped; per-field validity is
// enforced by decryptV0.
// looksLikeV0 在 blob 能完整解析为 v0 旧版 JSON 形状时返回 true：iv 与 data
// 字段都存在且 v 字段不存在（因此 v2 envelope 绝不会被误判为 v0）。空字符串仍
// 视为 v0 形状；各字段的有效性由 decryptV0 强制校验。
func looksLikeV0(blob []byte) bool {
	var probe struct {
		V    *int    `json:"v"`
		IV   *string `json:"iv"`
		Data *string `json:"data"`
	}
	if err := json.Unmarshal(blob, &probe); err != nil {
		return false
	}
	if probe.V != nil {
		return false
	}
	return probe.IV != nil && probe.Data != nil
}

// Key 是解密时提供的一个候选 key 及其 key id。
type Key struct {
	Bytes    []byte
	KeyIDHex string // lowercase hex（小写十六进制）
}

// DecryptResult 是一次解密操作的结果。
type DecryptResult struct {
	Plaintext   []byte
	KeyIDHex    string
	Version     Version
	NeedsRewrap bool
}

// Encrypt produces a v2 envelope around plaintext. The payload is sealed
// under a fresh random per-message data key (DEK); that DEK is then wrapped
// under the caller's key (the user's CEK, identified by aad.KeyIDHex). The
// caller is responsible for zeroing plaintext after the returned envelope is
// transmitted; the envelope JSON itself contains only ciphertext.
//
// Pipeline: gzip(plaintext) → AES-GCM-GCMSeal(DEK, …) for the payload, and
// AES-GCM-GCMSeal(CEK, DEK, …) for the wrapped key. Splitting the content key
// from the CEK lets rewrap reseal only the small data key and leave the
// (potentially large) payload ciphertext untouched, which is also why the
// payload AAD is independent of the CEK key id. Compression happens
// before encryption because ciphertext will not compress at any downstream
// layer.
// Encrypt 在 plaintext 周围生成一个 v2 envelope。payload 在一个新的随机每条
// 消息 data key（DEK）下密封；该 DEK 再在调用方的 key（用户的 CEK，由
// aad.KeyIDHex 标识）下包装。调用方负责在返回的 envelope 传输完成后清零
// plaintext；envelope JSON 本身只包含密文。
//
// 流水线为：对 payload 做 gzip(plaintext) → AES-GCM-GCMSeal(DEK, …)，对已
// 包装 key 做 AES-GCM-GCMSeal(CEK, DEK, …)。将 content key 与 CEK 分离，
// 使 rewrap 只需重新密封很小的 data key，而让（可能很大的）payload 密文保持
// 不变——这也是 payload AAD 独立于 CEK key id 的原因。压缩发生在加密之前，
// 因为密文在任何下游层都不会再被压缩。
func Encrypt(key []byte, plaintext []byte, aad AAD) ([]byte, error) {
	if len(key) != KeySize {
		return nil, ErrKeySize
	}
	payloadAAD, err := CanonicalPayloadAAD(aad)
	if err != nil {
		return nil, err
	}
	wrapAAD, err := CanonicalDEKWrapAAD(aad)
	if err != nil {
		return nil, err
	}
	// Reject plaintexts that won't fit under the decompress cap.
	// Without this check a write can succeed at seal time and then
	// fail every subsequent decrypt — turning oversized inputs into
	// silent data loss instead of an immediate, actionable error.
	// 拒绝那些在解压上限下放不下的明文。没有这个检查，写入可能在 seal 时成功，
	// 却在之后的每次解密中失败——把过大的输入变成静默的数据丢失，而不是立即、
	// 可处理的错误。
	if len(plaintext) > maxDecompressedBytes {
		return nil, ErrPlaintextTooLarge
	}
	compressed, err := gzipBytes(plaintext)
	if err != nil {
		return nil, err
	}
	// The compressed buffer holds a transformed copy of the
	// plaintext until ciphertext is produced; zero it on the way
	// out so we don't leave plaintext-derived material lying in
	// enclave memory longer than necessary.
	// 压缩缓冲区在密文生成之前一直持有明文的一份变换副本；在返回时将其清零，
	// 以免把由明文衍生的材料不必要地长时间留在 enclave 内存中。
	defer Zero(compressed)
	dek, err := RandomKey()
	if err != nil {
		return nil, err
	}
	defer Zero(dek)
	payloadIV, payloadCT, err := GCMSeal(dek, compressed, payloadAAD)
	if err != nil {
		return nil, err
	}
	wrapIV, wrappedDEK, err := GCMSeal(key, dek, wrapAAD)
	if err != nil {
		return nil, err
	}
	env := V2{
		V:    int(VersionV2),
		Alg:  AlgAESGCM,
		KID:  aad.KeyIDHex,
		WDEK: base64.StdEncoding.EncodeToString(wrappedDEK),
		WIV:  hex.EncodeToString(wrapIV),
		IV:   hex.EncodeToString(payloadIV),
		CT:   base64.StdEncoding.EncodeToString(payloadCT),
	}
	return json.Marshal(env)
}

// DecryptV2 parses and decrypts a v2 envelope. The envelope's `kid` selects
// which of the supplied keys unwraps the per-message data key; that data key
// then opens the payload. Only one key is used, so the operation is
// constant-cost. `aad` supplies the scope/id/clerk_user_id the two AAD layers
// are rebuilt from; the kid for the wrap AAD comes from the envelope itself.
// DecryptV2 解析并解密一个 v2 envelope。envelope 的 kid 选择所提供的哪个 key
// 来解包每条消息 data key；该 data key 再打开 payload。只用到一个 key，因此
// 操作开销恒定。aad 提供两个 AAD 层重建所需的 scope/id/clerk_user_id；wrap
// AAD 的 kid 来自 envelope 本身。
func DecryptV2(blob []byte, keys []Key, aad AAD) (DecryptResult, error) {
	var env V2
	if err := json.Unmarshal(blob, &env); err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV2Malformed, err)
	}
	if env.V != int(VersionV2) {
		return DecryptResult{}, ErrV2Malformed
	}
	if env.Alg != AlgAESGCM {
		return DecryptResult{}, ErrUnsupportedAlg
	}
	if len(env.KID) != 32 || !isLowerHex(env.KID) {
		return DecryptResult{}, ErrV2Malformed
	}
	if len(env.IV) != NonceSize*2 || len(env.WIV) != NonceSize*2 {
		return DecryptResult{}, ErrV2Malformed
	}
	iv, err := hex.DecodeString(env.IV)
	if err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV2Malformed, err)
	}
	wrapIV, err := hex.DecodeString(env.WIV)
	if err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV2Malformed, err)
	}
	// Both `ct` (payload) and `wdek` (wrapped data key) must be
	// exact-canonical RFC 4648 standard base64: only the 64-char
	// alphabet plus `=` padding at the tail, and no non-zero "spare"
	// bits in the final group. The canonical form is necessary because
	// two different aspects of a permissive decoder both produce
	// equal-decoded-bytes-different-wire-bytes ambiguity:
	//
	//   1. Spare bits in the last data char of a 1- or 2-pad
	//      group (`XXXY=` and `XXXZ=` decode identically when Y
	//      and Z differ only in their low 2 or 4 bits).
	//   2. Embedded whitespace — `\n`, `\r` — which `Strict()`
	//      still silently skips, so a tampered JSON envelope
	//      could carry a base64 string with `\n` injected anywhere
	//      and decrypt to the same bytes.
	//
	// `Strict()` catches (1) but not (2); the explicit alphabet
	// pre-check below catches (2) without an extra allocation.
	// `ct`（payload）和 `wdek`（已包装 data key）都必须是精确规范的 RFC 4648
	// 标准 base64：只有 64 个字符的字母表加尾部的 = padding，且最后一组没有非零
	// “空余”位。需要规范形式，是因为宽容解码器的两个不同方面都会产生“解码字节
	// 相同、线上字节不同”的含糊性：1) 1 或 2 个 padding 组的最后一个数据字符中
	// 的空余位；2) 内嵌空白。Strict() 能捕获 (1) 但不能捕获 (2)；下面的显式
	// 字母表预检查无需额外分配即可捕获 (2)。
	if !isCanonicalStdBase64(env.CT) || !isCanonicalStdBase64(env.WDEK) {
		return DecryptResult{}, ErrV2Malformed
	}
	ct, err := base64.StdEncoding.Strict().DecodeString(env.CT)
	if err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV2Malformed, err)
	}
	wrappedDEK, err := base64.StdEncoding.Strict().DecodeString(env.WDEK)
	if err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV2Malformed, err)
	}
	var match *Key
	for i := range keys {
		if keys[i].KeyIDHex == env.KID {
			match = &keys[i]
			break
		}
	}
	if match == nil {
		return DecryptResult{KeyIDHex: env.KID}, ErrNoMatchingKey
	}
	wrapAAD, err := CanonicalDEKWrapAAD(AAD{
		KeyIDHex:    env.KID,
		Scope:       aad.Scope,
		ID:          aad.ID,
		ClerkUserID: aad.ClerkUserID,
	})
	if err != nil {
		return DecryptResult{}, err
	}
	dek, err := GCMOpen(match.Bytes, wrapIV, wrappedDEK, wrapAAD)
	if err != nil {
		return DecryptResult{}, err
	}
	defer Zero(dek)
	// The authenticated unwrap proves the DEK bytes are exactly what was
	// sealed, so a wrong length signals a broken seal-side invariant rather
	// than tampering. Fail closed with a specific error here instead of
	// letting the payload open below surface a generic key-size error.
	// 已认证的解包证明 DEK 字节正是被密封的内容，因此长度错误意味着 seal 侧的
	// 不变式被破坏，而非篡改。这里以特定错误快速失败（fail closed），而不是让
	// 下方的 payload 打开操作抛出一个泛泛的 key 长度错误。
	if len(dek) != KeySize {
		return DecryptResult{}, ErrV2Malformed
	}
	payloadAAD, err := CanonicalPayloadAAD(aad)
	if err != nil {
		return DecryptResult{}, err
	}
	compressed, err := GCMOpen(dek, iv, ct, payloadAAD)
	if err != nil {
		return DecryptResult{}, err
	}
	defer Zero(compressed)
	pt, err := gunzip(compressed)
	if err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV2Malformed, err)
	}
	return DecryptResult{
		Plaintext:   pt,
		KeyIDHex:    env.KID,
		Version:     VersionV2,
		NeedsRewrap: false,
	}, nil
}

// DecryptLegacy attempts each provided key against a v0 or v1 blob. Legacy
// blobs were written without AAD; the caller passes nil to keep semantics
// identical to the original clients. NeedsRewrap is always true on success.
// DecryptLegacy 依次尝试每个提供的 key 来解密 v0 或 v1 blob。旧版 blob 写入
// 时不带 AAD；调用方传入 nil 以保持与原始 client 相同的语义。成功时
// NeedsRewrap 恒为 true。
func DecryptLegacy(blob []byte, keys []Key) (DecryptResult, error) {
	switch Detect(blob) {
	case VersionV0:
		return decryptV0(blob, keys)
	case VersionV1:
		return decryptV1(blob, keys)
	}
	return DecryptResult{}, ErrUnknownFormat
}

func decryptV0(blob []byte, keys []Key) (DecryptResult, error) {
	var raw legacyV0
	if err := json.Unmarshal(blob, &raw); err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV0Malformed, err)
	}
	if raw.IV == "" || raw.Data == "" {
		return DecryptResult{}, ErrV0Malformed
	}
	iv, err := decodeBase64OrHex(raw.IV, NonceSize)
	if err != nil {
		return DecryptResult{}, fmt.Errorf("%w: %v", ErrV0Malformed, err)
	}
	ct, err := base64.StdEncoding.DecodeString(raw.Data)
	if err != nil {
		// Some legacy clients used base64url without padding.
		// 一些旧版 client 使用了不带 padding 的 base64url。
		ct, err = base64.RawURLEncoding.DecodeString(raw.Data)
		if err != nil {
			return DecryptResult{}, fmt.Errorf("%w: %v", ErrV0Malformed, err)
		}
	}
	for _, k := range keys {
		if len(k.Bytes) != KeySize {
			continue
		}
		pt, err := GCMOpen(k.Bytes, iv, ct, nil)
		if err == nil {
			return DecryptResult{
				Plaintext:   pt,
				KeyIDHex:    k.KeyIDHex,
				Version:     VersionV0,
				NeedsRewrap: true,
			}, nil
		}
	}
	return DecryptResult{}, ErrLegacyDecrypt
}

// decryptV1 reverses the webapp's `compressAndEncrypt` pipeline:
//
//	IV(12) || AES-GCM-ciphertext(gzip(JSON))
//
// We split off the IV, AES-GCM-Open the rest, then gunzip the plaintext.
// Legacy v1 blobs were written without AAD, so callers pass nil here.
// decryptV1 反转 webapp 的 compressAndEncrypt 流水线：IV(12) || AES-GCM-密文
// (gzip(JSON))。我们先拆出 IV，对剩余部分做 AES-GCM-Open，再 gunzip 明文。
// 旧版 v1 blob 写入时不带 AAD，因此调用方这里传入 nil。
func decryptV1(blob []byte, keys []Key) (DecryptResult, error) {
	if len(blob) < NonceSize+TagSize {
		return DecryptResult{}, ErrV1Malformed
	}
	iv := blob[:NonceSize]
	ct := blob[NonceSize:]
	for _, k := range keys {
		if len(k.Bytes) != KeySize {
			continue
		}
		compressed, err := GCMOpen(k.Bytes, iv, ct, nil)
		if err != nil {
			continue
		}
		defer Zero(compressed)
		pt, err := gunzip(compressed)
		if err != nil {
			return DecryptResult{}, fmt.Errorf("%w: %v", ErrV1Malformed, err)
		}
		return DecryptResult{
			Plaintext:   pt,
			KeyIDHex:    k.KeyIDHex,
			Version:     VersionV1,
			NeedsRewrap: true,
		}, nil
	}
	return DecryptResult{}, ErrLegacyDecrypt
}

func gzipBytes(plaintext []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(plaintext); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzip(compressed []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	limited := io.LimitReader(gz, maxDecompressedBytes+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(out) > maxDecompressedBytes {
		return nil, errors.New("envelope: decompressed plaintext exceeds limit")
	}
	return out, nil
}

// isCanonicalStdBase64 returns true when s consists only of
// standard-base64 alphabet characters (A-Z, a-z, 0-9, +, /) with
// zero, one, or two `=` characters at the tail, and the total
// length is a non-zero multiple of 4.
//
// This is an alphabet-and-shape check, not a value check; combined
// with base64.StdEncoding.Strict().DecodeString it forbids both
// non-canonical spare-bit endings and embedded whitespace
// (\n, \r, ' ', \t) that the stdlib decoder otherwise tolerates.
// Both forms would let two distinct wire-level `ct` strings
// produce the same decoded ciphertext, which is a tampering
// ambiguity we want to surface as ErrV2Malformed instead of
// silently accepting either spelling.
// isCanonicalStdBase64 在 s 仅由标准 base64 字母表字符（A-Z、a-z、0-9、+、/）
// 构成、尾部有零个、一个或两个 = 字符，且总长度是 4 的非零倍数时返回 true。
//
// 这是字母表与形状检查，不是值检查；与 base64.StdEncoding.Strict().DecodeString
// 结合使用，可同时禁止非规范的空余位结尾与内嵌空白（\n、\r、' '、\t）。这两
// 种形式都会让两个不同的线上 ct 字符串解出相同的密文，这是一种我们想以
// ErrV2Malformed 暴露而不是静默接受任一写法的篡改含糊性。
func isCanonicalStdBase64(s string) bool {
	if len(s) == 0 || len(s)%4 != 0 {
		return false
	}
	end := len(s)
	pad := 0
	if s[end-1] == '=' {
		pad++
		if s[end-2] == '=' {
			pad++
		}
	}
	for i := 0; i < end-pad; i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '+', c == '/':
		default:
			return false
		}
	}
	return true
}

// decodeBase64OrHex accepts both encodings because two production clients
// historically wrote v0 IVs differently. The expected decoded length is
// enforced to avoid silent acceptance of garbage.
// decodeBase64OrHex 同时接受两种编码，因为历史上两个生产 client 写入 v0 IV 的
// 方式不同。强制校验期望的解码长度，以避免静默接受垃圾数据。
func decodeBase64OrHex(s string, want int) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) == want {
		return b, nil
	}
	if b, err := base64.RawStdEncoding.DecodeString(s); err == nil && len(b) == want {
		return b, nil
	}
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil && len(b) == want {
		return b, nil
	}
	if b, err := hex.DecodeString(s); err == nil && len(b) == want {
		return b, nil
	}
	return nil, errors.New("envelope: iv has wrong length or encoding")
}
