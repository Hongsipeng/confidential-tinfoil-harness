package seal

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
)

const (
	// 以下常量定义 envelope 各层 AAD 的 domain、格式版本与加密算法标识。
	AADDomain        = "tinfoil-sync-envelope-v2"
	DEKWrapAADDomain = "tinfoil-dek-wrap-v2"
	BundleAADDomain  = "tinfoil-key-bundle-v2"
	EnvelopeVersion  = 2
	AlgAESGCM        = "AES-256-GCM"

	// ProfileSingletonID is the canonical AAD `id` for a user's
	// profile row. The controlplane keys profile blobs by
	// clerk_user_id alone (no `:id` path segment on the wire), but
	// the AAD layer requires an explicit id so the crypto contract
	// is uniform across scopes. Callers writing/reading profile
	// blobs MUST pass this value — the AAD builder no longer
	// substitutes it silently.
	// ProfileSingletonID 是用户 profile 行的规范 AAD `id`。controlplane 仅以
	// clerk_user_id 作为 profile blob 的 key（线上没有 `:id` 路径段），但 AAD
	// 层要求显式的 id，以便 crypto 契约在各 scope 之间保持一致。写入/读取
	// profile blob 的调用方必须传入该值——AAD 构建器不再静默替换它。
	ProfileSingletonID = "profile"
)

// Scope 是 envelope 中数据所属范围的类型。
type Scope string

const (
	// 以下常量定义各 scope 的取值。
	ScopeChat            Scope = "chat"
	ScopeProfile         Scope = "profile"
	ScopeProject         Scope = "project"
	ScopeProjectDocument Scope = "project_document"
)

// Valid 报告该 Scope 是否为已知的有效范围。
func (s Scope) Valid() bool {
	switch s {
	case ScopeChat, ScopeProfile, ScopeProject, ScopeProjectDocument:
		return true
	}
	return false
}

// AAD 是 envelope 各层 AAD 所绑定的标识信息（key id、scope、行 id 与用户 id）。
type AAD struct {
	KeyIDHex    string
	Scope       Scope
	ID          string
	ClerkUserID string
}

var (
	// ErrAADInvalid 表示 AAD 输入无效。
	ErrAADInvalid = errors.New("invalid AAD inputs")
)

// CanonicalPayloadAAD returns the AAD that binds the payload layer of a v2
// envelope — the gzipped plaintext sealed under the per-message data key.
// It deliberately omits the CEK key id so the payload ciphertext is
// independent of which CEK is current; rewrapping only needs to reseal
// the data key and can leave the (potentially large) payload untouched.
// JSON: sorted keys, no insignificant whitespace, UTF-8.
// CanonicalPayloadAAD 返回绑定 v2 envelope payload 层（在每条消息的 data key
// 下密封的 gzip 后的明文）的 AAD。它有意省略 CEK key id，使 payload 密文独立
// 于当前使用的 CEK；重包装时只需重新密封 data key，而可以让（可能很大的）
// payload 保持不变。JSON：键已排序、无多余空白、UTF-8 编码。
func CanonicalPayloadAAD(a AAD) ([]byte, error) {
	if !a.Scope.Valid() {
		return nil, ErrAADInvalid
	}
	if a.ClerkUserID == "" {
		return nil, ErrAADInvalid
	}
	if a.Scope == ScopeProfile && a.ID != ProfileSingletonID {
		return nil, ErrAADInvalid
	}
	if a.ID == "" {
		return nil, ErrAADInvalid
	}

	fields := []kv{
		{"alg", AlgAESGCM},
		{"clerk_user_id", a.ClerkUserID},
		{"domain", AADDomain},
		{"id", a.ID},
		{"scope", string(a.Scope)},
		{"v", EnvelopeVersion},
	}
	return marshalCanonical(fields)
}

// CanonicalDEKWrapAAD returns the AAD that binds the key-wrap layer of a v2
// envelope — the per-message data key sealed under the user's CEK. It binds
// the wrap to the owning user, the row, the scope, and the specific CEK key
// id, so a stored wrapped data key cannot be lifted onto another row, user,
// scope, or key. The kid lives here, not in the payload AAD, so
// rewrapping can change it without touching the payload ciphertext.
// CanonicalDEKWrapAAD 返回绑定 v2 envelope key-wrap 层（在用户 CEK 下密封的
// 每条消息 data key）的 AAD。它将包装与所属用户、行、scope 以及具体的 CEK
// key id 绑定，因此存储的已包装 data key 无法被挪用到其他行、用户、scope 或
// key 上。kid 放在这里而不是 payload AAD 中，因此重包装可以在不触及 payload
// 密文的情况下更换它。
func CanonicalDEKWrapAAD(a AAD) ([]byte, error) {
	if !a.Scope.Valid() {
		return nil, ErrAADInvalid
	}
	if len(a.KeyIDHex) != 32 || !isLowerHex(a.KeyIDHex) {
		return nil, ErrAADInvalid
	}
	if a.ClerkUserID == "" {
		return nil, ErrAADInvalid
	}
	if a.Scope == ScopeProfile && a.ID != ProfileSingletonID {
		return nil, ErrAADInvalid
	}
	if a.ID == "" {
		return nil, ErrAADInvalid
	}

	fields := []kv{
		{"clerk_user_id", a.ClerkUserID},
		{"domain", DEKWrapAADDomain},
		{"id", a.ID},
		{"kid", a.KeyIDHex},
		{"scope", string(a.Scope)},
		{"v", EnvelopeVersion},
	}
	return marshalCanonical(fields)
}

// BundleAAD 是 key bundle AAD 所绑定的标识信息（用户 id、key id 与 credential id）。
type BundleAAD struct {
	ClerkUserID  string
	KeyIDHex     string
	CredentialID string
}

// CanonicalBundleAAD 返回绑定 key bundle 的规范 AAD 字节。
func CanonicalBundleAAD(a BundleAAD) ([]byte, error) {
	if a.ClerkUserID == "" || a.CredentialID == "" {
		return nil, ErrAADInvalid
	}
	if len(a.KeyIDHex) != 32 || !isLowerHex(a.KeyIDHex) {
		return nil, ErrAADInvalid
	}
	fields := []kv{
		{"clerk_user_id", a.ClerkUserID},
		{"credential_id", a.CredentialID},
		{"domain", BundleAADDomain},
		{"key_id", a.KeyIDHex},
	}
	return marshalCanonical(fields)
}

type kv struct {
	k string
	v any
}

func marshalCanonical(fields []kv) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, f := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		writeJSONString(&buf, f.k)
		buf.WriteByte(':')
		if err := writeCanonicalValue(&buf, f.v); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func writeCanonicalValue(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if x {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		writeJSONString(buf, x)
	case int:
		buf.WriteString(strconv.FormatInt(int64(x), 10))
	case int64:
		buf.WriteString(strconv.FormatInt(x, 10))
	case float64:
		// JSON numbers from generic decode arrive as float64. Emit without
		// trailing zeros; reject non-finite values.
		// 通用解码得到的 JSON 数字以 float64 形式出现。输出时不带尾随零，
		// 并拒绝非有限值。
		if !isFinite(x) {
			return errors.New("non-finite number in canonical JSON")
		}
		// Integer-valued floats serialize without a decimal point so they
		// match how clients typically encode them.
		// 整数值的 float 序列化时不带小数点，以便与 client 通常的编码方式
		// 保持一致。
		if x == float64(int64(x)) {
			buf.WriteString(strconv.FormatInt(int64(x), 10))
		} else {
			buf.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
		}
	case json.Number:
		buf.WriteString(string(x))
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeJSONString(buf, k)
			buf.WriteByte(':')
			if err := writeCanonicalValue(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonicalValue(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	default:
		return errors.New("unsupported type for canonical JSON")
	}
	return nil
}

func writeJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		default:
			if c < 0x20 {
				buf.WriteString(`\u00`)
				const hexDigits = "0123456789abcdef"
				buf.WriteByte(hexDigits[c>>4])
				buf.WriteByte(hexDigits[c&0x0f])
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('"')
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func isFinite(f float64) bool {
	return !(f != f || f > 1.7976931348623157e+308 || f < -1.7976931348623157e+308)
}
