package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// Respond 以 JSON 写出 result；若有错误，则按错误要求的状态码与响应头写出该错误。
func Respond(w http.ResponseWriter, result any, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		e := AsError(err)
		if e.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfter))
		}
		w.WriteHeader(e.HTTP)
		result = Object{"error": e}
	}
	if result == nil {
		result = Object{}
	}
	json.NewEncoder(w).Encode(result)
}

// MaxRequestBody 限制请求体的最大字节数。
const MaxRequestBody = 8 << 20

// ReadBody reads a request body of at most limit bytes that holds one JSON
// object.
// ReadBody 读取最多 limit 字节、承载单个 JSON 对象的请求体。
func ReadBody(w http.ResponseWriter, r *http.Request, limit int64) (json.RawMessage, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := ReadWipeable(r.Body)
	if err != nil {
		clear(body)
		return nil, BodyError(err, "Expected a JSON object")
	}
	if !IsObject(body) || !json.Valid(body) {
		clear(body)
		return nil, Err(400, "BAD_REQUEST", "Expected a JSON object")
	}
	return body, nil
}

const bodyReadChunk = 4 << 10

// ReadWipeable clears both the read scratch space and replaced growth buffers.
// The caller owns and clears the returned bytes.
// ReadWipeable 会清空读取用的临时空间与被替换的扩容缓冲区。
// 返回的字节由调用方持有并负责清空。
func ReadWipeable(r io.Reader) ([]byte, error) {
	chunk := make([]byte, bodyReadChunk)
	defer clear(chunk)
	var buffer wipingBuffer
	_, err := io.CopyBuffer(&buffer, r, chunk)
	return buffer.data, err
}

type wipingBuffer struct{ data []byte }

func (b *wipingBuffer) Write(p []byte) (int, error) {
	if size := len(b.data) + len(p); size > cap(b.data) {
		next := make([]byte, len(b.data), max(size, 2*cap(b.data)))
		copy(next, b.data)
		clear(b.data)
		b.data = next
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// BodyError tells a body over the size limit, or one that stopped
// arriving, from a malformed one.
// BodyError 用于区分请求体超过大小上限、请求体中断传输，以及格式错误的请求体。
func BodyError(err error, malformed string) *Error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return Err(http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "The request exceeds the size limit")
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return Err(http.StatusRequestTimeout, "REQUEST_TIMEOUT", "The request stopped sending its body")
	}
	return Err(400, "BAD_REQUEST", malformed)
}

// IsObject 报告 value 是否为一个 JSON 对象。
func IsObject(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// DecodeRequest decodes a JSON object into a request, naming the first
// field of the wrong type.
// DecodeRequest 将 JSON 对象解码到 request，并指出首个类型不匹配的字段。
func DecodeRequest(body []byte, in any) error {
	return RequestError(json.Unmarshal(body, in))
}

// DecodeBody consumes the owned HTTP buffer after copying its fields.
// DecodeBody 在复制字段之后，消耗（并清空）由 HTTP 持有的缓冲区。
func DecodeBody(body []byte, in any) error {
	defer clear(body)
	return DecodeRequest(body, in)
}

// KeyRequest 携带一个可选的 base64 编码密钥。
type KeyRequest struct {
	Key keyBytes `json:"key,omitempty"`
}

type keyBytes []byte

func (key *keyBytes) UnmarshalJSON(data []byte) error {
	clear(*key)
	*key = nil
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	if len(data) == 0 || data[0] != '"' {
		return Err(400, "BAD_REQUEST", "key must be a base64 string")
	}
	if err := json.Unmarshal(data, (*[]byte)(key)); err != nil {
		return Err(400, "BAD_REQUEST", "key must be a base64 string")
	}
	return nil
}

// ClearKey 清空并释放请求中的密钥。
func (in *KeyRequest) ClearKey() {
	clear(in.Key)
	in.Key = nil
}

// RequestError 将请求体解码错误规整为指出首个类型不匹配字段的客户端错误。
func RequestError(err error) error {
	var refused *Error
	if errors.As(err, &refused) {
		return refused
	}
	var mistyped *json.UnmarshalTypeError
	if err == nil || !errors.As(err, &mistyped) || mistyped.Field == "" {
		if err != nil {
			return Err(400, "BAD_REQUEST", "Expected a JSON object")
		}
		return nil
	}
	// A field path names list items by index; the field is the last name.
	// 字段路径用索引表示列表项；真正的字段名是最后一段。
	path := strings.Split(mistyped.Field, ".")
	for len(path) > 1 {
		if _, err := strconv.Atoi(path[len(path)-1]); err != nil {
			break
		}
		path = path[:len(path)-1]
	}
	return Err(400, "BAD_REQUEST", path[len(path)-1]+" must be "+jsonKind(mistyped.Type))
}

func jsonKind(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean"
	case reflect.Slice, reflect.Array:
		return "an array"
	case reflect.Struct, reflect.Map:
		return "an object"
	}
	return "a number"
}

// Nullable is a field whose absence differs from null: null clears what
// absence leaves as it is.
// Nullable 表示“缺省”与“null”语义不同的字段：null 会清空该字段，而缺省则保持原样。
type Nullable[T any] struct {
	Set, invalid bool
	Value        *T
}

// UnmarshalJSON 记录字段是否出现，并把非 null 的值解码到 Value。
func (n *Nullable[T]) UnmarshalJSON(b []byte) error {
	n.Set = true
	if string(b) == "null" {
		return nil
	}
	n.Value = new(T)
	if json.Unmarshal(b, n.Value) != nil {
		n.Value, n.invalid = nil, true
	}
	return nil
}

// IsZero 报告字段是否从未出现。
func (n Nullable[T]) IsZero() bool { return !n.Set }

// MarshalJSON 将字段的当前值序列化为 JSON。
func (n Nullable[T]) MarshalJSON() ([]byte, error) { return json.Marshal(n.Value) }

// check refuses a value of the wrong type, as decoding refuses other fields.
// Check 拒绝类型不匹配的值，正如解码会拒绝其他类型错误的字段。
func (n Nullable[T]) Check(name string) error {
	if n.invalid {
		return Err(400, "BAD_REQUEST", name+" must be "+jsonKind(reflect.TypeFor[T]()))
	}
	return nil
}

// maxIDLength bounds the ids a caller names its rows with. A thread id is
// echoed into every frame of its run's log, so an unbounded one is an
// unbounded frame.
// maxIDLength 限制调用方为其行命名所用的 id 长度。thread id 会被回显到其 run
// 日志的每一帧，因此无界的 id 就意味着无界的帧。
const maxIDLength = 128

// IsIdentifier holds a caller's id to printable ASCII, short of the length at
// which it stops being a name.
// IsIdentifier 将调用方的 id 限定为可打印 ASCII，且长度不得超过其作为名称的合理上限。
func IsIdentifier(s string) bool {
	if len(s) > maxIDLength {
		return false
	}
	for _, r := range s {
		if r < ' ' || r > '~' {
			return false
		}
	}
	return true
}

// RequireID refuses an id that cannot name a row. Ids become path segments
// of the control plane's URLs, so the dot segments are refused.
// RequireID 拒绝无法命名一行的 id。id 会成为控制面 URL 的路径段，因此会拒绝点分段。
func RequireID(field, id string) error {
	if id == "" || id == "." || id == ".." || !IsIdentifier(id) || strings.ContainsAny(id, "/\\?#") {
		return Err(400, "BAD_REQUEST", field+" must be a valid identifier")
	}
	return nil
}
