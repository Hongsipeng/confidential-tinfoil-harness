package api

import (
	"encoding/json"
	"time"
)

// Object is JSON whose shape the harness does not own: tool output, model
// messages, downstream errors, and the model catalog.
// Object 是 harness 并不拥有其形状的 JSON：工具输出、模型消息、下游错误以及模型目录。
type Object map[string]any

// Raw 将 v 序列化为 JSON 原始字节。
func Raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

// Str 返回 v 的字符串表示，非字符串时返回空串。
func Str(v any) string { s, _ := v.(string); return s }

// Obj 将 v 转为 Object，无法转换时返回空 Object。
func Obj(v any) Object {
	switch m := v.(type) {
	case Object:
		return m
	case map[string]any:
		return Object(m)
	}
	return Object{}
}

// Arr 将 v 转为 []any，为 nil 时返回空切片。
func Arr(v any) []any {
	a, _ := v.([]any)
	if values, ok := v.([]Object); ok {
		for _, value := range values {
			a = append(a, value)
		}
	}
	if a == nil {
		return []any{}
	}
	return a
}

// Bool 返回 v 的布尔值，否则返回 fallback。
func Bool(v any, fallback bool) bool {
	b, ok := v.(bool)
	if ok {
		return b
	}
	return fallback
}

// Int 将 v 转为 int，无法转换时返回 0。
func Int(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

// Now 返回当前 UTC 时间。
func Now() time.Time { return time.Now().UTC() }

// Timestamp 返回当前 UTC 时间的 RFC3339Nano 表示。
func Timestamp() string { return Now().Format(time.RFC3339Nano) }

// FirstString 返回 values 中第一个非空字符串。
func FirstString(values ...any) string {
	for _, v := range values {
		if s := Str(v); s != "" {
			return s
		}
	}
	return ""
}

// Parsed is the JSON value b holds, or nil.
// Parsed 返回 b 所持有的 JSON 值；若 b 非法则为 nil。
func Parsed(b []byte) any { var value any; json.Unmarshal(b, &value); return value }
