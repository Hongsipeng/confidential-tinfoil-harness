// 本文件针对特定模型的工具调用解码缺陷进行修复。
package domain

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

// Repairs for known bugs in the tool-call decoders of specific models and
// inference stacks, ported from the model router (toolruntime/quirks.go).
// 针对特定模型与推理栈的工具调用解码器已知缺陷的修复，移植自模型 router
// （toolruntime/quirks.go）。

// controlTokenPattern matches Kimi's <|...|> and Gemma's <|"| framing tokens.
// The inner class cannot span two tokens and is capped so a pathological
// argument cannot stall the stripper.
// controlTokenPattern 匹配 Kimi 的 <|...|> 与 Gemma 的 <|"| 框架 token。内部字符类不能
// 跨两个 token，且设有上限，以免病态参数拖垮清除逻辑。
var controlTokenPattern = regexp.MustCompile(`<\|[^|<>]{0,64}\|>?`)

// urlFields hold URLs and nothing else, so stripping them cannot touch user text.
// urlFields 只保存 URL，因此清除它们不会触及用户文本。
var urlFields = map[string]bool{"url": true, "urls": true, "fetch_url": true, "page_url": true, "href": true, "link": true}

// SanitizeToolCallArguments strips leaked control tokens from URL arguments in
// place.
// SanitizeToolCallArguments 原地清除 URL 参数中泄漏的控制 token。
func SanitizeToolCallArguments(args map[string]any) {
	DeepStripURLTokens(args)
}

// StripModelControlTokens 清除字符串中泄漏的模型控制 token。
func StripModelControlTokens(s string) string {
	if !strings.Contains(s, "<|") {
		return s
	}
	return controlTokenPattern.ReplaceAllString(s, "")
}

// looksLikeLeakedURL admits a string outside urlFields only when it is a URL
// once its tokens are gone.
// looksLikeLeakedURL 仅当 urlFields 之外的字符串在去掉 token 后是 URL 时才认定其为
// 泄漏的 URL。
func looksLikeLeakedURL(s string) bool {
	if !strings.Contains(s, "<|") {
		return false
	}
	stripped := StripModelControlTokens(s)
	return strings.HasPrefix(stripped, "http://") || strings.HasPrefix(stripped, "https://")
}

// DeepStripURLTokens 深度遍历并清除各层级中泄漏的 URL 控制 token。
func DeepStripURLTokens(v any) bool {
	strip := func(s string, url bool) (string, bool) {
		if url || looksLikeLeakedURL(s) {
			stripped := StripModelControlTokens(s)
			return stripped, stripped != s
		}
		return s, false
	}
	changed := false
	switch val := v.(type) {
	case map[string]any:
		for key, child := range val {
			url := urlFields[strings.ToLower(key)]
			switch c := child.(type) {
			case string:
				if s, ok := strip(c, url); ok {
					val[key], changed = s, true
				}
			case []any:
				for i, item := range c {
					if s, ok := item.(string); ok {
						if s, ok := strip(s, url); ok {
							c[i], changed = s, true
						}
					} else if DeepStripURLTokens(item) {
						changed = true
					}
				}
			case map[string]any:
				changed = DeepStripURLTokens(c) || changed
			}
		}
	case []any:
		for _, item := range val {
			changed = DeepStripURLTokens(item) || changed
		}
	}
	return changed
}

// SanitizeToolCallArgumentsJSON cuts a payload such as Kimi's
// `{"a":1}Now I will...` down to its leading object. Valid JSON, and anything
// that does not open with a complete object, is returned untouched.
// SanitizeToolCallArgumentsJSON 把如 Kimi 的 `{"a":1}Now I will...` 这类 payload 截到
// 其开头的对象为止。合法 JSON，以及不以完整对象开头的任何内容，都会原样返回。
func SanitizeToolCallArgumentsJSON(raw []byte) ([]byte, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' || json.Valid(trimmed) {
		return raw, false
	}
	object, ok := ExtractFirstJSONObject(string(trimmed))
	if !ok {
		return raw, false
	}
	return []byte(object), true
}

// ExtractFirstJSONObject returns the first balanced top-level object in s,
// honouring strings and their escapes.
// ExtractFirstJSONObject 返回 s 中第一个配平的顶层对象，并正确处理字符串及其转义。
func ExtractFirstJSONObject(s string) (string, bool) {
	if len(s) == 0 || s[0] != '{' {
		return "", false
	}
	depth, inString, escaped := 0, false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case escaped:
			escaped = false
		case inString:
			escaped, inString = c == '\\', c != '"'
		case c == '"':
			inString = true
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return s[:i+1], true
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	return "", false
}
