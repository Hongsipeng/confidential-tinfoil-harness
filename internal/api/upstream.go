package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// 重试间隔上限，单位为秒。
	maxRetryAfter      = 24 * 60 * 60 // seconds
	maxUpstreamMessage = 512
)

// RetryAfter reads one delay-seconds or HTTP-date Retry-After value in whole
// seconds, at least one so a retry never spins. Absent, repeated, or malformed
// values read as zero.
// RetryAfter 读取一个以“延迟秒数”或 HTTP 日期给出的 Retry-After 值，结果取整到
// 秒且至少为 1，避免重试空转。缺失、重复或格式错误的值按 0 处理。
func RetryAfter(header http.Header) int {
	values := header.Values("Retry-After")
	if len(values) != 1 {
		return 0
	}
	value := strings.TrimSpace(values[0])
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		return int(max(1, min(seconds, maxRetryAfter)))
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(1, min(int(math.Ceil(time.Until(at).Seconds())), maxRetryAfter))
	}
	return 0
}

// WithRetryAfter 在 header 含合法 Retry-After 时，将其填入 e。
func WithRetryAfter(e *Error, header http.Header) *Error {
	if seconds := RetryAfter(header); seconds > 0 {
		e.RetryAfter = seconds
	}
	return e
}

// OpenAIRefusal maps the OpenAI error shape that the router and model and
// document enclaves answer with, nested or flat. Other bodies keep
// their status mapping, as does a 401, which callers answer by refreshing
// the credential.
// OpenAIRefusal 将 router、model 与 document enclave 返回的 OpenAI 错误形状
// （嵌套或平铺）映射为错误。其他响应体保持其状态映射；401 亦然，调用方通过刷新
// 凭据来响应。
func OpenAIRefusal(status int, body []byte) *Error {
	var in Object
	if status == http.StatusUnauthorized || json.Unmarshal(body, &in) != nil {
		return nil
	}
	fields := Obj(in["error"])
	if len(fields) == 0 {
		if fields = in; Str(in["object"]) != "error" {
			return nil
		}
	}
	code, kind, message := Str(fields["code"]), Str(fields["type"]), Str(fields["message"])
	message = strings.ToValidUTF8(message[:min(len(message), maxUpstreamMessage)], "")
	e := Err(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	switch {
	case status == http.StatusPaymentRequired || code == "insufficient_quota" || kind == "insufficient_quota":
		e = Err(429, "QUOTA_EXHAUSTED", FirstString(message, "The request quota is exhausted"))
	case status == http.StatusTooManyRequests:
		e = Err(429, "RATE_LIMITED", FirstString(message, "Too many requests; retry shortly"))
	case status == http.StatusServiceUnavailable && code == "model_unavailable":
		e = Err(503, "MODEL_UNAVAILABLE", FirstString(message, "The model is temporarily unavailable")).WithAction(ActionRetry)
	case status == http.StatusServiceUnavailable && code == "server_is_overloaded":
		e = Err(503, "UPSTREAM_REFUSED", FirstString(message, "The model is overloaded"))
	case status/100 == 4 && (code == "context_length_exceeded" || strings.Contains(strings.ToLower(message), "context length")):
		e = Err(413, "CONTEXT_TOO_LARGE", FirstString(message, "The conversation exceeds the model's context window"))
	case (status/100 == 4 || strings.HasPrefix(code, "document_")) && message != "":
		// Document failures carry fixed messages that describe nothing in the file.
		// document 失败携带固定消息，这些消息不涉及文件内容。
		e = Err(502, "UPSTREAM_REFUSED", message)
	}
	RefusedAsSent(e, status)
	e.Status = status
	if strings.HasPrefix(code, "document_") {
		e.Upstream = code
		// A document that failed to convert is the file's doing, as its
		// message says, so the user sends another.
		// 转换失败的 document 是文件本身的问题（正如其消息所述），因此让用户重新发送一份。
		if e.Code == "UPSTREAM_REFUSED" {
			e.Action = ActionRevise
		}
	}
	return e
}

// RefusedAsSent has a client stop resending a request a downstream service
// refused with a 4xx it maps to nothing more specific, since sending it
// unchanged cannot succeed.
// RefusedAsSent 让客户端停止重发请求：当下游服务以 4xx 拒绝且没有更具体的映射时，
// 原样重发不可能成功。
func RefusedAsSent(e *Error, status int) {
	if e.Code == "UPSTREAM_REFUSED" && status/100 == 4 {
		e.Action = ActionStop
	}
}

// StreamError maps an error event inside an accepted stream, whose type stands
// in for the status. It is never 401: refreshing the credential would replay a
// request the model already began answering.
// StreamError 将已接受流中的错误事件映射为错误，其 type 字段充当状态码。它绝不会
// 是 401：刷新凭据会重放模型已开始应答的请求。
func StreamError(data []byte) *Error {
	var in Object
	json.Unmarshal(data, &in)
	fields := Obj(in["error"])
	if len(fields) == 0 {
		fields = in
	}
	status := http.StatusBadGateway
	switch Str(fields["type"]) {
	case "invalid_request_error", "BadRequestError":
		status = http.StatusBadRequest
	case "insufficient_quota":
		status = http.StatusPaymentRequired
	case "rate_limit_error", "RateLimitError":
		status = http.StatusTooManyRequests
	case "service_unavailable_error", "ServiceUnavailableError":
		status = http.StatusServiceUnavailable
	}
	if code := Int(fields["code"]); code >= 400 && code < 600 && code != http.StatusUnauthorized {
		status = code
	}
	return DownstreamError(status, data)
}
