package api

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Error 是 harness 返回给客户端的错误。
type Error struct {
	HTTP    int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
	// Action is what a client does about the error when it has no handling
	// of its own for the code.
	// Action 是客户端对该错误码没有自己的处理逻辑时，应针对该错误采取的动作。
	Action     string `json:"action"`
	Kind       string `json:"kind,omitempty"`
	ResetsAt   string `json:"resetsAt,omitempty"`
	RetryAfter int    `json:"retryAfter,omitempty"`
	Status     int    `json:"status,omitempty"`
	KeyID      string `json:"keyId,omitempty"`
	Upstream   string `json:"upstreamCode,omitempty"`
	// RateLimit is the caller's quota as an exhausted one's refusal leaves it.
	// RateLimit 是调用方在配额耗尽被拒绝后剩余的 quota。
	RateLimit *RateLimit `json:"rateLimit,omitempty"`

	// 控制面在推送的 chat 下没有找到的 attachment id 列表。
	Missing []string `json:"-"` // attachment ids the control plane did not find under the pushed chat
}

// The actions protocol/openapi.yaml describes as ErrorAction.
// protocol/openapi.yaml 中所描述的 ErrorAction 动作。
const (
	ActionReauthenticate = "reauthenticate"
	ActionReload         = "reload"
	ActionRetry          = "retry"
	ActionWait           = "wait"
	ActionUnlock         = "unlock"
	ActionRevise         = "revise"
	ActionStop           = "stop"
)

// Actions is what a client does about each code, unless an error says
// otherwise.
// Actions 是客户端针对每个错误码应采取的默认动作，除非具体错误另有说明。
var Actions = map[string]string{
	"ALREADY_APPLIED":         ActionReload,
	"ARCHIVE_TOO_LARGE":       ActionRevise,
	"ATTACHMENT_TOO_LARGE":    ActionRevise,
	"ATTACHMENT_UNSUPPORTED":  ActionRevise,
	"ATTACHMENTS_UNAVAILABLE": ActionRetry,
	"BAD_REQUEST":             ActionStop,
	"BROWSER_TIMEOUT":         ActionStop,
	"CHAT_FLAGGED":            ActionStop,
	"COMPACTION_FAILED":       ActionRetry,
	"CONTEXT_TOO_LARGE":       ActionRevise,
	"CREDENTIAL_REQUIRED":     ActionReauthenticate,
	"IMPORT_INVALID":          ActionRevise,
	"IMPORT_TOO_LARGE":        ActionRevise,
	"KEY_MISMATCH":            ActionUnlock,
	"KEY_REQUIRED":            ActionUnlock,
	"MAX_TURNS":               ActionRetry,
	"MISSING_ATTACHMENT":      ActionStop,
	"MODEL_UNAVAILABLE":       ActionRevise,
	"NOTHING_TO_COMPACT":      ActionStop,
	"NOTHING_TO_READ":         ActionStop,
	"PAYLOAD_TOO_LARGE":       ActionRevise,
	"PROJECTS_REQUIRE_CLOUD":  ActionStop,
	"QUEUE_FULL":              ActionRetry,
	"QUOTA_EXHAUSTED":         ActionWait,
	"RECORD_UNSUPPORTED":      ActionStop,
	"RATE_LIMITED":            ActionRetry,
	"RESEARCH_LIMIT_REACHED":  ActionWait,
	"REQUEST_TIMEOUT":         ActionRetry,
	"REVISION_CONFLICT":       ActionReload,
	"RUN_IN_FLIGHT":           ActionReload,
	"RUN_LOG_FULL":            ActionStop,
	"RUN_NOT_FOUND":           ActionReload,
	"SEARCH_INDEX_FAILED":     ActionRetry,
	"SERVICE_BUSY":            ActionRetry,
	"SYNC_OUTCOME_UNKNOWN":    ActionRetry,
	"TEMP_SPACE_BUSY":         ActionRetry,
	"THREAD_EXISTS":           ActionStop,
	"THREAD_NOT_FOUND":        ActionStop,
	"THREAD_TOO_LARGE":        ActionRevise,
	"TOOL_UNAVAILABLE":        ActionRetry,
	"UNAUTHENTICATED":         ActionReauthenticate,
	"UPSTREAM_REFUSED":        ActionRetry,
}

// Error 返回错误的提示消息。
func (e *Error) Error() string { return e.Message }

// Err 构造一个带有 HTTP 状态、错误码、消息与默认动作的错误。
func Err(status int, code, message string) *Error {
	return &Error{HTTP: status, Code: code, Message: message, Action: Actions[code]}
}

// withAction has a client take another action about a new error than its
// code's.
// withAction 让客户端针对某个新错误采取与其错误码默认动作不同的动作。
func (e *Error) WithAction(action string) *Error {
	e.Action = action
	return e
}

// AsError 将任意错误规整为 *Error。
func AsError(err error) *Error {
	var a *Error
	if errors.As(err, &a) {
		return a
	}
	var refusal *Refusal
	if errors.As(err, &refusal) {
		e := DownstreamError(refusal.Status, []byte(refusal.Detail))
		if refusal.RetryAfter > 0 {
			e.RetryAfter = refusal.RetryAfter
		}
		return e
	}
	return Err(502, "UPSTREAM_REFUSED", "A downstream request failed")
}

// DownstreamError 将下游服务的拒绝映射为面向客户端的错误。
func DownstreamError(status int, body []byte) *Error {
	var in Object
	json.Unmarshal(body, &in)
	if nested := Obj(in["error"]); len(nested) > 0 {
		in = nested
	}
	e := Err(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	e.Status = status
	if refused := OpenAIRefusal(status, body); refused != nil {
		if refused.RetryAfter == 0 {
			refused.RetryAfter = Int(in["retryAfter"])
		}
		return refused
	}
	switch status {
	case 401:
		e = Err(401, "UNAUTHENTICATED", "Authentication is required")
	case 404:
		e = Err(404, "THREAD_NOT_FOUND", "The requested data was not found")
	case 429:
		e = Err(429, "QUOTA_EXHAUSTED", "The request quota is exhausted")
		e.Kind = FirstString(in["kind"], in["type"])
		e.ResetsAt = FirstString(in["resetsAt"], in["resets_at"])
		e.RetryAfter = Int(in["retryAfter"])
	}
	RefusedAsSent(e, status)
	return e
}

// Refusal 是下游模型拒绝请求的原始信息。
type Refusal struct {
	Status     int
	Detail     string
	RetryAfter int
}

// Error 描述模型返回的拒绝状态与详情。
func (r *Refusal) Error() string { return fmt.Sprintf("model returned %d: %s", r.Status, r.Detail) }
