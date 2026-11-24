package engine

import (
	"cmp"
	"encoding/json"
	"regexp"
	"slices"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// RunRequest names a run, or a turn queued behind one, of a chat.
// RunRequest 指定聊天的一个 run，或排在其后面的一个 turn。
type RunRequest struct {
	api.KeyRequest
	ThreadID string `json:"threadId"`
	RunID    string `json:"runId"`
	QueueID  string `json:"queueId"`
}

// ThreadImportRequest stores a device chat in the cloud under its id.
// ThreadImportRequest 将设备的聊天以其 id 存储到云端。
type ThreadImportRequest struct {
	api.KeyRequest
	ThreadID string          `json:"threadId"`
	Thread   json.RawMessage `json:"thread"`
}

// Validate 校验线程导入请求的合法性。
func (in *ThreadImportRequest) Validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !api.IsObject(in.Thread) {
		return api.Err(400, "BAD_REQUEST", "thread must be an object")
	}
	return domain.ValidateLocalThread(in.Thread)
}

// TurnRequest is one user action on a chat, which its run carries out.
// TurnRequest 是对聊天的一次用户操作，由该聊天的 run 来执行。
type TurnRequest struct {
	api.KeyRequest
	ClientRequestID string               `json:"clientRequestId"`
	ThreadID        string               `json:"threadId"`
	Kind            string               `json:"kind"`
	Ephemeral       bool                 `json:"ephemeral,omitempty"`
	Local           bool                 `json:"local,omitempty"`
	Thread          json.RawMessage      `json:"thread,omitempty"`
	Profile         json.RawMessage      `json:"profile,omitempty"`
	ProjectID       *string              `json:"projectId,omitempty"`
	PresetID        api.Nullable[string] `json:"presetId,omitzero"`
	Content         *string              `json:"content,omitempty"`
	Quote           *string              `json:"quote,omitempty"`
	MessageID       string               `json:"messageId,omitempty"`
	ToolCallID      string               `json:"toolCallId,omitempty"`
	Attachments     []attach.Sent        `json:"attachments,omitzero"`
	Widgets         []string             `json:"widgets,omitzero"`
	Browser         *BrowserAccess       `json:"browser,omitempty"`
	// CompactLatest has a compaction on request summarize the chat's latest
	// exchange too, for a client that shows a marker at the end of a chat.
	// CompactLatest 让按请求进行的 compaction 也汇总聊天最新的 exchange，
	// 供在聊天末尾显示标记的客户端使用。
	CompactLatest bool        `json:"compactLatest,omitempty"`
	Options       turnOptions `json:"options"`
}

type turnOptions struct {
	Model            string `json:"model,omitempty"`
	AutoIntelligence string `json:"autoIntelligence,omitempty"`
	ReasoningEffort  string `json:"reasoningEffort,omitempty"`
	Timezone         string `json:"timezone,omitempty"`
	Locale           string `json:"locale,omitempty"`
	Thinking         *bool  `json:"thinking,omitempty"`
	WebSearch        *bool  `json:"webSearch,omitempty"`
	PIICheck         *bool  `json:"piiCheck,omitempty"`
	GenUI            *bool  `json:"genUI,omitempty"`
	// Research has the turn research its question on the web at length
	// before it answers with a report.
	// Research 让该 turn 在给出报告式回答之前，先在网页上对其问题进行深入研究。
	Research bool `json:"research,omitempty"`
	// CacheSecret scopes the prompt cache to the client that holds it, as
	// the Tinfoil SDKs scope theirs: 256 random bits, hex encoded.
	// CacheSecret 将 prompt 缓存限定到持有它的客户端，正如 Tinfoil SDK 所做的那样：
	// 256 位随机数，十六进制编码。
	CacheSecret string `json:"cacheSecret,omitempty"`
}

var cacheSecretPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

var (
	turnKinds = []string{"send", "edit", "regenerate", "continue", "retryToolCall", "ask", "retry", "compact"}
	// researchKinds are the turns that answer a question anew, which a
	// research may answer.
	// researchKinds 是重新回答一个问题的那些 turn，research 可以回答这些 turn。
	researchKinds = []string{"send", "edit", "regenerate", "retry"}
	// compactingKinds are the turns that may compact a chat on request: a
	// compaction, and a retry, which compacts again after one failed.
	// compactingKinds 是可按请求对聊天做 compaction 的那些 turn：一次 compaction，
	// 以及一次 retry（在一次 compaction 失败后会再次压缩）。
	compactingKinds = []string{"compact", "retry"}
)

// Validate refuses a turn its run could not carry out. Who may send it is
// checked with the caller.
// Validate 拒绝那些其 run 无法执行的 turn。谁可以发送它，则与调用方一起校验。
func (in *TurnRequest) Validate() error {
	if _, err := uuid.Parse(in.ClientRequestID); err != nil {
		return api.Err(400, "BAD_REQUEST", "clientRequestId must be a UUID")
	}
	in.Kind = cmp.Or(in.Kind, "send")
	if !slices.Contains(turnKinds, in.Kind) {
		return api.Err(400, "BAD_REQUEST", "Unknown turn kind")
	}
	// An ask is never stored, so it may still say so.
	// ask 从不被存储，因此它仍可声明这一点。
	if in.Ephemeral && in.Kind != "ask" {
		return api.Err(400, "BAD_REQUEST", "Temporary chats are kept on the device")
	}
	if secret := in.Options.CacheSecret; secret != "" && !cacheSecretPattern.MatchString(secret) {
		return api.Err(400, "BAD_REQUEST", "cacheSecret must be 64 hex characters")
	}
	if in.Options.Research && !slices.Contains(researchKinds, in.Kind) {
		return api.Err(400, "BAD_REQUEST", "Only a question can be researched")
	}
	if in.CompactLatest && !slices.Contains(compactingKinds, in.Kind) {
		return api.Err(400, "BAD_REQUEST", "Only a compaction compacts the latest exchange")
	}
	if in.Browser != nil {
		if !in.Local || in.Kind != "send" || in.Options.Research {
			return api.Err(400, "BAD_REQUEST", "Browser actions require a local send turn without research")
		}
		if _, err := uuid.Parse(in.Browser.ExecutorID); err != nil || !cacheSecretPattern.MatchString(in.Browser.Token) {
			return api.Err(400, "BAD_REQUEST", "Browser access requires an executor UUID and a 256-bit token")
		}
	}
	return in.PresetID.Check("presetId")
}

// validateContent refuses a turn whose content cannot make its message.
// validateContent 拒绝那些其内容无法构成其消息的 turn。
func (in *TurnRequest) validateContent() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if in.Local && in.ProjectID != nil {
		return api.Err(400, "BAD_REQUEST", "Projects require cloud storage")
	}
	for _, field := range []struct {
		name  string
		value json.RawMessage
	}{{"thread", in.Thread}, {"profile", in.Profile}} {
		if field.value == nil {
			continue
		}
		if !in.Local {
			return api.Err(400, "BAD_REQUEST", field.name+" is only accepted for local threads")
		}
		if !api.IsObject(field.value) {
			return api.Err(400, "BAD_REQUEST", field.name+" must be an object")
		}
	}
	if in.Thread != nil {
		if err := domain.ValidateLocalThread(in.Thread); err != nil {
			return err
		}
	} else if in.Local && in.Kind == "ask" {
		// An ask about a device chat reads the conversation its client sends.
		// 关于设备聊天的 ask 会读取其客户端发送的会话内容。
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	pixels := int64(0)
	for i := range in.Attachments {
		if err := in.Attachments[i].Validate(); err != nil {
			return err
		}
		pixels += in.Attachments[i].ResizePixels()
	}
	if pixels > attach.MaxTurnResizePixels {
		return api.Err(413, "ATTACHMENT_TOO_LARGE", "The turn's images hold more pixels than one turn can resize")
	}
	return nil
}

// digest identifies a turn's content, so a retry of the request is told from
// another request under the same id.
// digest 标识一个 turn 的内容，这样同一次请求的重试就能与同一 id 下的另一次请求区分开。
func (in *TurnRequest) digest() string {
	copy := *in
	copy.Key = nil
	return api.HashHex(api.Raw(copy))
}
