package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Completed first-party and API-key chat conversations are submitted to the
// safeguards sidecar for acceptable-use classification, as the model router submits the
// chats it serves. Ported from the router's safeguards package.
// 已完成的一方（first-party）和 API-key 聊天会话会被提交给 safeguards sidecar 做可接受用途分类，
// 正如模型 router 提交它所服务的聊天那样。此实现移植自 router 的 safeguards 包。

const (
	safeguardsTimeout = 5 * time.Second
	// maxSubmissionBytes is the sidecar's request cap, MAX_REQUEST_BYTES in
	// tinfoil-config.yml. The sidecar reads only a transcript's tail.
	// maxSubmissionBytes 是 sidecar 的请求上限，即 tinfoil-config.yml 中的 MAX_REQUEST_BYTES。
	// sidecar 只读取记录的尾部。
	maxSubmissionBytes = 4 << 20
	// jsonEscapeGrowth is the most one byte grows when encoded, as \u00XX.
	// jsonEscapeGrowth 是一个字节在编码时最多增长到的长度，如 \u00XX。
	jsonEscapeGrowth = 6
)

// SafeguardMessage 是提交给 safeguards sidecar 的一条消息。
type SafeguardMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Submission 是提交给 safeguards sidecar 的一次会话。
type Submission struct {
	Credential     string             `json:"credential"`
	ConversationID string             `json:"conversation_id,omitempty"`
	Messages       []SafeguardMessage `json:"messages"`
}

// Submitter delivers a conversation from the run that produced it, after the
// run's terminal frame, so the reply is already delivered. The run waits up
// to safeguardsTimeout for the delivery before it releases what it holds,
// and a failed delivery never fails it.
// Submitter 在 run 的终止帧之后，投递由该 run 产生的会话，这样回复已经送达。
// run 在释放它所持有的内容之前，最多等待 safeguardsTimeout 来完成投递，投递失败绝不会
// 使 run 失败。
type Submitter struct {
	endpoint string
	client   *http.Client
}

// NewSubmitter is nil without a sidecar; a nil submitter does nothing.
// NewSubmitter 在没有 sidecar 时返回 nil；nil 的 submitter 什么都不做。
func NewSubmitter(baseURL string) *Submitter {
	if baseURL == "" {
		return nil
	}
	return &Submitter{
		endpoint: strings.TrimRight(baseURL, "/") + "/ingest",
		client:   &http.Client{Timeout: safeguardsTimeout},
	}
}

// submit delivers a conversation that ends in an assistant reply; without one
// there is nothing to judge.
// submit 投递一条以 assistant 回复结尾的会话；没有这样的回复则无可评判。
func (s *Submitter) submit(credential, conversationID string, messages []SafeguardMessage) {
	if s == nil || len(messages) == 0 || messages[len(messages)-1].Role != "assistant" || messages[len(messages)-1].Content == "" {
		return
	}
	s.deliver(encodeSubmission(credential, conversationID, messages))
}

// encodeSubmission keeps the newest messages that fit the sidecar's cap, and
// cuts the newest from the front when it alone does not fit.
// encodeSubmission 保留能放入 sidecar 上限的最新消息，当仅最新一条都放不下时，
// 就从其开头截断。
func encodeSubmission(credential, conversationID string, messages []SafeguardMessage) []byte {
	envelope, _ := json.Marshal(Submission{Credential: credential, ConversationID: conversationID, Messages: []SafeguardMessage{}})
	room, start := maxSubmissionBytes-len(envelope), len(messages)
	for ; start > 0; start-- {
		encoded, _ := json.Marshal(messages[start-1])
		if len(encoded)+len(",") > room {
			break
		}
		room -= len(encoded) + len(",")
	}
	kept := messages[start:]
	if len(kept) == 0 {
		last := messages[len(messages)-1]
		empty, _ := json.Marshal(SafeguardMessage{Role: last.Role})
		keep := min(len(last.Content), max(0, room-len(empty))/jsonEscapeGrowth)
		last.Content = strings.ToValidUTF8(last.Content[len(last.Content)-keep:], "")
		kept = []SafeguardMessage{last}
	}
	body, _ := json.Marshal(Submission{Credential: credential, ConversationID: conversationID, Messages: kept})
	return body
}

func (s *Submitter) deliver(body []byte) {
	defer func() { api.Rescued("safeguards submission", recover()) }()
	req, err := http.NewRequest(http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		slog.Warn("safeguards submission failed", "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		slog.Warn("safeguards sidecar rejected submission", "status", resp.StatusCode)
	}
}

// flattenContent renders string or part-array content as text, replacing
// each non-text part with a placeholder so no binary leaves the enclave.
// flattenContent 将字符串或 parts 数组形式的内容渲染为文本，用占位符替换每个非文本部分，
// 这样就不会有任何二进制内容离开 enclave。
func flattenContent(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var b strings.Builder
		for i, part := range c {
			p, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if i > 0 {
				b.WriteByte('\n')
			}
			if kind := api.Str(p["type"]); kind != "text" {
				fmt.Fprintf(&b, "[%s]", kind)
			} else if text, ok := p["text"].(string); ok {
				b.WriteString(text)
			} else {
				b.WriteString(api.Str(p["refusal"]))
			}
		}
		return b.String()
	}
	return ""
}

// screening is what a run submits for classification once it completes.
// screening 是 run 完成后提交分类的内容。
type screening struct {
	conversation string
	messages     []SafeguardMessage
}

// screening captures the prompt and the chat's whole history a run was
// asked, without tool results or the summaries that stand in for compacted
// messages. Turns through auto are screened here too: the router is not
// told their chat, so the control plane ignores the router's report of them.
// screening 捕获 run 被要求处理的 prompt 和聊天的整个历史，不含工具结果或代替已 compact
// 消息的摘要。通过 auto 进行的 turn 也会在这里接受审查：router 并不知道它们的聊天，
// 因此 control plane 会忽略 router 对它们的报告。
func (eng *Engine) screening(req *Request) *screening {
	if eng.Safeguards == nil {
		return nil
	}
	var messages []SafeguardMessage
	if req.System != "" {
		messages = append(messages, SafeguardMessage{Role: "system", Content: req.System})
	}
	for _, group := range req.history {
		for _, encoded := range group {
			var message struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			}
			if json.Unmarshal(encoded, &message) == nil && message.Role != "" && message.Role != "tool" {
				messages = append(messages, SafeguardMessage{Role: message.Role, Content: flattenContent(message.Content)})
			}
		}
	}
	return &screening{conversation: req.conversation, messages: messages}
}

// screen submits a run that completed with a reply, under a first-party chat
// credential or an API key, for classification.
// screen 将在 first-party 聊天 credential 或 API key 下完成且带有回复的 run 提交分类。
func (eng *Engine) screen(r *Run, reply []domain.Message, runErr error) {
	eng.Live.mu.Lock()
	deleted := r.Thread.deleted
	eng.Live.mu.Unlock()
	if r.screening == nil || runErr != nil || r.cancelled || deleted || r.retryTarget != nil || !r.credential.FirstParty && !r.credential.APIKey {
		return
	}
	var text []string
	for _, m := range reply {
		if m.Content != "" {
			text = append(text, m.Content)
		}
	}
	history := r.screening.messages
	// A continued reply, whole, takes the place of its earlier text.
	// 被 continue 的回复会整体取代其早先的文本。
	if r.turn.Kind == "continue" && len(history) > 0 && history[len(history)-1].Role == "assistant" {
		history = history[:len(history)-1]
	}
	eng.Safeguards.submit(string(r.access.Current()), r.screening.conversation, append(history, SafeguardMessage{Role: "assistant", Content: strings.Join(text, "\n")}))
}
