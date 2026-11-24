// 本文件定义线程来源（云端或设备）及其校验。
package domain

import (
	"encoding/json"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// ThreadSource is where the chat an operation changes is kept: in the cloud,
// named by id with the content key, or on the device that sends it with
// the settings its prompt reads.
// ThreadSource 指明操作所修改聊天的存放位置：在云端，以 id 加 content key 指明；或在
// 发送它的设备上，随其 prompt 读取的设置一同发送。
type ThreadSource struct {
	Local   bool            `json:"local"`
	Thread  json.RawMessage `json:"thread"`
	Profile json.RawMessage `json:"profile"`
}

// Validate refuses a source that names a chat in both places, or a device
// chat whose copy cannot be read.
// Validate 拒绝同时在两处指明聊天的来源，或副本无法读取的设备聊天。
func (s *ThreadSource) Validate() error {
	if !s.Local {
		if s.Thread != nil || s.Profile != nil {
			return api.Err(400, "BAD_REQUEST", "thread and profile are only accepted for local threads")
		}
		return nil
	}
	if !api.IsObject(s.Thread) {
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	if s.Profile != nil && !api.IsObject(s.Profile) {
		return api.Err(400, "BAD_REQUEST", "profile must be an object")
	}
	return ValidateLocalThread(s.Thread)
}

// threadInput is what a client's copy of a thread must hold before the
// legacy reader reads the rest.
// threadInput 是客户端 thread 副本在 legacy 读取器读取其余部分之前必须持有的字段。
type threadInput struct {
	Title            *string        `json:"title"`
	TitleState       *string        `json:"titleState"`
	Model            *string        `json:"model"`
	LastRequestID    *string        `json:"lastRequestId"`
	Pinned           *bool          `json:"pinned"`
	WebSearchEnabled *bool          `json:"webSearchEnabled"`
	PresetID         *string        `json:"presetId"`
	Messages         []messageInput `json:"messages"`
}

type messageInput struct {
	ID          string            `json:"id"`
	Role        string            `json:"role"`
	Content     *string           `json:"content"`
	Attachments []json.RawMessage `json:"attachments"`
	Timeline    []json.RawMessage `json:"timeline"`
}

// maxLocalMessages bounds the messages a client's thread may hold.
// maxLocalMessages 是客户端 thread 可持有的消息数上限。
const maxLocalMessages = 10000

// MessageRoles are the roles a client's message may have.
// MessageRoles 是客户端消息可拥有的角色。
var MessageRoles = []string{"user", "assistant", CompactionRole}

// ValidateLocalThread 校验客户端发送的本地 thread。
func ValidateLocalThread(data []byte) error {
	var thread threadInput
	if err := api.DecodeRequest(data, &thread); err != nil {
		return err
	}
	if len(thread.Messages) > maxLocalMessages {
		return api.Err(413, "CONTEXT_TOO_LARGE", "The thread has too many messages")
	}
	for _, message := range thread.Messages {
		if !slices.Contains(MessageRoles, message.Role) {
			return api.Err(400, "BAD_REQUEST", "A message role must be user, assistant or compaction")
		}
		if message.ID != "" && !api.IsIdentifier(message.ID) {
			return api.Err(400, "BAD_REQUEST", "A message id must be a valid identifier")
		}
	}
	return nil
}
