// Package store 从 chat 明文解析附件引用与活动时间。
package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
)

// maxAttachmentRefsPerChat is the most attachments the control plane
// accepts a chat naming in one push.
// maxAttachmentRefsPerChat 是 control plane 在一次 push 中接受单个 chat 引用的最大 attachment 数量。
const maxAttachmentRefsPerChat = 500

// chatAttachmentRefs is the distinct ids of the stored attachments a chat
// names: every attachment carrying a server key. One without a key was
// never uploaded, so has no index row to keep alive. Only the fields
// liveness needs are read, so a chat field of an unexpected type cannot
// hide a live attachment. A plaintext without that shape returns nil: its
// references are unknown, and the control plane keeps every blob under
// the chat.
// chatAttachmentRefs 是 chat 所引用的已存储 attachment 的去重 id：即每个携带服务器 key 的 attachment。
// 没有 key 的从未上传，因此没有需要保活的索引 row。只读取保活所需的字段，
// 这样类型意外的 chat 字段不会掩盖一个存活的 attachment。形状不符的明文返回 nil：
// 其引用未知，control plane 会保留该 chat 下的每个 blob。
func chatAttachmentRefs(plaintext []byte) []string {
	var chat struct {
		Messages []struct {
			Attachments []map[string]json.RawMessage `json:"attachments"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(plaintext, &chat); err != nil {
		return nil
	}
	refs := []string{}
	seen := make(map[string]struct{})
	for _, message := range chat.Messages {
		for _, attachment := range message.Attachments {
			var id string
			if json.Unmarshal(attachment["id"], &id) != nil || id == "" || attachmentServerKey(attachment) == "" {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			refs = append(refs, id)
		}
	}
	return refs
}

// attachmentServerKey is the per-attachment key stored on an attachment,
// under the field older clients used as well as the current one.
// attachmentServerKey 是存储在 attachment 上的每个 attachment 的 key，
// 既查旧客户端使用的字段，也查当前字段。
func attachmentServerKey(raw map[string]json.RawMessage) string {
	for _, field := range []string{"encryptionKey", "key"} {
		var value string
		if json.Unmarshal(raw[field], &value) == nil && value != "" {
			return value
		}
	}
	return ""
}

// chatActivityAt is a chat's last conversation activity: the later of its
// creation time and its newest message timestamp. Clients order the chat
// list by the same rule, so it must stay in step with the web app and
// iOS. Timestamps that are missing or not RFC 3339 strings are ignored,
// and ok is false when none remain.
// chatActivityAt 是 chat 最后一次会话活动时间：取其创建时间与最新消息时间戳中的较晚者。
// 客户端按同一规则对 chat 列表排序，因此必须与 web 应用及 iOS 保持一致。
// 缺失或不是 RFC 3339 字符串的时间戳会被忽略，当没有剩余时 ok 为 false。
func chatActivityAt(plaintext []byte) (activityAt time.Time, ok bool) {
	var chat struct {
		CreatedAt json.RawMessage   `json:"createdAt"`
		Messages  []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(plaintext, &chat); err != nil {
		return time.Time{}, false
	}
	consider := func(raw json.RawMessage) bool {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return false
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return false
		}
		if parsed.After(activityAt) {
			activityAt = parsed
		}
		return true
	}
	consider(chat.CreatedAt)
	for _, rawMessage := range chat.Messages {
		var message struct {
			Timestamp json.RawMessage `json:"timestamp"`
			CreatedAt json.RawMessage `json:"createdAt"`
		}
		if json.Unmarshal(rawMessage, &message) == nil {
			if !consider(message.CreatedAt) {
				consider(message.Timestamp)
			}
		}
	}
	return activityAt, !activityAt.IsZero()
}

// backfillChatActivity records the activity time of a chat whose row was
// written before the control plane tracked it, from the plaintext just
// opened. etag is the row's current etag, which differs from the one read
// when the pull resealed the row. It is best effort: the control plane
// applies it only while the row is still at etag and has no value, and a
// failure leaves the row sorting by its write time until the next pull.
// backfillChatActivity 为那些 row 写在 control plane 开始追踪活动时间之前的 chat
// 记录活动时间，取自刚打开的明文。etag 是 row 当前的 etag，可能与 pull reseal 该 row 时
// 读取到的不同。这是尽力而为：control plane 只在 row 仍处于该 etag 且尚无值时才应用它，
// 失败则让 row 继续按写入时间排序，直到下一次 pull。
func (r *Rows) backfillChatActivity(ctx context.Context, p *auth.Principal, scope, id string, blob *controlplane.GetBlobResponse, etag string, plaintext []byte) {
	if scope != "chat" || blob.ActivityAt != nil {
		return
	}
	activityAt, ok := chatActivityAt(plaintext)
	if !ok {
		return
	}
	_ = r.Controlplane.BackfillChatActivity(ctx, string(p.JWT), p.ID, id, etag, activityAt)
}
