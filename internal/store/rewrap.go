package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// Legacy rows and their attachments: rows older clients sealed in a format
// without a key id, which a read reseals under the current key, moving the
// attachments they embed keys for into buckets.
// 旧格式 row 及其 attachment：旧客户端以不带 key id 的格式封存的 row，读取时会在当前 key 下 reseal，
// 并将其内嵌 key 的 attachment 移入 buckets。

const (
	// legacyAttachmentKeySize is the per-attachment key older clients
	// embedded in the chat.
	// legacyAttachmentKeySize 是旧客户端在 chat 中内嵌的每个 attachment 的 key 长度。
	legacyAttachmentKeySize = 32
	// bucketsCleanupTimeout bounds the removal of a deleted chat's
	// attachments, which outlives the request.
	// bucketsCleanupTimeout 为已删除 chat 的 attachment 清理设界，该清理会在请求结束后继续。
	bucketsCleanupTimeout = 30 * time.Second
)

// rewrapRow reseals a legacy row under target and writes it back at the
// revision it was read at. A chat's legacy attachments move into buckets
// first, under the attachment id and the key the chat embeds, so the
// embedded key keeps addressing them. Returns the new etag.
// rewrapRow 将旧格式 row 在 target 下重新封存，并在读取时的修订版本上写回。
// chat 的旧 attachment 会先移入 buckets，使用其 attachment id 与 chat 内嵌的 key，
// 这样内嵌的 key 仍能寻址它们。返回新的 etag。
func (r *Rows) rewrapRow(ctx context.Context, p *auth.Principal, scope, id string, plaintext []byte, priorETag string, target seal.Key) (string, error) {
	key, kidHex := target.Bytes, target.KeyIDHex
	sealed := plaintext
	if scope == "chat" {
		normalized, err := r.rewrapChatAttachments(ctx, p, id, plaintext)
		if err != nil {
			return "", err
		}
		if normalized != nil {
			sealed = normalized
			defer seal.Zero(normalized)
		}
	}
	aad := seal.AAD{KeyIDHex: kidHex, Scope: seal.Scope(scope), ID: aadID(scope, id), ClerkUserID: p.ID}
	payloadAAD, err := seal.CanonicalPayloadAAD(aad)
	if err != nil {
		return "", err
	}
	envelope, err := seal.Encrypt(key, sealed, aad)
	if err != nil {
		return "", err
	}
	nonce := "rewrap:" + kidHex + ":" + id + ":" + priorETag
	opKey, err := seal.DeriveOpHashKey(key)
	if err != nil {
		return "", err
	}
	defer seal.Zero(opKey)
	resp, err := r.Controlplane.PutBlob(ctx, controlplane.PutBlobRequest{
		Scope:          scope,
		ID:             id,
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kidHex,
		IfMatch:        priorETag,
		IdempotencyKey: nonce,
		Rewrap:         true,
		Ciphertext:     envelope,
		OperationHash: seal.ComputeOperationHash(opKey, seal.CanonicalInput{
			Method:         http.MethodPost,
			Path:           controlplane.RewrapPath,
			KeyIDHex:       kidHex,
			IfMatch:        priorETag,
			IdempotencyKey: nonce,
			Body:           sealed,
			AAD:            payloadAAD,
		}),
	})
	if err != nil {
		return "", err
	}
	return resp.ETag, nil
}

// rewrapChatAttachments moves every image attachment a chat embeds a key
// for into buckets, under the same id and key. A key older clients stored
// as `key` is copied to `encryptionKey`, so later reads address the
// bucket directly; `key` stays for clients that only know it. Returns the
// chat with that change, or nil when the plaintext is not a chat or no
// key needed copying, so the row reseals as it is.
// rewrapChatAttachments 将 chat 内嵌了 key 的每个图片 attachment 移入 buckets（使用相同的 id 与 key）。
// 旧客户端存于 `key` 字段的 key 会被复制到 `encryptionKey`，使后续读取能直接寻址 bucket；
// `key` 字段则保留，以兼容只认识它的客户端。返回带有该改动的 chat；当明文不是 chat
// 或没有需要复制的 key 时返回 nil，从而按原样 reseal 该 row。
func (r *Rows) rewrapChatAttachments(ctx context.Context, p *auth.Principal, chatID string, plaintext []byte) ([]byte, error) {
	// Numbers stay as written so re-marshaling the chat cannot round a
	// large size or timestamp, or turn 1.0 into 1.
	// 数字保持原样，使重新序列化 chat 时不会对大尺寸或时间戳做四舍五入，也不会把 1.0 变成 1。
	dec := json.NewDecoder(bytes.NewReader(plaintext))
	dec.UseNumber()
	var parsed map[string]any
	if err := dec.Decode(&parsed); err != nil {
		return nil, nil
	}
	rawMessages, ok := parsed["messages"].([]any)
	if !ok {
		return nil, nil
	}
	// Every attachment is attempted even after one fails, so the next
	// read starts from a smaller backlog; the reseal still fails as soon
	// as any one did, since a resealed chat that still embeds a legacy
	// attachment could never be read whole again.
	// 即使某个 attachment 失败，其余仍会尝试，这样下一次读取面对的是更小的积压；
	// 但只要有任何一个失败，reseal 就会失败，因为仍内嵌旧 attachment 的已 reseal chat 无法再被完整读取。
	var failures []error
	normalized := false
	for _, m := range rawMessages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		rawAtts, ok := msg["attachments"].([]any)
		if !ok {
			continue
		}
		for _, a := range rawAtts {
			att, ok := a.(map[string]any)
			if !ok || att["type"] != "image" {
				continue
			}
			rawID, _ := att["id"].(string)
			rawKey, _ := att["encryptionKey"].(string)
			keyFromLegacyField := false
			if rawKey == "" {
				rawKey, _ = att["key"].(string)
				keyFromLegacyField = rawKey != ""
			}
			if rawID == "" || rawKey == "" {
				continue
			}
			if err := r.promoteAttachment(ctx, p, chatID, rawID, rawKey); err != nil {
				failures = append(failures, err)
				continue
			}
			if keyFromLegacyField {
				att["encryptionKey"] = rawKey
				normalized = true
			}
		}
	}
	if len(failures) > 0 {
		return nil, errors.Join(failures...)
	}
	if normalized {
		return json.Marshal(parsed)
	}
	return nil, nil
}

// promoteAttachment reads one legacy attachment from the control plane,
// opens it with the key the chat embeds, stores the bytes in buckets
// under the same id and key, and registers the attachment as held there.
// promoteAttachment 从 control plane 读取一个旧 attachment，用 chat 内嵌的 key 打开，
// 将字节以相同 id 与 key 存入 buckets，并把该 attachment 注册为已存放在那里。
func (r *Rows) promoteAttachment(ctx context.Context, p *auth.Principal, chatID, attID, legacyKeyB64 string) error {
	if !r.Buckets.Configured() {
		return errors.New("rewrap: buckets backend not configured")
	}
	ciphertext, err := r.Controlplane.GetLegacyAttachment(ctx, string(p.JWT), p.ID, attID)
	if err != nil {
		// A missing attachment has nothing to move; one already moved is
		// addressed by the embedded key as it is.
		// 缺失的 attachment 无需迁移；已迁移的则已由内嵌的 key 原样寻址。
		if errors.Is(err, controlplane.ErrLegacyAttachmentNotFound) || errors.Is(err, controlplane.ErrLegacyAttachmentGone) {
			return nil
		}
		return fmt.Errorf("rewrap: fetch legacy attachment %s: %w", attID, err)
	}
	legacyKey, err := base64.StdEncoding.DecodeString(legacyKeyB64)
	if err != nil || len(legacyKey) != legacyAttachmentKeySize {
		return fmt.Errorf("rewrap: invalid legacy attachment key for %s", attID)
	}
	defer seal.Zero(legacyKey)
	plaintext, err := openLegacyAttachment(ciphertext, legacyKey)
	if err != nil {
		return fmt.Errorf("rewrap: decrypt legacy attachment %s: %w", attID, err)
	}
	defer seal.Zero(plaintext)
	if err := r.Buckets.Put(ctx, p.ID, attID, plaintext, legacyKey); err != nil {
		return fmt.Errorf("rewrap: promote attachment %s to buckets: %w", attID, err)
	}
	// With the bytes in buckets but the index not yet told, the next read
	// registers them again; storing the same id and key is idempotent.
	// 字节已入 buckets 而索引尚未被告知时，下一次读取会再次注册它们；以相同 id 与 key 存储是幂等的。
	if err := r.Controlplane.RegisterAttachmentIndex(ctx, string(p.JWT), p.ID, attID, chatID); err != nil {
		return fmt.Errorf("rewrap: register attachment index %s: %w", attID, err)
	}
	return nil
}

// deleteBucketAttachments removes the attachments of deleted chats from
// buckets, best effort. The ids come from the control plane's answer to
// the delete, never from a chat's plaintext, which could name another
// user's attachment. An orphan left in buckets is unaddressable without
// the per-attachment key, which went with the chat.
// deleteBucketAttachments 尽最大努力从 buckets 删除已删除 chat 的 attachment。
// 这些 id 来自 control plane 对删除操作的应答，绝不来自 chat 明文（后者可能指向其他用户的 attachment）。
// 遗留在 buckets 中的孤儿没有每个 attachment 的 key 就无法寻址，而该 key 已随 chat 一并消失。
func deleteBucketAttachments(ctx context.Context, bk *buckets.Client, owner string, ids []string) {
	if !bk.Configured() || len(ids) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bucketsCleanupTimeout)
	defer cancel()
	for _, attID := range ids {
		_ = bk.Delete(cleanupCtx, owner, attID)
	}
}

// openLegacyAttachment opens an attachment as older clients sealed it:
// a nonce, then AES-GCM ciphertext and tag, with no AAD.
// openLegacyAttachment 按旧客户端的封存方式打开 attachment：一个 nonce，
// 随后是 AES-GCM 密文与 tag，不带 AAD。
func openLegacyAttachment(blob, key []byte) ([]byte, error) {
	if len(blob) < seal.NonceSize {
		return nil, errors.New("rewrap: attachment ciphertext too short")
	}
	return seal.GCMOpen(key, blob[:seal.NonceSize], blob[seal.NonceSize:], nil)
}
