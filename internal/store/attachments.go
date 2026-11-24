package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"golang.org/x/crypto/hkdf"
)

// Attachments are the bytes a chat's attachments hold: stored in buckets
// under a key of their own, which the chat embeds, and indexed by the
// control plane under the chat, which keeps them while the chat names
// them.
// 附件（attachment）是 chat 的附件所承载的字节：以各自的 key 存入 buckets，该 key 由 chat 内嵌，
// 并由 control plane 在 chat 下建立索引；只要 chat 仍引用它们，索引就会保留。

const (
	// attachmentIDBytes is the length of an attachment id before hex.
	// attachmentIDBytes 是 attachment id 在十六进制编码前的字节长度。
	attachmentIDBytes = 18
	// attachmentKeySize is the per-attachment key buckets seals under.
	// attachmentKeySize 是 buckets 为每个 attachment 封存所依据的 key 长度。
	attachmentKeySize = seal.KeySize
	// An attachment's id and key are derived from its upload, so the same
	// upload stored again under the same content key lands on the same
	// object. Nothing derives a stored attachment's again: reads use the
	// id and key its chat embeds.
	// attachment 的 id 与 key 由其上传内容派生，因此同一上传在同一 content key 下再次存储时会落到同一对象。
	// 已存储 attachment 的 id 与 key 不会被再次派生：读取时使用的是其 chat 内嵌的 id 与 key。
	attachmentIDInfo   = "tinfoil-attachment-id-v1"
	attachmentKeyInfo  = "tinfoil-attachment-key-v1"
	attachmentSaltInfo = "tinfoil-attachment-salt-v1"
	// reserveAttempts bounds the reservations of an id whose earlier
	// copy the control plane is still purging; a purge finishes within
	// seconds, and each wait is a step longer than the last.
	// reserveAttempts 限制对某个 id 的预留次数——该 id 的旧副本仍在被 control plane 清除；
	// 清除通常在数秒内完成，且每次等待都比上一次更长。
	reserveAttempts = 3
	reserveWaitStep = time.Second
)

var (
	errAttachmentMissing     = api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
	errAttachmentKeyInvalid  = api.Err(422, "ATTACHMENT_UNSUPPORTED", "The attachment key is invalid")
	errAttachmentKeyMismatch = api.Err(422, "ATTACHMENT_UNSUPPORTED", "The attachment key does not match")
	errAttachmentNotStored   = api.Err(502, "UPSTREAM_REFUSED", "The attachment could not be stored")
	errAttachmentNotRead     = api.Err(502, "UPSTREAM_REFUSED", "The attachment could not be read")
	errAttachmentKeyless     = errors.New("an attachment is derived under a content key")
)

// PutAttachment stores data as an attachment of chatID and returns its id
// and the key that opens it, base64, as a chat embeds it. The same upload
// stored again under the same content key is the same id and key over the
// same object; the same client id with other bytes, or under another chat,
// is another object.
// PutAttachment 将 data 作为 chatID 的 attachment 存储，并返回其 id 以及用于打开它的 key（base64，
// 正如 chat 所内嵌的形式）。同一上传在同一 content key 下再次存储，会得到同一 id、同一 key 与同一对象；
// 同一客户端 id 搭配不同字节、或位于另一个 chat 下，则是另一个对象。
func (s *Store) PutAttachment(ctx context.Context, p *auth.Principal, contentKey auth.ContentKey, chatID, clientID string, data []byte) (string, string, error) {
	defer clear(contentKey[:])
	if chatID == "" || clientID == "" {
		return "", "", api.Err(400, "BAD_REQUEST", "a chat id and an attachment id are required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsPutAttachment(chatID) {
		return "", "", errRunScope
	}
	id, key, err := attachmentMaterials(contentKey, clientID, chatID, p.ID, data)
	if err != nil {
		return "", "", err
	}
	defer seal.Zero(key)
	// The reservation lets the control plane's sweep find bytes whose
	// chat is never saved, so it comes before the bytes.
	// 预留让 control plane 的 sweep 能找到那些 chat 从未保存的字节，因此它发生在字节之前。
	if err := s.reserveAttachment(ctx, p, id, chatID); err != nil {
		return "", "", err
	}
	if err := s.Buckets.Put(ctx, p.ID, id, data, key); err != nil {
		return "", "", errAttachmentNotStored
	}
	if err := s.Controlplane.RegisterAttachmentIndex(ctx, string(p.JWT), p.ID, id, chatID); err != nil {
		// Only a refusal proves no index row names the bytes, so only
		// then are they removed here. After any other failure the
		// control plane may hold the row, and its sweep, which checks
		// the index first, decides.
		// 只有被拒绝才证明没有任何索引 row 引用这些字节，因此也只有此时才在这里删除它们。
		// 其他失败后，control plane 可能仍持有该 row，此时由先检查索引的 sweep 决定。
		if refusedByControlplane(err) {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bucketsCleanupTimeout)
			_ = s.Buckets.Delete(cleanupCtx, p.ID, id)
			cancel()
		}
		return "", "", FromControlplane(err)
	}
	return id, base64.StdEncoding.EncodeToString(key), nil
}

// RetireAttachments removes attachments stored for a chat that never came
// to name them: each leaves the index, then buckets. One whose index entry
// the control plane could not drop keeps its bytes, since a row may still
// name it; the control plane's sweeps decide. The removal outlives ctx.
// RetireAttachments 删除为某个最终没有引用它们的 chat 所存储的 attachment：每个都先离开索引、再离开 buckets。
// 若 control plane 无法删除某 attachment 的索引条目，其字节会被保留，因为可能仍有 row 引用它；
// 此时由 control plane 的 sweep 决定。删除操作会在 ctx 结束后继续完成。
func (s *Store) RetireAttachments(ctx context.Context, p *auth.Principal, ids []string) {
	if len(ids) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bucketsCleanupTimeout)
	defer cancel()
	for _, id := range ids {
		if err := s.Controlplane.DeleteAttachmentIndex(cleanupCtx, string(p.JWT), p.ID, id); err != nil {
			continue
		}
		_ = s.Buckets.Delete(cleanupCtx, p.ID, id)
	}
}

// reserveAttachment has the control plane expect the bytes. A reservation
// refused while the id's earlier copy is still being purged is tried
// again shortly.
// reserveAttachment 让 control plane 预知这些字节的到来。若因该 id 的旧副本仍在清除而被拒绝预留，
// 会稍后重试。
func (s *Store) reserveAttachment(ctx context.Context, p *auth.Principal, id, chatID string) error {
	for attempt := 1; ; attempt++ {
		err := s.Controlplane.ReservePendingAttachmentWrite(ctx, string(p.JWT), p.ID, id, chatID)
		if err == nil {
			return nil
		}
		if !controlplane.IsCode(err, controlplane.StatusAttachmentPurgeInProgress) || attempt == reserveAttempts {
			return FromControlplane(err)
		}
		select {
		case <-time.After(time.Duration(attempt) * reserveWaitStep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// attachmentMaterials is an attachment's id and key, derived from who
// stores it, the chat it is under, the client's id for the upload and a
// digest of the bytes, salted with a subkey of the content key. The
// control plane knows all but the bytes, so without the salt it could
// check any guess at an attachment's bytes against its id. Each input is
// length-prefixed, so none can run into the next.
// attachmentMaterials 生成 attachment 的 id 与 key，派生自存储者、其所在的 chat、客户端为上传指定的 id
// 以及字节的摘要，并用 content key 的子 key 加盐。control plane 知道除字节外的所有信息，
// 因此若不加盐，它就能拿任何对 attachment 字节的猜测去与 id 比对。每个输入都带长度前缀，
// 从而不会与下一个输入粘连。
func attachmentMaterials(contentKey auth.ContentKey, clientID, chatID, owner string, data []byte) (string, []byte, error) {
	defer clear(contentKey[:])
	if contentKey == (auth.ContentKey{}) {
		return "", nil, errAttachmentKeyless
	}
	salt := make([]byte, seal.KeySize)
	defer seal.Zero(salt)
	if _, err := io.ReadFull(hkdf.New(sha256.New, contentKey[:], nil, []byte(attachmentSaltInfo)), salt); err != nil {
		return "", nil, err
	}
	digest := sha256.Sum256(data)
	ikm := encodeIKM(clientID, chatID, owner, hex.EncodeToString(digest[:]))
	id := make([]byte, attachmentIDBytes)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, salt, []byte(attachmentIDInfo)), id); err != nil {
		return "", nil, err
	}
	key := make([]byte, attachmentKeySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, salt, []byte(attachmentKeyInfo)), key); err != nil {
		return "", nil, err
	}
	return hex.EncodeToString(id), key, nil
}

// encodeIKM is parts as one byte string: each as its length, big-endian
// in eight bytes, then its bytes.
// encodeIKM 将各组成部分拼接为一个字节串：每个部分先写其长度（8 字节大端序），再写其字节。
func encodeIKM(parts ...string) []byte {
	total := 0
	for _, part := range parts {
		total += 8 + len(part)
	}
	out := make([]byte, 0, total)
	for _, part := range parts {
		out = binary.BigEndian.AppendUint64(out, uint64(len(part)))
		out = append(out, part...)
	}
	return out
}

// GetAttachment reads an attachment's bytes with the key the chat embeds,
// base64. Knowing the id and the key is the whole proof of access, so a
// share's reader reads one as its owner does. An attachment the index
// does not know may be one an older client stored in the control plane
// itself, which only its owner reads from there.
// GetAttachment 用 chat 内嵌的 key（base64）读取 attachment 的字节。知道 id 与 key 即是完整的访问凭证，
// 因此 share 的读者与所有者读取方式相同。索引不认识的 attachment 可能是旧客户端直接存储在 control plane
// 中的，只有其所有者能从那里读取。
func (s *Store) GetAttachment(ctx context.Context, p *auth.Principal, id, key string) ([]byte, error) {
	attKey, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(attKey) != attachmentKeySize {
		return nil, errAttachmentKeyInvalid
	}
	defer seal.Zero(attKey)
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opGetAttachment) {
		return nil, errRunScope
	}
	owner, err := s.Controlplane.ResolveAttachmentOwner(ctx, id)
	if errors.Is(err, controlplane.ErrAttachmentNotFound) {
		if p.Anonymous {
			return nil, errAttachmentMissing
		}
		return s.legacyAttachment(ctx, p, id, attKey)
	}
	if err != nil {
		return nil, FromControlplane(err)
	}
	data, err := s.Buckets.Get(ctx, owner, id, attKey)
	switch {
	case errors.Is(err, buckets.ErrNotFound):
		return nil, errAttachmentMissing
	case errors.Is(err, buckets.ErrForbidden):
		return nil, errAttachmentKeyMismatch
	case err != nil:
		return nil, errAttachmentNotRead
	}
	return data, nil
}

// legacyAttachment reads an attachment as an older client stored it, in
// the control plane itself, and opens it with the key the chat embeds.
// legacyAttachment 按旧客户端的存储方式（位于 control plane 自身）读取 attachment，
// 并用 chat 内嵌的 key 打开它。
func (s *Store) legacyAttachment(ctx context.Context, p *auth.Principal, id string, key []byte) ([]byte, error) {
	ciphertext, err := s.Controlplane.GetLegacyAttachment(ctx, string(p.JWT), p.ID, id)
	if errors.Is(err, controlplane.ErrLegacyAttachmentNotFound) || errors.Is(err, controlplane.ErrLegacyAttachmentGone) {
		return nil, errAttachmentMissing
	}
	if err != nil {
		return nil, FromControlplane(err)
	}
	data, err := openLegacyAttachment(ciphertext, key)
	if err != nil {
		return nil, errAttachmentKeyMismatch
	}
	return data, nil
}
