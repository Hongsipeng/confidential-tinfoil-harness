// Package store 在 enclave 中读写 control plane 为该调用者加密存储的 row。
package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"golang.org/x/sync/errgroup"
)

// Rows is what the control plane stores for a caller, sealed under the
// caller's content key: read, written and deleted here, in the enclave,
// with the key the request carried.
// Rows 是 control plane 为调用者存储的内容，在其 content key 下封存：在 enclave 中、
// 用请求携带的 key 在这里进行读取、写入与删除。
type Rows struct {
	Controlplane *controlplane.Client
	// Buckets holds attachments; a legacy row's attachments move into it
	// as the row is resealed, and a deleted chat's leave it.
	// Buckets 存放 attachment；旧 row 的 attachment 会在该 row 被 reseal 时移入其中，
	// 已删除 chat 的 attachment 则从中移除。
	Buckets *buckets.Client
	// Index keeps the search index in step with chats, when search is on.
	// Index 在启用 search 时让 search 索引与 chats 保持同步。
	Index ChatIndexer // nil until search moves in
	// 在 search 接入之前为 nil。
}

// ChatIndexer keeps a caller's search index in step with the chats stored:
// each chat as it is stored, and each as it is removed, after the write
// returns.
// ChatIndexer 让调用者的 search 索引与已存储的 chats 保持同步：
// 每个 chat 在写入返回后按存储与移除两种情况分别更新。
type ChatIndexer interface {
	ChatStored(p *auth.Principal, key auth.ContentKey, id string, plaintext []byte, etag string, at time.Time, revision int64)
	ChatRemoved(p *auth.Principal, key auth.ContentKey, id string, revision int64)
}

// RowStore is what the store reads and writes rows through: Rows, or a
// test's stand-in for it.
// RowStore 是 store 读写 row 所经由的接口：即 Rows，或测试中的替身。
type RowStore interface {
	AuthorizeRun(context.Context, *auth.Principal, auth.ContentKey, string, *string, time.Duration) (*auth.StorageGrant, error)
	AuthorizeStorage(context.Context, *auth.Principal, auth.ContentKey, string, string) (*auth.StorageGrant, error)
	Load(context.Context, *auth.Principal, auth.ContentKey, string, string) (*domain.StoredRow, error)
	Pull(context.Context, *auth.Principal, auth.ContentKey, string, []string) ([]domain.StoredRow, map[string]error, error)
	List(context.Context, *auth.Principal, auth.ContentKey, string, string, string, int) (Page, error)
	// Revisions is one page of the rows List would open, unopened.
	// Revisions 是 List 将打开的那些 row 中的一页，但保持未打开。
	Revisions(context.Context, *auth.Principal, string, string, string, int) ([]Revision, string, error)
	Push(context.Context, *auth.Principal, auth.ContentKey, string, *domain.StoredRow, string) (string, error)
	// Remove deletes a row whatever its revision; a missing row is gone.
	// Remove 删除一个 row，无论其修订版本如何；缺失的 row 视为已删除。
	Remove(context.Context, *auth.Principal, auth.ContentKey, string, string) error
	RemoveRevision(context.Context, *auth.Principal, auth.ContentKey, string, string) error
	// IDs lists every row of a scope, or a project's chats, unopened.
	// IDs 列出某个 scope 的全部 row，或某个 project 的 chats，保持未打开。
	IDs(context.Context, *auth.Principal, string, string) ([]string, error)
}

// Page is one page of rows, and the cursor of the next.
// Page 是一页 row，以及下一页的游标。
type Page struct {
	Rows []domain.StoredRow
	Next string
	// Rows the control plane holds but this harness cannot open, such as
	// one sealed to a retired key, and why. A page lists them rather than
	// failing, so one row cannot hide the rest.
	// control plane 持有但本 harness 无法打开的 row（例如封存到已弃用 key 下的 row）及其原因。
	// 页面会列出它们而非直接失败，这样单个 row 不会掩盖其余 row。
	Unreadable map[string]error
}

// Revision is a row as a listing names it, unopened: its id and revision,
// and for a chat the project the control plane files it under.
// Revision 是列表所命名的、未打开的 row：其 id 与修订版本，对 chat 而言还包括 control plane
// 归档它所用的 project。
type Revision struct {
	ID, ETag string
	Project  *string
}

const (
	// PullBatch is the most rows one fetch from the control plane opens.
	// PullBatch 是单次从 control plane 获取时最多打开的 row 数量。
	PullBatch = 100
	// ListStatusPage is the largest page of row ids the control plane
	// lists.
	// ListStatusPage 是 control plane 所列出的 row id 的最大页大小。
	ListStatusPage = 500
	// MaxCommitAttempts bounds the pushes of one change to a row.
	// MaxCommitAttempts 限制对某个 row 的一次改动所进行的 push 次数。
	MaxCommitAttempts = 3
	// grantLifetime is how long an operation may use the rows. A run's
	// grant lasts as long as the engine gives the run.
	// grantLifetime 是某操作可使用这些 row 的时长。run 的 grant 与其 engine 赋予该 run 的时长一致。
	grantLifetime = time.Hour
	// pullConcurrency bounds the rows fetched from the control plane at
	// once, so a full batch finishes well within a request without
	// saturating the connection pool.
	// pullConcurrency 限制同时从 control plane 获取的 row 数量，
	// 使一整批能在单个请求内尽快完成，又不会占满连接池。
	pullConcurrency = 8
	// removeAttempts bounds the deletes of a row another writer keeps
	// changing under them.
	// removeAttempts 限制对某个 row 的删除次数——该 row 可能被其他写入者不断在其下改动。
	removeAttempts = 3
	// profileSyncProtocol is the profile merge protocol this harness
	// writes, which the control plane refuses older writers for.
	// profileSyncProtocol 是本 harness 写入的 profile 合并协议，
	// control plane 会据此拒绝使用旧协议的写入者。
	profileSyncProtocol = controlplane.ProfileSyncProtocolV2
	// A page of rows is at most maxListPage; a page size outside that is
	// defaultListPage.
	// 一页 row 最多 maxListPage 个；超出该范围的页大小会回落到 defaultListPage。
	maxListPage     = 100
	defaultListPage = 50
)

// AuthorizeRun grants a run the chat it is for and the project its context
// reads, for lifetime: an existing chat's own project, including one a move
// changed only in storage metadata, or the project a new chat names.
// AuthorizeRun 为 run 授予它所属的 chat 以及其上下文读取的 project，时长为 lifetime：
// 即现有 chat 自身的 project（包括仅因移动而在存储元数据中变化的情况），
// 或新 chat 所指定的 project。
func (r *Rows) AuthorizeRun(ctx context.Context, p *auth.Principal, key auth.ContentKey, threadID string, projectID *string, lifetime time.Duration) (*auth.StorageGrant, error) {
	defer clear(key[:])
	project := domain.ValueOr(projectID, "")
	if !runIDValid(threadID) || project != "" && !runIDValid(project) {
		return nil, api.Err(400, "BAD_REQUEST", "invalid chat or project id")
	}
	rows, failed, err := r.Pull(ctx, p, key, "chat", []string{threadID})
	if err != nil {
		return nil, err
	}
	if err := failed[threadID]; err != nil {
		return nil, err
	}
	for i := range rows {
		row := &rows[i]
		var chat struct {
			ProjectID string `json:"projectId"`
		}
		err := json.Unmarshal(row.Data, &chat)
		seal.Zero(row.Data)
		switch {
		case row.ProjectSet:
			project = domain.ValueOr(row.Project, "")
		case err != nil:
			return nil, api.Err(400, "BAD_REQUEST", "invalid chat")
		default:
			project = chat.ProjectID
		}
	}
	if project != "" && !runIDValid(project) {
		return nil, api.Err(400, "BAD_REQUEST", "invalid project id")
	}
	return &auth.StorageGrant{ChatID: threadID, ProjectID: project, KeyID: keyID(key), ExpiresAt: api.Now().Add(lifetime)}, nil
}

// AuthorizeStorage grants an operation its rows: an import's writes, an
// export's reads, or a project upload's documents.
// AuthorizeStorage 为某个操作授予其 row：import 的写入、export 的读取，或 project 上传的文档。
func (r *Rows) AuthorizeStorage(ctx context.Context, p *auth.Principal, key auth.ContentKey, operation, projectID string) (*auth.StorageGrant, error) {
	defer clear(key[:])
	switch operation {
	case "import", "export":
	case "project-upload":
		if !runIDValid(projectID) {
			return nil, api.Err(400, "BAD_REQUEST", "invalid project id")
		}
	default:
		return nil, api.Err(400, "BAD_REQUEST", "invalid storage operation")
	}
	return &auth.StorageGrant{Operation: operation, ProjectID: projectID, KeyID: keyID(key), ExpiresAt: api.Now().Add(grantLifetime)}, nil
}

// Push seals a row under key and writes it at the revision it was read at,
// or creates it when it has none. The proof of possession the control
// plane checks covers the plaintext, not the sealed bytes, so a retry
// under the same nonce after a lost answer replays the first write.
// Push 将 row 在 key 下封存，并在读取时的修订版本上写入；没有修订版本时则新建。
// control plane 校验的持有证明覆盖明文而非封存字节，因此在应答丢失后以相同 nonce 重试
// 会重放第一次写入。
func (r *Rows) Push(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, row *domain.StoredRow, nonce string) (string, error) {
	defer clear(key[:])
	if !seal.Scope(scope).Valid() {
		return "", api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if row.ID == "" || nonce == "" {
		return "", api.Err(400, "BAD_REQUEST", "a row id and an idempotency key are required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsPush(key, scope, row) {
		return "", errRunScope
	}
	path, err := controlplane.PathFor(scope, row.ID)
	if err != nil {
		return "", api.Err(400, "BAD_REQUEST", "invalid row id")
	}
	kidHex := keyID(key)
	aad := seal.AAD{KeyIDHex: kidHex, Scope: seal.Scope(scope), ID: row.ID, ClerkUserID: p.ID}
	payloadAAD, err := seal.CanonicalPayloadAAD(aad)
	if err != nil {
		return "", api.Err(400, "BAD_REQUEST", "invalid envelope inputs")
	}
	envelope, err := seal.Encrypt(key[:], row.Data, aad)
	if err != nil {
		return "", FromControlplane(err)
	}
	ifMatch := controlplane.IfMatchCreateOnly
	if row.ETag != "" {
		ifMatch = row.ETag
	}
	protocol := 0
	if scope == "profile" {
		protocol = profileSyncProtocol
	}
	req := controlplane.PutBlobRequest{
		Scope:               scope,
		ID:                  row.ID,
		JWT:                 string(p.JWT),
		ClerkUserID:         p.ID,
		KeyIDHex:            kidHex,
		IfMatch:             ifMatch,
		IdempotencyKey:      nonce,
		OperationHash:       operationHash(key, http.MethodPut, path, kidHex, ifMatch, nonce, protocol, payloadAAD, row.Data),
		ProfileSyncProtocol: protocol,
		Ciphertext:          envelope,
	}
	if scope == "chat" {
		// The project is metadata beside the sealed row, so a move shows
		// in a listing without opening it; an empty one clears it. Which
		// attachments the chat still names tells the control plane what
		// to keep under it, and the activity time orders the chat list.
		// Both come from the plaintext the proof of possession covers.
		// project 是封存 row 旁的元数据，因此移动无需打开 row 即可在列表中体现；空值则清除它。
		// chat 仍引用的 attachment 告知 control plane 在其下保留什么，
		// 活动时间则用于对 chat 列表排序。两者都来自持有证明所覆盖的明文。
		req.ProjectIDSet = true
		if row.Project != nil && *row.Project != "" {
			project := *row.Project
			req.ProjectID = &project
		}
		req.AttachmentRefs = chatAttachmentRefs(row.Data)
		if len(req.AttachmentRefs) > maxAttachmentRefsPerChat {
			return "", api.Err(400, "BAD_REQUEST", fmt.Sprintf("The chat references more than %d attachments", maxAttachmentRefsPerChat))
		}
		if at, ok := chatActivityAt(row.Data); ok {
			req.ActivityAt = &at
		}
	}
	resp, err := r.Controlplane.PutBlob(ctx, req)
	if err != nil {
		return "", FromControlplane(err)
	}
	if scope == "chat" && r.Index != nil {
		// The plaintext and key exist only for this request, so the index
		// reads the chat now, and takes it once the write has returned; a
		// failure degrades search, which a rebuild repairs, rather than
		// failing a stored write.
		// 明文与 key 只存在于本次请求中，因此索引在此时读取 chat，并在写入返回后采纳；
		// 失败只会使 search 降级（可通过重建修复），而不会让已存储的写入失败。
		r.Index.ChatStored(p, key, row.ID, row.Data, resp.ETag, api.Now(), resp.SourceRevision)
	}
	return resp.ETag, nil
}

// operationHash is the proof of possession a change to a row carries: a
// MAC under a key derived from the content key, over the request as the
// control plane sees it and the plaintext, so the control plane can match
// a retry without reading either.
// operationHash 是 row 变更所携带的持有证明：在由 content key 派生的 key 下，对
// control plane 所见的请求及明文计算的 MAC，使 control plane 无需读取两者即可匹配一次重试。
func operationHash(key auth.ContentKey, method, path, kidHex, ifMatch, nonce string, protocol int, aad, plaintext []byte) string {
	opKey, _ := seal.DeriveOpHashKey(key[:])
	defer seal.Zero(opKey)
	protocolValue := ""
	if protocol > 0 {
		protocolValue = strconv.Itoa(protocol)
	}
	return seal.ComputeOperationHash(opKey, seal.CanonicalInput{
		Method:              method,
		Path:                path,
		KeyIDHex:            kidHex,
		IfMatch:             ifMatch,
		IdempotencyKey:      nonce,
		Body:                plaintext,
		AAD:                 aad,
		ProfileSyncProtocol: protocolValue,
	})
}

// Pull returns the rows it could open, one per distinct id in request
// order, and the error for each it could not. An absent id is neither.
// Pull 返回它能打开的 row（按请求顺序、每个不同 id 一个），以及每个无法打开的 id 对应的错误。
// 不存在的 id 两者皆无。
func (r *Rows) Pull(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, ids []string) ([]domain.StoredRow, map[string]error, error) {
	defer clear(key[:])
	rows, failed := []domain.StoredRow{}, map[string]error{}
	if len(ids) == 0 {
		return rows, failed, nil
	}
	if !seal.Scope(scope).Valid() {
		return nil, nil, api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsPull(key, scope, ids) {
		return nil, nil, errRunScope
	}
	target := seal.Key{Bytes: key[:], KeyIDHex: keyID(key)}
	// A legacy row is resealed under the key as it is read, but the control
	// plane accepts that only under the registered current key. One probe
	// per pull, made only once a legacy row is read, spares every row a
	// refused reseal when the key is another.
	// 旧 row 在读取时会在该 key 下 reseal，但 control plane 只接受在已注册当前 key 下的 reseal。
	// 每次 pull 只在读到旧 row 时才做一次探测，从而在其他 key 的情况下避免每个 row 都被拒绝 reseal。
	canRewrap := sync.OnceValue(func() bool { return r.currentPrimaryKeyIs(ctx, p, target.KeyIDHex) })
	for start := 0; start < len(ids); start += PullBatch {
		r.pullBatch(ctx, p, target, scope, ids[start:min(len(ids), start+PullBatch)], canRewrap, true, &rows, failed)
	}
	return rows, failed, nil
}

// pulled is one row as a fetch found it: opened, absent (neither row nor
// error), or failed; transient marks a failure a second fetch may not meet.
// pulled 表示一次获取所得到的单个 row：已打开、不存在（既无 row 也无错误），或失败；
// transient 标记的是第二次获取可能不会再遇到的失败。
type pulled struct {
	row       *domain.StoredRow
	err       error
	transient bool
}

// pullBatch fetches each distinct id once, concurrently, and appends the
// rows opened in request order. Ids the control plane could not serve are
// fetched once more together; those it still cannot are reported as such.
// pullBatch 并发地获取每个不同 id（各一次），并按请求顺序追加已打开的 row。
// control plane 未能提供的 id 会一起再获取一次；仍无法获取的则如实报告。
func (r *Rows) pullBatch(ctx context.Context, p *auth.Principal, target seal.Key, scope string, ids []string, canRewrap func() bool, retry bool, rows *[]domain.StoredRow, failed map[string]error) {
	// Concurrent reads of one legacy id would race their reseal, leaving
	// one copy at a stale revision.
	// 对同一旧 id 的并发读取会与其 reseal 竞争，导致某个副本停留在过期修订版本。
	results := make(map[string]*pulled, len(ids))
	var group errgroup.Group
	group.SetLimit(pullConcurrency)
	for _, id := range ids {
		if _, seen := results[id]; seen {
			continue
		}
		item := &pulled{}
		results[id] = item
		group.Go(func() error {
			*item = r.pullOne(ctx, p, target, scope, id, canRewrap)
			return nil
		})
	}
	_ = group.Wait()
	again, seen := []string{}, map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		item := results[id]
		switch {
		case item.transient && retry:
			again = append(again, id)
		case item.transient:
			failed[id] = ErrRowNotFetched
		case item.err != nil:
			failed[id] = item.err
		case item.row != nil:
			*rows = append(*rows, *item.row)
		}
	}
	if len(again) > 0 {
		r.pullBatch(ctx, p, target, scope, again, canRewrap, false, rows, failed)
	}
}

func (r *Rows) pullOne(ctx context.Context, p *auth.Principal, target seal.Key, scope, id string, canRewrap func() bool) pulled {
	blob, err := r.Controlplane.GetBlob(ctx, scope, id, string(p.JWT), p.ID)
	if err != nil {
		var cpe *controlplane.Error
		switch {
		case errors.As(err, &cpe) && cpe.StatusCode == http.StatusNotFound:
			return pulled{}
		case errors.As(err, &cpe) && cpe.StatusCode/100 == 4:
			return pulled{err: FromControlplane(err)}
		}
		return pulled{err: FromControlplane(err), transient: true}
	}
	keys := []seal.Key{target}
	row := &domain.StoredRow{ID: id, ETag: blob.ETag, Project: blob.ProjectID, ProjectSet: blob.ProjectIDSet, UpdatedAt: blob.UpdatedAt}
	legacy := false
	switch seal.Detect(blob.Ciphertext) {
	case seal.VersionV2:
		dec, err := seal.DecryptV2(blob.Ciphertext, keys, seal.AAD{Scope: seal.Scope(scope), ID: aadID(scope, id), ClerkUserID: p.ID})
		if errors.Is(err, seal.ErrNoMatchingKey) {
			e := api.Err(409, "KEY_MISMATCH", "The stored data uses another content key")
			e.KeyID = dec.KeyIDHex
			return pulled{err: e}
		}
		if err != nil {
			return pulled{err: errRowUnreadable}
		}
		row.Data = dec.Plaintext
	case seal.VersionV0, seal.VersionV1:
		dec, err := seal.DecryptLegacy(blob.Ciphertext, keys)
		if err != nil {
			return pulled{err: api.Err(409, "KEY_MISMATCH", "The stored data uses another content key")}
		}
		row.Data, legacy = dec.Plaintext, true
	default:
		return pulled{err: errRowUnreadable}
	}
	if trimmed := bytes.TrimSpace(row.Data); len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		seal.Zero(row.Data)
		return pulled{err: errRowUnreadable}
	}
	if legacy && canRewrap() {
		// Best effort: a row that stays legacy is still read, and the
		// next read reseals it.
		// 尽力而为：保持旧格式的 row 仍会被读取，下一次读取会将其 reseal。
		if etag, err := r.rewrapRow(ctx, p, scope, id, row.Data, blob.ETag, target); err == nil {
			row.ETag = etag
		}
	}
	r.backfillChatActivity(ctx, p, scope, id, blob, row.ETag, row.Data)
	return pulled{row: row}
}

// aadID is the id a row is sealed under. The control plane files a user's
// profile under the user id, and lists it so, while its envelope names the
// profile singleton.
// aadID 是封存 row 所用的 id。control plane 将用户的 profile 归档在用户 id 下并以该 id 列出，
// 而其 envelope 则命名为 profile 单例。
func aadID(scope, id string) string {
	if scope == "profile" {
		return seal.ProfileSingletonID
	}
	return id
}

// currentPrimaryKeyIs is whether the caller's registered current key is
// kidHex. A probe that fails answers no, which leaves rows as they are.
// currentPrimaryKeyIs 判断调用者已注册的当前 key 是否为 kidHex。
// 探测失败时回答“否”，从而使 row 保持原样。
func (r *Rows) currentPrimaryKeyIs(ctx context.Context, p *auth.Principal, kidHex string) bool {
	current, err := r.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	return err == nil && current != nil && current.KeyID == kidHex
}

// Load 读取单个 row，返回它或相应的错误。
func (r *Rows) Load(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) (*domain.StoredRow, error) {
	rows, failed, err := r.Pull(ctx, p, key, scope, []string{id})
	if err != nil {
		return nil, err
	}
	if err := failed[id]; err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].ID == id {
			return &rows[i], nil
		}
	}
	return nil, api.Err(404, "THREAD_NOT_FOUND", "The requested data was not found")
}

// listStatus is one page of rows, newest first: a project's chats, or a
// project's documents, or every row of a scope. It leaves deletes out,
// which would otherwise page it and serve the same rows again.
// listStatus 返回一页 row，按最新优先：某个 project 的 chats、某个 project 的文档，
// 或某个 scope 的全部 row。它会排除删除项，否则会使其分页并再次返回相同的 row。
func (r *Rows) listStatus(ctx context.Context, p *auth.Principal, scope, cursor, project string, limit int) ([]Revision, string, error) {
	if !seal.Scope(scope).Valid() {
		return nil, "", api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).permitsList(scope, project) {
		return nil, "", errRunScope
	}
	chatsOf := ""
	if scope == "chat" {
		chatsOf = project
	}
	list, err := r.Controlplane.ListStatus(ctx, scope, cursor, limit, string(p.JWT), p.ID, chatsOf, "desc", true)
	if err != nil {
		return nil, "", FromControlplane(err)
	}
	listed := []Revision{}
	for _, item := range list.Updates {
		// The control plane files only chats by project. Document ids name
		// their project, so select them before any document is opened.
		// control plane 只按 project 归档 chats。文档 id 会标明其 project，
		// 因此在打开任何文档之前就先行筛选。
		if scope == "project_document" && project != "" && !strings.HasPrefix(item.ID, project+"/") {
			continue
		}
		listed = append(listed, Revision{ID: item.ID, ETag: item.ETag, Project: item.ProjectID})
	}
	return listed, list.NextCursor, nil
}

// revisionIDs are the ids of listed rows, in order.
// revisionIDs 是已列出 row 的 id，按顺序排列。
func revisionIDs(listed []Revision) []string {
	ids := make([]string, len(listed))
	for i, row := range listed {
		ids[i] = row.ID
	}
	return ids
}

// IDs 列出某个 scope 的全部 row（或某个 project 的 chats）的 id，保持未打开。
func (r *Rows) IDs(ctx context.Context, p *auth.Principal, scope, project string) ([]string, error) {
	all, cursor := []string{}, ""
	for {
		listed, next, err := r.listStatus(ctx, p, scope, cursor, project, ListStatusPage)
		if err != nil {
			return nil, err
		}
		all = append(all, revisionIDs(listed)...)
		if next == "" {
			return all, nil
		}
		if next == cursor {
			return nil, api.Err(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = next
	}
}

// Revisions 返回某个 scope（或某个 project）的一页 row 列表，保持未打开。
func (r *Rows) Revisions(ctx context.Context, p *auth.Principal, scope, cursor, project string, limit int) ([]Revision, string, error) {
	if limit <= 0 || limit > maxListPage {
		limit = defaultListPage
	}
	return r.listStatus(ctx, p, scope, cursor, project, limit)
}

// List 返回某个 scope（或某个 project）的一页已打开 row，以及下一页的游标。
func (r *Rows) List(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, cursor, project string, limit int) (Page, error) {
	defer clear(key[:])
	listed, next, err := r.Revisions(ctx, p, scope, cursor, project, limit)
	if err != nil {
		return Page{}, err
	}
	page := Page{Rows: []domain.StoredRow{}, Next: next}
	if len(listed) != 0 {
		page.Rows, page.Unreadable, err = r.Pull(ctx, p, key, scope, revisionIDs(listed))
	}
	return page, err
}

// Remove deletes a row whatever its revision: it reads the revision,
// deletes at it, and tries again when another writer got in between. A
// missing row is gone. A chat's attachments go with it.
// Remove 删除一个 row，无论其修订版本如何：先读取修订版本，在其上删除，
// 若期间有其他写入者介入则重试。缺失的 row 视为已删除。chat 的 attachment 会随之一并删除。
func (r *Rows) Remove(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) error {
	defer clear(key[:])
	if !seal.Scope(scope).Valid() {
		return api.Err(400, "BAD_REQUEST", "invalid scope")
	}
	if id == "" {
		return api.Err(400, "BAD_REQUEST", "a row id is required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opRemove) {
		return errRunScope
	}
	path, err := controlplane.PathFor(scope, id)
	if err != nil {
		return api.Err(400, "BAD_REQUEST", "invalid row id")
	}
	// Each attempt is its own operation to the control plane: a replay of
	// the first could otherwise answer for a later one that deleted a
	// newer revision.
	// 每次尝试对 control plane 而言都是独立操作：否则第一次的重放可能会为
	// 删除更新修订版本的后续操作作答。
	nonce := uuid.NewString()
	for attempt := 0; attempt < removeAttempts; attempt++ {
		blob, err := r.Controlplane.GetBlob(ctx, scope, id, string(p.JWT), p.ID)
		if err != nil {
			var cpe *controlplane.Error
			if errors.As(err, &cpe) && cpe.StatusCode == http.StatusNotFound {
				// The row may be gone while the search index still names
				// it, after an earlier delete that got no further.
				// 在之前某次未继续的删除之后，row 可能已消失而 search 索引仍引用它。
				if scope == "chat" && r.Index != nil {
					r.Index.ChatRemoved(p, key, id, 0)
				}
				return nil
			}
			return FromControlplane(err)
		}
		attemptNonce := nonce
		if attempt > 0 {
			attemptNonce = fmt.Sprintf("%s:retry:%d", nonce, attempt)
		}
		resp, err := r.removeAt(ctx, p, key, scope, path, id, blob.ETag, attemptNonce)
		if err == nil {
			r.finishRemove(ctx, p, key, scope, id, resp)
			return nil
		}
		if controlplane.IsCode(err, controlplane.StatusStaleBlob) {
			continue
		}
		return FromControlplane(err)
	}
	return api.Err(409, "REVISION_CONFLICT", "The row changed during every delete attempt")
}

// RemoveRevision deletes a chat at a revision; one that changed since is a
// conflict.
// RemoveRevision 在某个修订版本上删除 chat；此后发生变化的 chat 会报冲突。
func (r *Rows) RemoveRevision(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, revision string) error {
	defer clear(key[:])
	if id == "" || revision == "" {
		return api.Err(400, "BAD_REQUEST", "a row id and a revision are required")
	}
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opRemove) {
		return errRunScope
	}
	path, err := controlplane.PathFor("chat", id)
	if err != nil {
		return api.Err(400, "BAD_REQUEST", "invalid row id")
	}
	resp, err := r.removeAt(ctx, p, key, "chat", path, id, revision, uuid.NewString())
	if err != nil {
		return FromControlplane(err)
	}
	r.finishRemove(ctx, p, key, "chat", id, resp)
	return nil
}

func (r *Rows) removeAt(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, path, id, revision, nonce string) (*controlplane.DeleteBlobResponse, error) {
	return r.Controlplane.DeleteBlob(ctx, controlplane.DeleteBlobRequest{
		Scope:          scope,
		ID:             id,
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		IfMatch:        revision,
		IdempotencyKey: nonce,
		OperationHash:  operationHash(key, http.MethodDelete, path, keyID(key), revision, nonce, 0, nil, nil),
	})
}

// finishRemove sees a deleted chat out: its attachments, as the control
// plane names them, leave buckets, and the search index forgets it.
// finishRemove 完成已删除 chat 的善后：按 control plane 所列的 attachment 使其离开 buckets，
// 并让 search 索引遗忘该 chat。
func (r *Rows) finishRemove(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string, resp *controlplane.DeleteBlobResponse) {
	if scope != "chat" {
		return
	}
	deleteBucketAttachments(ctx, r.Buckets, p.ID, resp.WipedV2Attachments)
	if r.Index != nil {
		r.Index.ChatRemoved(p, key, id, resp.SourceRevision)
	}
}
