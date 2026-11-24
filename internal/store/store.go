package store

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/buckets"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
)

// Store 是 harness 读写调用者数据（rows、attachments、keys）的统一入口。
type Store struct {
	Repair Repairer
	Rows   RowStore
	// Controlplane keeps the attachment index, the key registry and the
	// change ledger; Buckets holds attachment bytes.
	// Controlplane 维护 attachment 索引、key 注册表与变更账本；Buckets 存放 attachment 字节。
	Controlplane *controlplane.Client
	Buckets      *buckets.Client
	// Reseals runs each caller's reseal of the rows an older key sealed.
	// Reseals 运行每个调用者对旧 key 所封存 row 的 reseal。
	Reseals *jobs.Runner[ResealReport]
	// Search forgets a caller's search index when a new key fences it off.
	// Search 在新的 key 隔离某个调用者的 search 索引时使其遗忘。
	Search SearchResetter

	summariesOnce sync.Once
	summaries     *expirable.LRU[summaryKey, keptSummary]
}

// SearchResetter drops what is held for a caller's search index once the
// control plane has fenced it off, and the object it named.
// SearchResetter 在 control plane 隔离某个调用者的 search 索引后，丢弃为其保留的内容及其所指对象。
type SearchResetter interface {
	Forget(ctx context.Context, owner, objectKey string)
}

// The store is the harness's typed view of what a caller keeps: chats, the
// profile, projects and their documents, attachments and keys. Nothing
// else reaches the rows. Rows are read through the legacy reader, so a row any
// client wrote reads in the current format.
// store 是 harness 对调用者所保存内容的类型化视图：chats、profile、projects 及其文档、
// attachments 与 keys。除此之外不会有其他内容到达这些 row。row 经由旧格式读取器读取，
// 因此任何客户端写入的 row 都会以当前格式读出。

var errEveryCommitConflicted = api.Err(409, "REVISION_CONFLICT", "The row changed during every commit attempt")

// Push writes a row at the revision it was read at; an empty one creates
// it. A push whose outcome is unknown is sent again under the same
// idempotency key, which the control plane answers with the first push's outcome.
// Push 在读取时的修订版本上写入 row；没有修订版本时则新建。结果未知的 push
// 会以相同的幂等 key 再次发送，control plane 会以第一次 push 的结果应答。
func (s *Store) Push(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope string, row *domain.StoredRow) error {
	nonce := uuid.NewString()
	for attempt := 1; ; attempt++ {
		etag, err := s.Rows.Push(ctx, p, key, scope, row, nonce)
		if err == nil {
			row.ETag = etag
			return nil
		}
		if api.AsError(err).Code != "SYNC_OUTCOME_UNKNOWN" || attempt == MaxCommitAttempts {
			return err
		}
	}
}

// CommitRow applies edit to a row as read now and pushes it at that
// revision, again after a conflict, so a conflict never overwrites another
// writer's change with an old copy. With create, a missing row is made.
// CommitRow 将 edit 应用于当前读取的 row，并在该修订版本上 push；发生冲突后重试，
// 因此冲突绝不会用旧副本覆盖其他写入者的改动。启用 create 时，缺失的 row 会被创建。
func (s *Store) CommitRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string, create bool, edit func(row *domain.StoredRow, exists bool) error) error {
	for attempt := 0; attempt < MaxCommitAttempts; attempt++ {
		row, err := s.Rows.Load(ctx, p, key, scope, id)
		exists := err == nil
		if !exists {
			if !create || api.AsError(err).HTTP != 404 {
				return err
			}
			row = &domain.StoredRow{ID: id}
		}
		if err := edit(row, exists); err != nil {
			return err
		}
		err = s.Push(ctx, p, key, scope, row)
		if err == nil || api.AsError(err).Code != "REVISION_CONFLICT" {
			return err
		}
	}
	return errEveryCommitConflicted
}

// EachRowOrUnreadable visits every row of a scope, or of a project's chats,
// and names each row the control plane holds that the key cannot open.
// EachRowOrUnreadable 遍历某个 scope 或某个 project 的 chats 的每个 row，
// 并逐一指出 control plane 持有但该 key 无法打开的 row。
func (s *Store) EachRowOrUnreadable(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, project string, visit func(*domain.StoredRow) error, unreadable func(string)) error {
	return s.EachPage(ctx, p, key, scope, project, func(rows []domain.StoredRow, unopened []string) error {
		for _, id := range unopened {
			unreadable(id)
		}
		for _, row := range rows {
			if err := visit(&row); err != nil {
				return err
			}
		}
		return nil
	})
}

// EachPage visits every page of a scope's rows, or of a project's chats,
// with the ids of the rows on it the key cannot open. Each row is visited
// once: reading a chat written before the control plane kept its activity
// records it, which files the chat behind the pages already listed, and
// the next pages list it again.
// EachPage 遍历某 scope 或某 project 的 chats 的每一页 row，并携带其中 key 无法打开的 row 的 id。
// 每个 row 只访问一次：读取写在 control plane 开始记录活动时间之前的 chat 会记录其活动时间，
// 从而把该 chat 排到已列出页面的后面，导致后续页再次列出它。
func (s *Store) EachPage(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, project string, visit func(rows []domain.StoredRow, unreadable []string) error) error {
	cursor, seen := "", map[string]bool{}
	again := func(id string) bool {
		listed := seen[id]
		seen[id] = true
		return listed
	}
	for {
		page, err := s.Rows.List(ctx, p, key, scope, cursor, project, 100)
		if err != nil {
			return err
		}
		rows := slices.DeleteFunc(page.Rows, func(row domain.StoredRow) bool { return again(row.ID) })
		unreadable := slices.DeleteFunc(slices.Sorted(maps.Keys(page.Unreadable)), again)
		if err := visit(rows, unreadable); err != nil {
			return err
		}
		if page.Next == "" {
			return nil
		}
		if cursor == page.Next {
			return api.Err(502, "UPSTREAM_REFUSED", "The storage cursor did not advance")
		}
		cursor = page.Next
	}
}

// EachRow 遍历某个 scope（或某个 project 的 chats）的每个 row。
func (s *Store) EachRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, project string, visit func(*domain.StoredRow) error) error {
	return s.EachRowOrUnreadable(ctx, p, key, scope, project, visit, func(string) {})
}

// RefuseUnreadable fails a read that would leave out a row this harness
// cannot read, as a turn of a project does: it reads every document, and one
// left out would change what the model is told.
// RefuseUnreadable 使会遗漏本 harness 无法读取的 row 的读取失败，
// 正如 project 的一个 turn 所做的：它会读取每个文档，遗漏任何一个都会改变告知 model 的内容。
func RefuseUnreadable(_ string, err error) error { return err }

// Chats
// 聊天

// LoadThread 读取单个 chat 并解析为 ThreadRow。
func (s *Store) LoadThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) (*domain.ThreadRow, error) {
	row, err := s.Rows.Load(ctx, p, key, "chat", id)
	if err != nil {
		return nil, err
	}
	return domain.ReadThreadRow(row)
}

// ThreadBatch 读取多个 chat（按 id），返回可读的那些 chat 的 ThreadRow。
func (s *Store) ThreadBatch(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]*domain.ThreadRow, error) {
	rows, failed, err := s.Rows.Pull(ctx, p, key, "chat", ids)
	if err != nil {
		return nil, err
	}
	for _, err := range failed {
		if api.AsError(err).HTTP != 409 {
			return nil, err
		}
	}
	threads := make([]*domain.ThreadRow, 0, len(rows))
	for i := range rows {
		// A chat this harness cannot read is left out, as one the key cannot
		// open is.
		// 本 harness 无法读取的 chat 会被略过，正如 key 无法打开的一样。
		if t, err := domain.ReadThreadRow(&rows[i]); err == nil {
			threads = append(threads, t)
		}
	}
	return threads, nil
}

// RemoveThreadRevision 在某个修订版本上删除 chat。
func (s *Store) RemoveThreadRevision(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, revision string) error {
	return s.Rows.RemoveRevision(ctx, p, key, id, revision)
}

// Repairer places the attachments a pushed chat names that the control
// plane does not hold under it.
// Repairer 放置被 push 的 chat 所引用、但 control plane 尚未在其下持有的 attachment。
type Repairer interface {
	RepairAttachments(context.Context, *auth.Principal, auth.ContentKey, *domain.Thread, []string) error
}

// SaveThread pushes a chat at the revision it was read at. A chat naming
// attachments the control plane does not hold under it is repaired once and pushed again.
// Its summary is kept at the new revision, so a listing need not open it.
// SaveThread 在读取时的修订版本上 push chat。引用了 control plane 未在其下持有的 attachment 的 chat
// 会先修复一次再重新 push。其 summary 会保留在新的修订版本上，因此列表无需打开它。
func (s *Store) SaveThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, row *domain.ThreadRow) error {
	row.RecordVersion = domain.RecordVersion
	row.UpdatedAt = api.Timestamp()
	for repaired := false; ; repaired = true {
		stored := domain.StoredRow{ID: row.ID, ETag: row.ETag, Data: api.Raw(row.Thread), Project: row.ProjectID, ProjectSet: true}
		err := s.Push(ctx, p, key, "chat", &stored)
		if err == nil {
			row.ETag = stored.ETag
			s.keepSummary(p, keyID(key), row)
			return nil
		}
		if e := api.AsError(err); e.Code != "MISSING_ATTACHMENT" || repaired {
			return err
		} else if err := s.Repair.RepairAttachments(ctx, p, key, &row.Thread, e.Missing); err != nil {
			return err
		}
	}
}

// MutateThread applies change to a stored chat as read now, again after a
// conflict, so a conflict never overwrites another writer's change.
// MutateThread 将 change 应用于当前读取的已存储 chat，冲突后重试，
// 因此冲突绝不会覆盖其他写入者的改动。
func (s *Store) MutateThread(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, change func(*domain.Thread) error) (*domain.ThreadRow, error) {
	for attempt := 0; attempt < MaxCommitAttempts; attempt++ {
		row, err := s.LoadThread(ctx, p, key, id)
		if err != nil {
			return nil, err
		}
		if err := change(&row.Thread); err != nil {
			return nil, err
		}
		err = s.SaveThread(ctx, p, key, row)
		if err == nil {
			return row, nil
		}
		if api.AsError(err).Code != "REVISION_CONFLICT" {
			return nil, err
		}
	}
	return nil, errEveryCommitConflicted
}

// A deletion needs no revision: the store reads the row's, retries a race, and
// treats a missing row as deleted.
// 删除无需修订版本：store 读取 row 的修订版本、在竞争时重试，并将缺失的 row 视为已删除。
func (s *Store) RemoveRow(ctx context.Context, p *auth.Principal, key auth.ContentKey, scope, id string) error {
	return s.Rows.Remove(ctx, p, key, scope, id)
}

// ChatIDs lists every chat id of a user, or of a project, unopened.
// ChatIDs 列出用户（或某个 project）的全部 chat id，保持未打开。
func (s *Store) ChatIDs(ctx context.Context, p *auth.Principal, project string) ([]string, error) {
	return s.Rows.IDs(ctx, p, "chat", project)
}

// SearchResults are the chats a search found, in its order, and how many of
// them this harness cannot open, which are left out. A chat deleted since
// it was indexed is not a result. One the control plane could not serve in
// time fails the search, which may find it when run again.
// SearchResults 是 search 找到的 chats（按其顺序），以及其中本 harness 无法打开的数量（会被略过）。
// 自索引后被删除的 chat 不是结果。control plane 未能及时提供的 chat 会使 search 失败，
// 再次运行时或许能找到它。
func (s *Store) SearchResults(ctx context.Context, p *auth.Principal, key auth.ContentKey, ids []string) ([]*domain.ThreadRow, int, error) {
	rows, failed, err := s.Rows.Pull(ctx, p, key, "chat", ids)
	if err != nil {
		return nil, 0, err
	}
	pulled := map[string]*domain.StoredRow{}
	for i := range rows {
		pulled[rows[i].ID] = &rows[i]
	}
	found, unreadable := []*domain.ThreadRow{}, 0
	for _, id := range ids {
		row, pulledRow := pulled[id]
		switch err := failed[id]; {
		case errors.Is(err, ErrRowNotFetched):
			return nil, 0, err
		case err != nil:
			unreadable++
		case pulledRow:
			if thread, err := domain.ReadThreadRow(row); err != nil {
				unreadable++
			} else {
				found = append(found, thread)
			}
		}
	}
	return found, unreadable, nil
}

// Profile
// 个人资料

// LoadProfile is the user's profile, or nil without one.
// LoadProfile 是用户的 profile，没有时返回 nil。
func (s *Store) LoadProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey) (*domain.StoredProfile, error) {
	row, err := s.Rows.Load(ctx, p, key, "profile", "profile")
	if err != nil {
		if api.AsError(err).HTTP == 404 {
			return nil, nil
		}
		return nil, err
	}
	profile, err := domain.ReadProfile(row.Data)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

// MutateProfile 将 change 应用于用户的 profile 并保存；未发生变化时视为无操作。
func (s *Store) MutateProfile(ctx context.Context, p *auth.Principal, key auth.ContentKey, change func(*domain.StoredProfile) error) (domain.StoredProfile, error) {
	var profile domain.StoredProfile
	err := s.CommitRow(ctx, p, key, "profile", "profile", true, func(row *domain.StoredRow, exists bool) error {
		profile = domain.StoredProfile{}
		if exists {
			var err error
			if profile, err = domain.ReadProfile(row.Data); err != nil {
				return err
			}
		}
		before := api.Raw(profile)
		if err := change(&profile); err != nil {
			return err
		}
		if bytes.Equal(before, api.Raw(profile)) {
			return errProfileUnchanged
		}
		profile.PrepareWrite(row.Data, nextRevision(row, exists), api.Now())
		row.Data = api.Raw(profile)
		return nil
	})
	if errors.Is(err, errProfileUnchanged) {
		err = nil
	}
	return profile, err
}

var errProfileUnchanged = errors.New("profile unchanged")

// nextRevision is the revision a push of row creates, as the control plane
// counts them, or 0 when its revision is not a count. A row created where one
// was deleted continues the deleted row's count, which a load cannot see.
// nextRevision 是 row 的 push 将创建的修订版本（按 control plane 的计数方式），
// 当其修订版本不是计数时返回 0。在已删除 row 处新建的 row 会沿用被删 row 的计数，
// 这是 load 无法看到的。
func nextRevision(row *domain.StoredRow, exists bool) int {
	if !exists {
		return 1
	}
	n, err := strconv.Atoi(row.ETag)
	if err != nil {
		return 0
	}
	return n + 1
}

// Projects
// 项目

// LoadProject 读取单个 project 并解析为 Project。
func (s *Store) LoadProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string) (domain.Project, error) {
	row, err := s.Rows.Load(ctx, p, key, "project", id)
	if err != nil {
		return domain.Project{}, err
	}
	return domain.ReadProject(row)
}

// ProjectPage 返回一页 project（按游标），以及下一页的游标。
func (s *Store) ProjectPage(ctx context.Context, p *auth.Principal, key auth.ContentKey, cursor string, limit int) ([]domain.Project, string, error) {
	page, err := s.Rows.List(ctx, p, key, "project", cursor, "", limit)
	if err != nil {
		return nil, "", err
	}
	projects := make([]domain.Project, 0, len(page.Rows))
	for i := range page.Rows {
		// A project this harness cannot read is left out, as one the key cannot
		// open is.
		// 本 harness 无法读取的 project 会被略过，正如 key 无法打开的一样。
		if project, err := domain.ReadProject(&page.Rows[i]); err == nil {
			projects = append(projects, project)
		}
	}
	return projects, page.Next, nil
}

// MutateProject applies change to a project, or with create to a new one.
// MutateProject 将 change 应用于某个 project，或（启用 create 时）应用于新建的 project。
func (s *Store) MutateProject(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, create bool, change func(*domain.Project)) (domain.Project, error) {
	var project domain.Project
	err := s.CommitRow(ctx, p, key, "project", id, create, func(row *domain.StoredRow, exists bool) error {
		project = domain.Project{ID: id, CreatedAt: api.Timestamp()}
		var origin *domain.ImportOrigin
		if exists {
			var err error
			if project, err = domain.ReadProject(row); err != nil {
				return err
			}
			origin = domain.ReadImportOrigin(row)
		}
		change(&project)
		project.UpdatedAt = api.Timestamp()
		row.Data = api.Raw(domain.StoredProject{Project: project, RecordVersion: domain.RecordVersion, ImportedFrom: origin})
		return nil
	})
	return project, err
}

// RemoveAllProjects deletes every project and project document in one
// step, and is how many projects went.
// RemoveAllProjects 一步删除所有 project 及其文档，返回已删除的 project 数量。
func (s *Store) RemoveAllProjects(ctx context.Context, p *auth.Principal, key auth.ContentKey) (int, error) {
	defer clear(key[:])
	if p.Grant != nil && !(*grantScope)(p.Grant).allows(opRemove) {
		return 0, errRunScope
	}
	kidHex, nonce := keyID(key), uuid.NewString()
	resp, err := s.Controlplane.DeleteAllProjects(ctx, controlplane.DeleteAllProjectsRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kidHex,
		IdempotencyKey: nonce,
		OperationHash:  operationHash(key, http.MethodDelete, controlplane.DeleteAllProjectsPath, kidHex, "", nonce, 0, nil, nil),
	})
	if err != nil {
		return 0, FromControlplane(err)
	}
	if !resp.OK {
		return 0, api.Err(502, "UPSTREAM_REFUSED", "The projects were not deleted")
	}
	return resp.Deleted, nil
}

// ProjectDocuments are every document of a project. A document the key cannot
// open, or this harness cannot read, is passed to unreadable with why, which
// fails the read by returning an error or leaves the document out by
// returning nil.
// ProjectDocuments 返回某个 project 的全部文档。key 无法打开或本 harness 无法读取的文档
// 会连同原因传给 unreadable，后者可通过返回错误使读取失败，或返回 nil 将该文档略过。
func (s *Store) ProjectDocuments(ctx context.Context, p *auth.Principal, key auth.ContentKey, id string, unreadable func(row string, err error) error) ([]domain.ProjectDocument, error) {
	// Unfiltered pages can repeat a row that changed while listing; the
	// later copy is the newer one.
	// 未过滤的分页可能重复列出在列表过程中发生变化的 row；后一份副本更新。
	var order []string
	latest := map[string]documentCopy{}
	keep := func(rowID string, listed documentCopy) {
		if _, ok := latest[rowID]; !ok {
			order = append(order, rowID)
		}
		latest[rowID] = listed
	}
	for cursor := ""; ; {
		page, err := s.Rows.List(ctx, p, key, "project_document", cursor, id, 100)
		if err != nil {
			return nil, err
		}
		for i := range page.Rows {
			keep(page.Rows[i].ID, documentCopy{row: &page.Rows[i]})
		}
		for _, rowID := range slices.Sorted(maps.Keys(page.Unreadable)) {
			keep(rowID, documentCopy{err: page.Unreadable[rowID]})
		}
		if page.Next == "" {
			break
		}
		if page.Next == cursor {
			return nil, api.Err(502, "UPSTREAM_REFUSED", "The document cursor did not advance")
		}
		cursor = page.Next
	}
	list := []domain.ProjectDocument{}
	for _, rowID := range order {
		listed := latest[rowID]
		doc, err := domain.ProjectDocument{}, listed.err
		if err == nil {
			doc, err = domain.ReadProjectDocument(listed.row, id)
		}
		if err != nil {
			if err := unreadable(rowID, err); err != nil {
				return nil, err
			}
			continue
		}
		list = append(list, doc)
	}
	return list, nil
}

// documentCopy is a document row as a page listed it, or why it could not
// be opened.
// documentCopy 是一页所列出的文档 row，或它无法被打开的原因。
type documentCopy struct {
	row *domain.StoredRow
	err error
}

// CreateProjectDocument stores a new document under its project.
// CreateProjectDocument 在其 project 下存储新文档。
func (s *Store) CreateProjectDocument(ctx context.Context, p *auth.Principal, key auth.ContentKey, doc domain.ProjectDocument) (domain.ProjectDocument, error) {
	id := doc.ProjectID + "/" + doc.ID
	err := s.CommitRow(ctx, p, key, "project_document", id, true, func(row *domain.StoredRow, exists bool) error {
		row.Project, row.ProjectSet = &doc.ProjectID, true
		doc.CreatedAt = api.Timestamp()
		var origin *domain.ImportOrigin
		if exists {
			stored, err := domain.ReadProjectDocument(row, doc.ProjectID)
			if err != nil {
				return err
			}
			doc.CreatedAt = stored.CreatedAt
			origin = domain.ReadImportOrigin(row)
		}
		doc.UpdatedAt = api.Timestamp()
		row.Data, row.Project, row.ProjectSet = api.Raw(domain.StoredDocument{ProjectDocument: doc, RecordVersion: domain.RecordVersion, ImportedFrom: origin}), &doc.ProjectID, true
		return nil
	})
	return doc, err
}

// Archives
// 归档

// AuthorizeArchive 为 archive 操作授予存储访问权限（给定 key 时）。
func (s *Store) AuthorizeArchive(ctx context.Context, p *auth.Principal, keyBytes []byte, operation string) error {
	if len(keyBytes) == 0 {
		return nil
	}
	key, err := auth.DecodeContentKey(keyBytes)
	if err != nil {
		return err
	}
	defer clear(key[:])
	grant, err := s.Rows.AuthorizeStorage(ctx, p, key, operation, "")
	if err != nil {
		return err
	}
	p.Grant = grant
	return nil
}
