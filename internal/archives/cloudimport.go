package archives

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
)

// A cloud import stores what an archive put in the cloud: its projects,
// then their documents, then its chats with their images, in the
// background while the client polls. Every row it stores records where it
// came from, under an id derived from the source, so the same import run
// again skips what it stored and takes the next id for an entity when
// another row holds the one it maps to. An entity the account holds already
// under the id the archive gives it, in a row the key reads, as an archive
// this service wrote from the same account does, is skipped too.
// cloud import 在客户端轮询期间于后台存储归档放入云端的内容：先 project，再其 document，最后是带 image 的 chat。
// 它存储的每一行都记录其来源，并置于由来源派生的 id 下，因此再次运行同一 import 会跳过已存储的内容，
// 并在某实体映射到的 id 已被其他行占用时为其取下一个 id。账户已在该归档给定 id 下、且该 key 可读的行中
// 持有的实体——正如本服务从同一账户写入的归档那样——也会被跳过。

const (
	// ImportBudget bounds one import. Each entity costs a few round trips
	// to the control plane, so an archive at the import's limits takes
	// well over an hour; one that takes longer fails as timed out and keeps
	// what it stored, which the import run again skips.
	// ImportBudget 限制单次 import 的时长。每个实体会与 control plane 发生几次往返，因此处于 import
	// 上限的归档要花远超一小时；超时的会失败，但保留已存储的内容，再次运行时会跳过。
	ImportBudget = 2 * time.Hour
	// ImportRetention is how long an ended import still answers for the
	// caller, so a client polling for its end sees it.
	// ImportRetention 是已结束的 import 仍可为调用方应答的时长，使轮询其结束的客户端能看到结果。
	ImportRetention = 5 * time.Minute
	// maxImportGenerations bounds the ids tried for one entity whose
	// mapped rows other rows hold.
	// maxImportGenerations 限制对某个映射行已被占用的实体所尝试的 id 数。
	maxImportGenerations = 100
	// maxImportNotes bounds the warnings, and the errors, a report keeps.
	// maxImportNotes 限制报告中保留的 warning 与 error 数量。
	maxImportNotes = 100
	// importNotifyTimeout bounds telling the control plane how an import
	// ended, which outlives the job.
	// importNotifyTimeout 限制通知 control plane import 结果的时长，该通知比 job 更久。
	importNotifyTimeout = 30 * time.Second
	// The derivations of an imported row's id and of its images'
	// identities are a stored format: the rows an earlier import stored,
	// and the attachments under them, are named by them. A write's nonce
	// is not stored, and is its job's own.
	// 导入行 id 及其 image 身份的派生方式是一种存储格式：先前 import 存储的行及其下的 attachment 都由此命名。
	// 写入的 nonce 不会被存储，而是属于其 job 自身。
	importIDInfo    = "tinfoil-restore-id-v1"
	importWriteInfo = "tinfoil-restore-write-v1"
	importImageInfo = "import-att"
	// An imported row's id is a UUID-shaped slice of a digest.
	// 导入行的 id 是摘要的 UUID 形状切片。
	importIDHexLen = 32
	// importNonceBytes is the length of a write's nonce before hex.
	// importNonceBytes 是写入 nonce 在十六进制编码前的字节长度。
	importNonceBytes = 16
	// Why an import ended before it was done, as the control plane's
	// email and the client name it.
	// import 提前结束的原因，与 control plane 邮件及客户端的命名一致。
	importFailureTimeout       = "timeout"
	importFailureAuthorization = "authorization_failed"
	importFailureWorker        = "worker_failed"
	importFailureInternal      = "internal"
)

var (
	errImportNotFound = api.Err(404, "THREAD_NOT_FOUND", "The import job was not found")
	errImportRunning  = api.Err(409, "RUN_IN_FLIGHT", "Another import is still running for this account")
)

// importStates are a job's states as a client reads them.
// importStates 是客户端读到的 job 状态。
var importStates = map[jobs.State]string{jobs.Running: "running", jobs.Complete: "completed", jobs.Failed: "failed"}

// importFailureMessages say in the report why an import ended early; the
// control plane's email carries the longer explanation.
// importFailureMessages 在报告中说明 import 提前结束的原因；更长的解释由 control plane 的邮件给出。
var importFailureMessages = map[string]string{
	importFailureTimeout:       "import timed out",
	importFailureAuthorization: "Tinfoil could not authorize access to cloud storage for the import; please retry from a signed-in device",
	importFailureWorker:        "the import worker stopped unexpectedly; please retry the import",
	importFailureInternal:      "import failed",
}

// ImportKindCounts is what an import did with one kind of entity: stored,
// found stored by an earlier run, failed, or blocked by its project's
// failure.
// ImportKindCounts 是 import 对某一类实体的处理结果：已存储、发现先前运行已存储、失败，或因所属 project 失败而被阻塞。
type ImportKindCounts struct {
	Imported int `json:"imported"`
	Skipped  int `json:"skipped"`
	Failed   int `json:"failed"`
	Blocked  int `json:"blocked"`
}

// ImportReport is where a cloud import stands, as the client polls it.
// ImportReport 是客户端轮询到的 cloud import 当前状态。
type ImportReport struct {
	Phase         string                      `json:"phase,omitempty"`
	Imported      int                         `json:"imported"`
	Failed        int                         `json:"failed"`
	Total         int                         `json:"total"`
	Counts        map[string]ImportKindCounts `json:"counts,omitempty"`
	Warnings      []string                    `json:"warnings,omitempty"`
	Errors        []string                    `json:"errors,omitempty"`
	FailureReason string                      `json:"failureReason,omitempty"`
}

// importStatusView is an import job as its client polls it.
// importStatusView 是客户端轮询到的 import job。
type importStatusView struct {
	JobID  string `json:"jobId"`
	Status string `json:"status"`
	ImportReport
}

// ImportStatus 返回调用方某个 import job 的当前状态。
func (ar *Service) ImportStatus(_ context.Context, p *auth.Principal, _ auth.ContentKey, in *importStatusRequest) (any, error) {
	job := ar.Imports.Get(p.ID)
	if job == nil || job.ID() != in.JobID {
		return nil, errImportNotFound
	}
	status := job.Status()
	return importStatusView{JobID: job.ID(), Status: importStates[status.State], ImportReport: status.Report}, nil
}

// startImport runs the job that stores a finished cloud archive, which the
// job owns from here, and is the job's id. A caller has one import at a
// time; one that ended gives way to the next.
// startImport 启动存储已生成 cloud 归档的 job，该 job 从此拥有此归档，并返回 job 的 id。每个调用者同时只能有一个 import；已结束的会让位给下一个。
func (ar *Service) startImport(ctx context.Context, p *auth.Principal, key auth.ContentKey, archive *scratchFile, source string) (string, error) {
	defer clear(key[:])
	// The job outlives the request, so it takes copies of the key and of
	// who the caller is, and the archive leaves the request's lease.
	imp := &cloudImport{ar: ar, p: &auth.Principal{ID: p.ID, JWT: p.JWT}, key: key, file: archive, source: source, named: make(chan string, 1), held: map[string]map[string]bool{}}
	archive.lease.detach(archive)
	job, started := ar.Imports.Restart(ctx, p.ID, imp.run)
	if !started {
		archive.release()
		clear(imp.key[:])
		return "", errImportRunning
	}
	imp.named <- job.ID()
	return job.ID(), nil
}

// cloudImport is one import's job: who it is for, under which key, the
// archive it reads, and the report it keeps up to date.
// cloudImport 是一次 import 的 job：服务对象、所用 key、读取的归档，以及它保持更新的报告。
type cloudImport struct {
	ar     *Service
	p      *auth.Principal
	key    auth.ContentKey
	file   *scratchFile
	source string
	// named carries the job's id, known once it has started, which the
	// control plane is told at the end.
	// named 承载 job 的 id，它在 job 启动后才可知，最后会告知 control plane。
	named chan string
	id    string

	entries map[string]*zip.File
	report  ImportReport
	publish func(ImportReport)
	// held is the ids of each scope's rows the account held as the import
	// began storing that scope.
	// held 是在 import 开始存储某 scope 时，账户在该 scope 下已持有的行的 id。
	held map[string]map[string]bool
}

func (j *cloudImport) run(ctx context.Context, report func(ImportReport)) (out ImportReport, err error) {
	defer j.file.release()
	defer clear(j.key[:])
	j.id, j.publish = <-j.named, report
	defer func() {
		if recovered := recover(); recovered != nil {
			err = api.Rescued("a cloud import", recovered)
			j.report.FailureReason = importFailureWorker
		}
		if err != nil {
			j.note(&j.report.Errors, importFailureMessages[j.report.FailureReason])
		}
		out = j.snapshot()
		j.tell(ctx, err)
	}()
	zr, err := zip.NewReader(j.file, j.file.Size())
	if err != nil {
		return out, j.fail(importFailureInternal, err)
	}
	j.entries = make(map[string]*zip.File, len(zr.File))
	for _, f := range zr.File {
		j.entries[f.Name] = f
	}
	var manifest cloudManifest
	if err := j.entity(ManifestPath, &manifest); err != nil {
		return out, j.fail(importFailureInternal, err)
	}
	return out, j.store(ctx, manifest.Entities)
}

// store stores every entity, projects first so that documents and chats
// can name theirs. An entity's failure counts against it alone, except
// one that every later entity would meet too: the job's time running
// out, or the caller's credential refused.
// store 存储每个实体，先 project，使 document 和 chat 能引用其所属 project。单个实体的失败只计到它自己，
// 但每个后续实体都会遇到的失败除外：job 运行超时，或调用方凭证被拒。
func (j *cloudImport) store(ctx context.Context, entities []cloudEntity) error {
	j.report.Total = len(entities)
	j.report.Counts = map[string]ImportKindCounts{"project": {}, "document": {}, "chat": {}}
	projects, failedProjects := map[string]string{}, map[string]bool{}
	j.phase("projects")
	if err := j.hold(ctx, entities, "project"); err != nil {
		return err
	}
	for _, entity := range entities {
		if entity.Kind != "project" {
			continue
		}
		id, outcome, err := j.restore(ctx, entity, "", func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error) {
			var project cloudProject
			if err := j.entity(entity.Path, &project); err != nil {
				return nil, nil, err
			}
			now := api.Timestamp()
			stored := domain.StoredProject{Project: domain.Project{ID: id, Name: project.Name, Description: project.Description, SystemInstructions: project.SystemInstructions, Color: domain.Present(project.Color), CreatedAt: now, UpdatedAt: now},
				RecordVersion: domain.RecordVersion, ImportedFrom: origin}
			return &domain.StoredRow{Data: api.Raw(stored)}, nil, nil
		})
		if err != nil {
			failedProjects[entity.SourceID] = true
		} else {
			projects[entity.SourceID] = id
		}
		if stop := j.count(ctx, entity.Kind, outcome, err); stop != nil {
			return stop
		}
	}
	j.phase("documents")
	if err := j.hold(ctx, entities, "document"); err != nil {
		return err
	}
	for _, entity := range entities {
		if entity.Kind != "document" {
			continue
		}
		projectID, ok := projects[entity.ProjectSourceID]
		if !ok || failedProjects[entity.ProjectSourceID] {
			j.block(entity.Kind)
			continue
		}
		_, outcome, err := j.restore(ctx, entity, projectID, func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error) {
			var doc cloudDocument
			if err := j.entity(entity.Path, &doc); err != nil {
				return nil, nil, err
			}
			now := api.Timestamp()
			stored := domain.StoredDocument{ProjectDocument: domain.ProjectDocument{ID: id, ProjectID: projectID, Filename: doc.Filename, ContentType: doc.ContentType, SizeBytes: doc.SizeBytes, Content: doc.Content, CreatedAt: now, UpdatedAt: now},
				RecordVersion: domain.RecordVersion, ImportedFrom: origin}
			return &domain.StoredRow{Data: api.Raw(stored), Project: &projectID, ProjectSet: true}, nil, nil
		})
		if stop := j.count(ctx, entity.Kind, outcome, err); stop != nil {
			return stop
		}
	}
	j.phase("chats")
	if err := j.hold(ctx, entities, "chat"); err != nil {
		return err
	}
	for _, entity := range entities {
		if entity.Kind != "chat" {
			continue
		}
		projectID := ""
		if entity.ProjectSourceID != "" {
			var ok bool
			if projectID, ok = projects[entity.ProjectSourceID]; !ok || failedProjects[entity.ProjectSourceID] {
				j.block(entity.Kind)
				continue
			}
		}
		_, outcome, err := j.restore(ctx, entity, "", func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error) {
			var chat cloudChat
			if err := j.entity(entity.Path, &chat); err != nil {
				return nil, nil, err
			}
			thread, images := j.chat(ctx, id, projectID, origin, &chat)
			row := &domain.StoredRow{Data: api.Raw(thread), ProjectSet: true}
			if projectID != "" {
				row.Project = &projectID
			}
			return row, images, nil
		})
		if stop := j.count(ctx, entity.Kind, outcome, err); stop != nil {
			return stop
		}
	}
	j.phase("complete")
	return nil
}

// restored is what became of one entity: stored now, or found stored by
// an earlier run of the same import or held already by the account.
// restored 是某个实体的最终去向：现在存储，或发现已被同一次 import 的先前运行存储，或账户早已持有。
type restored int

const (
	storedNow restored = iota
	storedBefore
)

// occupant is what holds the row an entity maps to: nothing, the entity
// itself as an earlier run of the import stored it, another row the key
// reads, or a row sealed under another key.
// occupant 是占用实体所映射行的对象：无、先前运行存储的该实体本身、该 key 可读的其他行，或另一 key 封存的行。
type occupant int

const (
	vacant occupant = iota
	sameImport
	anotherRow
	otherKey
)

// restore stores one entity under the first of its ids whose row is
// vacant, and is that id without the project a document's row is filed
// under. A row the account holds under the entity's own id and the key
// reads, or the same import stored before, is found rather than stored
// again; one another row holds is passed over. build makes the row for an
// id, and names the attachments it stored for it, which are retired when
// the row is not stored after all. A write whose answer was lost is
// settled by reading the row back.
// restore 将实体存储到其行空闲的第一个 id 下，返回该 id（document 行的项目前缀除外）。账户在实体自身 id 下
// 持有且该 key 可读的行，或同一次 import 之前已存储的，会直接找到而非重复存储；被其他行占用的则跳过。
// build 为某 id 构造行，并列出为其存储的 attachment；若行最终未存储，这些 attachment 会被回收。
// 应答丢失的写入通过读回该行来解决。
func (j *cloudImport) restore(ctx context.Context, entity cloudEntity, projectID string, build func(id string, origin *domain.ImportOrigin) (*domain.StoredRow, []string, error)) (string, restored, error) {
	scope := rowScope(entity.Kind)
	// A document's id in an archive names its project's, so the account
	// holds it only where its project kept that id. A row sealed under
	// another key holds nothing the import can stand on, and the archive
	// may hold the only copy the key reads.
	// 归档中 document 的 id 会带上其 project 的 id，因此账户仅在其 project 保留该 id 的位置持有它。
	// 在另一 key 下封存的行不含 import 可依赖的内容，而归档可能持有该 key 可读的唯一副本。
	if j.held[scope][entity.SourceID] && (entity.Kind != "document" || projectID == entity.ProjectSourceID) {
		switch holder, err := j.inspect(ctx, scope, entity.SourceID, nil); {
		case err != nil:
			return "", 0, err
		case holder == anotherRow && entity.Kind == "document":
			return strings.TrimPrefix(entity.SourceID, projectID+"/"), storedBefore, nil
		case holder == anotherRow:
			return entity.SourceID, storedBefore, nil
		}
	}
	for generation := 0; generation < maxImportGenerations; generation++ {
		id := importedID(j.p.ID, j.source, entity.Kind, entity.SourceID, generation)
		rowID := id
		if entity.Kind == "document" {
			rowID = projectID + "/" + id
		}
		origin := &domain.ImportOrigin{Format: cloudImportFormat, Source: j.source, Kind: entity.Kind, SourceID: entity.SourceID, Generation: generation}
		switch holder, err := j.inspect(ctx, scope, rowID, origin); {
		case err != nil:
			return "", 0, err
		case holder == sameImport:
			return id, storedBefore, nil
		case holder != vacant:
			continue
		}
		row, attachments, err := build(id, origin)
		if err != nil {
			return "", 0, err
		}
		row.ID = rowID
		_, err = j.ar.Store.Rows.Push(ctx, j.p, j.key, scope, row, importedNonce(j.id, entity.Kind, entity.SourceID, generation))
		if err == nil {
			return id, storedNow, nil
		}
		holder, inspectErr := j.inspect(ctx, scope, rowID, origin)
		if inspectErr != nil {
			return "", 0, err
		}
		if holder == sameImport {
			if conflicted(err) {
				return id, storedBefore, nil
			}
			return id, storedNow, nil
		}
		j.ar.Store.RetireAttachments(ctx, j.p, attachments)
		if conflicted(err) {
			continue
		}
		return "", 0, err
	}
	return "", 0, errors.New("every id for the entity is taken")
}

// inspect is what holds a row, which is the entity of origin, when given,
// as an earlier run stored it. One that cannot be read in time is not
// known either way.
// inspect 返回占用某行的对象；给定 origin 时，若与先前运行所存的一致，则为该实体本身。无法及时读取的则无从判断。
func (j *cloudImport) inspect(ctx context.Context, scope, id string, origin *domain.ImportOrigin) (occupant, error) {
	row, err := j.ar.Store.Rows.Load(ctx, j.p, j.key, scope, id)
	if err != nil {
		switch e := api.AsError(err); {
		case e.HTTP == http.StatusNotFound:
			return vacant, nil
		case e.Code == "KEY_MISMATCH":
			return otherKey, nil
		}
		return 0, err
	}
	if found := domain.ReadImportOrigin(row); found != nil && origin != nil && *found == *origin {
		return sameImport, nil
	}
	return anotherRow, nil
}

// hold lists the ids of the rows the account holds where an archive's
// entities of a kind are stored, when it has any. An import that cannot
// learn them ends, rather than store rows that might copy them.
// hold 列出账户在归档某类实体存储处所持有行的 id（若有）。无法得知时会终止 import，而不是存储可能复制它们的行。
func (j *cloudImport) hold(ctx context.Context, entities []cloudEntity, kind string) error {
	if !slices.ContainsFunc(entities, func(entity cloudEntity) bool { return entity.Kind == kind }) {
		return nil
	}
	scope := rowScope(kind)
	listed, err := j.ar.Store.Rows.IDs(ctx, j.p, scope, "")
	switch {
	case err == nil:
	case ctx.Err() != nil:
		return j.fail(importFailureTimeout, ctx.Err())
	case api.AsError(err).HTTP == http.StatusUnauthorized:
		return j.fail(importFailureAuthorization, err)
	default:
		return j.fail(importFailureInternal, err)
	}
	ids := make(map[string]bool, len(listed))
	for _, id := range listed {
		ids[id] = true
	}
	j.held[scope] = ids
	return nil
}

// rowScope is the scope an entity of a kind is stored in.
// rowScope 是某类实体存储所在的 scope。
func rowScope(kind string) string {
	if kind == "document" {
		return "project_document"
	}
	return kind
}

// conflicted is whether a create met a row already at its id.
// conflicted 判断创建时是否遇到该 id 下已存在的行。
func conflicted(err error) bool { return api.AsError(err).Code == "REVISION_CONFLICT" }

// importedID is the id an entity is stored under: a UUID-shaped digest of
// who imports it, the import's source, the entity and which copy this is.
// importedID 是实体存储所用的 id：由导入者、import 来源、实体及第几个副本构成的 UUID 形状摘要。
func importedID(user, source, kind, sourceID string, generation int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s\x00%s\x00%s\x00%d", importIDInfo, user, source, kind, sourceID, generation))
	h := hex.EncodeToString(sum[:])[:importIDHexLen]
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:])
}

// importedNonce is the nonce an entity's write carries, which is its
// job's own. The control plane answers a nonce it has seen with the
// outcome it first gave, so a retry of the write within the job is
// settled, and a later job's write of a row deleted since is made rather
// than answered with the deleted row's outcome.
// importedNonce 是实体写入携带的 nonce，属于其 job 自身。control plane 对见过的 nonce 返回它最初给出的结果，
// 因此在同一 job 内重试写入可被确定，而之后 job 对被删除行的写入会真正执行，而非返回已删除行的结果。
func importedNonce(jobID, kind, sourceID string, generation int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s\x00%s\x00%d", importWriteInfo, jobID, kind, sourceID, generation))
	return hex.EncodeToString(sum[:importNonceBytes])
}

// importedImage names an image upload for the attachment store, which
// derives the attachment's id and key from it: the chat, the blob and
// which upload of the chat's this is.
// importedImage 为 attachment 存储命名一次 image 上传，存储据此派生 attachment 的 id 与 key：包括 chat、blob 及这是该 chat 的第几次上传。
func importedImage(chatID, blobPath string, index int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%s:%d", importImageInfo, chatID, blobPath, index))
	return hex.EncodeToString(sum[:importNonceBytes])
}

// chat is a prepared thread as the import stores it: its images stored
// as attachments of the chat, under keys the chat embeds, each with the
// thumbnail an upload makes unless it came with one, and documents
// without any content left out. It names the attachments stored.
// chat 返回 import 要存储的、已准备好的 thread：其 image 作为 chat 的 attachment 存储，置于 chat 内嵌的 key 下，
// 每张都带上传生成的 thumbnail（除非本就有），且不含无内容的 document。返回值为已存储 attachment 的 id。
func (j *cloudImport) chat(ctx context.Context, id, projectID string, origin *domain.ImportOrigin, chat *cloudChat) (*domain.Thread, []string) {
	t := chat.Thread
	t.ID, t.ImportedFrom, t.RecordVersion, t.ProjectID = id, origin, domain.RecordVersion, nil
	if projectID != "" {
		t.ProjectID = &projectID
	}
	t.Messages = make([]domain.Message, 0, len(chat.Messages))
	var stored []string
	uploads := 0
	for _, message := range chat.Messages {
		m := message.Message
		m.Attachments = nil
		for _, attachment := range message.Attachments {
			a := attachment.Attachment
			if a.IsImage() {
				data, err := j.blob(attachment.ArchivePath)
				if err != nil {
					j.warn("image attachment blob missing")
					continue
				}
				kind := http.DetectContentType(data)
				if !slices.Contains(sniffedImages, kind) {
					j.warn("image attachment type rejected")
					continue
				}
				attID, attKey, err := j.ar.Store.PutAttachment(ctx, j.p, j.key, id, importedImage(id, attachment.ArchivePath, uploads), data)
				uploads++
				if err != nil {
					j.warn("image attachment upload failed")
					continue
				}
				stored = append(stored, attID)
				a.ID, a.EncryptionKey, a.MimeType, a.Base64 = attID, attKey, kind, ""
				if a.Thumbnail == "" {
					if _, thumb, err := attach.ResizeImage(data); err == nil {
						a.Thumbnail = base64.StdEncoding.EncodeToString(thumb)
					} else {
						j.warn("image attachment stored without a thumbnail")
					}
				}
			} else if !documentHasContent(&a) {
				j.warn("document attachment without content skipped")
				continue
			}
			m.Attachments = append(m.Attachments, a)
		}
		t.Messages = append(t.Messages, m)
	}
	return &t, stored
}

// documentHasContent is whether a document attachment holds any text, or
// a page with text or an image.
// documentHasContent 判断 document attachment 是否持有文本，或有文本或 image 的 page。
func documentHasContent(a *domain.Attachment) bool {
	if strings.TrimSpace(a.TextContent) != "" {
		return true
	}
	for _, page := range a.Pages {
		if strings.TrimSpace(page.Text) != "" || strings.TrimSpace(page.Image) != "" {
			return true
		}
	}
	return false
}

// entity reads one of the archive's JSON entries.
// entity 读取归档的一个 JSON 条目。
func (j *cloudImport) entity(name string, v any) error {
	f := j.entries[name]
	if f == nil {
		return fmt.Errorf("the archive holds no %s", name)
	}
	data, err := readEntry(f, maxArchiveJSONEntry)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// blob reads one of the archive's image entries.
// blob 读取归档的一个 image 条目。
func (j *cloudImport) blob(name string) ([]byte, error) {
	f := j.entries[name]
	if f == nil {
		return nil, fmt.Errorf("the archive holds no %s", name)
	}
	return readEntry(f, maxArchiveImage)
}

// count records what became of an entity and publishes the report. It is
// the failure that ends the import when the entity's is one every later
// entity would meet too.
// count 记录某实体的去向并发布报告。当该实体的失败是每个后续实体都会遇到的失败时，它将终结 import。
func (j *cloudImport) count(ctx context.Context, kind string, outcome restored, err error) error {
	counts := j.report.Counts[kind]
	switch {
	case err != nil:
		counts.Failed++
		j.note(&j.report.Errors, kind+" failed")
	case outcome == storedBefore:
		counts.Skipped++
	default:
		counts.Imported++
	}
	j.report.Counts[kind] = counts
	j.progress()
	if err == nil {
		return nil
	}
	switch {
	case ctx.Err() != nil:
		return j.fail(importFailureTimeout, ctx.Err())
	case api.AsError(err).HTTP == http.StatusUnauthorized:
		return j.fail(importFailureAuthorization, err)
	}
	return nil
}

// block records an entity whose project was not stored.
// block 记录一个因所属 project 未被存储而被阻塞的实体。
func (j *cloudImport) block(kind string) {
	counts := j.report.Counts[kind]
	counts.Blocked++
	j.report.Counts[kind] = counts
	j.progress()
}

func (j *cloudImport) phase(phase string) {
	j.report.Phase = phase
	j.progress()
}

func (j *cloudImport) warn(text string) {
	j.note(&j.report.Warnings, text)
	j.progress()
}

// note keeps text in a report's list, up to the list's bound.
// note 将文本保留在报告的列表中，最多到列表上限。
func (j *cloudImport) note(list *[]string, text string) {
	if len(*list) < maxImportNotes {
		*list = append(*list, text)
	}
}

// fail records why the import ends before it is done, and is the error
// that ends it.
// fail 记录 import 提前结束的原因，并返回终结它的错误。
func (j *cloudImport) fail(reason string, err error) error {
	j.report.FailureReason = reason
	return err
}

// progress publishes the report as it stands, with the totals brought up
// to date.
// progress 发布当前报告，并更新总数。
func (j *cloudImport) progress() {
	j.report.Imported, j.report.Failed = 0, 0
	for _, counts := range j.report.Counts {
		j.report.Imported += counts.Imported
		j.report.Failed += counts.Failed
	}
	j.publish(j.snapshot())
}

// snapshot is a copy of the report that later changes leave as it is.
// snapshot 是报告的一份副本，之后的修改不会影响它。
func (j *cloudImport) snapshot() ImportReport {
	out := j.report
	out.Counts, out.Warnings, out.Errors = maps.Clone(j.report.Counts), slices.Clone(j.report.Warnings), slices.Clone(j.report.Errors)
	return out
}

// tell has the control plane tell the user how the import ended. The
// report already says, so a notification that fails is let go.
// tell 让 control plane 告知用户 import 的结束情况。报告已说明，因此失败的通知会被忽略。
func (j *cloudImport) tell(ctx context.Context, err error) {
	notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), importNotifyTimeout)
	defer cancel()
	outcome := controlplane.ImportOutcome{ClerkUserID: j.p.ID, JobID: j.id, Source: cloudImportSource, Status: controlplane.ImportOutcomeCompleted, Imported: j.report.Imported, Failed: j.report.Failed}
	if err != nil {
		outcome.Status, outcome.FailureReason = controlplane.ImportOutcomeFailed, j.report.FailureReason
	}
	_ = j.ar.Store.Controlplane.NotifyImportOutcome(notifyCtx, outcome)
}
