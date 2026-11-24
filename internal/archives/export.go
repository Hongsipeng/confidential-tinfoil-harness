package archives

import (
	"archive/zip"
	"bytes"
	"cmp"
	"compress/flate"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

// Service 提供归档的 share、export 与 import 等 HTTP 处理。
type Service struct {
	CPClient     *http.Client
	Scratch      *Scratch
	Attachments  *attach.Service
	Auth         *auth.Authenticator
	Controlplane string
	Store        *store.Store
	// Imports runs each caller's cloud import.
	// Imports 运行每个调用方的 cloud import。
	Imports *jobs.Runner[ImportReport]
	// anonymous are the signed-out callers with an archive running.
	// anonymous 是正在运行归档的匿名调用者。
	anonymous anonymousArchives
}

// ShareThread 生成一个可分享的聊天链接，并返回其 URL。
func (ar *Service) ShareThread(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *ShareRequest) (any, error) {
	defer in.ClearKey()
	id := in.ThreadID
	var t domain.Thread
	if in.Local {
		var err error
		if t, err = domain.ReadDeviceThread(id, in.Thread); err != nil {
			return nil, err
		}
	} else {
		key, err := auth.DecodeContentKey(in.Key)
		in.ClearKey()
		if err != nil {
			return nil, err
		}
		defer clear(key[:])
		row, err := ar.Store.LoadThread(ctx, p, key, id)
		clear(key[:])
		if err != nil {
			return nil, err
		}
		t = row.Thread
	}
	payload := domain.SharePayload{V: 1, Title: t.Title, Messages: []domain.ShareMessage{}, CreatedAt: domain.Millis(t.CreatedAt)}
	// A share shows the conversation; the summaries the model read in place
	// of its older messages are not part of it.
	// 分享展示对话内容；模型为替代旧消息而读取的摘要不属于其中。
	for _, m := range t.Messages {
		if m.Role == domain.CompactionRole {
			continue
		}
		shared := domain.ShareMessage{Role: m.Role, Content: m.Content, Timestamp: domain.Millis(m.CreatedAt), Timeline: domain.CheckedWidgets(m.Timeline), ModelDisplayName: m.ModelDisplayName, IsError: m.IsError, Quote: m.Quote, Annotations: m.Annotations, Answers: m.Answers}
		for _, a := range m.Attachments {
			view := domain.ViewAttachment(&a)
			view.EncryptionKey = a.EncryptionKey
			shared.Attachments = append(shared.Attachments, view)
		}
		payload.Messages = append(payload.Messages, shared)
	}
	ciphertext, shareKey, err := seal.SealShare(api.Raw(payload))
	if errors.Is(err, seal.ErrShareTooLarge) {
		return nil, api.Err(413, "PAYLOAD_TOO_LARGE", "The chat is too large to share")
	}
	if err != nil {
		return nil, err
	}
	// A share is keyed by its chat, like the web client's: the control plane keeps a shared
	// chat's blobs, and the owner can revoke the link by chat.
	// 分享按其 chat 作 key，与 web 客户端一致：control plane 保存已分享 chat 的 blob，所有者可按 chat 撤销链接。
	if _, err := ar.shareRequest(ctx, p, http.MethodPut, id, ciphertext); err != nil {
		return nil, err
	}
	return struct {
		URL string `json:"url"`
	}{domain.ChatURL + "/share/" + url.PathEscape(id) + "#v2:" + shareKey}, nil
}

// The share endpoints authenticate the owner's Clerk session.
// 分享相关端点验证所有者的 Clerk 会话。
func (ar *Service) shareRequest(ctx context.Context, p *auth.Principal, method, id string, body []byte) ([]byte, error) {
	ctx = auth.WithCallerKey(ctx, string(p.JWT))
	path := ar.Controlplane + "/api/shares/" + url.PathEscape(id)
	if method == http.MethodGet {
		path += "/status"
	}
	req, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Format-Version", "1")
	}
	resp, err := ar.CPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode/100 != 2 {
		return nil, api.DownstreamError(resp.StatusCode, b)
	}
	return b, nil
}

// RevokeShare 撤销一个分享链接。
func (ar *Service) RevokeShare(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *api.ThreadRequest) (any, error) {
	if _, err := ar.shareRequest(ctx, p, http.MethodDelete, in.ThreadID, nil); err != nil && api.AsError(err).HTTP != 404 {
		return nil, err
	}
	return struct{}{}, nil
}

// ShareStatus 返回某个聊天分享链接的状态。
func (ar *Service) ShareStatus(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *api.ThreadRequest) (any, error) {
	b, err := ar.shareRequest(ctx, p, http.MethodGet, in.ThreadID, nil)
	if err != nil {
		return nil, err
	}
	var status struct {
		Shared *bool `json:"shared"`
	}
	if json.Unmarshal(b, &status) != nil || status.Shared == nil {
		return nil, api.Err(502, "UPSTREAM_REFUSED", "Invalid share status")
	}
	return struct {
		Shared bool `json:"shared"`
	}{*status.Shared}, nil
}

// SharedThreadView is an opened share's payload, read like any old chat.
// SharedThreadView 是已打开分享的载荷，像普通聊天一样读取。
func SharedThreadView(id string, payload []byte) (domain.SharedThread, error) {
	t, err := domain.ReadThread(id, payload)
	if err != nil {
		return domain.SharedThread{}, err
	}
	shared := domain.SharedThread{Title: t.Title, CreatedAt: domain.Present(t.CreatedAt), Messages: []domain.SharedMessage{}}
	for i := range t.Messages {
		m := &t.Messages[i]
		view := domain.SharedMessage{MessageView: domain.ViewMessage(m)}
		for j, a := range view.MessageView.Attachments {
			view.Attachments = append(view.Attachments, domain.SharedAttachment{Attachment: a, AttKey: domain.Present(m.Attachments[j].EncryptionKey)})
		}
		shared.Messages = append(shared.Messages, view)
	}
	return shared, nil
}

// OpenShare 打开一个分享链接并返回其内容。
func (ar *Service) OpenShare(ctx context.Context, _ *auth.Principal, _ auth.ContentKey, in *openShareRequest) (any, error) {
	id := in.ShareID
	fragment, ok := strings.CutPrefix(in.Fragment, "v2:")
	if !ok {
		return nil, api.Err(400, "BAD_REQUEST", "A v2 share fragment is required")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", ar.Controlplane+"/api/shares/"+url.PathEscape(id), nil)
	resp, err := ar.CPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	ciphertext, err := io.ReadAll(io.LimitReader(resp.Body, seal.MaxShareBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, api.DownstreamError(resp.StatusCode, ciphertext)
	}
	plain, err := seal.OpenShare(fragment, ciphertext)
	if err != nil {
		return nil, api.Err(400, "BAD_REQUEST", "Invalid shared thread")
	}
	defer clear(plain)
	var version struct {
		V float64 `json:"v"`
	}
	if trimmed := bytes.TrimSpace(plain); len(trimmed) == 0 || trimmed[0] != '{' || json.Unmarshal(trimmed, &version) != nil || int(version.V) != 1 {
		return nil, api.Err(400, "BAD_REQUEST", "Invalid shared thread")
	}
	return SharedThreadView(id, plain)
}

// An export is one streamed ZIP archive of the chats a client keeps on its
// device, which it sends, and, with its content key, everything it stores
// in the cloud. Native backups hold every chat whole; a conversations
// export holds them in the Claude-compatible format the web app wrote.
// export 是一个流式输出的 ZIP 归档，包含客户端保存在设备上的聊天（由客户端发送），以及在提供 content key 时
// 其存储在云端的所有内容。原生 backup 完整保存每个 chat；conversations export 则以 web 应用所写的
// Claude 兼容格式保存。

const (
	// maxExportBody bounds an export request: its device chats and the
	// images they show.
	// maxExportBody 限制 export 请求的大小：其设备 chat 及其展示的 image。
	maxExportBody = 512 << 20
	// maxExportParts bounds the chats and images an export request sends.
	// maxExportParts 限制 export 请求发送的 chat 与 image 数量。
	maxExportParts = maxArchiveEntries
	// maxProjectsExport bounds the Claude-compatible projects file.
	// maxProjectsExport 限制 Claude 兼容 projects 文件的大小。
	maxProjectsExport = 64 << 20
)

type exportOptions struct {
	api.KeyRequest
	Format string `json:"format"`
}

// exportForm is an export request: its options, and its device chats and
// the originals of their images, spooled to scratch space.
// exportForm 是一个 export 请求：其选项，以及暂存到临时空间的设备 chat 及其 image 原件。
type exportForm struct {
	options   exportOptions
	spool     *scratchFile
	threads   []spooledPart
	originals map[string]spooledPart
}

type spooledPart struct {
	offset, size int64
}

// HandleExport 处理 export 请求，按格式流式输出归档或项目文件。
func (ar *Service) HandleExport(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	// An archive calls no model, so any caller may make one, with or
	// without chat access: a signed-out caller of the chats its tab keeps.
	// 归档不调用模型，因此任何调用方都可创建，无论是否拥有 chat 访问权：匿名调用方只能访问其标签页持有的 chat。
	p, err := ar.Auth.Caller(r, true)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	limit := int64(maxExportBody)
	if p.Anonymous {
		limit = maxAnonymousExportBody
		end, err := ar.anonymous.start(p)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		defer end()
	}
	size, err := requestSize(r, limit)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	lease, err := ar.Scratch.Lease(r.Context(), size)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer lease.Release()
	lift := ar.Scratch.idleBody(w, r)
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	form, err := readExportForm(r, lease, func(options exportOptions) error {
		if p.Anonymous && len(options.Key) != 0 {
			return auth.ErrAnonymousCloud
		}
		return ar.Store.AuthorizeArchive(r.Context(), p, options.Key, "export")
	})
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer form.options.ClearKey()
	lift()
	var key *auth.ContentKey
	if len(form.options.Key) != 0 {
		decoded, err := auth.DecodeContentKey(form.options.Key)
		form.options.ClearKey()
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		defer clear(decoded[:])
		key = &decoded
	}
	ctx := r.Context()
	switch form.options.Format {
	case "native":
		streamArchive(w, "tinfoil-backup.zip", func(out *archiveResponse) error { return ar.writeBackup(ctx, p, key, form, out, started) })
	case "conversations":
		streamArchive(w, "tinfoil-chats.zip", func(out *archiveResponse) error { return ar.writeConversations(ctx, p, key, form, out, started) })
	case "claude-projects":
		if key == nil {
			api.Respond(w, nil, api.Err(401, "KEY_REQUIRED", "A content key is required"))
			return
		}
		projects, err := ar.claudeProjects(ctx, p, *key)
		if err == nil {
			w.Header().Set("Content-Disposition", `attachment; filename="projects.json"`)
		}
		api.Respond(w, projects, err)
	default:
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "format must be native, conversations or claude-projects"))
	}
}

func readExportForm(r *http.Request, lease *Lease, authorize func(exportOptions) error) (_ *exportForm, err error) {
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, api.Err(400, "BAD_REQUEST", "Expected multipart form data")
	}
	spool, err := lease.create()
	if err != nil {
		return nil, err
	}
	form := &exportForm{spool: spool, originals: map[string]spooledPart{}}
	defer func() {
		if err != nil {
			form.options.ClearKey()
		}
	}()
	hasOptions, ids := false, map[string]bool{}
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= maxExportParts {
			return nil, formError(err)
		}
		switch part.FormName() {
		case "options":
			if hasOptions {
				return nil, api.Err(400, "BAD_REQUEST", "Duplicate multipart field")
			}
			hasOptions = true
			if err := decodeFormField(part, &form.options); err != nil {
				return nil, err
			}
			if err := authorize(form.options); err != nil {
				return nil, err
			}
		case "thread":
			if !hasOptions {
				return nil, api.Err(400, "BAD_REQUEST", "options must precede the chats")
			}
			data, err := io.ReadAll(io.LimitReader(part, domain.MaxTurnBody+1))
			if err != nil {
				return nil, formError(err)
			}
			if len(data) > domain.MaxTurnBody {
				return nil, api.Err(413, "PAYLOAD_TOO_LARGE", "A chat exceeds the size limit")
			}
			if !api.IsObject(data) {
				return nil, api.Err(400, "BAD_REQUEST", "Each thread must be an object")
			}
			var named api.IDRequest
			json.Unmarshal(data, &named)
			if err := named.Validate(); err != nil {
				return nil, err
			}
			if err := domain.ValidateLocalThread(data); err != nil {
				return nil, err
			}
			if ids[named.ID] {
				return nil, api.Err(400, "BAD_REQUEST", "Each thread is sent once")
			}
			ids[named.ID] = true
			spooled, err := form.spoolPart(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			form.threads = append(form.threads, spooled)
		case "file":
			if !hasOptions {
				return nil, api.Err(400, "BAD_REQUEST", "options must precede the files")
			}
			id := part.FileName()
			if err := api.RequireID("file name", id); err != nil {
				return nil, err
			}
			if _, ok := form.originals[id]; ok {
				return nil, api.Err(400, "BAD_REQUEST", "Each file is sent once")
			}
			spooled, err := form.spoolPart(io.LimitReader(part, int64(attach.ImageUploads.MaxBytes)+1))
			if err != nil {
				return nil, err
			}
			if spooled.size > int64(attach.ImageUploads.MaxBytes) {
				return nil, api.Err(413, "ATTACHMENT_TOO_LARGE", "A file exceeds the size limit")
			}
			form.originals[id] = spooled
		default:
			return nil, api.Err(400, "BAD_REQUEST", "Unknown multipart field: "+part.FormName())
		}
		part.Close()
	}
	if !hasOptions {
		return nil, api.Err(400, "BAD_REQUEST", "options are required")
	}
	return form, nil
}

func (f *exportForm) spoolPart(r io.Reader) (spooledPart, error) {
	offset := f.spool.Size()
	n, err := io.Copy(f.spool, r)
	if err != nil {
		return spooledPart{}, formError(err)
	}
	return spooledPart{offset: offset, size: n}, nil
}

func (f *exportForm) read(part spooledPart) ([]byte, error) {
	return io.ReadAll(f.spool.section(part.offset, part.size))
}

// thread reads one of the request's device chats.
// thread 读取请求中的一个设备 chat。
func (f *exportForm) thread(i int) (domain.Thread, error) {
	data, err := f.read(f.threads[i])
	if err != nil {
		return domain.Thread{}, err
	}
	var named api.IDRequest
	json.Unmarshal(data, &named)
	return domain.ReadDeviceThread(named.ID, data)
}

// deviceImage is a device chat image's bytes: the original its client
// sent, or the copy the chat holds.
// deviceImage 是设备 chat 中 image 的字节：客户端发送的原件，或 chat 持有的副本。
func (f *exportForm) deviceImage(a *domain.Attachment) ([]byte, error) {
	if part, ok := f.originals[a.ID]; ok {
		return f.read(part)
	}
	if a.Base64 == "" {
		return nil, errAttachmentMissing
	}
	data, err := base64.StdEncoding.DecodeString(a.Base64)
	if err != nil {
		return nil, errAttachmentMissing
	}
	return data, nil
}

// noDocument is a device chat's document, which holds whatever content it has.
// noDocument 表示设备 chat 的 document，即保留其所拥有的任何内容。
func noDocument(*domain.Attachment) error { return errAttachmentMissing }

// cloudImage is a stored image's bytes; one buckets no longer holds is missing.
// cloudImage 返回已存储 image 的字节；bucket 已不再持有的则视为缺失。
func (ar *Service) cloudImage(ctx context.Context, p *auth.Principal) func(*domain.Attachment) ([]byte, error) {
	return func(a *domain.Attachment) ([]byte, error) {
		if a.Base64 == "" && a.StorageKey() == "" {
			return nil, errAttachmentMissing
		}
		return readStored(ctx, func() ([]byte, error) { return ar.Attachments.Bytes(ctx, p, a) })
	}
}

func (ar *Service) cloudDocument(ctx context.Context, p *auth.Principal) func(*domain.Attachment) error {
	return func(a *domain.Attachment) error {
		_, err := readStored(ctx, func() (struct{}, error) { return struct{}{}, ar.Attachments.InflateDocument(ctx, p, a) })
		return err
	}
}

// missing is whether an attachment's content is left out: its source does
// not hold it, or did not serve it.
// missing 判断 attachment 的内容是否被省略：其来源未持有或未提供。
func missing(err error) bool { return err == errAttachmentMissing || err == errAttachmentUnavailable }

// logFailedExport records an export that failed as it was written: why, how
// long its request ran, and how many chats it held by then.
// logFailedExport 记录写入过程中失败的 export：原因、请求运行时长，以及当时已处理的 chat 数。
func logFailedExport(ctx context.Context, format string, started time.Time, chats int, err error) {
	if err == nil {
		return
	}
	e := api.AsError(err)
	slog.Warn("export failed", "format", format, "code", e.Code, "status", e.HTTP, "canceled", ctx.Err() != nil, "elapsed", time.Since(started).Round(time.Millisecond), "chats", chats)
}

// streamArchive answers with an archive as it is written. A failure before
// its first bytes is answered as an error; one after cuts the response,
// so the client never keeps a partial archive.
// streamArchive 边写边输出归档。在首字节之前失败会以错误应答；之后失败则中断响应，使客户端不会保留残缺归档。
func streamArchive(w http.ResponseWriter, name string, write func(*archiveResponse) error) {
	out := &archiveResponse{w: w, name: name}
	if err := write(out); err != nil {
		if !out.started {
			api.Respond(w, nil, err)
			return
		}
		panic(http.ErrAbortHandler)
	}
}

type archiveResponse struct {
	w       http.ResponseWriter
	name    string
	started bool
}

// Write 在首次写入时设置响应头与状态码，然后写出数据。
func (a *archiveResponse) Write(p []byte) (int, error) {
	if !a.started {
		a.started = true
		a.w.Header().Set("Content-Type", "application/zip")
		a.w.Header().Set("Cache-Control", "no-store")
		a.w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": a.name}))
		a.w.WriteHeader(http.StatusOK)
	}
	return a.w.Write(p)
}

// Flush sends the client what the archive has written so far.
// Flush 把归档目前已写入的内容发送给客户端。
func (a *archiveResponse) Flush() error {
	if !a.started {
		return nil
	}
	return http.NewResponseController(a.w).Flush()
}

// writeBackup writes a native backup: the cloud's projects, documents and
// chats when the key is given, then the device's chats.
// writeBackup 写一个原生 backup：给定 key 时先写云端的 project、document 与 chat，再写设备 chat。
func (ar *Service) writeBackup(ctx context.Context, p *auth.Principal, key *auth.ContentKey, form *exportForm, out io.Writer, started time.Time) (err error) {
	b := NewBackupWriter(out)
	defer func() {
		logFailedExport(ctx, "native", started, b.manifest.Counts.CloudChats+b.manifest.Counts.LocalChats, err)
	}()
	rel := backupRelationships{ProjectChats: []projectChatLink{}, ProjectDocuments: []projectDocumentLink{}, ChatImages: []chatImageLink{}}
	cloudChats := map[string]bool{}
	if key != nil {
		if err := ar.renewGrant(ctx, p, key); err != nil {
			return err
		}
		projects := map[string]bool{}
		err := ar.Store.EachRowOrUnreadable(ctx, p, *key, "project", "", func(row *domain.StoredRow) error {
			project, err := domain.ReadProject(row)
			if err != nil {
				b.omit(BackupOmission{Kind: "project", SourceID: row.ID, Category: "invalid", Reason: "row_unreadable"})
				return nil
			}
			if err := b.addJSON("projects", BackupEntityPath("projects", project.ID), project); err != nil {
				return err
			}
			b.manifest.Counts.Projects++
			projects[project.ID] = true
			docs, err := ar.Store.ProjectDocuments(ctx, p, *key, project.ID, func(row string, _ error) error {
				b.omit(BackupOmission{Kind: "project_document", SourceID: strings.TrimPrefix(row, project.ID+"/"), ParentSourceID: project.ID, Category: "invalid", Reason: "row_unreadable"})
				return nil
			})
			if err != nil {
				return err
			}
			for _, doc := range docs {
				entry := BackupDocument{ID: doc.ID, ProjectID: project.ID, Filename: doc.Filename, ContentType: doc.ContentType, SizeBytes: doc.SizeBytes, ExtractedText: doc.Content, CreatedAt: domain.LooseTime(doc.CreatedAt), UpdatedAt: domain.LooseTime(doc.UpdatedAt)}
				if err := b.addJSON("project_documents", BackupDocumentPath(project.ID, doc.ID), entry); err != nil {
					return err
				}
				b.manifest.Counts.ProjectDocuments++
				rel.ProjectDocuments = append(rel.ProjectDocuments, projectDocumentLink{project.ID, doc.ID})
			}
			return nil
		}, func(id string) {
			b.omit(BackupOmission{Kind: "project", SourceID: id, Category: "invalid", Reason: "row_unreadable"})
		})
		if err != nil {
			return err
		}
		err = ar.eachStoredChat(ctx, p, key, func(id string) {
			b.omit(BackupOmission{Kind: "cloud_chat", SourceID: id, Category: "invalid", Reason: "row_unreadable"})
		}, nil, func(t *domain.Thread, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error) error {
			cloudChats[t.ID] = true
			if project := domain.ValueOr(t.ProjectID, ""); project != "" {
				if projects[project] {
					rel.ProjectChats = append(rel.ProjectChats, projectChatLink{project, t.ID})
				} else {
					t.ProjectID = nil
					b.omit(BackupOmission{Kind: "relationship", SourceID: t.ID, ParentSourceID: project, Category: "unavailable", Reason: "project_reference_unavailable"})
				}
			}
			return backupChat(b, &rel, "cloud_chats", t, image, document)
		})
		if err != nil {
			return err
		}
	}
	for i := range form.threads {
		t, err := form.thread(i)
		if err != nil {
			return err
		}
		// A chat moved to the cloud whose device copy remains is kept once.
		// 已上云但设备上仍有副本的 chat 只保留一次。
		if cloudChats[t.ID] {
			b.omit(BackupOmission{Kind: "local_chat", SourceID: t.ID, Category: "invalid", Reason: "duplicate_chat_id"})
			continue
		}
		if err := backupChat(b, &rel, "local_chats", &t, form.deviceImage, noDocument); err != nil {
			return err
		}
	}
	if err := b.addJSON("relationships", relationshipsPath, rel); err != nil {
		return err
	}
	b.manifest.Counts.Relationships = rel.count()
	return b.close(uuid.NewString(), api.Now())
}

// eachStoredChat hands write the stored chats page by page, with the reads
// of each chat's attachments, leaving out those skip, when given, names. It
// names each chat this harness or the key cannot read.
// eachStoredChat 逐页把已存储 chat 交给 write，并附上每个 chat 的 attachment 读取；若给定 skip，则略过其命名的 chat。
// 它报告本 harness 或该 key 无法读取的每个 chat。
func (ar *Service) eachStoredChat(ctx context.Context, p *auth.Principal, key *auth.ContentKey, unreadable func(id string), skip func(*domain.Thread) bool, write func(t *domain.Thread, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error) error) error {
	if err := ar.renewGrant(ctx, p, key); err != nil {
		return err
	}
	return ar.Store.EachPage(ctx, p, *key, "chat", "", func(rows []domain.StoredRow, unopened []string) error {
		for _, id := range unopened {
			unreadable(id)
		}
		chats := make([]*domain.Thread, 0, len(rows))
		for i := range rows {
			read, err := domain.ReadThreadRow(&rows[i])
			if err != nil {
				unreadable(rows[i].ID)
				continue
			}
			if skip == nil || !skip(&read.Thread) {
				chats = append(chats, &read.Thread)
			}
		}
		if err := ar.renewGrant(ctx, p, key); err != nil {
			return err
		}
		reads := ar.readAhead(ctx, p, chats)
		defer reads.close()
		for k, t := range chats {
			image, document := reads.chat(k)
			if err := write(t, image, document); err != nil {
				return err
			}
		}
		return nil
	})
}

// renewGrant has the export's grant last another while from now. An export
// of many chats can outlive the grant it began with; the request it serves,
// which ends with it, bounds it.
// renewGrant 让 export 的授权从此刻起再持续一段时间。包含大量 chat 的 export 可能超过其初始授权的时长；
// 它服务的请求会随其结束而结束，从而约束它。
func (ar *Service) renewGrant(ctx context.Context, p *auth.Principal, key *auth.ContentKey) error {
	return ar.Store.AuthorizeArchive(ctx, p, key[:], "export")
}

// backupChat writes a chat with each of its images beside it. An image or
// document whose content cannot be read is left out and listed as omitted.
// backupChat 写出一个 chat 及其每张 image。无法读取内容的 image 或 document 会被略过并列为 omitted。
func backupChat(b *BackupWriter, rel *backupRelationships, kind string, t *domain.Thread, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error) error {
	entry := backupThread{Thread: *t, Messages: make([]backupMessage, 0, len(t.Messages))}
	entry.ActiveRun, entry.LastRequestID, entry.LastRequestKind, entry.Flagged, entry.HiddenCount, entry.RecordVersion, entry.ImportedFrom = nil, "", "", false, 0, 0, nil
	omit := func(id string, err error) {
		reason := "attachment_not_found"
		if err == errAttachmentUnavailable {
			reason = "attachment_unreadable"
		}
		b.omit(BackupOmission{Kind: "attachment", SourceID: id, ParentSourceID: t.ID, Category: "unavailable", Reason: reason})
	}
	addImage := func(meta backupImage, data []byte) error {
		meta.MimeType, meta.SizeBytes = http.DetectContentType(data), new(int64(len(data)))
		if err := b.addJSON("images", BackupImagePath(meta.ID, ".json"), meta); err != nil {
			return err
		}
		if err := b.Add("images", BackupImagePath(meta.ID, ".bin"), data); err != nil {
			return err
		}
		b.manifest.Counts.Images++
		rel.ChatImages = append(rel.ChatImages, chatImageLink{t.ID, meta.ID})
		return nil
	}
	for i, m := range t.Messages {
		message := backupMessage{Message: m}
		for j, a := range m.Attachments {
			id := fmt.Sprintf("%s/%d/%d", t.ID, i, j)
			if !a.IsImage() && !a.HasContent() && a.StorageKey() != "" {
				if err := document(&a); missing(err) {
					omit(id, err)
					continue
				} else if err != nil {
					return err
				}
			}
			attachment := backupAttachment{Attachment: a}
			attachment.Base64, attachment.EncryptionKey, attachment.Key, attachment.StorageThreadID, attachment.Attachment.Pages = "", "", "", "", nil
			if a.IsImage() {
				data, err := image(&a)
				if missing(err) {
					omit(id, err)
					continue
				}
				if err != nil {
					return err
				}
				if err := addImage(backupImage{ID: id, ChatID: t.ID, MessageIndex: i, AttachmentID: a.ID, FileName: cmp.Or(a.FileName, "image"), Description: a.Description}, data); err != nil {
					return err
				}
				attachment.ImageID = id
			}
			for k, page := range a.Pages {
				shown := backupPage{Page: page.Page, Text: page.Text, IsScanned: page.IsScanned}
				if page.Image != "" {
					pageID := fmt.Sprintf("%s/page/%d", id, k)
					_, encoded, found := strings.Cut(page.Image, ",")
					if !found || !strings.HasPrefix(page.Image, "data:") {
						encoded = page.Image
					}
					data, err := base64.StdEncoding.DecodeString(encoded)
					if err != nil {
						omit(pageID, errAttachmentMissing)
					} else if err := addImage(backupImage{ID: pageID, ChatID: t.ID, MessageIndex: i, AttachmentID: a.ID, Page: new(page.Page), FileName: fmt.Sprintf("%s-page-%d.jpg", a.ID, page.Page)}, data); err != nil {
						return err
					} else {
						shown.ImageID = pageID
					}
				}
				attachment.Pages = append(attachment.Pages, shown)
			}
			message.Attachments = append(message.Attachments, attachment)
		}
		entry.Messages = append(entry.Messages, message)
	}
	if err := b.addJSON(kind, BackupEntityPath(kind, t.ID), entry); err != nil {
		return err
	}
	if kind == "cloud_chats" {
		b.manifest.Counts.CloudChats++
	} else {
		b.manifest.Counts.LocalChats++
	}
	return nil
}

// The conversations export is Claude's conversation format with Tinfoil's
// attachments, as the web app wrote it: conversations.json, each image's
// bytes under attachments/, and a manifest of them.
// conversations export 是带 Tinfoil attachment 的 Claude 对话格式，如 web 应用所写：conversations.json、
// 每张 image 的字节放在 attachments/ 下，外加一个它们的 manifest。

type exportConversation struct {
	UUID         string          `json:"uuid"`
	Name         string          `json:"name"`
	CreatedAt    string          `json:"created_at"`
	UpdatedAt    string          `json:"updated_at"`
	ProjectID    string          `json:"projectId,omitempty"`
	ChatMessages []exportMessage `json:"chat_messages"`
}

type exportMessage struct {
	UUID        string             `json:"uuid"`
	Text        string             `json:"text"`
	Sender      string             `json:"sender"`
	CreatedAt   string             `json:"created_at"`
	Content     []exportThinking   `json:"content,omitempty"`
	Attachments []exportAttachment `json:"attachments,omitempty"`
}

type exportThinking struct {
	Type     string `json:"type"`
	Thinking string `json:"thinking"`
}

type exportAttachment struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	FileName    string `json:"fileName"`
	MimeType    string `json:"mimeType,omitempty"`
	FileSize    *int64 `json:"fileSize,omitempty"`
	ExportPath  string `json:"exportPath,omitempty"`
	TextContent string `json:"textContent,omitempty"`
}

// ConversationsManifest 是 conversations export 的 manifest，记录其中的 chat 与 attachment。
type ConversationsManifest struct {
	Version         int            `json:"version"`
	CreatedAt       string         `json:"created_at"`
	ChatCount       int            `json:"chat_count"`
	AttachmentCount int            `json:"attachment_count"`
	Attachments     []exportedFile `json:"attachments"`
	Warnings        []string       `json:"warnings"`
}

type exportedFile struct {
	ExportPath string `json:"exportPath"`
	SHA256     string `json:"sha256"`
	FileSize   int    `json:"fileSize"`
}

// conversationsExport sends conversations.json as it reads the chats, then
// the images they show, then the manifest. Each image is read before its
// chat is written, so a chat names only the images the archive holds, and
// read again once every chat is written: kept meanwhile, an export's images
// could take more scratch space than there is.
// conversationsExport 边读 chat 边写 conversations.json，再写它们展示的 image，最后写 manifest。每张 image
// 在其 chat 写入前读取，因此 chat 只命名归档实际持有的 image；全部 chat 写完后再次读取：若中途保留，
// export 的 image 可能占用超过现有量的临时空间。
type conversationsExport struct {
	zip *zip.Writer
	out *archiveResponse
	// conversations is conversations.json, which deflate compresses.
	// conversations 是 conversations.json，用 deflate 压缩。
	conversations io.Writer
	deflate       *flate.Writer
	used          map[string]bool
	manifest      ConversationsManifest
	// images are those the chats written so far show, in the order they
	// are written.
	// images 是到目前为止已写 chat 所展示的 image，按写入顺序排列。
	images []exportedImage
	// again is the chat last read again for the images it holds.
	// again 是最近一次为读取其 image 而重读的 chat。
	again struct {
		id   string
		chat domain.Thread
	}
}

// exportedImage is an image a chat names, to write once the chats are: the
// path it names, the digest of the bytes it was read as, and what reading
// it again takes. One whose bytes its chat holds is read from the chat,
// read again, rather than kept meanwhile.
// exportedImage 是 chat 命名的 image，等 chat 写完后再写：含其路径、读取时字节的摘要，以及再次读取所需信息。
// 字节由其 chat 持有的，会从 chat 中再次读取，而非中途保留。
type exportedImage struct {
	path, sha256 string
	attachment   domain.Attachment
	held         *heldImage
}

// heldImage is an image in its chat: the chat, how to read it again, and
// the image's place in it.
// heldImage 是其 chat 中的 image：含 chat、再次读取方式，以及该 image 在其中的位置。
type heldImage struct {
	chatID      string
	chat        func() (domain.Thread, error)
	message, at int
}

// imagesPerGrant bounds the stored images an export reads again under one
// renewal of its grant, as a page bounds the chats it reads under one.
// imagesPerGrant 限制 export 在一次授权续期下再次读取的已存储 image 数，正如 page 限制一次读取的 chat 数。
const imagesPerGrant = 100

func (ar *Service) writeConversations(ctx context.Context, p *auth.Principal, key *auth.ContentKey, form *exportForm, out *archiveResponse, started time.Time) (err error) {
	e := &conversationsExport{zip: zip.NewWriter(out), out: out, used: map[string]bool{}, manifest: ConversationsManifest{Version: 1, Attachments: []exportedFile{}, Warnings: []string{}}}
	e.zip.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		fw, err := flate.NewWriter(w, flate.DefaultCompression)
		e.deflate = fw
		return fw, err
	})
	defer func() { logFailedExport(ctx, "conversations", started, e.manifest.ChatCount, err) }()
	if e.conversations, err = e.zip.CreateHeader(&zip.FileHeader{Name: "conversations.json", Method: zip.Deflate, Modified: api.Now()}); err != nil {
		return err
	}
	device := map[string]bool{}
	for i := range form.threads {
		t, err := form.thread(i)
		if err != nil {
			return err
		}
		device[t.ID] = true
		if err := e.chat(&t, "", form.deviceImage, noDocument, func() (domain.Thread, error) { return form.thread(i) }); err != nil {
			return err
		}
	}
	deviceImages := e.images
	e.images = nil
	if key != nil {
		// A chat this harness or the key cannot read is left out, and listed.
		// 本 harness 或该 key 无法读取的 chat 会被略过并列入清单。
		err := ar.eachStoredChat(ctx, p, key, func(id string) {
			e.manifest.Warnings = append(e.manifest.Warnings, "Could not read chat "+id)
		}, func(t *domain.Thread) bool { return device[t.ID] }, func(t *domain.Thread, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error) error {
			id := t.ID
			return e.chat(t, domain.ValueOr(t.ProjectID, ""), image, document, func() (domain.Thread, error) {
				row, err := ar.Store.LoadThread(ctx, p, *key, id)
				if err != nil {
					return domain.Thread{}, err
				}
				return row.Thread, nil
			})
		})
		if err != nil {
			return err
		}
	}
	end := "\n]"
	if e.manifest.ChatCount == 0 {
		end = "[]"
	}
	if _, err := io.WriteString(e.conversations, end); err != nil {
		return err
	}
	for _, image := range deviceImages {
		data, err := e.readAgain(image, form.deviceImage)
		if err := e.writeImage(image, data, err); err != nil {
			return err
		}
	}
	for start := 0; start < len(e.images); start += imagesPerGrant {
		if err := ar.writeStoredImages(ctx, p, key, e, e.images[start:min(start+imagesPerGrant, len(e.images))]); err != nil {
			return err
		}
	}
	e.manifest.CreatedAt = archiveTime(api.Now())
	e.manifest.AttachmentCount = len(e.manifest.Attachments)
	manifest, err := indentedJSON(e.manifest, "")
	if err != nil {
		return err
	}
	if err := writeEntry(e.zip, ManifestPath, manifest, true); err != nil {
		return err
	}
	return e.zip.Close()
}

// writeStoredImages reads stored images again under a renewal of the
// export's grant: those storage keeps a few at a time ahead of the one it
// writes.
// writeStoredImages 在 export 授权续期下再次读取已存储 image：存储每次提前保留几个，赶在写入的那个之前。
func (ar *Service) writeStoredImages(ctx context.Context, p *auth.Principal, key *auth.ContentKey, e *conversationsExport, images []exportedImage) error {
	if err := ar.renewGrant(ctx, p, key); err != nil {
		return err
	}
	var kept []domain.Attachment
	for _, image := range images {
		if image.held == nil {
			kept = append(kept, image.attachment)
		}
	}
	reads := ar.readImagesAhead(ctx, p, kept)
	defer reads.close()
	held, k := ar.cloudImage(ctx, p), 0
	for _, image := range images {
		read := held
		if image.held == nil {
			read, _ = reads.chat(k)
			k++
		}
		data, err := e.readAgain(image, read)
		if err := e.writeImage(image, data, err); err != nil {
			return err
		}
	}
	return nil
}

// readAgain reads an image again as read does: from its chat, read again,
// when the chat holds its bytes.
// readAgain 像 read 那样再次读取 image：当 chat 持有其字节时，从该 chat 再次读取。
func (e *conversationsExport) readAgain(image exportedImage, read func(*domain.Attachment) ([]byte, error)) ([]byte, error) {
	in := image.held
	if in == nil {
		return read(&image.attachment)
	}
	if e.again.id != in.chatID {
		t, err := in.chat()
		if err != nil {
			return nil, err
		}
		e.again.id, e.again.chat = in.chatID, t
	}
	messages := e.again.chat.Messages
	if in.message >= len(messages) || in.at >= len(messages[in.message].Attachments) {
		return nil, errAttachmentUnavailable
	}
	return read(&messages[in.message].Attachments[in.at])
}

// writeImage writes an image a chat names, read again. One that cannot be
// read as it first was fails the export, whose chats already name it.
// writeImage 写出 chat 命名的、已再次读取的 image。无法按首次读取方式读到的会使 export 失败，因为其 chat 已命名它。
func (e *conversationsExport) writeImage(image exportedImage, data []byte, err error) error {
	if err != nil {
		return err
	}
	if api.HashHex(data) != image.sha256 {
		return errAttachmentUnavailable
	}
	return writeEntry(e.zip, image.path, data, false)
}

// flush sends the client what the archive holds so far.
// flush 把归档当前持有的内容发送给客户端。
func (e *conversationsExport) flush() error {
	if err := e.deflate.Flush(); err != nil {
		return err
	}
	if err := e.zip.Flush(); err != nil {
		return err
	}
	return e.out.Flush()
}

// indentedJSON is a value as JSON.stringify(value, null, 2) writes it,
// each line after the first prefixed.
// indentedJSON 按 JSON.stringify(value, null, 2) 的格式输出值，第一行之后每行都加前缀。
func indentedJSON(v any, prefix string) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent(prefix, "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// chat writes a chat, which reread reads again.
// chat 写出一个 chat，reread 可再次读取它。
func (e *conversationsExport) chat(t *domain.Thread, projectID string, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error, reread func() (domain.Thread, error)) error {
	c := exportConversation{UUID: t.ID, Name: t.Title, CreatedAt: exportTime(t.CreatedAt, t.UpdatedAt), UpdatedAt: exportTime(t.UpdatedAt, t.CreatedAt), ProjectID: projectID, ChatMessages: []exportMessage{}}
	// The summaries of compacted messages are not part of the conversation,
	// nor counted in its messages' ids.
	// 已 compacted 消息的摘要不属于对话，也不计入其消息 id。
	for i, m := range t.Messages {
		if m.Role == domain.CompactionRole {
			continue
		}
		sender := "assistant"
		if m.Role == "user" {
			sender = "human"
		}
		message := exportMessage{UUID: fmt.Sprintf("%s_msg_%d", t.ID, len(c.ChatMessages)), Text: m.Content, Sender: sender, CreatedAt: exportTime(m.CreatedAt, t.CreatedAt)}
		if m.Role == "assistant" {
			message.Text = cmp.Or(m.Content, TimelineText(m.Timeline, "content"))
			if thoughts := TimelineText(m.Timeline, "thinking"); thoughts != "" {
				message.Content = []exportThinking{{Type: "thinking", Thinking: thoughts}}
			}
		}
		for j, a := range m.Attachments {
			exported, err := e.attachment(a, image, document, heldImage{chatID: t.ID, chat: reread, message: i, at: j})
			if err != nil {
				return err
			}
			message.Attachments = append(message.Attachments, exported)
		}
		c.ChatMessages = append(c.ChatMessages, message)
	}
	encoded, err := indentedJSON(c, "  ")
	if err != nil {
		return err
	}
	separator := ",\n  "
	if e.manifest.ChatCount == 0 {
		separator = "[\n  "
	}
	e.manifest.ChatCount++
	if _, err := io.WriteString(e.conversations, separator); err != nil {
		return err
	}
	if _, err := e.conversations.Write(encoded); err != nil {
		return err
	}
	return e.flush()
}

func (e *conversationsExport) attachment(a domain.Attachment, image func(*domain.Attachment) ([]byte, error), document func(*domain.Attachment) error, place heldImage) (exportAttachment, error) {
	if !a.IsImage() {
		if !a.HasContent() {
			if err := document(&a); missing(err) {
				e.manifest.Warnings = append(e.manifest.Warnings, "Could not fetch document "+a.ID)
			} else if err != nil {
				return exportAttachment{}, err
			}
		}
		return exportAttachment{ID: a.ID, Type: "document", FileName: a.FileName, MimeType: a.MimeType, FileSize: a.Size, TextContent: exportDocumentText(a.TextContent, a.Pages)}, nil
	}
	safeID := sanitizeFilename(a.ID, "attachment")
	safeName := sanitizeFilename(a.FileName, safeID+".bin")
	exportPath := e.uniquePath(safeID, safeName)
	exported := exportAttachment{ID: a.ID, Type: "image", FileName: safeName, MimeType: a.MimeType, FileSize: a.Size}
	data, err := image(&a)
	if missing(err) {
		e.manifest.Warnings = append(e.manifest.Warnings, "Could not fetch attachment "+a.ID)
		return exported, nil
	}
	if err != nil {
		return exportAttachment{}, err
	}
	sum := api.HashHex(data)
	kept := exportedImage{path: exportPath, sha256: sum, attachment: domain.Attachment{ID: a.ID, Kind: a.Kind, EncryptionKey: a.EncryptionKey, Key: a.Key}}
	if a.Base64 != "" {
		kept.held = &place
	}
	e.images = append(e.images, kept)
	exported.ExportPath, exported.FileSize = exportPath, new(int64(len(data)))
	e.manifest.Attachments = append(e.manifest.Attachments, exportedFile{ExportPath: exportPath, SHA256: sum, FileSize: len(data)})
	return exported, nil
}

// uniquePath is where an image is written, numbered past any image of the
// same id and name before it.
// uniquePath 是 image 的写入位置，在出现相同 id 与名称的 image 时追加序号。
func (e *conversationsExport) uniquePath(safeID, safeName string) string {
	base := "attachments/" + safeID + "/" + safeName
	if !e.used[base] {
		e.used[base] = true
		return base
	}
	root, extension := safeName, ""
	if dot := strings.LastIndex(safeName, "."); dot > 0 {
		root, extension = safeName[:dot], safeName[dot:]
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("attachments/%s/%s-%d%s", safeID, root, suffix, extension)
		if !e.used[candidate] {
			e.used[candidate] = true
			return candidate
		}
	}
}

// sanitizeFilename is a name made safe as one archive path segment, as the
// web app made it: its last segment, with every character outside a small
// set, counted in UTF-16 units, replaced.
// sanitizeFilename 把名称做成安全的单段归档路径，与 web 应用一致：取最后一段，将小集合之外的字符（按 UTF-16 计）替换掉。
func sanitizeFilename(name, fallback string) string {
	return cmp.Or(cleanFilename(name), cleanFilename(fallback), "file")
}

func cleanFilename(name string) string {
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r < 0x20 || r == 0x7f:
		case r < 0x80 && (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._ ()+-", r)):
			b.WriteRune(r)
		case r > 0xffff:
			b.WriteString("__")
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(strings.TrimLeft(b.String(), "."), " ")
}

// exportDocumentText is a document's text, or its pages' when it has none.
// exportDocumentText 返回 document 的文本，无文本时返回其 page 的文本。
func exportDocumentText(text string, pages []domain.DocumentPage) string {
	if strings.TrimSpace(text) != "" {
		return text
	}
	var parts []string
	for _, page := range pages {
		if strings.TrimSpace(page.Text) != "" {
			parts = append(parts, page.Text)
		}
	}
	joined := strings.Join(parts, "\n\n---\n\n")
	if strings.TrimSpace(joined) == "" {
		return ""
	}
	return joined
}

// TimelineText joins the text of a reply's blocks of one type.
// TimelineText 拼接回复中某一类型 block 的文本。
func TimelineText(timeline []domain.Block, kind string) string {
	var b strings.Builder
	for _, block := range timeline {
		if block.Type == kind {
			b.WriteString(block.Content)
		}
	}
	return b.String()
}

// exportTime is the first time given that parses, written as JavaScript
// writes a date, or the epoch when none does.
// exportTime 返回给定值中第一个能解析的时间，以 JavaScript 写日期的方式输出；都不解析时返回纪元。
func exportTime(values ...string) string {
	for _, value := range values {
		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return archiveTime(t)
		}
	}
	return archiveTime(time.UnixMilli(0))
}

// claudeProject is a project in Claude's export format, with its
// documents' text.
// claudeProject 是 Claude export 格式中的 project，含其 document 文本。
type claudeProject struct {
	UUID           string           `json:"uuid"`
	Name           string           `json:"name"`
	Description    string           `json:"description"`
	PromptTemplate string           `json:"prompt_template"`
	CreatedAt      string           `json:"created_at"`
	UpdatedAt      string           `json:"updated_at"`
	Docs           []claudeDocument `json:"docs"`
}

type claudeDocument struct {
	UUID      string `json:"uuid"`
	Filename  string `json:"filename"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

func (ar *Service) claudeProjects(ctx context.Context, p *auth.Principal, key auth.ContentKey) ([]claudeProject, error) {
	projects := []claudeProject{}
	total := 0
	err := ar.Store.EachRow(ctx, p, key, "project", "", func(row *domain.StoredRow) error {
		// A document this harness cannot read is left out, as a chat is.
		// 本 harness 无法读取的 document 会被略过，chat 亦然。
		docs, err := ar.Store.ProjectDocuments(ctx, p, key, row.ID, func(string, error) error { return nil })
		if err != nil {
			return err
		}
		project, err := domain.ReadProject(row)
		if err != nil {
			return nil
		}
		exported := claudeProject{UUID: project.ID, Name: project.Name, Description: project.Description, PromptTemplate: project.SystemInstructions, CreatedAt: project.CreatedAt, UpdatedAt: project.UpdatedAt, Docs: []claudeDocument{}}
		for _, doc := range docs {
			exported.Docs = append(exported.Docs, claudeDocument{UUID: doc.ID, Filename: doc.Filename, Content: doc.Content, CreatedAt: doc.CreatedAt})
		}
		if total += len(api.Raw(exported)); total > maxProjectsExport {
			return api.Err(413, "ATTACHMENT_TOO_LARGE", "The project export exceeds 64 MiB")
		}
		projects = append(projects, exported)
		return nil
	})
	return projects, err
}
