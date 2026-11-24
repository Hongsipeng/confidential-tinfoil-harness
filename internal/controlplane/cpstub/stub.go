// Package cpstub is an in-memory stand-in for the control plane's storage
// API as the harness uses it: sealed blobs with their revisions and
// metadata, the key registry with its passkey bundles, the legacy
// attachments and the attachment index with the purges, pending writes
// and orphans its maintenance reclaims, chat activity, the rows left to
// reseal with their cooldowns, the search index's publication with the
// objects retired from it, the imports reported ended, and the answers
// to changes made under an idempotency key, which a retry gets again.
// Tests seed its state, read it back after a call, and inject the
// failures the control plane can answer with.
// 包 cpstub 是 control plane 存储 API 的内存替身，模拟 harness 使用它的方式：带 revision 与元数据的密封
// blob、带 passkey bundle 的密钥注册表、旧版附件，以及附件索引连同其维护回收的 purges、pending
// writes 与 orphans、chat activity、待重新密封且带冷却期的行、搜索索引的发布及其退役的对象、已上报
// 结束的导入，以及对幂等 key 下所做变更的应答（重试会再次得到同一应答）。
// 测试会预置其状态、在调用后读回状态，并注入 control plane 可能返回的各类失败。
package cpstub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
)

// Blob is one stored row: its revision, the key id it was sealed under,
// the sealed bytes, and the metadata the control plane keeps beside it.
// Blob 是一行已存储的数据：其 revision、用于密封它的 key id、密封字节，以及 control plane 保存在其旁边的元数据。
type Blob struct {
	ETag         int64
	KeyID        string
	Body         []byte
	ProjectIDSet bool
	ProjectID    *string
	UpdatedAt    time.Time
	ActivityAt   *time.Time
	// AttachmentRefs is the attachments a chat's last write said it
	// still names; nil when the write did not say.
	AttachmentRefs []string
}

// ActivityBackfill is one request to record a chat's activity time.
// ActivityBackfill 是一次记录 chat 活动时间的请求。
type ActivityBackfill struct {
	ID         string `json:"id"`
	ETag       string `json:"etag"`
	ActivityAt string `json:"activity_at"`
}

// Store is the stub's state. Handlers run under Mu, and a test holds it
// while reading or changing the state once the server is up.
// Store 是 stub 的状态。处理器在 Mu 下运行，测试在服务器启动后读取或修改状态时会持有该锁。
type Store struct {
	Mu sync.Mutex
	// Blobs by "scope/id"; a profile's id is "profile".
	// 按 "scope/id" 索引的 Blob；profile 的 id 为 "profile"。
	Blobs map[string]*Blob
	// The registered current key: its id, the revision a register names
	// to replace it, how and when it was made, and the passkey bundles by
	// key id and credential id.
	// 已注册的当前密钥：其 id、替换它所指定的 revision、创建方式与时间，以及按 key id 和 credential id 索引的 passkey bundle。
	CurrentKeyID      string
	KeyETag           int64
	KeyCreatedVia     string
	KeyRegisteredAt   time.Time
	Bundles           map[string]map[string]controlplane.CurrentKeyBundle
	LegacyAttachments map[string][]byte // attachment id to ciphertext
	// LegacyAttachments 是附件 id 到密文的映射。
	GoneAttachments map[string]bool // attachment ids already moved to buckets
	// GoneAttachments 是已迁移到 buckets 的附件 id 集合。
	AttachmentIndex map[string]string // attachment id to chat id
	// AttachmentIndex 是附件 id 到 chat id 的映射。
	AttachmentOwners map[string]string // attachment id to the user it was registered for
	// AttachmentOwners 是附件 id 到其注册所属用户的映射。
	// AttachmentPurges are the attachments dropped from the index whose
	// bytes are still to be deleted, in order; a claim leases a batch of
	// them under a token, and an ack under that token drops them.
	// AttachmentPurges 是从索引中删除、其字节仍有待删除的附件（按顺序）；claim 会在某个 token 下租借一批，而在该 token 下的 ack 会将其移除。
	AttachmentPurges []controlplane.AttachmentPurge
	purgeClaims      map[string]string // attachment id to the token that claimed it
	// purgeClaims 是附件 id 到认领它的 token 的映射。
	// PendingAttachmentWrites are the writes the control plane releases to
	// the next sweep, and OrphanAttachments the index entries without an
	// owner it drops for the next, each handed over once.
	// PendingAttachmentWrites 是 control plane 释放给下一次 sweep 的写入，OrphanAttachments 则是它为下一次删除的无主索引条目，每项只移交一次。
	PendingAttachmentWrites []controlplane.PendingAttachmentRow
	OrphanAttachments       []controlplane.OrphanAttachmentRow
	// MigrationFailures counts the recorded reseal failures by "scope/id",
	// the id as the needs-migration list names it; a row with one is in
	// its cooldown and is not listed.
	// MigrationFailures 按 "scope/id" 统计已记录的重新密封失败次数（id 与 needs-migration 列表中的命名一致）；有一条失败记录的行处于冷却期，不会被列出。
	MigrationFailures map[string]int
	// SourceRevision counts the chat writes and deletes, which the search
	// index's publication is measured against.
	// SourceRevision 统计 chat 的写入与删除次数，搜索索引的发布以此作为衡量基准。
	SourceRevision int64
	// SearchState is the search index's publication: the object that
	// holds it, under which key and model, at which generation and fence,
	// and through which source revision it covers. SourceRevision and the
	// derived Incomplete are filled in as it is served.
	// SearchState 是搜索索引的发布：持有它的对象、所使用的 key 与 model、所处的 generation 与 fence，以及它覆盖到哪个 source revision。SourceRevision 与派生出的 Incomplete 会在对外提供时填充。
	SearchState controlplane.SearchIndexState
	// SearchIndexDeletions are the index objects retired from publication
	// and not yet acknowledged as deleted, by id; a claim leases a batch
	// of them under a token, and an ack under that token drops them.
	// SearchIndexDeletions 是已从发布中退役、但尚未确认删除的索引对象（按 id）；claim 会在某个 token 下租借一批，而在该 token 下的 ack 会将其移除。
	SearchIndexDeletions []controlplane.SearchIndexDeletion
	searchClaims         map[string]string // deletion id to the token that claimed it
	// searchClaims 是删除记录 id 到认领它的 token 的映射。
	// ImportOutcomes are the imports the enclave reported ended, in order.
	// ImportOutcomes 是 enclave 上报已结束的导入，按顺序排列。
	ImportOutcomes []controlplane.ImportOutcome
	// ResearchStarts are when each user last started a deep research, by
	// user id; the stub allows one a researchWindow, as the control plane does.
	// ResearchStarts 按 user id 记录每个用户最近一次启动 deep research 的时间；与 control plane 一样，stub 在一个 researchWindow 内只允许一次。
	ResearchStarts map[string]time.Time
	// RenewChatToken answers a renewal of the chat token a user's run holds
	// with the next token, or with the status the control plane refuses it
	// by. Unset, every renewal is refused as not found.
	// RenewChatToken 用下一个 token 来应答用户 run 所持有 chat token 的续期，或用 control plane 拒绝续期时的状态码来应答。未设置时，每次续期都会被当作 not found 拒绝。
	RenewChatToken func(user, held string) (token string, refused int)
	// answered are the changes made under an idempotency key, by the
	// caller and the key. The control plane keeps them for a day, the
	// stub for its life.
	// answered 是按调用者与 key 记录的在幂等 key 下所做的变更。control plane 会保留一天，stub 则在其整个生命周期内保留。
	answered map[string]idempotentAnswer
	// MinimumProfileSyncProtocol refuses profile overwrites by older
	// writers when set.
	// MinimumProfileSyncProtocol 设置后，会拒绝旧版本 writer 对 profile 的覆盖写入。
	MinimumProfileSyncProtocol int
	ActivityBackfills          []ActivityBackfill
	// Secret signs the claim on a legacy attachment, as the control plane
	// does when it shares one with the enclave. Empty omits the claim.
	// Secret 为旧版附件的 claim 签名，就像 control plane 与 enclave 共享附件时做的那样。为空则省略 claim。
	Secret string

	// Failure injection. A count is how many calls fail before the next
	// succeeds; a list of statuses is answered in turn. A reservation
	// purge refuses the reservation of an id while its earlier copy is
	// still being purged. A search conflict is another replica's
	// publication landing just before the next one, which is then
	// refused with the state that replica left.
	// 故障注入。计数表示在下一次成功之前有多少次调用会失败；状态码列表会依次返回。reservation purge 会在某个 id 的旧副本仍在清除时拒绝该 id 的预约。search conflict 表示另一个 replica 的发布恰好落在下一次发布之前，随后以该 replica 留下的状态拒绝下一次发布。
	BeforePutBlob    func(scope, id string)
	BeforeDeleteBlob func(scope, id string)
	PutBlobFailures  map[string]int
	GetBlobFailures  map[string][]int
	// ListStatusFailures is how many listings of each scope are answered
	// with a server error before the stub lists the scope again.
	// ListStatusFailures 表示每个 scope 的列表在被 stub 正常列出之前，会有多少次以服务器错误应答。
	ListStatusFailures map[string]int
	// NeedsMigrationFailures is the status a listing of rows to reseal
	// answers with, by the listing's number since the stub was made.
	// NeedsMigrationFailures 是待重新密封行列表按自 stub 创建以来的列表序号所应答的状态码。
	NeedsMigrationFailures          map[int]int
	needsMigrationListings          int
	DeleteAttachmentIndexFailures   map[string]int
	RegisterAttachmentIndexFailures map[string][]int
	AttachmentOwnerFailures         map[string][]int
	ReserveAttachmentPurges         map[string]int
	SearchConflict                  func(controlplane.SearchIndexState) controlplane.SearchIndexState
	// CaptureHeaders sees every request before it is served, outside Mu.
	// CaptureHeaders 会在每个请求被处理之前（在 Mu 之外）看到该请求。
	CaptureHeaders func(r *http.Request)

	mux *http.ServeMux
}

// New 返回一个已就绪的空 stub，所有路由均已注册。
func New() *Store {
	s := &Store{
		Blobs:                           map[string]*Blob{},
		Bundles:                         map[string]map[string]controlplane.CurrentKeyBundle{},
		AttachmentIndex:                 map[string]string{},
		AttachmentOwners:                map[string]string{},
		MigrationFailures:               map[string]int{},
		PutBlobFailures:                 map[string]int{},
		ListStatusFailures:              map[string]int{},
		GetBlobFailures:                 map[string][]int{},
		NeedsMigrationFailures:          map[int]int{},
		DeleteAttachmentIndexFailures:   map[string]int{},
		RegisterAttachmentIndexFailures: map[string][]int{},
		AttachmentOwnerFailures:         map[string][]int{},
		ReserveAttachmentPurges:         map[string]int{},
		ResearchStarts:                  map[string]time.Time{},
		purgeClaims:                     map[string]string{},
		searchClaims:                    map[string]string{},
		answered:                        map[string]idempotentAnswer{},
		mux:                             http.NewServeMux(),
	}
	s.mux.HandleFunc("PUT /api/sync/blob/chat/{id}", s.idempotent(s.handlePutBlob("chat")))
	s.mux.HandleFunc("PUT /api/sync/blob/profile", s.idempotent(s.handlePutBlob("profile")))
	s.mux.HandleFunc("PUT /api/sync/blob/project/{id}", s.idempotent(s.handlePutBlob("project")))
	s.mux.HandleFunc("PUT /api/sync/blob/project_document/{pid}/{did}", s.idempotent(s.handlePutBlob("project_document")))
	s.mux.HandleFunc("GET /api/sync/blob/chat/{id}", s.handleGetBlob("chat"))
	s.mux.HandleFunc("GET /api/sync/blob/profile", s.handleGetBlob("profile"))
	s.mux.HandleFunc("GET /api/sync/blob/project/{id}", s.handleGetBlob("project"))
	s.mux.HandleFunc("GET /api/sync/blob/project_document/{pid}/{did}", s.handleGetBlob("project_document"))
	s.mux.HandleFunc("HEAD /api/sync/blob/chat/{id}", s.handleHeadBlob("chat"))
	s.mux.HandleFunc("HEAD /api/sync/blob/profile", s.handleHeadBlob("profile"))
	s.mux.HandleFunc("HEAD /api/sync/blob/project/{id}", s.handleHeadBlob("project"))
	s.mux.HandleFunc("HEAD /api/sync/blob/project_document/{pid}/{did}", s.handleHeadBlob("project_document"))
	s.mux.HandleFunc("DELETE /api/sync/blob/chat/{id}", s.idempotent(s.handleDeleteBlob("chat")))
	s.mux.HandleFunc("DELETE /api/sync/blob/profile", s.idempotent(s.handleDeleteBlob("profile")))
	s.mux.HandleFunc("DELETE /api/sync/blob/project/{id}", s.idempotent(s.handleDeleteBlob("project")))
	s.mux.HandleFunc("DELETE /api/sync/blob/project_document/{pid}/{did}", s.idempotent(s.handleDeleteBlob("project_document")))
	s.mux.HandleFunc("DELETE "+controlplane.DeleteAllProjectsPath, s.idempotent(s.handleDeleteAllProjects))
	s.mux.HandleFunc("POST "+controlplane.RewrapPath, s.idempotent(s.handleRewrap))
	s.mux.HandleFunc("POST "+controlplane.ChatActivityPath, s.handleBackfillChatActivity)
	s.mux.HandleFunc("GET /api/sync/list-status", s.handleListStatus)
	s.mux.HandleFunc("GET "+controlplane.RevisionSummaryPath, s.handleRevisionSummary)
	s.mux.HandleFunc("GET /api/sync/needs-migration", s.handleNeedsMigration)
	s.mux.HandleFunc("POST /api/sync/migration-failure", s.handleMigrationFailure)
	s.mux.HandleFunc("GET "+controlplane.SearchIndexStatePath, s.handleGetSearchIndexState)
	s.mux.HandleFunc("PUT "+controlplane.SearchIndexStatePath, s.handlePublishSearchIndex)
	s.mux.HandleFunc("POST "+controlplane.ClaimSearchIndexDeletionsPath, s.handleClaimSearchIndexDeletions)
	s.mux.HandleFunc("POST "+controlplane.AckSearchIndexDeletionsPath, s.handleAckSearchIndexDeletions)
	s.mux.HandleFunc("GET /api/sync/keys/current", s.handleCurrentKey)
	s.mux.HandleFunc("POST "+controlplane.RegisterKeyPath, s.idempotent(s.handleRegisterKey))
	s.mux.HandleFunc("POST /api/sync/keys/{kid}/bundles", s.idempotent(s.handleAddBundle))
	s.mux.HandleFunc("DELETE /api/sync/keys/{kid}/bundles/{cid}", s.idempotent(s.handleRemoveBundle))
	s.mux.HandleFunc("GET /api/storage/attachment/{aid}", s.handleLegacyAttachment)
	s.mux.HandleFunc("POST /api/sync/attachment-index/{aid}", s.handleRegisterAttachmentIndex)
	s.mux.HandleFunc("DELETE /api/sync/attachment-index/{aid}", s.handleDeleteAttachmentIndex)
	s.mux.HandleFunc("GET /api/sync/attachment-owner/{aid}", s.handleAttachmentOwner)
	s.mux.HandleFunc("POST /api/sync/pending-attachments/{aid}", s.handleReservePendingAttachment)
	s.mux.HandleFunc("POST /api/sync/pending-attachments/sweep", s.handleSweepPendingAttachments)
	s.mux.HandleFunc("DELETE /api/sync/orphan-attachments", s.handleOrphanAttachments)
	s.mux.HandleFunc("POST /api/sync/attachment-purges/purge", s.handlePurgeAttachments)
	s.mux.HandleFunc("POST /api/sync/attachment-purges/claim", s.handleClaimAttachmentPurges)
	s.mux.HandleFunc("POST /api/sync/attachment-purges/ack", s.handleAckAttachmentPurges)
	s.mux.HandleFunc("POST /api/sync/notify-import-complete", s.handleNotifyImportOutcome)
	s.mux.HandleFunc("GET "+controlplane.ResearchPath, s.handleResearchAllowance)
	s.mux.HandleFunc("POST "+controlplane.ResearchPath, s.handleStartResearch)
	s.mux.HandleFunc("POST "+controlplane.ChatTokenPath, s.handleRenewChatToken)
	return s
}

func (s *Store) handleRenewChatToken(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || s.RenewChatToken == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	token, refused := s.RenewChatToken(r.Header.Get(controlplane.HeaderClerkUserID), in.Token)
	if refused != 0 {
		writeJSON(w, refused, map[string]string{"error": "refused"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": token})
}

// researchWindow is how long after a user starts a deep research the
// control plane refuses them another.
// researchWindow 是用户启动一次 deep research 后，control plane 拒绝其再次启动的时长。
const researchWindow = time.Hour

// researchLimit is when a user's last deep research stops refusing another,
// and whether it still does.
// researchLimit 返回用户上一次 deep research 何时不再拒绝下一次，以及当前是否仍在拒绝。
func (s *Store) researchLimit(user string) (time.Time, bool) {
	started, ok := s.ResearchStarts[user]
	resets := started.Add(researchWindow)
	return resets, ok && time.Now().Before(resets)
}

func (s *Store) handleResearchAllowance(w http.ResponseWriter, r *http.Request) {
	resets, limited := s.researchLimit(r.Header.Get(controlplane.HeaderClerkUserID))
	if !limited {
		writeJSON(w, http.StatusOK, controlplane.ResearchAllowance{Available: true})
		return
	}
	writeJSON(w, http.StatusOK, controlplane.ResearchAllowance{ResetsAt: resets.UTC().Format(time.RFC3339), RetryAfterSeconds: int64(time.Until(resets).Seconds()) + 1})
}

func (s *Store) handleStartResearch(w http.ResponseWriter, r *http.Request) {
	user := r.Header.Get(controlplane.HeaderClerkUserID)
	if resets, limited := s.researchLimit(user); limited {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"code": controlplane.StatusDeepResearchLimitReached, "error": "deep research limit reached",
			"resets_at": resets.UTC().Format(time.RFC3339), "retry_after_seconds": int64(time.Until(resets).Seconds()) + 1})
		return
	}
	s.ResearchStarts[user] = time.Now()
	writeJSON(w, http.StatusOK, map[string]string{"started_at": s.ResearchStarts[user].UTC().Format(time.RFC3339)})
}

// ServeHTTP 在持有 Mu 的情况下将请求路由到对应的处理器。
func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Mu.Lock()
	capture := s.CaptureHeaders
	s.Mu.Unlock()
	if capture != nil {
		capture(r)
	}
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.mux.ServeHTTP(w, r)
}

// Key is the Blobs key of a row.
// Key 是一行数据在 Blobs 中的键。
func Key(scope, id string) string { return scope + "/" + id }

// SetBlob seeds a row as written now, under Mu.
// SetBlob 在 Mu 下预置一行数据，写入时间为当前时间。
func (s *Store) SetBlob(scope, id string, blob Blob) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if blob.UpdatedAt.IsZero() {
		blob.UpdatedAt = time.Now().UTC()
	}
	s.Blobs[Key(scope, id)] = &blob
}

// FormatETag is a revision as the control plane sends it.
// FormatETag 将 revision 格式化为 control plane 发送的字符串形式。
func FormatETag(n int64) string { return strconv.FormatInt(n, 10) }

func extractID(scope string, r *http.Request) string {
	switch scope {
	case "profile":
		return "profile"
	case "project_document":
		return r.PathValue("pid") + "/" + r.PathValue("did")
	}
	return r.PathValue("id")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func staleBlob(w http.ResponseWriter, currentETag string) {
	writeJSON(w, http.StatusPreconditionFailed, map[string]string{"code": controlplane.StatusStaleBlob, "current_etag": currentETag})
}

// Every write and delete names the revision it expects, as the control
// plane requires.
// 与 control plane 的要求一致，每次写入和删除都要声明它预期的 revision。
func preconditionRequired(w http.ResponseWriter) {
	writeJSON(w, http.StatusPreconditionRequired, map[string]string{"code": controlplane.StatusPreconditionRequired, "message": "If-Match is required"})
}

// idempotentAnswer is a change made under an idempotency key: the request
// a retry must repeat, and what the control plane answered.
// idempotentAnswer 是在幂等 key 下所做的一次变更：重试必须重复的请求，以及 control plane 当时的应答。
type idempotentAnswer struct {
	method, path, operationHash string
	status                      int
	etag                        string
	body                        []byte
}

// idempotent serves a change as the control plane does under an
// idempotency key: a change without an operation hash is refused, a retry
// of a change made under the caller's key is answered as the change was
// and not made again, another request under the key is refused, and a new
// change is made and, once it succeeds, its answer kept.
// idempotent 在幂等 key 下按 control plane 的方式处理一次变更：没有 operation hash 的变更会被拒绝；对调用者 key 下已完成变更的重试会按原样应答而不再执行；同一 key 下的其他请求会被拒绝；新变更会执行，一旦成功就保留其应答。
func (s *Store) idempotent(change http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(controlplane.HeaderIdempotency)
		if key == "" {
			change(w, r)
			return
		}
		if r.Header.Get(controlplane.HeaderOperationHash) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "X-Operation-Hash is required"})
			return
		}
		key = r.Header.Get(controlplane.HeaderClerkUserID) + "\x00" + key
		request := idempotentAnswer{method: r.Method, path: r.URL.Path, operationHash: r.Header.Get(controlplane.HeaderOperationHash)}
		if answer, ok := s.answered[key]; ok {
			for _, compared := range []struct{ field, expected, got string }{
				{"method", answer.method, request.method}, {"path", answer.path, request.path}, {"operation_hash", answer.operationHash, request.operationHash},
			} {
				if compared.expected != compared.got {
					writeJSON(w, http.StatusConflict, map[string]string{"code": controlplane.StatusIdempotencyConflict, "error": "idempotency conflict", "field": compared.field, "expected": compared.expected, "got": compared.got})
					return
				}
			}
			if answer.etag != "" {
				w.Header().Set(controlplane.HeaderETag, answer.etag)
			}
			var cached struct {
				KeyID string `json:"key_id"`
			}
			if json.Unmarshal(answer.body, &cached) == nil && cached.KeyID != "" {
				w.Header().Set(controlplane.HeaderKeyID, cached.KeyID)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(answer.status)
			_, _ = w.Write(answer.body)
			return
		}
		served := httptest.NewRecorder()
		change(served, r)
		if served.Code/100 == 2 {
			request.status, request.etag, request.body = served.Code, served.Header().Get(controlplane.HeaderETag), served.Body.Bytes()
			s.answered[key] = request
		}
		maps.Copy(w.Header(), served.Header())
		w.WriteHeader(served.Code)
		_, _ = w.Write(served.Body.Bytes())
	}
}

func (s *Store) handlePutBlob(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := extractID(scope, r)
		key := Key(scope, id)
		if s.BeforePutBlob != nil {
			s.BeforePutBlob(scope, id)
		}
		if s.PutBlobFailures[key] > 0 {
			s.PutBlobFailures[key]--
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ifMatch := r.Header.Get(controlplane.HeaderIfMatch)
		if ifMatch == "" {
			preconditionRequired(w)
			return
		}
		blob := s.Blobs[key]
		if scope == "profile" && blob != nil && s.MinimumProfileSyncProtocol > 0 {
			protocol, _ := strconv.Atoi(r.Header.Get(controlplane.HeaderProfileSyncProtocol))
			if protocol < s.MinimumProfileSyncProtocol {
				writeJSON(w, http.StatusUpgradeRequired, map[string]any{"code": controlplane.StatusProfileSyncUpgradeRequired, "minimum_protocol": s.MinimumProfileSyncProtocol})
				return
			}
		}
		if blob != nil && ifMatch != FormatETag(blob.ETag) {
			staleBlob(w, FormatETag(blob.ETag))
			return
		}
		if blob == nil && ifMatch != controlplane.IfMatchCreateOnly {
			staleBlob(w, "0")
			return
		}
		body, _ := io.ReadAll(r.Body)
		var nextETag int64 = 1
		if blob != nil {
			nextETag = blob.ETag + 1
		}
		stored := &Blob{ETag: nextETag, KeyID: r.Header.Get(controlplane.HeaderKeyID), Body: body, UpdatedAt: time.Now().UTC()}
		if blob != nil {
			// A write that says nothing about the project leaves it as it is.
			// 未提及 project 的写入会保持 project 现状不变。
			stored.ProjectIDSet, stored.ProjectID = blob.ProjectIDSet, blob.ProjectID
		}
		if scope == "chat" && r.Header.Get(controlplane.HeaderProjectIDSet) == "1" {
			stored.ProjectIDSet, stored.ProjectID = true, nil
			if projectID := r.Header.Get(controlplane.HeaderProjectID); projectID != "" {
				stored.ProjectID = &projectID
			}
		}
		if activityAt, err := time.Parse(time.RFC3339Nano, r.Header.Get(controlplane.HeaderActivityAt)); err == nil {
			stored.ActivityAt = &activityAt
		}
		if refs, sent := r.Header[controlplane.HeaderAttachmentRefs]; sent {
			stored.AttachmentRefs = []string{}
			if refs[0] != "" {
				stored.AttachmentRefs = strings.Split(refs[0], ",")
			}
		}
		s.Blobs[key] = stored
		if scope == "chat" {
			s.SourceRevision++
		}
		w.Header().Set(controlplane.HeaderETag, FormatETag(nextETag))
		w.Header().Set(controlplane.HeaderKeyID, stored.KeyID)
		writeJSON(w, http.StatusOK, map[string]any{"etag": FormatETag(nextETag), "source_revision": s.SourceRevision})
	}
}

func (s *Store) handleGetBlob(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := Key(scope, extractID(scope, r))
		if pending := s.GetBlobFailures[key]; len(pending) > 0 {
			s.GetBlobFailures[key] = pending[1:]
			w.WriteHeader(pending[0])
			return
		}
		blob, ok := s.Blobs[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set(controlplane.HeaderETag, FormatETag(blob.ETag))
		w.Header().Set(controlplane.HeaderKeyID, blob.KeyID)
		if scope == "chat" && blob.ProjectIDSet {
			w.Header().Set(controlplane.HeaderProjectIDSet, "1")
			if blob.ProjectID != nil {
				w.Header().Set(controlplane.HeaderProjectID, *blob.ProjectID)
			}
		}
		if scope == "chat" && blob.ActivityAt != nil {
			w.Header().Set(controlplane.HeaderActivityAt, blob.ActivityAt.Format(time.RFC3339Nano))
		}
		w.Header().Set(controlplane.HeaderUpdatedAt, blob.UpdatedAt.UTC().Format(time.RFC3339Nano))
		_, _ = w.Write(blob.Body)
	}
}

// handleHeadBlob answers a row's revision and key id without its bytes.
// handleHeadBlob 仅返回一行的 revision 和 key id，而不返回其字节内容。
func (s *Store) handleHeadBlob(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		blob, ok := s.Blobs[Key(scope, extractID(scope, r))]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set(controlplane.HeaderETag, FormatETag(blob.ETag))
		w.Header().Set(controlplane.HeaderKeyID, blob.KeyID)
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Store) handleDeleteBlob(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := extractID(scope, r)
		key := Key(scope, id)
		if s.BeforeDeleteBlob != nil {
			s.BeforeDeleteBlob(scope, id)
		}
		ifMatch := r.Header.Get(controlplane.HeaderIfMatch)
		if ifMatch == "" {
			preconditionRequired(w)
			return
		}
		blob, ok := s.Blobs[key]
		if !ok {
			staleBlob(w, "0")
			return
		}
		if ifMatch != FormatETag(blob.ETag) {
			staleBlob(w, FormatETag(blob.ETag))
			return
		}
		delete(s.Blobs, key)
		// A chat's delete drops its attachment index rows and names them,
		// so the enclave can wipe the bytes in buckets.
		// 删除 chat 时会丢弃其附件索引条目并列出它们，这样 enclave 就能清除 buckets 中的字节。
		wiped := []string{}
		if scope == "chat" {
			s.SourceRevision++
			for aid, cid := range s.AttachmentIndex {
				if cid == id {
					wiped = append(wiped, aid)
					delete(s.AttachmentIndex, aid)
					delete(s.AttachmentOwners, aid)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "wiped_v2_attachments": wiped, "source_revision": s.SourceRevision})
	}
}

// handleDeleteAllProjects removes every project and project document, and
// answers how many projects went.
// handleDeleteAllProjects 删除所有 project 与 project document，并应答删除了多少个 project。
func (s *Store) handleDeleteAllProjects(w http.ResponseWriter, r *http.Request) {
	deleted := 0
	for key := range s.Blobs {
		switch scope, _, _ := strings.Cut(key, "/"); scope {
		case "project":
			deleted++
			delete(s.Blobs, key)
		case "project_document":
			delete(s.Blobs, key)
		}
	}
	writeJSON(w, http.StatusOK, controlplane.DeleteAllProjectsResponse{OK: true, Deleted: deleted})
}

// handleRewrap replaces a row's sealed bytes and key id at a revision
// without the write counting as a change to the row.
// handleRewrap 在某个 revision 上替换一行的密封字节和 key id，而这次写入不计为该行的变更。
func (s *Store) handleRewrap(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Scope         string `json:"scope"`
		ID            string `json:"id"`
		KeyID         string `json:"key_id"`
		IfMatch       string `json:"if_match"`
		CiphertextB64 string `json:"ciphertext_b64"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ciphertext, err := base64.StdEncoding.DecodeString(req.CiphertextB64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	id := req.ID
	if req.Scope == "profile" {
		id = "profile"
	}
	key := Key(req.Scope, id)
	blob, ok := s.Blobs[key]
	if !ok {
		staleBlob(w, "0")
		return
	}
	if req.IfMatch != FormatETag(blob.ETag) {
		staleBlob(w, FormatETag(blob.ETag))
		return
	}
	resealed := *blob
	resealed.ETag, resealed.KeyID, resealed.Body = blob.ETag+1, req.KeyID, ciphertext
	s.Blobs[key] = &resealed
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "etag": FormatETag(resealed.ETag), "key_id": req.KeyID})
}

func (s *Store) handleBackfillChatActivity(w http.ResponseWriter, r *http.Request) {
	var req ActivityBackfill
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.ActivityBackfills = append(s.ActivityBackfills, req)
	updated := false
	activityAt, err := time.Parse(time.RFC3339Nano, req.ActivityAt)
	if blob := s.Blobs[Key("chat", req.ID)]; err == nil && blob != nil && blob.ActivityAt == nil && FormatETag(blob.ETag) == req.ETag {
		// As the control plane does, it records no time after the write.
		// 与 control plane 一样，它不会记录写入之后的时间。
		if activityAt.After(blob.UpdatedAt) {
			activityAt = blob.UpdatedAt
		}
		blob.ActivityAt = &activityAt
		updated = true
	}
	writeJSON(w, http.StatusOK, map[string]bool{"updated": updated})
}

// listCursor is the last row a page listed, by the time it was listed by
// and its id.
// listCursor 是某一页所列出的最后一行，以其被列出的时间和 id 表示。
type listCursor struct {
	At time.Time `json:"at"`
	ID string    `json:"id"`
}

// handleListStatus lists a scope's rows, the last written first unless
// asked for the oldest, in pages of limit; chats can be narrowed to a
// project. As the control plane does, a page that fills names a cursor at
// its last row, and the next page lists the rows that sort after it as
// they sort then.
// handleListStatus 以 limit 为分页大小列出某个 scope 的行，默认最后写入的在前（除非要求最旧的在前）；chat 可以按 project 缩小范围。与 control plane 一样，填满的一页会以最后一行作为 cursor，下一页则列出按当时排序位于其后的行。
func (s *Store) handleListStatus(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	scope, project := query.Get("scope"), query.Get("project_id")
	if s.ListStatusFailures[scope] > 0 {
		s.ListStatusFailures[scope]--
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	limit, _ := strconv.Atoi(query.Get("limit"))
	var after *listCursor
	if raw := query.Get("cursor"); raw != "" {
		after = &listCursor{}
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(decoded, after) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	type listed struct {
		id   string
		blob *Blob
	}
	var all []listed
	for key, blob := range s.Blobs {
		rowScope, id, _ := strings.Cut(key, "/")
		if rowScope != scope || project != "" && (blob.ProjectID == nil || *blob.ProjectID != project) {
			continue
		}
		all = append(all, listed{id, blob})
	}
	// Newest activity first, as the control plane lists: a chat's
	// activity time when it has one, else its write time, then id.
	// 与 control plane 的列表方式一样，最近活动在前：chat 有活动时间时用活动时间，否则用写入时间，然后按 id。
	listedAt := func(b *Blob) time.Time {
		if b.ActivityAt != nil {
			return *b.ActivityAt
		}
		return b.UpdatedAt
	}
	ascending := query.Get("direction") == "asc"
	first := func(at time.Time, id string, other time.Time, otherID string) bool {
		if !at.Equal(other) {
			return at.After(other) != ascending
		}
		return id != otherID && id > otherID != ascending
	}
	sort.Slice(all, func(i, j int) bool {
		return first(listedAt(all[i].blob), all[i].id, listedAt(all[j].blob), all[j].id)
	})
	if after != nil {
		all = slices.DeleteFunc(all, func(item listed) bool { return !first(after.At, after.ID, listedAt(item.blob), item.id) })
	}
	page, next := all, ""
	if limit > 0 && len(page) >= limit {
		page = page[:limit]
		last := page[len(page)-1]
		raw, _ := json.Marshal(listCursor{listedAt(last.blob), last.id})
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	updates := []controlplane.BlobMeta{}
	for _, item := range page {
		updates = append(updates, controlplane.BlobMeta{ID: item.id, ETag: FormatETag(item.blob.ETag), KeyID: item.blob.KeyID, ProjectID: item.blob.ProjectID, UpdatedAt: item.blob.UpdatedAt, ActivityAt: item.blob.ActivityAt})
	}
	writeJSON(w, http.StatusOK, controlplane.ListStatusResponse{Updates: updates, NextCursor: next})
}

// handleRevisionSummary is where the chats' ledger stands.
// handleRevisionSummary 返回 chat 账本的当前进度。
func (s *Store) handleRevisionSummary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, controlplane.RevisionSummaryResponse{CurrentRevision: strconv.FormatInt(s.SourceRevision, 10)})
}

// The needs-migration list is at most maxMigrationPage ids, by id, and
// defaultMigrationPage without a limit.
// needs-migration 列表按 id 最多返回 maxMigrationPage 个，未指定 limit 时为 defaultMigrationPage 个。
const (
	maxMigrationPage     = 500
	defaultMigrationPage = 100
)

// handleNeedsMigration lists a scope's rows not sealed under the current
// key and outside their cooldown, by id, with how many such rows remain
// and how many are in a cooldown. The control plane files a profile under
// its user, so it is named by the user id.
// handleNeedsMigration 按 id 列出某个 scope 下未使用当前密钥密封且不在冷却期内的行，并说明还剩多少这样的行以及有多少行处于冷却期。control plane 会把 profile 归档到其用户之下，因此用 user id 命名。
func (s *Store) handleNeedsMigration(w http.ResponseWriter, r *http.Request) {
	listing := s.needsMigrationListings
	s.needsMigrationListings++
	if status, failed := s.NeedsMigrationFailures[listing]; failed {
		w.WriteHeader(status)
		return
	}
	scope := r.URL.Query().Get("scope")
	switch scope {
	case "chat", "profile", "project", "project_document":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid scope"})
		return
	}
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		limit = defaultMigrationPage
	}
	limit = min(limit, maxMigrationPage)
	resp := controlplane.ListNeedsMigrationResponse{IDs: []string{}}
	for key, blob := range s.Blobs {
		rowScope, id, _ := strings.Cut(key, "/")
		if rowScope != scope || blob.KeyID != "" && blob.KeyID == s.CurrentKeyID {
			continue
		}
		if scope == "profile" {
			id = r.Header.Get(controlplane.HeaderClerkUserID)
		}
		if s.MigrationFailures[Key(scope, id)] > 0 {
			resp.BlockedUnmigrated++
			continue
		}
		resp.RetryableRemaining++
		resp.IDs = append(resp.IDs, id)
	}
	sort.Strings(resp.IDs)
	resp.IDs = resp.IDs[:min(len(resp.IDs), limit)]
	writeJSON(w, http.StatusOK, resp)
}

func (s *Store) handleMigrationFailure(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scope string `json:"scope"`
		ID    string `json:"id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	s.MigrationFailures[Key(body.Scope, body.ID)]++
	w.WriteHeader(http.StatusNoContent)
}

// The search index's publication, as the control plane keeps it: one
// object key per publication, named for the fence it was published under.
// control plane 所保存的搜索索引发布：每次发布对应一个 object key，以发布时的 fence 命名。

// searchIndexObjectKey is the shape of an index object's key: the prefix,
// the fence it was published under, and a random suffix.
// searchIndexObjectKey 是索引对象 key 的形态：前缀、发布时的 fence 以及一个随机后缀。
var searchIndexObjectKey = regexp.MustCompile(`^search-index-v1-([0-9]+)-[a-f0-9]{32}$`)

const (
	maxSearchIndexObjectKeyLength = 256
	maxSearchIndexModelLength     = 128
	// A claim of retired index objects is defaultSearchDeletionBatch
	// without a batch size, and at most maxSearchDeletionBatch.
	// 未指定 batch size 时，对退役索引对象的 claim 为 defaultSearchDeletionBatch 个，最多为 maxSearchDeletionBatch 个。
	defaultSearchDeletionBatch = 25
	maxSearchDeletionBatch     = 100
)

// searchState is the publication as the control plane serves it: with the
// source revision, and incomplete when the publication says so or covers
// less than the source.
// searchState 是 control plane 对外提供的发布状态：带有 source revision，且当发布自身声明不完整或覆盖范围小于 source 时为 incomplete。
func (s *Store) searchState() controlplane.SearchIndexState {
	state := s.SearchState
	state.SourceRevision = s.SourceRevision
	state.Incomplete = state.PublicationIncomplete || state.PublishedSourceRevision < state.SourceRevision
	return state
}

// fenceSearchIndex retires the published index object when the caller's
// key changes or starts fresh: the publication and fence generations
// advance, and nothing is published until a new object is.
// fenceSearchIndex 在调用者的密钥变更或重新开始时退役已发布的索引对象：publication 与 fence 的 generation 前进一步，且在新对象发布之前不会发布任何内容。
func (s *Store) fenceSearchIndex() {
	state := s.searchState()
	s.SearchState = controlplane.SearchIndexState{
		PublicationGeneration: state.PublicationGeneration + 1,
		FenceGeneration:       state.FenceGeneration + 1,
		PublicationIncomplete: true,
	}
}

func (s *Store) handleGetSearchIndexState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.searchState())
}

func searchIndexConflict(w http.ResponseWriter, state controlplane.SearchIndexState) {
	writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "code": controlplane.StatusSearchIndexConflict, "state": state})
}

// handlePublishSearchIndex publishes an index object at the generation and
// fence the publisher expects, covering a source revision no earlier than
// the published one and no later than the source, and incomplete when it
// covers less than the source; a publication under another key than the
// registered one, or at another generation, is a conflict that answers
// with the current state.
// handlePublishSearchIndex 在发布者预期的 generation 和 fence 上发布一个索引对象，覆盖的 source revision 不早于已发布值且不晚于当前 source，若覆盖范围小于 source 则为 incomplete；使用非注册密钥或其他 generation 的发布会视为冲突，并返回当前状态。
func (s *Store) handlePublishSearchIndex(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExpectedGeneration    int64  `json:"expected_generation"`
		ExpectedFence         int64  `json:"expected_fence"`
		CoveredSourceRevision int64  `json:"covered_source_revision"`
		ObjectKey             string `json:"object_key"`
		KeyID                 string `json:"key_id"`
		Model                 string `json:"model"`
		Incomplete            bool   `json:"incomplete"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid request body"})
		return
	}
	if req.ExpectedGeneration < 0 || req.ExpectedFence < 0 || req.CoveredSourceRevision < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "search index revisions must be non-negative"})
		return
	}
	match := searchIndexObjectKey.FindStringSubmatch(req.ObjectKey)
	if len(req.ObjectKey) > maxSearchIndexObjectKeyLength || len(match) != 2 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid search index object_key"})
		return
	}
	if fence, err := strconv.ParseInt(match[1], 10, 64); err != nil || fence != req.ExpectedFence {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "search index object_key fence mismatch"})
		return
	}
	if !isLowerHex32(req.KeyID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid key_id"})
		return
	}
	if strings.TrimSpace(req.Model) == "" || len(req.Model) > maxSearchIndexModelLength {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid model"})
		return
	}
	if s.SearchConflict != nil {
		conflict := s.SearchConflict
		s.SearchConflict = nil
		s.SearchState = conflict(s.searchState())
		searchIndexConflict(w, s.searchState())
		return
	}
	state := s.searchState()
	if req.ExpectedGeneration != state.PublicationGeneration || req.ExpectedFence != state.FenceGeneration ||
		req.CoveredSourceRevision < state.PublishedSourceRevision || req.CoveredSourceRevision > state.SourceRevision ||
		req.KeyID != s.CurrentKeyID {
		searchIndexConflict(w, state)
		return
	}
	s.SearchState = controlplane.SearchIndexState{
		PublicationGeneration:   state.PublicationGeneration + 1,
		FenceGeneration:         state.FenceGeneration,
		PublishedSourceRevision: req.CoveredSourceRevision,
		ObjectKey:               req.ObjectKey,
		KeyID:                   req.KeyID,
		Model:                   req.Model,
		PublicationIncomplete:   req.Incomplete || req.CoveredSourceRevision < state.SourceRevision,
	}
	writeJSON(w, http.StatusOK, s.searchState())
}

// handleClaimSearchIndexDeletions leases a batch of the retired index
// objects not yet claimed under a fresh token.
// handleClaimSearchIndexDeletions 在一个新 token 下租借一批尚未被认领的退役索引对象。
func (s *Store) handleClaimSearchIndexDeletions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BatchSize int `json:"batch_size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.BatchSize < 0 || req.BatchSize > maxSearchDeletionBatch {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid batch_size"})
		return
	}
	if req.BatchSize == 0 {
		req.BatchSize = defaultSearchDeletionBatch
	}
	claim := controlplane.SearchIndexDeletionClaim{ClaimToken: uuid.NewString(), Deletions: []controlplane.SearchIndexDeletion{}}
	for _, deletion := range s.SearchIndexDeletions {
		if len(claim.Deletions) == req.BatchSize {
			break
		}
		if _, claimed := s.searchClaims[deletion.ID]; claimed {
			continue
		}
		s.searchClaims[deletion.ID] = claim.ClaimToken
		claim.Deletions = append(claim.Deletions, deletion)
	}
	writeJSON(w, http.StatusOK, claim)
}

// handleAckSearchIndexDeletions drops the retired objects acknowledged
// under the token that claimed them.
// handleAckSearchIndexDeletions 删除在认领它们的 token 下被确认的退役对象。
func (s *Store) handleAckSearchIndexDeletions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClaimToken string   `json:"claim_token"`
		IDs        []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ClaimToken == "" || len(req.IDs) == 0 || len(req.IDs) > maxSearchDeletionBatch {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid claim_token or ids"})
		return
	}
	for _, id := range req.IDs {
		if s.searchClaims[id] != req.ClaimToken {
			continue
		}
		delete(s.searchClaims, id)
		s.SearchIndexDeletions = slices.DeleteFunc(s.SearchIndexDeletions, func(d controlplane.SearchIndexDeletion) bool { return d.ID == id })
	}
	w.WriteHeader(http.StatusNoContent)
}

// keyTimestamp is a time as the control plane reports when a key or a
// bundle was registered.
// keyTimestamp 是 control plane 上报密钥或 bundle 注册时间时使用的时间格式。
func keyTimestamp(at time.Time) string { return at.UTC().Format("2006-01-02T15:04:05.000Z") }

// currentKeyAnswer is the control plane's answer about the current key;
// without one it names no key and leaves out when it was made.
// currentKeyAnswer 是 control plane 关于当前密钥的应答；没有当前密钥时不会给出 key，也不会给出其创建时间。
type currentKeyAnswer struct {
	KeyID      string                                   `json:"key_id"`
	ETag       string                                   `json:"etag"`
	Bundles    map[string]controlplane.CurrentKeyBundle `json:"bundles"`
	CreatedVia string                                   `json:"created_via"`
	CreatedAt  string                                   `json:"created_at,omitempty"`
	HasData    bool                                     `json:"has_data"`
}

func (s *Store) handleCurrentKey(w http.ResponseWriter, r *http.Request) {
	answer := currentKeyAnswer{Bundles: map[string]controlplane.CurrentKeyBundle{}, HasData: s.hasDataUnderOtherKey(s.CurrentKeyID, true)}
	if s.CurrentKeyID != "" {
		answer.KeyID, answer.ETag, answer.CreatedVia, answer.CreatedAt = s.CurrentKeyID, FormatETag(s.KeyETag), s.KeyCreatedVia, keyTimestamp(s.KeyRegisteredAt)
		for cid, bundle := range s.Bundles[s.CurrentKeyID] {
			answer.Bundles[cid] = bundle
		}
	}
	writeJSON(w, http.StatusOK, answer)
}

// hasDataUnderOtherKey is whether any row is sealed under a key other
// than claim, counting rows sealed under none when legacy is set.
// hasDataUnderOtherKey 表示是否存在任何一行不是用 claim 指定的密钥密封的；当 legacy 为 true 时，也会把未使用任何密钥密封的行计入。
func (s *Store) hasDataUnderOtherKey(claim string, legacy bool) bool {
	for _, blob := range s.Blobs {
		if blob.KeyID == "" && legacy || blob.KeyID != "" && blob.KeyID != claim {
			return true
		}
	}
	return false
}

func keyConflict(w http.ResponseWriter, code string, details map[string]string) {
	body := map[string]string{"code": code, "error": strings.ToLower(strings.ReplaceAll(code, "_", " "))}
	for k, v := range details {
		body[k] = v
	}
	writeJSON(w, http.StatusConflict, body)
}

func isLowerHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// handleRegisterKey registers a key as the control plane does. Without a
// key, If-Match must be "*", and a key that is not a recovery or a manual
// one is refused over existing rows unless it starts fresh, which wipes
// them. Over a key, If-Match must be its revision; "*" re-asserts the same
// key and refuses another. A key change drops the bundles of the old key
// and the cooldowns of the rows left to reseal.
// handleRegisterKey 按 control plane 的方式注册密钥。没有密钥时，If-Match 必须为 "*"，非 recovery 或 manual 的密钥若已有数据行会被拒绝，除非它 start fresh（那样会清空数据）。已有密钥时，If-Match 必须为其 revision；"*" 会重申同一密钥并拒绝其他密钥。密钥变更会丢弃旧密钥的 bundle 以及待重新密封行的冷却期。
func (s *Store) handleRegisterKey(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(controlplane.HeaderIdempotency) == "" || r.Header.Get(controlplane.HeaderOperationHash) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "X-Idempotency-Key and X-Operation-Hash are required"})
		return
	}
	var body struct {
		KeyID         string                          `json:"key_id"`
		CreatedVia    string                          `json:"created_via"`
		InitialBundle *controlplane.RegisterKeyBundle `json:"initial_bundle"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !isLowerHex32(body.KeyID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid key_id"})
		return
	}
	switch body.CreatedVia {
	case "passkey", "manual", "recovery", "start_fresh":
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid created_via"})
		return
	}
	ifMatch := r.Header.Get(controlplane.HeaderIfMatch)
	if ifMatch == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "If-Match is required"})
		return
	}
	if body.InitialBundle != nil && (body.InitialBundle.CredentialID == "" || body.InitialBundle.KEKIV == "" || body.InitialBundle.EncryptedKeys == "") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "initial_bundle requires credential_id, kek_iv, encrypted_keys"})
		return
	}
	asserting := body.CreatedVia == "recovery" || body.CreatedVia == "manual"
	wiped := []string{}
	switch {
	case s.CurrentKeyID == "":
		if ifMatch != controlplane.IfMatchAnyKey {
			keyConflict(w, controlplane.StatusStaleKey, map[string]string{"current_etag": ""})
			return
		}
		if !asserting && s.hasDataUnderOtherKey(body.KeyID, true) {
			if body.CreatedVia != "start_fresh" {
				keyConflict(w, controlplane.StatusExistingDataUnderOtherKey, map[string]string{"current_key_id": ""})
				return
			}
			wiped = s.wipeAll()
		}
	case ifMatch == controlplane.IfMatchAnyKey:
		if s.CurrentKeyID == body.KeyID && body.CreatedVia != "start_fresh" {
			s.addBundle(body.KeyID, body.InitialBundle)
			writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key_id": s.CurrentKeyID, "etag": FormatETag(s.KeyETag), "wiped_v2_attachments": wiped})
			return
		}
		keyConflict(w, controlplane.StatusExistingDataUnderOtherKey, map[string]string{"current_key_id": s.CurrentKeyID})
		return
	default:
		if ifMatch != FormatETag(s.KeyETag) {
			keyConflict(w, controlplane.StatusStaleKey, map[string]string{"current_etag": FormatETag(s.KeyETag), "current_key_id": s.CurrentKeyID})
			return
		}
		if s.CurrentKeyID != body.KeyID && s.hasDataUnderOtherKey(body.KeyID, !asserting) {
			if asserting || body.CreatedVia != "start_fresh" {
				keyConflict(w, controlplane.StatusExistingDataUnderOtherKey, map[string]string{"current_key_id": s.CurrentKeyID})
				return
			}
			wiped = s.wipeAll()
		}
	}
	changed := s.CurrentKeyID != body.KeyID
	// A search index sealed under the key that was registered, or built
	// over rows a fresh start wipes, is fenced off: a key registered over
	// none fences only when it starts fresh, since no index could have
	// been published without a key.
	// 使用刚注册密钥密封、或建立在 start fresh 所清空的数据行之上的搜索索引会被隔离：在没有密钥之上注册密钥时，只有 start fresh 才会隔离，因为没有密钥就不可能发布过索引。
	fenced := body.CreatedVia == "start_fresh" || s.CurrentKeyID != "" && changed
	if changed {
		s.KeyRegisteredAt = time.Now()
	}
	s.CurrentKeyID, s.KeyCreatedVia = body.KeyID, body.CreatedVia
	s.KeyETag++
	if changed {
		for kid := range s.Bundles {
			if kid != body.KeyID {
				delete(s.Bundles, kid)
			}
		}
		clear(s.MigrationFailures)
	}
	if fenced {
		s.fenceSearchIndex()
		w.Header().Set(controlplane.HeaderSearchIndexFenced, "true")
	}
	s.addBundle(body.KeyID, body.InitialBundle)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key_id": s.CurrentKeyID, "etag": FormatETag(s.KeyETag), "wiped_v2_attachments": wiped})
}

// wipeAll drops every row and attachment, and names the attachments held
// in buckets so the caller can drop those too.
// wipeAll 删除所有数据行和附件，并列出保存在 buckets 中的附件，以便调用者也一并删除。
func (s *Store) wipeAll() []string {
	for key := range s.Blobs {
		if strings.HasPrefix(key, "chat/") {
			s.SourceRevision++
		}
		delete(s.Blobs, key)
	}
	wiped := []string{}
	for aid := range s.AttachmentIndex {
		wiped = append(wiped, aid)
	}
	sort.Strings(wiped)
	clear(s.AttachmentIndex)
	clear(s.AttachmentOwners)
	clear(s.LegacyAttachments)
	return wiped
}

func (s *Store) addBundle(kid string, bundle *controlplane.RegisterKeyBundle) {
	if bundle == nil {
		return
	}
	if s.Bundles[kid] == nil {
		s.Bundles[kid] = map[string]controlplane.CurrentKeyBundle{}
	}
	s.Bundles[kid][bundle.CredentialID] = controlplane.CurrentKeyBundle{CredentialID: bundle.CredentialID, KEKIV: bundle.KEKIV, EncryptedKeys: bundle.EncryptedKeys, RegisteredAt: keyTimestamp(time.Now())}
}

// bundleKey is the key a bundle request names, once it is the current
// one: without a key the request is not found, and with another it is
// stale.
// bundleKey 返回 bundle 请求所指定的密钥（前提是它为当前密钥）：没有密钥时请求为 not found，指定了其他密钥则为 stale。
func (s *Store) bundleKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	kid := r.PathValue("kid")
	switch {
	case r.Header.Get(controlplane.HeaderIdempotency) == "" || r.Header.Get(controlplane.HeaderOperationHash) == "":
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "X-Idempotency-Key and X-Operation-Hash are required"})
	case !isLowerHex32(kid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid key id"})
	case s.CurrentKeyID == "":
		writeJSON(w, http.StatusNotFound, map[string]string{"code": "NOT_FOUND", "error": "no current key registered"})
	case kid != s.CurrentKeyID:
		keyConflict(w, controlplane.StatusStaleKey, map[string]string{"current_key_id": s.CurrentKeyID, "current_etag": FormatETag(s.KeyETag)})
	default:
		return kid, true
	}
	return "", false
}

func (s *Store) handleAddBundle(w http.ResponseWriter, r *http.Request) {
	var bundle controlplane.RegisterKeyBundle
	if err := json.NewDecoder(r.Body).Decode(&bundle); err != nil || bundle.CredentialID == "" || bundle.KEKIV == "" || bundle.EncryptedKeys == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "credential_id, kek_iv, encrypted_keys are required"})
		return
	}
	kid, ok := s.bundleKey(w, r)
	if !ok {
		return
	}
	s.addBundle(kid, &bundle)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleRemoveBundle drops a credential's bundle; one the key does not
// have is already gone.
// handleRemoveBundle 删除某个 credential 的 bundle；密钥本就没有的 bundle 视为已删除。
func (s *Store) handleRemoveBundle(w http.ResponseWriter, r *http.Request) {
	kid, ok := s.bundleKey(w, r)
	if !ok {
		return
	}
	delete(s.Bundles[kid], r.PathValue("cid"))
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleLegacyAttachment serves an attachment as older clients stored it,
// with the control plane's claim on it when a secret is set.
// handleLegacyAttachment 按旧客户端存储的方式提供附件；设置 secret 时附上 control plane 对它的 claim。
func (s *Store) handleLegacyAttachment(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	if s.GoneAttachments[aid] {
		w.WriteHeader(http.StatusGone)
		return
	}
	body, ok := s.LegacyAttachments[aid]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if s.Secret != "" {
		w.Header().Set(controlplane.HeaderLegacyClaim, legacyClaim(s.Secret, r.Header.Get(controlplane.HeaderClerkUserID), aid, body))
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(body)
}

// legacyClaim signs who may move an attachment: the user, the id and a
// digest of the bytes, as the control plane does.
// legacyClaim 对允许移动附件的主体签名：用户、id 以及字节的摘要，与 control plane 的做法一致。
func legacyClaim(secret, clerkUserID, aid string, ciphertext []byte) string {
	digest := sha256.Sum256(ciphertext)
	payload, _ := json.Marshal(struct {
		ClerkUserID string `json:"clerk_user_id"`
		ID          string `json:"id"`
		Scope       string `json:"scope"`
		SHA256      string `json:"sha256"`
	}{clerkUserID, aid, "attachment", hex.EncodeToString(digest[:])})
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// handleRegisterAttachmentIndex records an attachment as held in buckets
// for the user the request names, and drops the legacy bytes, so a later
// legacy read answers not found.
// handleRegisterAttachmentIndex 为请求所指定的用户将附件记录为保存在 buckets 中，并删除旧版字节，这样之后的旧版读取会返回 not found。
func (s *Store) handleRegisterAttachmentIndex(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	var body struct {
		ChatID string `json:"chat_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if pending := s.RegisterAttachmentIndexFailures[aid]; len(pending) > 0 {
		s.RegisterAttachmentIndexFailures[aid] = pending[1:]
		w.WriteHeader(pending[0])
		return
	}
	if s.AttachmentIndex == nil {
		s.AttachmentIndex = map[string]string{}
	}
	s.AttachmentIndex[aid] = body.ChatID
	s.AttachmentOwners[aid] = r.Header.Get(controlplane.HeaderClerkUserID)
	delete(s.LegacyAttachments, aid)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Store) handleDeleteAttachmentIndex(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	if s.DeleteAttachmentIndexFailures[aid] > 0 {
		s.DeleteAttachmentIndexFailures[aid]--
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if _, ok := s.AttachmentIndex[aid]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	delete(s.AttachmentIndex, aid)
	delete(s.AttachmentOwners, aid)
	w.WriteHeader(http.StatusNoContent)
}

// handleAttachmentOwner names the user an indexed attachment belongs to.
// handleAttachmentOwner 返回某个已索引附件所属的用户。
func (s *Store) handleAttachmentOwner(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	if pending := s.AttachmentOwnerFailures[aid]; len(pending) > 0 {
		s.AttachmentOwnerFailures[aid] = pending[1:]
		w.WriteHeader(pending[0])
		return
	}
	owner, ok := s.AttachmentOwners[aid]
	if !ok || owner == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"clerk_user_id": owner})
}

// The pending-write ledger accepts every reservation, except of an id
// whose earlier copy is queued for purging or being purged.
// pending-write 账本接受所有预约，但某个 id 的旧副本已排队清除或正在清除时除外。
func (s *Store) handleReservePendingAttachment(w http.ResponseWriter, r *http.Request) {
	aid := r.PathValue("aid")
	queued := slices.ContainsFunc(s.AttachmentPurges, func(purge controlplane.AttachmentPurge) bool { return purge.AttachmentID == aid })
	if queued || s.ReserveAttachmentPurges[aid] > 0 {
		if s.ReserveAttachmentPurges[aid] > 0 {
			s.ReserveAttachmentPurges[aid]--
		}
		writeJSON(w, http.StatusConflict, map[string]string{"code": controlplane.StatusAttachmentPurgeInProgress})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSweepPendingAttachments hands the sweep the pending writes the
// control plane releases, up to the limit, which it then forgets.
// handleSweepPendingAttachments 将 control plane 释放的 pending 写入（最多 limit 条）交给 sweep，之后即遗忘。
func (s *Store) handleSweepPendingAttachments(w http.ResponseWriter, r *http.Request) {
	limit := limitParam(r, defaultSweepLimit, maxSweepLimit)
	rows := s.PendingAttachmentWrites[:min(limit, len(s.PendingAttachmentWrites))]
	s.PendingAttachmentWrites = slices.Clone(s.PendingAttachmentWrites[len(rows):])
	writeJSON(w, http.StatusOK, map[string]any{"rows": slices.Clone(rows)})
}

// handleOrphanAttachments drops the index entries without an owner, up
// to the limit, and names them for the sweep.
// handleOrphanAttachments 删除无主的索引条目（最多 limit 条），并将其列出交给 sweep。
func (s *Store) handleOrphanAttachments(w http.ResponseWriter, r *http.Request) {
	limit := limitParam(r, defaultSweepLimit, maxSweepLimit)
	rows := s.OrphanAttachments[:min(limit, len(s.OrphanAttachments))]
	s.OrphanAttachments = slices.Clone(s.OrphanAttachments[len(rows):])
	for _, row := range rows {
		delete(s.AttachmentIndex, row.AttachmentID)
		delete(s.AttachmentOwners, row.AttachmentID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"attachments": slices.Clone(rows)})
}

// limitParam is a request's limit, within the bounds the control plane
// gives the route.
// limitParam 返回请求的 limit，并限制在 control plane 为该路由设定的范围内。
func limitParam(r *http.Request, fallback, most int) int {
	if limit, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && limit > 0 && limit <= most {
		return limit
	}
	return fallback
}

const (
	// A sweep of pending writes or orphans hands over defaultSweepLimit
	// rows without a limit, and at most maxSweepLimit.
	// 未指定 limit 时，对 pending writes 或 orphans 的 sweep 移交 defaultSweepLimit 条，最多 maxSweepLimit 条。
	defaultSweepLimit = 500
	maxSweepLimit     = 500
	// A purge queues defaultPurgeBatch attachments without a batch size
	// and at most maxPurgeBatch; a claim leases defaultPurgeClaimBatch
	// and at most maxPurgeClaimBatch, which an ack names at most.
	// 未指定 batch size 时，purge 会排队 defaultPurgeBatch 个附件，最多 maxPurgeBatch 个；claim 租借 defaultPurgeClaimBatch 个，最多 maxPurgeClaimBatch 个，这也是 ack 最多能指定的数量。
	defaultPurgeBatch      = 100
	maxPurgeBatch          = 500
	defaultPurgeClaimBatch = 25
	maxPurgeClaimBatch     = 100
)

// batchSize is a purge request's batch size, within its bounds.
// batchSize 返回 purge 请求的 batch size，并限制在其范围内。
func batchSize(w http.ResponseWriter, r *http.Request, fallback, most int) (int, bool) {
	var req struct {
		BatchSize int `json:"batch_size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF || req.BatchSize < 0 || req.BatchSize > most {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid batch_size"})
		return 0, false
	}
	if req.BatchSize == 0 {
		req.BatchSize = fallback
	}
	return req.BatchSize, true
}

// handlePurgeAttachments moves the index entries no chat names any more
// into the purge queue, up to the batch size: those whose chat is gone,
// and those a chat's last write left out of the attachments it names.
// handlePurgeAttachments 将不再被任何 chat 引用的索引条目移入 purge 队列（最多 batch 条）：包括其 chat 已消失的条目，以及 chat 最近一次写入未列在其所引用附件中的条目。
func (s *Store) handlePurgeAttachments(w http.ResponseWriter, r *http.Request) {
	batch, ok := batchSize(w, r, defaultPurgeBatch, maxPurgeBatch)
	if !ok {
		return
	}
	queued := 0
	for _, aid := range slices.Sorted(maps.Keys(s.AttachmentIndex)) {
		if queued == batch {
			break
		}
		chat := s.AttachmentIndex[aid]
		blob := s.Blobs[Key("chat", chat)]
		if blob != nil && (blob.AttachmentRefs == nil || slices.Contains(blob.AttachmentRefs, aid)) {
			continue
		}
		s.AttachmentPurges = append(s.AttachmentPurges, controlplane.AttachmentPurge{AttachmentID: aid, ClerkUserID: s.AttachmentOwners[aid], ChatID: chat})
		delete(s.AttachmentIndex, aid)
		delete(s.AttachmentOwners, aid)
		queued++
	}
	writeJSON(w, http.StatusOK, map[string]int{"queued": queued})
}

// handleClaimAttachmentPurges leases a batch of the queued purges not yet
// claimed under a fresh token.
// handleClaimAttachmentPurges 在一个新 token 下租借一批尚未被认领的已排队 purge。
func (s *Store) handleClaimAttachmentPurges(w http.ResponseWriter, r *http.Request) {
	batch, ok := batchSize(w, r, defaultPurgeClaimBatch, maxPurgeClaimBatch)
	if !ok {
		return
	}
	claim := controlplane.AttachmentPurgeClaim{ClaimToken: uuid.NewString(), Purges: []controlplane.AttachmentPurge{}}
	for _, purge := range s.AttachmentPurges {
		if len(claim.Purges) == batch {
			break
		}
		if _, claimed := s.purgeClaims[purge.AttachmentID]; claimed {
			continue
		}
		s.purgeClaims[purge.AttachmentID] = claim.ClaimToken
		claim.Purges = append(claim.Purges, purge)
	}
	writeJSON(w, http.StatusOK, claim)
}

// LapsePurgeLeases ends the leases on the claimed purges, as the control
// plane does once their lease time has passed, so the next claim leases
// them again.
// LapsePurgeLeases 结束已认领 purge 的租约，就像 control plane 在租约到期后做的那样，这样下一次 claim 就能再次租借它们。
func (s *Store) LapsePurgeLeases() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	clear(s.purgeClaims)
}

// handleAckAttachmentPurges drops the purges acknowledged under the token
// that claimed them.
// handleAckAttachmentPurges 删除在认领它们的 token 下被确认的 purge。
func (s *Store) handleAckAttachmentPurges(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClaimToken    string   `json:"claim_token"`
		AttachmentIDs []string `json:"attachment_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || uuid.Validate(req.ClaimToken) != nil || len(req.AttachmentIDs) == 0 || len(req.AttachmentIDs) > maxPurgeClaimBatch {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid claim_token or attachment_ids"})
		return
	}
	for _, aid := range req.AttachmentIDs {
		if s.purgeClaims[aid] != req.ClaimToken {
			continue
		}
		delete(s.purgeClaims, aid)
		s.AttachmentPurges = slices.DeleteFunc(s.AttachmentPurges, func(p controlplane.AttachmentPurge) bool { return p.AttachmentID == aid })
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleNotifyImportOutcome records how an import ended, for the user the
// request names, as the control plane does before emailing them.
// handleNotifyImportOutcome 为请求所指定的用户记录一次导入的结果，正如 control plane 在给他们发邮件之前所做的那样。
func (s *Store) handleNotifyImportOutcome(w http.ResponseWriter, r *http.Request) {
	var body struct {
		JobID         string `json:"jobId"`
		Source        string `json:"source"`
		Status        string `json:"status"`
		Imported      int    `json:"importedCount"`
		Failed        int    `json:"failedCount"`
		FailureReason string `json:"failureReason"`
		FailureDetail string `json:"failureDetail"`
	}
	user := r.Header.Get(controlplane.HeaderClerkUserID)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || user == "" || body.JobID == "" || body.Status == "failed" && body.FailureReason == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "BAD_REQUEST", "error": "invalid import outcome"})
		return
	}
	s.ImportOutcomes = append(s.ImportOutcomes, controlplane.ImportOutcome{ClerkUserID: user, JobID: body.JobID, Source: body.Source, Status: controlplane.ImportOutcomeStatus(body.Status),
		Imported: body.Imported, Failed: body.Failed, FailureReason: body.FailureReason, FailureDetail: body.FailureDetail})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
