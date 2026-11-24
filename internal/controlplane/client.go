// Package controlplane is the harness's client for the control plane's
// storage API: the sealed rows, the key registry, the attachment index,
// the search index state and the cleanup queues it keeps for the enclave.
// 包 controlplane 是 harness 针对 control plane 存储 API 的客户端：为 enclave 维护的密封行、密钥注册表、附件索引、搜索索引状态以及清理队列。

package controlplane

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to the controlplane on behalf of an authenticated user.
// User-driven calls forward the user's JWT verbatim in the
// Authorization header plus the enclave service credential required
// by the /api/sync surface. A small set of enclave-internal
// maintenance calls (attachment and search-index cleanup) skip the
// JWT because they have no associated user request; they
// authenticate to the controlplane with the service credential
// alone.
// Client 代表已认证用户与 controlplane 通信。
// 用户驱动的调用会把用户的 JWT 原样放入 Authorization header，并附上 /api/sync 接口所要求的
// enclave 服务凭据。少数 enclave 内部维护调用（附件与搜索索引清理）会跳过 JWT，因为它们
// 没有关联的用户请求；它们仅用服务凭据向 controlplane 认证。
type Client struct {
	baseURL       string
	httpClient    *http.Client
	serviceSecret string
}

// Option 是 Client 的可选配置。
type Option func(*Client)

// WithServiceSecret 设置 Client 的服务密钥。
func WithServiceSecret(secret string) Option {
	return func(c *Client) {
		c.serviceSecret = secret
	}
}

// NewClient 构造一个指向 baseURL 的 controlplane 客户端。
func NewClient(baseURL string, httpClient *http.Client, opts ...Option) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Header* and Status* below mirror the canonical wire contract
// defined in github.com/tinfoilsh/controlplane/pkg/contract
// (headers.go, sentinels.go, wirecodes.go). Keeping a local copy
// rather than importing the controlplane module preserves the
// enclave's standalone build surface (see go.mod — no controlplane
// dependency by design); the pin test in client_contract_test.go
// fails CI if any constant drifts from the canonical value.
//
// HeaderContentType / HeaderServiceSecret / HeaderAuth are enclave-only
// (not part of the public /api/sync/* contract) and live here for
// convenience; the same applies to StatusLegacyBlobNotMigrated, which
// the enclave returns to its own clients but the controlplane does not
// emit.
// 下面的 Header* 与 Status* 常量镜像了 github.com/tinfoilsh/controlplane/pkg/contract
// （headers.go、sentinels.go、wirecodes.go）中定义的规范线上契约。保留本地副本而非导入
// controlplane 模块，是为了保持 enclave 的独立构建面（参见 go.mod —— 按设计不依赖
// controlplane）；client_contract_test.go 中的 pin 测试会在任何常量偏离规范值时使 CI 失败。
//
// HeaderContentType / HeaderServiceSecret / HeaderAuth 是仅限 enclave 使用的（不属于公开的
// /api/sync/* 契约），为方便起见定义于此；StatusLegacyBlobNotMigrated 同样如此，该状态由
// enclave 返回给它自己的客户端，而 controlplane 并不发出它。
const (
	HeaderAuth                = "Authorization"
	HeaderKeyID               = "X-Key-Id"
	HeaderIfMatch             = "If-Match"
	HeaderIdempotency         = "X-Idempotency-Key"
	HeaderOperationHash       = "X-Operation-Hash"
	HeaderProfileSyncProtocol = "X-Profile-Sync-Protocol"
	HeaderProjectID           = "X-Project-Id"
	HeaderProjectIDSet        = "X-Project-Id-Set"
	HeaderAttachmentRefs      = "X-Attachment-Refs"
	HeaderActivityAt          = "X-Activity-At"
	HeaderUpdatedAt           = "X-Updated-At"
	HeaderETag                = "ETag"
	HeaderRequestID           = "X-Request-ID"
	HeaderSearchIndexFenced   = "X-Search-Index-Fenced"
	HeaderContentType         = "Content-Type"
	HeaderServiceSecret       = "X-Sync-Enclave-Secret"
	// HeaderClerkUserID carries the user id the enclave already
	// verified out of the user's JWT. Paired with HeaderServiceSecret
	// it stands in for a fresh Clerk bearer token on the
	// controlplane, so long migrations do not break when the proxied
	// JWT expires mid-loop.
	// HeaderClerkUserID 携带 enclave 已经从用户 JWT 中验证出的用户 id。它与 HeaderServiceSecret
	// 配合，在 controlplane 上代替一个全新的 Clerk bearer token，这样当被代理的 JWT 在循环中途
	// 过期时，长时间运行的迁移也不会中断。
	HeaderClerkUserID = "X-Clerk-User-Id"
)

// If-Match 哨兵值与同步协议版本常量。
const (
	IfMatchCreateOnly     = "0"
	IfMatchAnyKey         = "*"
	SyncProtocolV3        = 3
	ProfileSyncProtocolV2 = 2
)

// controlplane 返回的状态码哨兵值。
const (
	StatusPreconditionRequired       = "PRECONDITION_REQUIRED"
	StatusStaleKey                   = "STALE_KEY"
	StatusStaleBlob                  = "STALE_BLOB"
	StatusMissingAttachment          = "MISSING_ATTACHMENT"
	StatusAttachmentPurgeInProgress  = "ATTACHMENT_PURGE_IN_PROGRESS"
	StatusExistingDataUnderOtherKey  = "EXISTING_DATA_UNDER_OTHER_KEY"
	StatusIdempotencyConflict        = "IDEMPOTENCY_CONFLICT"
	StatusSearchIndexConflict        = "SEARCH_INDEX_CONFLICT"
	StatusProfileSyncUpgradeRequired = "PROFILE_SYNC_UPGRADE_REQUIRED"
	StatusLegacyBlobNotMigrated      = "LEGACY_BLOB_NOT_MIGRATED"
	StatusDeepResearchLimitReached   = "DEEP_RESEARCH_LIMIT_REACHED"
)

// maxLegacyAttachmentBytes caps the body read for legacy attachment
// downloads. var (not const) so package tests can shrink it to keep
// oversized-body regressions cheap to exercise.
// maxLegacyAttachmentBytes 限制读取旧版附件下载内容的字节数。声明为 var（而非 const），以便包内
// 测试可以调小它，从而以低成本覆盖超大响应体的回归场景。
var maxLegacyAttachmentBytes = 64 << 20

var maxSyncJSONResponseBytes = 32 << 20

// PutBlob 的单次尝试超时与最大尝试次数。
const (
	PutBlobAttemptTimeout = 20 * time.Second
	PutBlobMaxAttempts    = 2
)

// Error is a structured error returned from the controlplane. It contains
// the parsed error code plus any contextual fields the controlplane sent.
// Error 是 controlplane 返回的结构化错误。它包含解析出的错误码以及 controlplane 发送的任何上下文字段。
type Error struct {
	StatusCode   int    `json:"-"`
	Code         string `json:"code"`
	Message      string `json:"message,omitempty"`
	CurrentKeyID string `json:"current_key_id,omitempty"`
	CurrentETag  string `json:"current_etag,omitempty"`
	// MinimumProtocol accompanies upgrade-required errors (e.g.
	// PROFILE_SYNC_UPGRADE_REQUIRED) and names the lowest protocol
	// version the controlplane still accepts.
	// MinimumProtocol 伴随需要升级的错误（例如 PROFILE_SYNC_UPGRADE_REQUIRED），指明 controlplane 仍接受的最低协议版本。
	MinimumProtocol int `json:"minimum_protocol,omitempty"`
	// ResetsAt accompanies a limit the caller reached: when it lifts.
	// ResetsAt 伴随调用者触达的某个限制：表示该限制何时解除。
	ResetsAt string `json:"resets_at,omitempty"`
	// MissingAttachments accompanies MISSING_ATTACHMENT: the ids the
	// pushed chat referenced that the controlplane does not hold.
	// MissingAttachments 伴随 MISSING_ATTACHMENT：即被推送的 chat 所引用、但 controlplane 并不持有的 id。
	MissingAttachments []string        `json:"missing_attachments,omitempty"`
	RetryAfterSeconds  int64           `json:"retry_after_seconds,omitempty"`
	Raw                json.RawMessage `json:"-"`
}

// Error 返回该错误的字符串描述。
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Code != "" {
		return fmt.Sprintf("controlplane: %s (status %d)", e.Code, e.StatusCode)
	}
	if e.Message != "" {
		return fmt.Sprintf("controlplane: %s (status %d)", e.Message, e.StatusCode)
	}
	return fmt.Sprintf("controlplane: status %d", e.StatusCode)
}

// BlobMeta describes one row's controlplane-visible metadata. ETag is the
// CAS token; KeyID is the row's last writer's KeyID. ProjectID is a
// chat-scope side-channel that lets clients observe cross-project moves
// without decrypting the row.
// BlobMeta 描述一行数据对 controlplane 可见的元数据。ETag 是 CAS token；KeyID 是该行最后写入者的 KeyID。ProjectID 是一个 chat 作用域的旁路通道，让客户端无需解密该行即可观察跨 project 的移动。
type BlobMeta struct {
	ID        string    `json:"id"`
	ETag      string    `json:"etag"`
	KeyID     string    `json:"key_id"`
	ProjectID *string   `json:"project_id,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
	// ActivityAt is a chat row's list-ordering time: its last
	// conversation activity, or UpdatedAt when none was recorded.
	// ActivityAt 是 chat 行的列表排序时间：其最后一次会话活动时间，若未记录则取 UpdatedAt。
	ActivityAt *time.Time `json:"activity_at,omitempty"`
	Cursor     string     `json:"cursor,omitempty"`
}

// BlobDelete 是列表中的一条删除记录。
type BlobDelete struct {
	ID        string    `json:"id"`
	Scope     string    `json:"scope"`
	DeletedAt time.Time `json:"deleted_at"`
	Cursor    string    `json:"cursor,omitempty"`
}

// ListStatusResponse 是 list-status 的一页结果。
type ListStatusResponse struct {
	Updates    []BlobMeta   `json:"updates"`
	Deletes    []BlobDelete `json:"deletes"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// revision 摘要与删除全部 project 的端点路径。
const (
	RevisionSummaryPath   = "/api/sync/revision-summary"
	DeleteAllProjectsPath = "/api/sync/blob/projects"
)

// RevisionSummaryResponse 是 revision 摘要的应答。
type RevisionSummaryResponse struct {
	CurrentRevision          string `json:"current_revision"`
	OldestReplayableRevision string `json:"oldest_replayable_revision"`
}

// PutBlobRequest 是一次写入（Put）请求的参数。
type PutBlobRequest struct {
	Scope string
	ID    string
	JWT   string
	// ClerkUserID is the verified subject from the user JWT at the
	// enclave's auth boundary. Forwarded as X-Clerk-User-Id so the
	// controlplane can accept the call via service auth and skip
	// re-verifying the proxied JWT, which may expire mid-migration.
	// ClerkUserID 是 enclave 认证边界处从用户 JWT 验证出的 subject。它以 X-Clerk-User-Id 转发，使
	// controlplane 可以通过服务认证接受该调用，并跳过对可能中途过期的被代理 JWT 的再次验证。
	ClerkUserID         string
	KeyIDHex            string
	IfMatch             string
	IdempotencyKey      string
	OperationHash       string
	RequestID           string
	ProfileSyncProtocol int
	Rewrap              bool
	Ciphertext          []byte
	// ProjectIDSet means "overwrite the controlplane's project_id
	// column with ProjectID". When ProjectIDSet is false the column
	// is left untouched. Nil ProjectID + ProjectIDSet=true clears
	// the column. Only meaningful for chat scope.
	// ProjectIDSet 表示“用 ProjectID 覆盖 controlplane 的 project_id 列”。当 ProjectIDSet 为 false
	// 时该列保持不变。ProjectID 为 nil 且 ProjectIDSet=true 会清空该列。仅对 chat 作用域有意义。
	ProjectIDSet bool
	ProjectID    *string
	// AttachmentRefs lists the buckets-backed attachment ids the chat
	// plaintext references. Sent as X-Attachment-Refs on every chat
	// push (empty list included) so the controlplane can track which
	// index rows are live and purge the rest. Nil means "do not send
	// the header", which is only correct for non-chat scopes.
	// AttachmentRefs 列出 chat 明文所引用的、由 buckets 承载的附件 id。它在每次 chat push 时以
	// X-Attachment-Refs 发送（包括空列表），使 controlplane 能跟踪哪些索引行仍在使用并清理其余行。
	// nil 表示“不发送该 header”，这仅对非 chat 作用域才正确。
	AttachmentRefs []string
	// ActivityAt is the chat's last conversation activity derived from
	// the plaintext, sent as X-Activity-At. Nil omits the header, which
	// makes the controlplane order the row by its write time.
	// ActivityAt 是从明文推导出的 chat 最后一次会话活动时间，以 X-Activity-At 发送。nil 会省略该
	// header，使 controlplane 按写入时间对该行排序。
	ActivityAt *time.Time
}

// PutBlobResponse 是写入成功后的应答。
type PutBlobResponse struct {
	ETag           string `json:"etag"`
	KeyID          string `json:"key_id"`
	SourceRevision int64  `json:"source_revision,omitempty"`
}

// PutBlobAmbiguity 标识写入结果不确定的一种类别。
type PutBlobAmbiguity string

// PutBlob 结果不确定的各种原因。
const (
	PutBlobTransportFailure PutBlobAmbiguity = "transport_failure"
	PutBlobUpstreamFailure  PutBlobAmbiguity = "upstream_failure"
	PutBlobResponseRead     PutBlobAmbiguity = "response_read_failure"
	PutBlobResponseDecode   PutBlobAmbiguity = "response_decode_failure"
	PutBlobInvalidResponse  PutBlobAmbiguity = "invalid_response"
)

// AmbiguousPutBlobError identifies an attempt whose write may have committed
// even though the client could not confirm its outcome.
// AmbiguousPutBlobError 标识一次尝试，其写入可能已经提交，但客户端无法确认结果。
type AmbiguousPutBlobError struct {
	Kind PutBlobAmbiguity
	Err  error
}

// Error 返回该错误的字符串描述。
func (e *AmbiguousPutBlobError) Error() string {
	return fmt.Sprintf("controlplane: ambiguous PutBlob outcome (%s): %v", e.Kind, e.Err)
}

// Unwrap 返回底层错误。
func (e *AmbiguousPutBlobError) Unwrap() error { return e.Err }

// PutBlobOutcomeUnknownError means a dispatched write could not be confirmed,
// so the caller must reconcile before writing again.
// PutBlobOutcomeUnknownError 表示一次已派发的写入无法被确认，因此调用者必须在下一次写入前先进行对账。
type PutBlobOutcomeUnknownError struct {
	Err error
}

// Error 返回该错误的字符串描述。
func (e *PutBlobOutcomeUnknownError) Error() string {
	return fmt.Sprintf("controlplane: PutBlob outcome unknown: %v", e.Err)
}

// Unwrap 返回底层错误。
func (e *PutBlobOutcomeUnknownError) Unwrap() error { return e.Err }

// GetBlobResponse 是一次读取（Get）请求的应答。
type GetBlobResponse struct {
	Ciphertext   []byte
	ETag         string
	KeyID        string
	ProjectIDSet bool
	ProjectID    *string
	// ActivityAt is the chat row's recorded activity time. Nil means the
	// row predates activity tracking and sorts by its write time.
	// ActivityAt 是 chat 行已记录的活动时间。nil 表示该行早于活动跟踪功能，按写入时间排序。
	ActivityAt *time.Time
	// UpdatedAt is when the row was last written; zero when the control
	// plane did not say.
	// UpdatedAt 是该行最后一次被写入的时间；若 control plane 未提供则为零值。
	UpdatedAt time.Time
}

func (c *Client) doRequest(req *http.Request) (*http.Response, error) {
	if c.serviceSecret != "" {
		req.Header.Set(HeaderServiceSecret, c.serviceSecret)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("controlplane: request: %w", err)
	}
	return resp, nil
}

// addAuth stamps the Authorization header and, when the caller passes
// the user id verified at the enclave's auth boundary, the
// X-Clerk-User-Id header that lets controlplane accept the request
// via service auth without re-verifying a JWT that may expire
// mid-migration. The user id is REQUIRED to be the verified
// `sess.Claims.Subject` from the caller — never extracted from the
// JWT payload here, so a malformed or attacker-supplied token cannot
// influence the asserted identity.
// addAuth 会盖上 Authorization header；当调用者传入在 enclave 认证边界验证过的用户 id 时，还会加上
// X-Clerk-User-Id header，使 controlplane 可以通过服务认证接受请求，而无需再次验证可能中途过期的
// JWT。该用户 id 必须是来自调用者的、已验证的 `sess.Claims.Subject` —— 绝不在此处从 JWT 载荷中提取，
// 因此格式错误或攻击者提供的 token 无法影响所断言的标识。
func (c *Client) addAuth(req *http.Request, rawJWT, clerkUserID string) {
	if rawJWT != "" {
		req.Header.Set(HeaderAuth, "Bearer "+rawJWT)
	}
	if clerkUserID != "" {
		req.Header.Set(HeaderClerkUserID, clerkUserID)
	}
}

func (c *Client) urlFor(scope, id string) (string, error) {
	path, err := PathFor(scope, id)
	if err != nil {
		return "", err
	}
	return c.baseURL + path, nil
}

// PathFor returns the controlplane path (no scheme/host) for a given
// scope+id. Exported so callers — most importantly the operation-hash
// builder — can produce a canonical tuple that exactly matches what
// will be sent on the wire.
//
// All blob traffic lives under /api/sync/blob/* — a dedicated surface
// for the enclave that does not overlap with the legacy /api/storage/*
// and /api/profile/* endpoints the webapp drives directly today.
// PathFor 返回给定 scope+id 对应的 controlplane 路径（不含 scheme/host）。导出它，是为了让调用者——
// 尤其是 operation-hash 构建器——能够生成与线上实际发送内容完全一致的规范元组。
//
// 所有 blob 流量都位于 /api/sync/blob/* 之下——这是专供 enclave 使用的接口面，不与 webapp
// 如今直接驱动的旧版 /api/storage/* 和 /api/profile/* 端点重叠。
func PathFor(scope, id string) (string, error) {
	switch scope {
	case "chat":
		return "/api/sync/blob/chat/" + url.PathEscape(id), nil
	case "profile":
		return "/api/sync/blob/profile", nil
	case "project":
		return "/api/sync/blob/project/" + url.PathEscape(id), nil
	case "project_document":
		parent, doc, ok := splitProjectDocID(id)
		if !ok {
			return "", fmt.Errorf("controlplane: invalid project_document id %q", id)
		}
		return "/api/sync/blob/project_document/" + url.PathEscape(parent) + "/" + url.PathEscape(doc), nil
	}
	return "", fmt.Errorf("controlplane: unknown scope %q", scope)
}

func splitProjectDocID(id string) (parent, doc string, ok bool) {
	idx := strings.Index(id, "/")
	if idx <= 0 || idx == len(id)-1 {
		return "", "", false
	}
	return id[:idx], id[idx+1:], true
}

// PutBlob 写入一个 blob，并对结果不确定的失败做有界重试。
func (c *Client) PutBlob(ctx context.Context, req PutBlobRequest) (*PutBlobResponse, error) {
	if req.Rewrap {
		return c.rewrapBlob(ctx, req)
	}
	for attempt := 0; attempt < PutBlobMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := c.putBlobOnce(ctx, req)
		if err == nil {
			return resp, nil
		}
		var ambiguous *AmbiguousPutBlobError
		if !errors.As(err, &ambiguous) {
			return nil, err
		}
		if ctx.Err() != nil || attempt == PutBlobMaxAttempts-1 {
			return nil, &PutBlobOutcomeUnknownError{Err: err}
		}
	}
	panic("unreachable")
}

func (c *Client) putBlobOnce(ctx context.Context, req PutBlobRequest) (*PutBlobResponse, error) {
	endpoint, err := c.urlFor(req.Scope, req.ID)
	if err != nil {
		return nil, err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, PutBlobAttemptTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(attemptCtx, http.MethodPut, endpoint, bytes.NewReader(req.Ciphertext))
	if err != nil {
		return nil, err
	}
	// Disable net/http's transparent body replay so the bounded loop above is
	// the only layer that can issue a second controlplane write.
	// 禁用 net/http 的透明请求体重放，使上面的有界循环成为唯一可能发起第二次 controlplane 写入的层。
	httpReq.GetBody = nil
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/octet-stream")
	httpReq.Header.Set(HeaderKeyID, req.KeyIDHex)
	if req.IfMatch != "" {
		httpReq.Header.Set(HeaderIfMatch, req.IfMatch)
	}
	if req.IdempotencyKey != "" {
		httpReq.Header.Set(HeaderIdempotency, req.IdempotencyKey)
	}
	if req.OperationHash != "" {
		httpReq.Header.Set(HeaderOperationHash, req.OperationHash)
	}
	if req.RequestID != "" {
		httpReq.Header.Set(HeaderRequestID, req.RequestID)
	}
	if req.ProfileSyncProtocol > 0 {
		httpReq.Header.Set(HeaderProfileSyncProtocol, strconv.Itoa(req.ProfileSyncProtocol))
	}
	if req.ProjectIDSet {
		httpReq.Header.Set(HeaderProjectIDSet, "1")
		if req.ProjectID != nil {
			httpReq.Header.Set(HeaderProjectID, *req.ProjectID)
		}
	}
	if req.AttachmentRefs != nil {
		httpReq.Header.Set(HeaderAttachmentRefs, strings.Join(req.AttachmentRefs, ","))
	}
	if req.ActivityAt != nil {
		httpReq.Header.Set(HeaderActivityAt, req.ActivityAt.UTC().Format(time.RFC3339Nano))
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, &AmbiguousPutBlobError{Kind: PutBlobTransportFailure, Err: err}
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		cpErr := parseError(resp.StatusCode, body)
		if isRetryablePutBlobStatus(resp.StatusCode) {
			return nil, &AmbiguousPutBlobError{Kind: PutBlobUpstreamFailure, Err: cpErr}
		}
		return nil, cpErr
	}
	if readErr != nil {
		return nil, &AmbiguousPutBlobError{Kind: PutBlobResponseRead, Err: readErr}
	}
	var out PutBlobResponse
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&out); err != nil {
		return nil, &AmbiguousPutBlobError{Kind: PutBlobResponseDecode, Err: err}
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, &AmbiguousPutBlobError{Kind: PutBlobResponseDecode, Err: errors.New("trailing data in PutBlob response")}
	}
	headerETag := resp.Header.Get(HeaderETag)
	if out.ETag == "" && headerETag == "" {
		return nil, &AmbiguousPutBlobError{Kind: PutBlobInvalidResponse, Err: errors.New("missing ETag in PutBlob response")}
	}
	if out.ETag != "" && headerETag != "" && out.ETag != headerETag {
		return nil, &AmbiguousPutBlobError{Kind: PutBlobInvalidResponse, Err: errors.New("inconsistent ETag in PutBlob response")}
	}
	if out.ETag == "" {
		out.ETag = headerETag
	}
	headerKeyID := resp.Header.Get(HeaderKeyID)
	if out.KeyID != "" && headerKeyID != "" && out.KeyID != headerKeyID {
		return nil, &AmbiguousPutBlobError{Kind: PutBlobInvalidResponse, Err: errors.New("inconsistent KeyID in PutBlob response")}
	}
	if out.KeyID == "" {
		out.KeyID = headerKeyID
	}
	if out.KeyID == "" {
		out.KeyID = req.KeyIDHex
	}
	return &out, nil
}

func isRetryablePutBlobStatus(status int) bool {
	return status >= 500 && status <= 599
}

// rewrapBlob targets the dedicated /api/sync/rewrap endpoint. Unlike
// PutBlob's primary path, rewrap is a JSON POST with the ciphertext
// base64-encoded inside the body. The controlplane handler updates
// the row's `data` + `key_id` + `etag` without bumping `sync_version`
// or `updated_at` — a rewrap is invisible to user-facing sync.
// RewrapPath is the controlplane path for the dedicated rewrap
// endpoint. Exported so the enclave can hash the same canonical path
// when computing X-Operation-Hash.
// rewrapBlob 针对专用的 /api/sync/rewrap 端点。与 PutBlob 的主路径不同，rewrap 是一个 JSON POST，
// 密文在请求体内以 base64 编码。controlplane 处理器会更新该行的 `data` + `key_id` + `etag`，而不
// 增加 `sync_version` 或 `updated_at` —— 因此 rewrap 对用户可见的同步是无感知的。
// RewrapPath 是专用 rewrap 端点对应的 controlplane 路径。导出它，是为了让 enclave 在计算
// X-Operation-Hash 时能够对同一规范路径进行哈希。
const RewrapPath = "/api/sync/rewrap"

// RewrapBody marshals the body the rewrap call sends to the
// controlplane. Exported so the enclave can compute X-Operation-Hash
// over exactly the bytes that will hit the wire.
// RewrapBody 序列化 rewrap 调用发送给 controlplane 的请求体。导出它，是为了让 enclave 能够对即将
// 上线的字节精确计算 X-Operation-Hash。
func RewrapBody(req PutBlobRequest) ([]byte, error) {
	return json.Marshal(map[string]string{
		"scope":          req.Scope,
		"id":             req.ID,
		"key_id":         req.KeyIDHex,
		"if_match":       req.IfMatch,
		"ciphertext_b64": base64.StdEncoding.EncodeToString(req.Ciphertext),
	})
}

func (c *Client) rewrapBlob(ctx context.Context, req PutBlobRequest) (*PutBlobResponse, error) {
	body, err := RewrapBody(req)
	if err != nil {
		return nil, err
	}
	endpoint := c.baseURL + RewrapPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/json")
	if req.IdempotencyKey != "" {
		httpReq.Header.Set(HeaderIdempotency, req.IdempotencyKey)
	}
	if req.OperationHash != "" {
		httpReq.Header.Set(HeaderOperationHash, req.OperationHash)
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, respBody)
	}
	var out struct {
		OK    bool   `json:"ok"`
		ETag  string `json:"etag"`
		KeyID string `json:"key_id"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode rewrap: %w", err)
	}
	if out.ETag == "" {
		return nil, errors.New("controlplane: missing etag in rewrap response")
	}
	if out.KeyID == "" {
		out.KeyID = req.KeyIDHex
	}
	return &PutBlobResponse{ETag: out.ETag, KeyID: out.KeyID}, nil
}

// GetBlob 读取一个 blob 的密文及其元数据。
func (c *Client) GetBlob(ctx context.Context, scope, id, jwt, clerkUserID string) (*GetBlobResponse, error) {
	endpoint, err := c.urlFor(scope, id)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	out := &GetBlobResponse{
		Ciphertext: body,
		ETag:       resp.Header.Get(HeaderETag),
		KeyID:      resp.Header.Get(HeaderKeyID),
	}
	if scope == "chat" && resp.Header.Get(HeaderProjectIDSet) == "1" {
		out.ProjectIDSet = true
		if projectID := resp.Header.Get(HeaderProjectID); projectID != "" {
			out.ProjectID = &projectID
		}
	}
	if scope == "chat" {
		if activityAt, err := time.Parse(time.RFC3339Nano, resp.Header.Get(HeaderActivityAt)); err == nil {
			out.ActivityAt = &activityAt
		}
	}
	if updatedAt, err := time.Parse(time.RFC3339Nano, resp.Header.Get(HeaderUpdatedAt)); err == nil {
		out.UpdatedAt = updatedAt
	}
	return out, nil
}

// HeadBlob 仅读取一个 blob 的元数据，不返回其内容。
func (c *Client) HeadBlob(ctx context.Context, scope, id, jwt, clerkUserID string) (*BlobMeta, error) {
	endpoint, err := c.urlFor(scope, id)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, parseError(resp.StatusCode, body)
	}
	return &BlobMeta{
		ID:    id,
		ETag:  resp.Header.Get(HeaderETag),
		KeyID: resp.Header.Get(HeaderKeyID),
	}, nil
}

// DeleteBlobRequest 是一次删除请求的参数。
type DeleteBlobRequest struct {
	Scope          string
	ID             string
	JWT            string
	ClerkUserID    string
	IfMatch        string
	IdempotencyKey string
	OperationHash  string
}

// DeleteBlobResponse mirrors the controlplane's `writeDeleteResponse*`
// shape. For chat deletes the controlplane returns the v2 attachment
// ids it dropped, so the enclave can wipe the matching buckets blobs.
// For non-chat scopes the field is empty.
// DeleteBlobResponse 镜像 controlplane 的 `writeDeleteResponse*` 结构。对于 chat 删除，controlplane
// 会返回它丢弃的 v2 附件 id，使 enclave 能擦除对应的 buckets blob。对于非 chat 作用域，该字段为空。
type DeleteBlobResponse struct {
	OK                 bool     `json:"ok"`
	WipedV2Attachments []string `json:"wiped_v2_attachments,omitempty"`
	SourceRevision     int64    `json:"source_revision,omitempty"`
}

// DeleteBlob 删除一个 blob。
func (c *Client) DeleteBlob(ctx context.Context, req DeleteBlobRequest) (*DeleteBlobResponse, error) {
	endpoint, err := c.urlFor(req.Scope, req.ID)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	if req.IfMatch != "" {
		httpReq.Header.Set(HeaderIfMatch, req.IfMatch)
	}
	if req.IdempotencyKey != "" {
		httpReq.Header.Set(HeaderIdempotency, req.IdempotencyKey)
	}
	if req.OperationHash != "" {
		httpReq.Header.Set(HeaderOperationHash, req.OperationHash)
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	out := &DeleteBlobResponse{}
	if len(body) > 0 {
		// The controlplane may return an empty body on older
		// deploys; treat that as a successful no-attachments delete
		// rather than an error.
		if err := json.Unmarshal(body, out); err != nil {
			return nil, fmt.Errorf("controlplane: decode delete response: %w", err)
		}
	}
	return out, nil
}

// DeleteAllProjectsRequest 是删除全部 project 请求的参数。
type DeleteAllProjectsRequest struct {
	JWT            string
	ClerkUserID    string
	KeyIDHex       string
	IdempotencyKey string
	OperationHash  string
}

// DeleteAllProjectsResponse 是删除全部 project 的应答。
type DeleteAllProjectsResponse struct {
	OK      bool `json:"ok"`
	Deleted int  `json:"deleted"`
}

// DeleteAllProjects 删除当前用户的所有 project 与 project document。
func (c *Client) DeleteAllProjects(ctx context.Context, req DeleteAllProjectsRequest) (*DeleteAllProjectsResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+DeleteAllProjectsPath, nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	httpReq.Header.Set(HeaderKeyID, req.KeyIDHex)
	httpReq.Header.Set(HeaderIdempotency, req.IdempotencyKey)
	httpReq.Header.Set(HeaderOperationHash, req.OperationHash)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	if readErr != nil {
		return nil, fmt.Errorf("controlplane: read delete-all-projects response: %w", readErr)
	}
	var out DeleteAllProjectsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode delete-all-projects response: %w", err)
	}
	return &out, nil
}

// listStatusOmitDeletes asks list-status for live rows alone: no deletes,
// and a cursor that pages past live rows only.
// listStatusOmitDeletes 让 list-status 只返回存活行：不含删除记录，且游标只对存活行进行分页。
const listStatusOmitDeletes = "omit"

// ListStatus is one page of a scope's rows and deletes. omitDeletes leaves
// the deletes out, so a listing of what exists is not paged by them; a
// control plane without the option still sends them.
// ListStatus 返回某个作用域的行与删除记录的一页。omitDeletes 会排除删除记录，因此对现存内容的列表不会因删除记录而分页；不支持该选项的 control plane 仍会发送它们。
func (c *Client) ListStatus(ctx context.Context, scope, cursor string, limit int, jwt, clerkUserID, projectID, direction string, omitDeletes bool) (*ListStatusResponse, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	endpoint := c.baseURL + "/api/sync/list-status"
	q := url.Values{}
	q.Set("scope", scope)
	q.Set("limit", fmt.Sprintf("%d", limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if projectID != "" {
		q.Set("project_id", projectID)
	}
	if direction != "" {
		q.Set("direction", direction)
	}
	if omitDeletes {
		q.Set("deletes", listStatusOmitDeletes)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	var out ListStatusResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode list-status: %w", err)
	}
	return &out, nil
}

// RevisionSummary 返回当前与最旧可回放的 revision。
func (c *Client) RevisionSummary(ctx context.Context, jwt, clerkUserID string) (*RevisionSummaryResponse, error) {
	var out RevisionSummaryResponse
	if err := c.getSyncJSON(ctx, RevisionSummaryPath, nil, jwt, clerkUserID, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) getSyncJSON(ctx context.Context, path string, query url.Values, jwt, clerkUserID string, out any) error {
	endpoint := c.baseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxSyncJSONResponseBytes)+1))
	if err != nil {
		return err
	}
	if len(body) > maxSyncJSONResponseBytes {
		return fmt.Errorf("controlplane: response for %s exceeds %d bytes", path, maxSyncJSONResponseBytes)
	}
	if resp.StatusCode >= 400 {
		return parseError(resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("controlplane: decode %s: %w", path, err)
	}
	return nil
}

// SearchIndexStatePath 是搜索索引状态的端点路径。
const SearchIndexStatePath = "/api/sync/search-index"

// SearchIndexState 描述搜索索引的发布状态。
type SearchIndexState struct {
	SourceRevision          int64  `json:"source_revision"`
	PublicationGeneration   int64  `json:"publication_generation"`
	FenceGeneration         int64  `json:"fence_generation"`
	PublishedSourceRevision int64  `json:"published_source_revision"`
	ObjectKey               string `json:"object_key,omitempty"`
	KeyID                   string `json:"key_id,omitempty"`
	Model                   string `json:"model,omitempty"`
	Incomplete              bool   `json:"incomplete"`
	PublicationIncomplete   bool   `json:"publication_incomplete"`
}

// PublishSearchIndexRequest 是一次搜索索引发布请求的参数。
type PublishSearchIndexRequest struct {
	JWT                   string
	ClerkUserID           string
	ExpectedGeneration    int64
	ExpectedFence         int64
	CoveredSourceRevision int64
	ObjectKey             string
	KeyID                 string
	Model                 string
	Incomplete            bool
}

// SearchIndexConflictError 表示搜索索引发布冲突。
type SearchIndexConflictError struct {
	State SearchIndexState
}

// Error 返回该错误的字符串描述。
func (e *SearchIndexConflictError) Error() string {
	return "controlplane: search index publication conflict"
}

// GetSearchIndexState 读取当前搜索索引状态。
func (c *Client) GetSearchIndexState(ctx context.Context, jwt, clerkUserID string) (*SearchIndexState, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+SearchIndexStatePath, nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	var state SearchIndexState
	if err := json.Unmarshal(body, &state); err != nil {
		return nil, fmt.Errorf("controlplane: decode search index state: %w", err)
	}
	return &state, nil
}

// PublishSearchIndex 发布一个新的搜索索引对象。
func (c *Client) PublishSearchIndex(ctx context.Context, req PublishSearchIndexRequest) (*SearchIndexState, error) {
	body, err := json.Marshal(map[string]any{
		"expected_generation":     req.ExpectedGeneration,
		"expected_fence":          req.ExpectedFence,
		"covered_source_revision": req.CoveredSourceRevision,
		"object_key":              req.ObjectKey,
		"key_id":                  req.KeyID,
		"model":                   req.Model,
		"incomplete":              req.Incomplete,
	})
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+SearchIndexStatePath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/json")
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusConflict {
		var conflict struct {
			Code  string           `json:"code"`
			State SearchIndexState `json:"state"`
		}
		if err := json.Unmarshal(respBody, &conflict); err == nil && conflict.Code == StatusSearchIndexConflict {
			return nil, &SearchIndexConflictError{State: conflict.State}
		}
	}
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, respBody)
	}
	var state SearchIndexState
	if err := json.Unmarshal(respBody, &state); err != nil {
		return nil, fmt.Errorf("controlplane: decode published search index state: %w", err)
	}
	return &state, nil
}

// ListNeedsMigrationResponse 是需要迁移列表的应答。
type ListNeedsMigrationResponse struct {
	IDs                []string `json:"ids"`
	RetryableRemaining int      `json:"retryable_remaining"`
	BlockedUnmigrated  int      `json:"blocked_unmigrated"`
}

// ListNeedsMigration 列出某个作用域中需要重新密封的行。
func (c *Client) ListNeedsMigration(ctx context.Context, scope string, limit int, jwt, clerkUserID string) (*ListNeedsMigrationResponse, error) {
	endpoint := c.baseURL + "/api/sync/needs-migration"
	q := url.Values{}
	q.Set("scope", scope)
	q.Set("limit", fmt.Sprintf("%d", limit))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	var out ListNeedsMigrationResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode needs-migration: %w", err)
	}
	return &out, nil
}

// RecordMigrationFailure tells the controlplane that a migration attempt
// for the given (scope, id) failed because none of the supplied keys could
// decrypt the legacy blob. The controlplane stamps the row with the current
// timestamp so it is excluded from the next batch until the 24h cooldown
// elapses.
// RecordMigrationFailure 告知 controlplane 对给定 (scope, id) 的迁移尝试失败，原因是所提供的密钥都无法解密旧 blob。controlplane 会用当前时间戳标记该行，使其在 24 小时冷却期结束前不会进入下一批。
func (c *Client) RecordMigrationFailure(ctx context.Context, scope, id, jwt, clerkUserID string) error {
	endpoint := c.baseURL + "/api/sync/migration-failure"
	body, _ := json.Marshal(map[string]string{"scope": scope, "id": id})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/json")
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return parseError(resp.StatusCode, respBody)
	}
	return nil
}

// ChatActivityPath is the controlplane endpoint that fills in the
// activity time of a chat row written before it was tracked.
// ChatActivityPath 是补填早于活动跟踪写入的 chat 行活动时间的 controlplane 端点。
const ChatActivityPath = "/api/sync/chat-activity"

// BackfillChatActivity records the activity time derived from a chat
// whose row has none. The controlplane applies it only while the row is
// still at etag and has no activity time, so losing that race is not an
// error.
// BackfillChatActivity 为没有活动时间的 chat 行记录从其推导出的活动时间。controlplane 只会在该行仍处于该 etag 且尚无活动时间时应用它，因此输掉该竞争不算错误。
func (c *Client) BackfillChatActivity(ctx context.Context, jwt, clerkUserID, id, etag string, activityAt time.Time) error {
	body, err := json.Marshal(map[string]string{
		"id":          id,
		"etag":        etag,
		"activity_at": activityAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+ChatActivityPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/json")
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return parseError(resp.StatusCode, respBody)
	}
	return nil
}

// --- key registry ----------------------------------------------------------
// --- 密钥注册表 ------------------------------------------------------------

// CurrentKeyResponse 是当前密钥的应答。
type CurrentKeyResponse struct {
	KeyID      string                      `json:"key_id"`
	ETag       string                      `json:"etag"`
	Bundles    map[string]CurrentKeyBundle `json:"bundles"`
	CreatedVia string                      `json:"created_via"`
	// CreatedAt and RegisteredAt are decoded as the controlplane's
	// already-formatted RFC3339 strings, not time.Time. The no-key
	// response carries an empty `created_at`, which fails time.Time
	// decoding; keeping them as strings tolerates that and avoids a
	// lossy parse-then-reformat round-trip on the way to the client.
	// CreatedAt 与 RegisteredAt 按 controlplane 已格式化好的 RFC3339 字符串解码，而不是 time.Time。
	// 无密钥应答会携带空的 `created_at`，这会导致 time.Time 解码失败；保持为字符串可以容忍这一点，
	// 并避免在转发给客户端时经历一次有损的“解析后再格式化”往返。
	CreatedAt string `json:"created_at"`
	HasData   bool   `json:"has_data"`
}

// CurrentKeyBundle 是当前密钥的某个 passkey bundle。
type CurrentKeyBundle struct {
	CredentialID  string `json:"credential_id"`
	KEKIV         string `json:"kek_iv"`
	EncryptedKeys string `json:"encrypted_keys"`
	RegisteredAt  string `json:"registered_at"`
}

// GetCurrentKey 读取当前密钥及其 bundle。
func (c *Client) GetCurrentKey(ctx context.Context, jwt, clerkUserID string) (*CurrentKeyResponse, error) {
	endpoint := c.baseURL + "/api/sync/keys/current"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	var out CurrentKeyResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode current-key: %w", err)
	}
	return &out, nil
}

// RegisterKeyRequest 是注册密钥请求的参数。
type RegisterKeyRequest struct {
	JWT            string
	ClerkUserID    string
	KeyIDHex       string
	IfMatch        string
	CreatedVia     string
	IdempotencyKey string
	OperationHash  string
	InitialBundle  *RegisterKeyBundle
}

// RegisterKeyBundle 是注册时提供的初始密钥 bundle。
type RegisterKeyBundle struct {
	CredentialID  string `json:"credential_id"`
	KEKIV         string `json:"kek_iv"`
	EncryptedKeys string `json:"encrypted_keys"`
}

// RegisterKeyBody returns the JSON body bytes that will be sent for the
// given RegisterKey request. Exported so callers can hash the exact
// bytes that go on the wire as part of the operation-hash canonical tuple.
// RegisterKeyBody 返回将随给定 RegisterKey 请求发送的 JSON 请求体字节。导出它，是为了让调用者能够
// 对即将上线的确切字节进行哈希，作为 operation-hash 规范元组的一部分。
func RegisterKeyBody(req RegisterKeyRequest) ([]byte, error) {
	payload := map[string]any{
		"key_id":      req.KeyIDHex,
		"created_via": req.CreatedVia,
	}
	if req.InitialBundle != nil {
		payload["initial_bundle"] = req.InitialBundle
	}
	return json.Marshal(payload)
}

// RegisterKeyPath is the controlplane path the RegisterKey call targets.
// RegisterKeyPath 是 RegisterKey 调用所针对的 controlplane 路径。
const RegisterKeyPath = "/api/sync/keys"

// RegisterKeyResponse mirrors the wire body the controlplane emits on
// a successful `POST /api/sync/keys`. `WipedV2Attachments` is
// populated only when the call hit the start-fresh bypass; on every
// other path it is nil/empty.
// RegisterKeyResponse 镜像 controlplane 在 `POST /api/sync/keys` 成功时返回的线上响应体。只有当调用
// 命中了 start-fresh 旁路时才会填充 `WipedV2Attachments`；在其余路径上它都是 nil/空。
type RegisterKeyResponse struct {
	OK                 bool     `json:"ok"`
	KeyID              string   `json:"key_id"`
	ETag               string   `json:"etag"`
	SearchIndexFenced  bool     `json:"-"`
	WipedV2Attachments []string `json:"wiped_v2_attachments,omitempty"`
}

// RegisterKey 注册或更换当前密钥。
func (c *Client) RegisterKey(ctx context.Context, req RegisterKeyRequest) (*RegisterKeyResponse, error) {
	endpoint := c.baseURL + RegisterKeyPath
	body, err := RegisterKeyBody(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/json")
	if req.IfMatch != "" {
		httpReq.Header.Set(HeaderIfMatch, req.IfMatch)
	}
	if req.IdempotencyKey != "" {
		httpReq.Header.Set(HeaderIdempotency, req.IdempotencyKey)
	}
	if req.OperationHash != "" {
		httpReq.Header.Set(HeaderOperationHash, req.OperationHash)
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, respBody)
	}
	out := &RegisterKeyResponse{}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return nil, fmt.Errorf("controlplane: decode register-key response: %w", err)
		}
	}
	out.SearchIndexFenced = strings.EqualFold(resp.Header.Get(HeaderSearchIndexFenced), "true")
	return out, nil
}

// AddBundleRequest 是添加密钥 bundle 请求的参数。
type AddBundleRequest struct {
	JWT            string
	ClerkUserID    string
	KeyIDHex       string
	CredentialID   string
	KEKIV          string
	EncryptedKeys  string
	IdempotencyKey string
	OperationHash  string
}

// AddBundleBody marshals the body of an add-bundle request. Exposed so
// the enclave can hash exactly what will hit the wire when computing
// X-Operation-Hash.
// AddBundleBody 序列化 add-bundle 请求的请求体。导出它，是为了让 enclave 在计算 X-Operation-Hash 时
// 能够对即将上线的内容进行精确哈希。
func AddBundleBody(req AddBundleRequest) ([]byte, error) {
	return json.Marshal(map[string]string{
		"credential_id":  req.CredentialID,
		"kek_iv":         req.KEKIV,
		"encrypted_keys": req.EncryptedKeys,
	})
}

// AddBundlePath is the controlplane path for the add-bundle endpoint.
// Used by the enclave to keep the op-hash canonical path aligned
// with the actual wire path.
// AddBundlePath 是 add-bundle 端点对应的 controlplane 路径。enclave 用它来让 op-hash 的规范路径与实际线上路径保持一致。
func AddBundlePath(keyIDHex string) string {
	return "/api/sync/keys/" + url.PathEscape(keyIDHex) + "/bundles"
}

// AddBundle 为当前密钥添加一个 passkey bundle。
func (c *Client) AddBundle(ctx context.Context, req AddBundleRequest) error {
	endpoint := c.baseURL + AddBundlePath(req.KeyIDHex)
	body, err := AddBundleBody(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/json")
	if req.IdempotencyKey != "" {
		httpReq.Header.Set(HeaderIdempotency, req.IdempotencyKey)
	}
	if req.OperationHash != "" {
		httpReq.Header.Set(HeaderOperationHash, req.OperationHash)
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return parseError(resp.StatusCode, respBody)
	}
	return nil
}

// RemoveBundleRequest 是移除密钥 bundle 请求的参数。
type RemoveBundleRequest struct {
	JWT            string
	ClerkUserID    string
	KeyIDHex       string
	CredentialID   string
	IdempotencyKey string
	OperationHash  string
}

// RemoveBundlePath returns the canonical wire path for a remove-bundle
// request so callers can compute the op-hash over the same string.
// RemoveBundlePath 返回 remove-bundle 请求的规范线上路径，以便调用者对同一字符串计算 op-hash。
func RemoveBundlePath(keyIDHex, credentialID string) string {
	return "/api/sync/keys/" + url.PathEscape(keyIDHex) + "/bundles/" + url.PathEscape(credentialID)
}

// RemoveBundle 从当前密钥移除一个 passkey bundle。
func (c *Client) RemoveBundle(ctx context.Context, req RemoveBundleRequest) error {
	endpoint := c.baseURL + RemoveBundlePath(req.KeyIDHex, req.CredentialID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	c.addAuth(httpReq, req.JWT, req.ClerkUserID)
	if req.IdempotencyKey != "" {
		httpReq.Header.Set(HeaderIdempotency, req.IdempotencyKey)
	}
	if req.OperationHash != "" {
		httpReq.Header.Set(HeaderOperationHash, req.OperationHash)
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return parseError(resp.StatusCode, respBody)
	}
	return nil
}

func parseError(status int, body []byte) error {
	if len(body) == 0 {
		return &Error{StatusCode: status}
	}
	var e Error
	if err := json.Unmarshal(body, &e); err != nil {
		return &Error{StatusCode: status, Message: string(body), Raw: body}
	}
	e.StatusCode = status
	e.Raw = body
	return &e
}

// IsCode reports whether err is a controlplane error with the given code.
// IsCode 报告 err 是否是指定 code 的 controlplane 错误。
func IsCode(err error, code string) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Code == code
}

// ErrLegacyAttachmentNotFound is a legacy attachment the control plane
// does not hold.
// ErrLegacyAttachmentNotFound 表示 control plane 并不持有的旧版附件。
var ErrLegacyAttachmentNotFound = errors.New("controlplane: legacy attachment not found")

// ErrLegacyAttachmentGone is a legacy attachment already moved to buckets,
// which the chat's embedded key addresses there as it is.
// ErrLegacyAttachmentGone 表示一个已经迁移到 buckets 的旧版附件，chat 内嵌的 key 可以就地寻址它。
var ErrLegacyAttachmentGone = errors.New("controlplane: legacy attachment already moved to buckets")

// ErrLegacyClaimInvalid is a legacy attachment served without the control
// plane's claim that it is the caller's, or with one the service secret
// does not verify.
// ErrLegacyClaimInvalid 表示某个旧版附件在提供时没有 control plane 关于它属于调用者的 claim，或者其 claim 无法通过服务密钥验证。
var ErrLegacyClaimInvalid = errors.New("controlplane: legacy attachment claim invalid")

// HeaderLegacyClaim carries the control plane's claim on a legacy
// attachment it serves to the enclave.
// HeaderLegacyClaim 携带 control plane 对它提供给 enclave 的旧版附件所做的 claim。
const HeaderLegacyClaim = "X-Legacy-Claim"

// legacyAttachmentClaim is what the control plane signs when it serves a
// legacy attachment: the user, the attachment, and a digest of the bytes.
// Fields are in alphabetical order, as the control plane marshals them.
// legacyAttachmentClaim 是 control plane 提供旧版附件时签名的内容：用户、附件以及字节的摘要。字段按字母顺序排列，与 control plane 序列化的顺序一致。
type legacyAttachmentClaim struct {
	ClerkUserID string `json:"clerk_user_id"`
	ID          string `json:"id"`
	Scope       string `json:"scope"`
	SHA256      string `json:"sha256"`
}

// GetLegacyAttachment reads an attachment as older clients stored it, in
// the control plane's own database rather than buckets, and checks the
// control plane's claim that it is clerkUserID's: an HMAC under the
// service secret over the user, the id and the bytes' digest.
// GetLegacyAttachment 读取旧客户端存储的附件（存于 control plane 自己的数据库而非 buckets），并校验
// control plane 关于它属于 clerkUserID 的 claim：即用服务密钥对用户、id 和字节摘要计算的 HMAC。
func (c *Client) GetLegacyAttachment(ctx context.Context, jwt, clerkUserID, attachmentID string) ([]byte, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("controlplane: attachment id is required")
	}
	endpoint := c.baseURL + "/api/storage/attachment/" + url.PathEscape(attachmentID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrLegacyAttachmentNotFound
	}
	if resp.StatusCode == http.StatusGone {
		return nil, ErrLegacyAttachmentGone
	}
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, parseError(resp.StatusCode, raw)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxLegacyAttachmentBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxLegacyAttachmentBytes {
		return nil, fmt.Errorf("controlplane: legacy attachment exceeds %d bytes", maxLegacyAttachmentBytes)
	}
	if !c.legacyClaimValid(resp.Header.Get(HeaderLegacyClaim), clerkUserID, attachmentID, body) {
		return nil, ErrLegacyClaimInvalid
	}
	return body, nil
}

func (c *Client) legacyClaimValid(claim, clerkUserID, attachmentID string, ciphertext []byte) bool {
	provided, err := hex.DecodeString(claim)
	if err != nil || len(provided) == 0 {
		return false
	}
	digest := sha256.Sum256(ciphertext)
	payload, err := json.Marshal(legacyAttachmentClaim{ClerkUserID: clerkUserID, ID: attachmentID, Scope: "attachment", SHA256: hex.EncodeToString(digest[:])})
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(c.serviceSecret))
	mac.Write(payload)
	return hmac.Equal(provided, mac.Sum(nil))
}

// RegisterAttachmentIndex records ownership of a v2 attachment with
// controlplane. The ciphertext itself lives in buckets.tinfoil.sh
// under a path the enclave derived from (CEK, attachmentID); the
// controlplane only learns "this attachment id exists for this chat,
// owned by this user, in v2 format." JWT is forwarded so the
// controlplane resolves the user from claims, exactly like every
// other storage route.
// RegisterAttachmentIndex 向 controlplane 记录某个 v2 附件的所有权。密文本身存放于 buckets.tinfoil.sh
// 下由 enclave 从 (CEK, attachmentID) 推导出的路径中；controlplane 只会得知“这个附件 id 存在于该 chat、
// 属于该用户、格式为 v2”。JWT 会被转发，以便 controlplane 像其他所有存储路由一样从 claim 中解析用户。
func (c *Client) RegisterAttachmentIndex(ctx context.Context, jwt, clerkUserID, attachmentID, chatID string) error {
	return c.postAttachmentChatID(
		ctx,
		jwt,
		clerkUserID,
		attachmentID,
		chatID,
		"/api/sync/attachment-index/",
		"attachment index",
	)
}

// ImportOutcomeStatus tells the controlplane whether an import job
// finished or stopped early so it can pick the matching email.
// ImportOutcomeStatus 告知 controlplane 某个导入任务是已完成还是提前停止，以便它选择相应的邮件。
type ImportOutcomeStatus string

// 导入结果的状态值。
const (
	ImportOutcomeCompleted ImportOutcomeStatus = "completed"
	ImportOutcomeFailed    ImportOutcomeStatus = "failed"
)

// ImportOutcome is the payload for the import notification callback.
// FailureReason and FailureDetail are only set when Status is
// ImportOutcomeFailed and carry the enclave's user-safe classification
// and the check that fired, never raw error text.
// ImportOutcome 是导入通知回调的载荷。FailureReason 与 FailureDetail 仅在 Status 为 ImportOutcomeFailed 时设置，携带 enclave 面向用户的安全分类以及触发的原因，绝不包含原始错误文本。
type ImportOutcome struct {
	ClerkUserID   string
	JobID         string
	Source        string
	Status        ImportOutcomeStatus
	Imported      int
	Failed        int
	FailureReason string
	FailureDetail string
}

// NotifyImportOutcome asks the controlplane to email the user that an
// off-device chat import finished or failed. The enclave has no JWT to
// forward for a detached job (it may have expired mid-import), so this
// is a service-secret call that asserts the user only via
// X-Clerk-User-Id.
// NotifyImportOutcome 请求 controlplane 向用户发送邮件，告知离线 chat 导入已完成或失败。对于分离的任务，
// enclave 没有可转发的 JWT（它可能已在导入中途过期），因此这是一个仅通过 X-Clerk-User-Id 断言用户
// 的服务密钥调用。
func (c *Client) NotifyImportOutcome(ctx context.Context, outcome ImportOutcome) error {
	if outcome.ClerkUserID == "" {
		return fmt.Errorf("controlplane: clerk user id is required")
	}
	payload := map[string]any{
		"jobId":         outcome.JobID,
		"source":        outcome.Source,
		"status":        string(outcome.Status),
		"importedCount": outcome.Imported,
		"failedCount":   outcome.Failed,
	}
	if outcome.Status == ImportOutcomeFailed && outcome.FailureReason != "" {
		payload["failureReason"] = outcome.FailureReason
		if outcome.FailureDetail != "" {
			payload["failureDetail"] = outcome.FailureDetail
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	clerkUserID := outcome.ClerkUserID
	endpoint := c.baseURL + "/api/sync/notify-import-complete"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	c.addAuth(httpReq, "", clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return parseError(resp.StatusCode, raw)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// ResearchPath is where the control plane counts each user's deep
// researches, which it allows one an hour.
// ResearchPath 是 control plane 统计每个用户深度研究次数的端点，它允许每小时一次。
const ResearchPath = "/api/sync/deep-research"

// ResearchAllowance is whether a user may start a deep research now, and if
// not, when they may.
// ResearchAllowance 表示用户当前是否可以发起一次深度研究，以及若不可以，何时可以。
type ResearchAllowance struct {
	Available         bool   `json:"available"`
	ResetsAt          string `json:"resets_at,omitempty"`
	RetryAfterSeconds int64  `json:"retry_after_seconds,omitempty"`
}

// ResearchAllowance reads whether the user may start a deep research now,
// without starting one. Like every call that asserts the user only through
// X-Clerk-User-Id, it forwards no JWT, which may expire before a queued
// turn starts.
// ResearchAllowance 在不发起深度研究的情况下读取用户现在是否可以进行深度研究。与所有仅通过
// X-Clerk-User-Id 断言用户的调用一样，它不转发 JWT，因为 JWT 可能在排队的 Turn 开始前就过期。
func (c *Client) ResearchAllowance(ctx context.Context, clerkUserID string) (*ResearchAllowance, error) {
	if clerkUserID == "" {
		return nil, fmt.Errorf("controlplane: clerk user id is required")
	}
	var out ResearchAllowance
	if err := c.getSyncJSON(ctx, ResearchPath, nil, "", clerkUserID, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StartResearch counts a deep research as the user's for the hour. The
// control plane refuses it with StatusDeepResearchLimitReached, and when the
// limit lifts, while the user's last one started less than an hour ago.
// StartResearch 将一次深度研究计入该用户本小时的名额。若用户上次研究开始不到一小时，control plane 会以 StatusDeepResearchLimitReached 拒绝，直到该限制解除。
func (c *Client) StartResearch(ctx context.Context, clerkUserID string) error {
	if clerkUserID == "" {
		return fmt.Errorf("controlplane: clerk user id is required")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+ResearchPath, nil)
	if err != nil {
		return err
	}
	c.addAuth(httpReq, "", clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return parseError(resp.StatusCode, raw)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// ChatTokenPath is where the control plane renews the chat access token of
// a reply the harness is still writing.
// ChatTokenPath 是 control plane 续期 harness 仍在写入的回复的 chat 访问 token 的端点。
const ChatTokenPath = "/api/sync/chat-token"

// maxChatTokenResponseBytes bounds the control plane's answer to a renewal.
// maxChatTokenResponseBytes 限制 control plane 对续期请求的应答大小。
const maxChatTokenResponseBytes = 32 << 10

// RenewChatToken trades the chat access token a user's run holds, expired
// or not, for a fresh one, so the run outlives the token it started with
// whether or not the user's client is there to mint another. The control
// plane refuses it once the user's subscription ends or their account is
// suspended, and not for their hourly cap, which a reply already started
// may finish past.
// RenewChatToken 用用户 run 所持有的 chat 访问 token（无论是否过期）换取一个新 token，这样即使用户的
// 客户端不在场签发新 token，该 run 也能活得比它启动时的 token 更久。一旦用户订阅结束或账户被暂停，
// control plane 会拒绝它；但不会因每小时的限额而拒绝，因为已开始的回复可以在超过该限额后完成。
func (c *Client) RenewChatToken(ctx context.Context, clerkUserID, held string) (string, error) {
	if clerkUserID == "" {
		return "", fmt.Errorf("controlplane: clerk user id is required")
	}
	body, err := json.Marshal(struct {
		Token string `json:"token"`
	}{held})
	if err != nil {
		return "", err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+ChatTokenPath, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set(HeaderContentType, "application/json")
	c.addAuth(httpReq, "", clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxChatTokenResponseBytes))
	if resp.StatusCode/100 != 2 {
		return "", parseError(resp.StatusCode, raw)
	}
	if err != nil {
		return "", fmt.Errorf("controlplane: read %s: %w", ChatTokenPath, err)
	}
	var renewed struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(raw, &renewed); err != nil || renewed.Key == "" {
		return "", fmt.Errorf("controlplane: %s returned no chat token", ChatTokenPath)
	}
	return renewed.Key, nil
}

// SafeguardFlagsPath lists the chats the safeguards flagged for a user.
// SafeguardFlagsPath 列出 safeguards 为用户标记的 chat。
const SafeguardFlagsPath = "/api/sync/safeguard-flags"

// SafeguardFlags are the ids of the user's chats the safeguards flagged.
// It forwards no JWT, so a turn that waited in a queue is checked as one
// that starts at once is.
// SafeguardFlags 返回 safeguards 标记的用户 chat id。它不转发 JWT，因此排队等待的 Turn 与立即开始的 Turn 会被同样地检查。
func (c *Client) SafeguardFlags(ctx context.Context, clerkUserID string) ([]string, error) {
	if clerkUserID == "" {
		return nil, fmt.Errorf("controlplane: clerk user id is required")
	}
	var listed struct {
		Flags []struct {
			ConversationID string `json:"conversation_id"`
		} `json:"flags"`
	}
	if err := c.getSyncJSON(ctx, SafeguardFlagsPath, nil, "", clerkUserID, &listed); err != nil {
		return nil, err
	}
	chats := make([]string, 0, len(listed.Flags))
	for _, flag := range listed.Flags {
		chats = append(chats, flag.ConversationID)
	}
	return chats, nil
}

// DeleteAttachmentIndex 删除某个附件的索引记录。
func (c *Client) DeleteAttachmentIndex(ctx context.Context, jwt, clerkUserID, attachmentID string) error {
	if attachmentID == "" {
		return fmt.Errorf("controlplane: attachment id is required")
	}
	endpoint := c.baseURL + "/api/sync/attachment-index/" + url.PathEscape(attachmentID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_, _ = io.Copy(io.Discard, resp.Body)
		return parseError(resp.StatusCode, raw)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// ReservePendingAttachmentWrite asks controlplane to stamp a guard
// row before the enclave writes bytes to buckets. A duplicate
// reservation for the same id is a no-op, so retries of the same
// upload don't proliferate guard rows. The companion call
// RegisterAttachmentIndex clears the row inside the same DB
// transaction; the sweeper drains anything that times out.
// ReservePendingAttachmentWrite 请求 controlplane 在 enclave 向 buckets 写入字节前打一个 guard 行。
// 对同一 id 的重复预留是无操作，因此同一上传的重试不会产生大量 guard 行。配套调用
// RegisterAttachmentIndex 会在同一 DB 事务中清除该行；sweeper 会清空任何超时的记录。
func (c *Client) ReservePendingAttachmentWrite(ctx context.Context, jwt, clerkUserID, attachmentID, chatID string) error {
	return c.postAttachmentChatID(
		ctx,
		jwt,
		clerkUserID,
		attachmentID,
		chatID,
		"/api/sync/pending-attachments/",
		"pending attachment",
	)
}

// postAttachmentChatID is the shared transport for the two
// attachment-scoped POSTs that take a `{chat_id}` body keyed by
// attachment id in the URL (attachment-index registration and the
// pending-write ledger reservation). Factored out so the two paths
// can't drift in headers, auth, or success-body handling.
// postAttachmentChatID 是两个以附件 id 为 URL 键、携带 `{chat_id}` 请求体的附件作用域 POST（附件索引
// 注册与 pending-write 台账预留）的共享传输层。抽离出来是为了让这两条路径在 header、认证或成功
// 响应体处理上不会产生偏差。
func (c *Client) postAttachmentChatID(
	ctx context.Context,
	jwt, clerkUserID, attachmentID, chatID, pathPrefix, opLabel string,
) error {
	if attachmentID == "" {
		return fmt.Errorf("controlplane: attachment id is required")
	}
	if chatID == "" {
		return fmt.Errorf("controlplane: chat id is required")
	}
	body, err := json.Marshal(struct {
		ChatID string `json:"chat_id"`
	}{ChatID: chatID})
	if err != nil {
		return fmt.Errorf("controlplane: marshal %s: %w", opLabel, err)
	}
	endpoint := c.baseURL + pathPrefix + url.PathEscape(attachmentID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.addAuth(httpReq, jwt, clerkUserID)
	httpReq.Header.Set(HeaderContentType, "application/json")
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return parseError(resp.StatusCode, raw)
	}
	// Drain the success body so the HTTP/1.1 connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// PendingAttachmentRow is one entry from a sweep response: the
// abandoned attachment id, its owner, and the chat it was meant for.
// The enclave's sweeper uses (clerk_user_id, attachment_id) to drive
// the buckets-side cleanup before logging the reconciliation.
// PendingAttachmentRow 是 sweep 响应中的一条记录：被遗弃的附件 id、其所有者以及它原本所属的 chat。
// enclave 的 sweeper 使用 (clerk_user_id, attachment_id) 来驱动 buckets 侧的清理，再记录对账结果。
type PendingAttachmentRow struct {
	AttachmentID string `json:"attachment_id"`
	ChatID       string `json:"chat_id"`
	ClerkUserID  string `json:"clerk_user_id"`
}

// SweepPendingAttachmentWrites pulls the next batch of expired guard
// rows from controlplane. The rows are deleted from the pending
// ledger by the same call, so the caller is the sole owner of the
// cleanup obligation for the returned ids. Enclave-only — the route
// is gated by the service secret on the CP side.
// SweepPendingAttachmentWrites 从 controlplane 拉取下一批过期的 guard 行。同一调用会将这些行从
// pending 台账中删除，因此调用者是所返回 id 清理义务的唯一承担者。仅限 enclave 使用——该路由在
// CP 侧由服务密钥把关。
func (c *Client) SweepPendingAttachmentWrites(ctx context.Context, limit int) ([]PendingAttachmentRow, error) {
	if limit <= 0 {
		limit = 200
	}
	endpoint := c.baseURL + "/api/sync/pending-attachments/sweep?limit=" + strconv.Itoa(limit)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	if readErr != nil {
		return nil, fmt.Errorf("controlplane: read pending attachments sweep: %w", readErr)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var out struct {
		Rows []PendingAttachmentRow `json:"rows"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode pending attachments sweep: %w", err)
	}
	return out.Rows, nil
}

// OrphanAttachmentRow pairs an orphaned v2 attachment id with its
// owning user so the reaper can address the buckets object under the
// owner's per-user tenant prefix.
// OrphanAttachmentRow 将无主的 v2 附件 id 与其所属用户配对，使 reaper 能在所有者的每用户 tenant 前缀下寻址该 buckets 对象。
type OrphanAttachmentRow struct {
	AttachmentID string `json:"attachment_id"`
	ClerkUserID  string `json:"clerk_user_id"`
}

// SearchIndexDeletion 是一条已退役的搜索索引对象记录。
type SearchIndexDeletion struct {
	ID          string `json:"id"`
	ClerkUserID string `json:"clerk_user_id"`
	ObjectKey   string `json:"object_key"`
}

// SearchIndexDeletionClaim 是一批已认领的搜索索引删除记录。
type SearchIndexDeletionClaim struct {
	ClaimToken string                `json:"claim_token"`
	Deletions  []SearchIndexDeletion `json:"deletions"`
}

// 搜索索引删除记录的 claim 与 ack 端点路径。
const (
	ClaimSearchIndexDeletionsPath = "/api/sync/search-index-deletions/claim"
	AckSearchIndexDeletionsPath   = "/api/sync/search-index-deletions/ack"
)

// ClaimSearchIndexDeletions 认领一批待删除的退役搜索索引对象。
func (c *Client) ClaimSearchIndexDeletions(ctx context.Context, batchSize int) (*SearchIndexDeletionClaim, error) {
	body, err := json.Marshal(struct {
		BatchSize int `json:"batch_size"`
	}{BatchSize: batchSize})
	if err != nil {
		return nil, err
	}
	endpoint := c.baseURL + ClaimSearchIndexDeletionsPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set(HeaderContentType, "application/json")
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, raw)
	}
	if readErr != nil {
		return nil, fmt.Errorf("controlplane: read search index deletion claim: %w", readErr)
	}
	var out SearchIndexDeletionClaim
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode search index deletion claim: %w", err)
	}
	return &out, nil
}

// AckSearchIndexDeletions 确认删除一批已认领的搜索索引对象。
func (c *Client) AckSearchIndexDeletions(ctx context.Context, claimToken string, ids []string) error {
	body, err := json.Marshal(struct {
		ClaimToken string   `json:"claim_token"`
		IDs        []string `json:"ids"`
	}{
		ClaimToken: claimToken,
		IDs:        ids,
	})
	if err != nil {
		return err
	}
	endpoint := c.baseURL + AckSearchIndexDeletionsPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set(HeaderContentType, "application/json")
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return parseError(resp.StatusCode, raw)
	}
	if readErr != nil {
		return fmt.Errorf("controlplane: read search index deletion ack: %w", readErr)
	}
	return nil
}

// AttachmentPurge 是一条待清除的附件记录。
type AttachmentPurge struct {
	AttachmentID string `json:"attachment_id"`
	ClerkUserID  string `json:"clerk_user_id"`
	ChatID       string `json:"chat_id"`
}

// AttachmentPurgeClaim 是一批已认领的附件清除记录。
type AttachmentPurgeClaim struct {
	ClaimToken string            `json:"claim_token"`
	Purges     []AttachmentPurge `json:"purges"`
}

// postJSON is the shared shape of the small control-channel calls the
// background workers make: JSON in, JSON (or nothing) out, controlplane
// error envelope on 4xx/5xx.
// postJSON 是后台 worker 发起的小型控制通道调用的通用形态：JSON 进、JSON（或空）出，4xx/5xx 时
// 使用 controlplane 错误信封。
func (c *Client) postJSON(ctx context.Context, path string, in any, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set(HeaderContentType, "application/json")
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return parseError(resp.StatusCode, raw)
	}
	if readErr != nil {
		return fmt.Errorf("controlplane: read %s: %w", path, readErr)
	}
	if out == nil || len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("controlplane: decode %s: %w", path, err)
	}
	return nil
}

// PurgeUnreferencedAttachments asks the controlplane to move index rows
// that have been unreferenced past its grace window into the purge
// queue. Returns how many were queued this call.
// PurgeUnreferencedAttachments 请求 controlplane 将超出宽限期且不再被引用的索引行移入清除队列。返回本次调用入队的数量。
func (c *Client) PurgeUnreferencedAttachments(ctx context.Context, batchSize int) (int, error) {
	var out struct {
		Queued int `json:"queued"`
	}
	err := c.postJSON(ctx, "/api/sync/attachment-purges/purge", struct {
		BatchSize int `json:"batch_size"`
	}{BatchSize: batchSize}, &out)
	return out.Queued, err
}

// ClaimAttachmentPurges 认领一批待清除的附件。
func (c *Client) ClaimAttachmentPurges(ctx context.Context, batchSize int) (*AttachmentPurgeClaim, error) {
	var out AttachmentPurgeClaim
	err := c.postJSON(ctx, "/api/sync/attachment-purges/claim", struct {
		BatchSize int `json:"batch_size"`
	}{BatchSize: batchSize}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// AckAttachmentPurges 确认清除一批已认领的附件。
func (c *Client) AckAttachmentPurges(ctx context.Context, claimToken string, attachmentIDs []string) error {
	return c.postJSON(ctx, "/api/sync/attachment-purges/ack", struct {
		ClaimToken    string   `json:"claim_token"`
		AttachmentIDs []string `json:"attachment_ids"`
	}{ClaimToken: claimToken, AttachmentIDs: attachmentIDs}, nil)
}

// DeleteOrphanedV2Attachments 删除一批无主的 v2 附件索引条目。
func (c *Client) DeleteOrphanedV2Attachments(ctx context.Context, limit int) ([]OrphanAttachmentRow, error) {
	if limit <= 0 {
		limit = 500
	}
	endpoint := c.baseURL + "/api/sync/orphan-attachments?limit=" + strconv.Itoa(limit)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return nil, parseError(resp.StatusCode, body)
	}
	// A read error on a 2xx response must not silently turn into the
	// empty-body "no orphans" path below — a connection that died
	// mid-stream would otherwise look identical to a real 204 and we
	// would skip whatever ids the controlplane was about to send.
	// 2xx 响应上的读取错误绝不能静默落入下面空响应体“无孤儿”的路径——否则中途断开的连接会与真正的
	// 204 看起来完全一样，从而跳过 controlplane 即将发送的任何 id。
	if readErr != nil {
		return nil, fmt.Errorf("controlplane: read orphan attachments: %w", readErr)
	}
	// 204 No Content and any other empty 2xx response means
	// "no orphans found"; treat it as a successful zero-row sweep
	// instead of a decode error.
	// 204 No Content 以及其他空的 2xx 响应都表示“未发现孤儿”；将其视为一次成功的零行清理，而不是解码错误。
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var out struct {
		Attachments []OrphanAttachmentRow `json:"attachments"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("controlplane: decode orphan attachments: %w", err)
	}
	return out.Attachments, nil
}

// ErrAttachmentNotFound is returned by ResolveAttachmentOwner when the
// controlplane has no index row for the requested attachment id.
// ErrAttachmentNotFound 由 ResolveAttachmentOwner 返回，表示 controlplane 没有所请求附件 id 的索引行。
var ErrAttachmentNotFound = errors.New("controlplane: attachment not found")

// ResolveAttachmentOwner returns the clerk user id that owns the given
// v2 attachment id. The read paths (/v1/attachment/get and the
// unauthenticated /v1/attachment/get-public) use it to derive the
// buckets tenant prefix from a trusted source instead of the caller.
// Enclave-only: authenticated with the service secret. A 404 surfaces
// as ErrAttachmentNotFound so the caller can return a clean 404.
// ResolveAttachmentOwner 返回拥有给定 v2 附件 id 的 clerk 用户 id。读取路径（/v1/attachment/get 和
// 未认证的 /v1/attachment/get-public）用它从可信来源而非调用者推导 buckets 的 tenant 前缀。
// 仅限 enclave 使用：通过服务密钥认证。404 会表现为 ErrAttachmentNotFound，使调用者能返回干净的 404。
func (c *Client) ResolveAttachmentOwner(ctx context.Context, attachmentID string) (string, error) {
	if attachmentID == "" {
		return "", errors.New("controlplane: attachment id is required")
	}
	endpoint := c.baseURL + "/api/sync/attachment-owner/" + url.PathEscape(attachmentID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.doRequest(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrAttachmentNotFound
	}
	if resp.StatusCode >= 400 {
		return "", parseError(resp.StatusCode, body)
	}
	if readErr != nil {
		return "", fmt.Errorf("controlplane: read attachment owner: %w", readErr)
	}
	var out struct {
		ClerkUserID string `json:"clerk_user_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("controlplane: decode attachment owner: %w", err)
	}
	if out.ClerkUserID == "" {
		return "", ErrAttachmentNotFound
	}
	return out.ClerkUserID, nil
}
