// Package buckets is the harness's client for the colocated
// tinfoil-buckets-sidecar, an S3-compatible service that runs on
// enclave loopback. Each user's attachments live under their own
// tenant namespace (`user-<clerkId>/<attId>`):
//
//  1. The access token is the attachment's enclave-minted id,
//     globally non-colliding and unguessable to any party that does
//     not already see the controlplane's `chat_attachments.id`
//     column. It is the S3 object key under the owner's tenant prefix.
//  2. The bytes stored at the slot are sealed by the sidecar under a
//     per-attachment AES-256-GCM key the enclave mints fresh on
//     upload and supplies per request via the X-Tinfoil-Encryption-Key
//     header. The enclave never persists that key itself: it returns
//     the key to the webapp on PUT so the webapp embeds it in the
//     chat JSON (in `attachments[i].encryptionKey`), exactly like the
//     legacy v0/v1 attachment scheme. The chat JSON is sealed under
//     the user's CEK, so the per-attachment keys inherit
//     confidentiality from the chat envelope.
//
// The per-user tenant prefix is not a confidentiality boundary — the
// per-attachment key is — but a storage namespace that scopes every
// read and delete to the owning user, so a forged or mismatched
// attachment id can only ever address the caller's own objects.
//
// The sidecar runs in multitenant mode, which requires every request
// to carry both X-Tinfoil-Tenant-Id and a base64 32-byte
// X-Tinfoil-Encryption-Key. Single deletes carry a throwaway key
// because the resolver mandates the header on every request but never
// uses it to decrypt on that path.
// 包 buckets 是 harness 对并置的 tinfoil-buckets-sidecar 的客户端，sidecar 是一个运行在 enclave
// loopback 上的 S3 兼容服务。每个用户的附件都存放在各自的 tenant 命名空间（`user-<clerkId>/<attId>`）下：
//
//  1. access token 是附件在 enclave 中铸造的 id，全局不冲突，且对于任何尚未见过 controlplane
//     `chat_attachments.id` 列的第三方都不可猜测。它是所有者 tenant 前缀下的 S3 object key。
//  2. 该槽位中存储的字节由 sidecar 用一个按附件的 AES-256-GCM key 密封，该 key 由 enclave 在
//     上传时新铸造，并通过每次请求的 X-Tinfoil-Encryption-Key header 提供。enclave 自身从不持久化
//     该 key：它在 PUT 时把 key 返回给 webapp，由 webapp 将其嵌入 chat JSON（位于
//     `attachments[i].encryptionKey`），与旧版 v0/v1 附件方案完全一致。chat JSON 由用户的 CEK 密封，
//     因此按附件的 key 从 chat 信封继承机密性。
//
// 按用户的 tenant 前缀不是机密性边界——按附件的 key 才是——它只是一个存储命名空间，把每次读取和
// 删除都限定到所属用户，这样伪造或不匹配的附件 id 最多只能访问调用者自己的对象。
//
// sidecar 以多租户模式运行，要求每个请求都携带 X-Tinfoil-Tenant-Id 和 base64 编码的 32 字节
// X-Tinfoil-Encryption-Key。单个删除会携带一个一次性的 key，因为 resolver 要求每个请求都有该
// header，但在删除路径上从不使用它来解密。
package buckets

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// ErrNotFound is returned when the requested access token is not
// present in buckets. Callers map this to a 404.
// ErrNotFound 在请求的 access token 不存在于 buckets 中时返回。调用者将其映射为 404。
var ErrNotFound = errors.New("buckets: item not found")

// ErrForbidden is returned when the sidecar cannot decrypt the object
// with the supplied key (S3 error code DecryptionFailed).
// ErrForbidden 在 sidecar 无法用提供的 key 解密对象（S3 错误码 DecryptionFailed）时返回。
var ErrForbidden = errors.New("buckets: forbidden")

// ErrTooLarge is returned when a bounded read exceeds the caller's
// maximum accepted response size.
// ErrTooLarge 在有界读取超过调用者接受的最大响应体大小时返回。
var ErrTooLarge = errors.New("buckets: item too large")

// HTTPError preserves an unexpected sidecar status without retaining its
// response body, which may contain private object or request details.
// HTTPError 保留意外的 sidecar 状态码，而不保留其响应体（响应体可能包含私有的对象或请求细节）。
type HTTPError struct {
	StatusCode int
}

// Error 返回包含状态码的错误描述。
func (e *HTTPError) Error() string {
	return fmt.Sprintf("buckets: status %d", e.StatusCode)
}

const (
	defaultRequestTimeout = 5 * time.Minute
	maxErrorBodyBytes     = 8192

	// encryptionKeySize is the AES-256 key length the sidecar's
	// multitenant resolver requires; it rejects any key that does not
	// decode to exactly 32 bytes with a 400.
	// encryptionKeySize 是 sidecar 多租户 resolver 要求的 AES-256 key 长度；任何不能恰好解码为 32 字节的 key 都会被以 400 拒绝。
	encryptionKeySize = 32

	// tenantPrefix namespaces every object under the owning user
	// (`user-<clerkId>/<accessToken>`). The combined tenant id must
	// match the sidecar's [A-Za-z0-9_-]{1,64} rule.
	// tenantPrefix 将每个对象划分到所属用户的命名空间（`user-<clerkId>/<accessToken>`）。合并后的 tenant id 必须符合 sidecar 的 [A-Za-z0-9_-]{1,64} 规则。
	tenantPrefix = "user-"

	headerTenantID      = "X-Tinfoil-Tenant-Id"
	headerEncryptionKey = "X-Tinfoil-Encryption-Key"

	// s3CodeDecryptionFailed is the error code the sidecar returns
	// (with HTTP 400) when the supplied key cannot decrypt the
	// requested object.
	// s3CodeDecryptionFailed 是 sidecar 在提供的 key 无法解密所请求对象时返回的错误码（伴随 HTTP 400）。
	s3CodeDecryptionFailed = "DecryptionFailed"
)

// tenantIDPattern mirrors the sidecar's MultiTenantResolver rule. A
// derived tenant that fails this would be rejected with a 400, so the
// client validates locally and fails loudly instead.
// tenantIDPattern 复刻 sidecar 的 MultiTenantResolver 规则。不符合此规则的派生 tenant 会被以 400 拒绝，因此客户端在本地校验并显式报错。
var tenantIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// bucketNamePattern is the modern S3 bucket naming character rule
// (lowercase letters, digits, dots, hyphens; 3-63 chars; starts and
// ends alphanumeric). The bucket becomes a raw path segment of every
// sidecar URL, so anything else (slashes, percent signs, spaces)
// could silently reroute requests instead of failing.
// bucketNamePattern 是现代 S3 bucket 命名字符规则（小写字母、数字、点、连字符；3-63 个字符；以字母数字开头和结尾）。bucket 会成为每个 sidecar URL 的原始路径段，因此其他任何字符（斜杠、百分号、空格）都可能静默地把请求改道，而不是直接失败。
var bucketNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// ipv4Pattern matches dotted-quad names, which S3 reserves: a bucket
// must not be formatted as an IP address.
// ipv4Pattern 匹配点分四段式的名称，S3 保留这些名称：bucket 不得格式化为 IP 地址。
var ipv4Pattern = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)

// validBucketName applies the S3 naming rules a bucket this client
// can legitimately target must satisfy, so a config typo fails fast
// via Configured instead of reaching the sidecar.
// validBucketName 应用本客户端可合法访问的 bucket 必须满足的 S3 命名规则，这样配置拼写错误就能通过 Configured 快速失败，而不必到达 sidecar。
func validBucketName(s string) bool {
	return bucketNamePattern.MatchString(s) &&
		!strings.Contains(s, "..") &&
		!ipv4Pattern.MatchString(s)
}

// deleteKeyPlaceholder satisfies the sidecar's mandatory
// X-Tinfoil-Encryption-Key header on the delete path, which never
// decrypts. The resolver rejects any request whose key does not decode
// to 32 bytes; a fixed all-zero key keeps the id-only delete working
// without a real per-attachment key.
// deleteKeyPlaceholder 满足 sidecar 在删除路径上强制要求的 X-Tinfoil-Encryption-Key header（删除路径从不解密）。resolver 会拒绝任何 key 无法解码为 32 字节的请求；固定的全零 key 让仅凭 id 的删除无需真实的按附件 key 即可工作。
var deleteKeyPlaceholder = make([]byte, encryptionKeySize)

// Client talks to the colocated buckets sidecar over its
// S3-compatible API. No authentication is required: the sidecar runs
// on enclave loopback and authorizes by the per-request tenant id and
// encryption key headers.
// Client 通过 S3 兼容 API 与并置的 buckets sidecar 通信。无需认证：sidecar 运行在 enclave loopback 上，并通过每个请求的 tenant id 和 encryption key header 进行授权。
type Client struct {
	baseURL    string
	bucket     string
	httpClient *http.Client
}

// NewClient builds a sidecar client. bucket is the S3 bucket the
// sidecar routes to: it honors whatever bucket the request path names
// (path-style `/{bucket}/{key}`), with IAM on the sidecar's
// credentials as the enforcement point for reachability. The value
// comes from deploy config, never user input, and must be a valid S3
// bucket name; Configured rejects anything else so requests are never
// built from a malformed value.
// NewClient 构建一个 sidecar 客户端。bucket 是 sidecar 路由到的 S3 bucket：它遵循请求路径所指定的 bucket（path-style `/{bucket}/{key}`），并以 sidecar 凭据上的 IAM 作为可达性的强制点。该值来自部署配置而非用户输入，且必须是合法的 S3 bucket 名；Configured 会拒绝其他任何值，因此绝不会用格式错误的值构建请求。
func NewClient(baseURL, bucket string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		bucket:     bucket,
		httpClient: httpClient,
	}
}

// Configured reports whether the client has a sidecar URL and a
// valid target bucket. Callers use this to fail fast with a clean 503
// when the service isn't wired up (local dev, smoke tests) or when a
// deploy-config typo would otherwise be interpolated into request
// URLs as a malformed path segment.
// Configured 报告客户端是否具有 sidecar URL 和合法的目标 bucket。调用者用它来在服务未接通（本地开发、冒烟测试）或部署配置拼写错误（否则会被插入到请求 URL 中成为格式错误的路径段）时，干净利落地以 503 快速失败。
func (c *Client) Configured() bool {
	return c != nil && c.baseURL != "" && validBucketName(c.bucket)
}

// Put stores plaintext for owner at the given access token, encrypted
// by the sidecar under the supplied 32-byte key. The body is a single
// path-style S3 PutObject; the sidecar requires Content-Length, which
// the request carries explicitly.
// Put 为 owner 在给定的 access token 处存储明文，由 sidecar 用提供的 32 字节 key 加密。请求体是一次 path-style S3 PutObject；sidecar 要求 Content-Length，请求会显式携带。
func (c *Client) Put(ctx context.Context, owner, accessToken string, plaintext, key []byte) error {
	if !c.Configured() {
		return errors.New("buckets: client not configured")
	}
	if len(key) != encryptionKeySize {
		return fmt.Errorf("buckets: encryption key must be %d bytes, got %d", encryptionKeySize, len(key))
	}
	tenant, err := tenantForUser(owner)
	if err != nil {
		return err
	}
	target, err := c.objectURL(accessToken)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(plaintext))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(plaintext))
	req.Header.Set("Content-Type", "application/octet-stream")
	c.setTenantHeaders(req, tenant, key)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("buckets: put request: %w", err)
	}
	defer resp.Body.Close()
	return c.expectOK(resp)
}

// Get fetches and returns the plaintext owner stored at the given
// access token. The supplied key must be the one the object was sealed
// under at PUT.
// Get 获取并返回 owner 存储在给定 access token 处的明文。提供的 key 必须是该对象在 PUT 时被密封所用的 key。
func (c *Client) Get(ctx context.Context, owner, accessToken string, key []byte) ([]byte, error) {
	return c.GetLimited(ctx, owner, accessToken, key, 0)
}

// GetLimited fetches plaintext like Get, but rejects successful
// responses whose body exceeds maxBytes. A non-positive maxBytes means
// unbounded, matching Get.
// GetLimited 与 Get 一样获取明文，但会拒绝响应体超过 maxBytes 的成功响应。非正的 maxBytes 表示不设上限，与 Get 一致。
func (c *Client) GetLimited(ctx context.Context, owner, accessToken string, key []byte, maxBytes int64) ([]byte, error) {
	if !c.Configured() {
		return nil, errors.New("buckets: client not configured")
	}
	if len(key) != encryptionKeySize {
		return nil, fmt.Errorf("buckets: encryption key must be %d bytes, got %d", encryptionKeySize, len(key))
	}
	tenant, err := tenantForUser(owner)
	if err != nil {
		return nil, err
	}
	target, err := c.objectURL(accessToken)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	c.setTenantHeaders(req, tenant, key)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("buckets: get request: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var reader io.Reader = resp.Body
		if maxBytes > 0 {
			reader = io.LimitReader(resp.Body, maxBytes+1)
		}
		plaintext, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("buckets: read get: %w", err)
		}
		if maxBytes > 0 && int64(len(plaintext)) > maxBytes {
			return nil, fmt.Errorf("buckets: get body exceeds %d bytes: %w", maxBytes, ErrTooLarge)
		}
		return plaintext, nil
	case http.StatusNotFound:
		// Drain the body so the HTTP/1.1 connection can be reused;
		// otherwise repeated cache misses keep stacking new TCP
		// connections.
		// 排空响应体，以便复用 HTTP/1.1 连接；否则反复的缓存未命中会不断堆叠新的 TCP 连接。
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, ErrNotFound
	case http.StatusBadRequest:
		// The only 400 a well-formed GET can trigger is a key/object
		// mismatch (DecryptionFailed); the headers we send are always
		// valid. Confirm the S3 error code before mapping so genuine
		// InvalidArgument bugs aren't silently treated as forbidden.
		// 格式良好的 GET 唯一可能触发的 400 是 key/object 不匹配（DecryptionFailed）；我们发送的 header 始终合法。在映射之前先确认 S3 错误码，以免把真正的 InvalidArgument 错误静默当作 forbidden 处理。
		code := s3ErrorCode(resp.Body)
		if code == s3CodeDecryptionFailed {
			return nil, ErrForbidden
		}
		return nil, &HTTPError{StatusCode: resp.StatusCode}
	default:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
		return nil, &HTTPError{StatusCode: resp.StatusCode}
	}
}

// Delete removes owner's item at the given access token. A 404 from
// the sidecar is treated as success: callers want an idempotent delete
// and the item being already-gone is the desired terminal state.
// Delete 删除 owner 在给定 access token 处的条目。sidecar 返回的 404 会被视为成功：调用者需要幂等的删除，而条目已不存在正是期望的最终状态。
func (c *Client) Delete(ctx context.Context, owner, accessToken string) error {
	if !c.Configured() {
		return errors.New("buckets: client not configured")
	}
	tenant, err := tenantForUser(owner)
	if err != nil {
		return err
	}
	target, err := c.objectURL(accessToken)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
	if err != nil {
		return err
	}
	c.setTenantHeaders(req, tenant, deleteKeyPlaceholder)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("buckets: delete request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return c.expectOK(resp)
}

// accessTokenPattern is what an access token may be: one path segment of
// the characters the harness mints its ids and object keys from.
// accessTokenPattern 定义 access token 允许的形式：由 harness 铸造其 id 与 object key 所用字符组成的一个路径段。
var accessTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)

func (c *Client) objectURL(accessToken string) (string, error) {
	if !accessTokenPattern.MatchString(accessToken) || accessToken == "." || accessToken == ".." {
		return "", fmt.Errorf("buckets: invalid access token %q", accessToken)
	}
	return c.baseURL + "/" + c.bucket + "/" + accessToken, nil
}

func (c *Client) setTenantHeaders(req *http.Request, tenant string, key []byte) {
	req.Header.Set(headerTenantID, tenant)
	req.Header.Set(headerEncryptionKey, base64.StdEncoding.EncodeToString(key))
}

func (c *Client) expectOK(resp *http.Response) error {
	if resp.StatusCode/100 == 2 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
	return &HTTPError{StatusCode: resp.StatusCode}
}

// TenantForUser exposes the per-user tenant derivation so test
// harnesses can address objects under the same prefix the client
// uses without duplicating the prefix rule.
// TenantForUser 暴露按用户的 tenant 派生逻辑，这样测试 harness 就能用客户端所使用的同一前缀来访问对象，而无需重复前缀规则。
func TenantForUser(owner string) (string, error) {
	return tenantForUser(owner)
}

// tenantForUser derives and validates the per-user tenant id. A
// malformed owner (empty, or one that pushes the tenant past the
// sidecar's character/length rule) fails here instead of as an opaque
// 400 from the sidecar.
// tenantForUser 派生并校验按用户的 tenant id。格式错误的 owner（为空，或使 tenant 超出 sidecar 的字符/长度规则）会在这里失败，而不是表现为 sidecar 返回的不透明 400。
func tenantForUser(owner string) (string, error) {
	if owner == "" {
		return "", errors.New("buckets: owner is required")
	}
	tenant := tenantPrefix + owner
	if !tenantIDPattern.MatchString(tenant) {
		return "", fmt.Errorf("buckets: owner %q yields invalid tenant id", owner)
	}
	return tenant, nil
}

type s3ErrorBody struct {
	Code string `xml:"Code"`
}

// s3ErrorCode reads a bounded S3 XML error body for sentinel matching.
// Malformed responses do not produce a code or expose the raw body.
// s3ErrorCode 读取有界的 S3 XML 错误体用于哨兵匹配。格式错误的响应不会产生错误码，也不会暴露原始响应体。
func s3ErrorCode(r io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(r, maxErrorBodyBytes))
	var e s3ErrorBody
	if err := xml.Unmarshal(raw, &e); err == nil && e.Code != "" {
		return e.Code
	}
	return ""
}
