// Package store 定义存储层的错误与拒绝应答。
package store

import (
	"errors"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

var (
	// errRunScope refuses an operation outside what a grant covers. The
	// harness only asks within its grants, so this names a bug, not a
	// condition a client can act on.
	// errRunScope 拒绝超出 grant 覆盖范围的操作。harness 只会在其 grant 范围内请求，
	// 因此这表示的是一个 bug，而非客户端能够应对的状况。
	errRunScope = errors.New("store: the operation is outside the grant's storage scope")
	// ErrRowNotFetched is a row the control plane could not serve twice
	// running. A later request may fetch it.
	// ErrRowNotFetched 表示 control plane 连续两次未能提供的 row。稍后的请求可能成功获取。
	ErrRowNotFetched = api.Err(502, "UPSTREAM_REFUSED", "The storage service could not fetch the data in time")
	// errRowUnreadable is a row the control plane holds in no format this
	// harness opens: an envelope it does not know, or one that fails its
	// authentication.
	// errRowUnreadable 表示 control plane 以本 harness 无法打开的任何格式保存的 row：
	// 一个不认识的 envelope，或一个未能通过认证的 envelope。
	errRowUnreadable = api.Err(502, "UPSTREAM_REFUSED", "The stored row cannot be opened")
)

// refusedByControlplane is whether the control plane answered the request
// with a refusal of its own, which proves it wrote nothing for it.
// refusedByControlplane 表示 control plane 是否以自己的拒绝应答了请求，
// 这证明它没有为该请求写入任何内容。
func refusedByControlplane(err error) bool {
	var cpe *controlplane.Error
	return errors.As(err, &cpe) && cpe.StatusCode/100 == 4
}

// ResearchLimit refuses a deep research its caller started another of within
// the hour, until resetsAt.
// ResearchLimit 拒绝一小时内已再次发起过的 deep research，直到 resetsAt。
func ResearchLimit(resetsAt string, retryAfter int64) *api.Error {
	e := api.Err(429, "RESEARCH_LIMIT_REACHED", "A deep research was already started within the hour")
	e.ResetsAt, e.RetryAfter = resetsAt, int(retryAfter)
	return e
}

// FromControlplane is the harness's error for what the control plane, or
// the sealing of a row, refused.
// FromControlplane 是 harness 针对 control plane 或 row 的封存所拒绝之事的错误。
func FromControlplane(err error) *api.Error {
	var known *api.Error
	if errors.As(err, &known) {
		return known
	}
	if errors.Is(err, seal.ErrPlaintextTooLarge) {
		return api.Err(413, "THREAD_TOO_LARGE", "The chat is too large to store")
	}
	var unknown *controlplane.PutBlobOutcomeUnknownError
	if errors.As(err, &unknown) {
		// The write may have been stored. Retrying reapplies the change
		// to a freshly pulled row, so retrying is safe.
		// 该写入可能已经存储。重试会在一份新拉取的 row 上重新应用改动，因此重试是安全的。
		return api.Err(503, "SYNC_OUTCOME_UNKNOWN", "The sync service may not have stored the change")
	}
	var cpe *controlplane.Error
	if !errors.As(err, &cpe) {
		return api.Err(502, "UPSTREAM_REFUSED", "A downstream request failed")
	}
	switch cpe.Code {
	case controlplane.StatusStaleKey, controlplane.StatusExistingDataUnderOtherKey, "UNKNOWN_KEY", "KEY_MISMATCH":
		e := api.Err(409, "KEY_MISMATCH", "The stored data uses another content key")
		e.KeyID = cpe.CurrentKeyID
		return e
	case controlplane.StatusStaleBlob:
		return api.Err(409, "REVISION_CONFLICT", "The stored row changed")
	case controlplane.StatusMissingAttachment:
		e := api.Err(409, "MISSING_ATTACHMENT", "The chat references attachments that are not stored with it")
		e.Missing = cpe.MissingAttachments
		return e
	case "PAYLOAD_TOO_LARGE":
		return api.Err(413, "THREAD_TOO_LARGE", "The chat is too large to store")
	case controlplane.StatusDeepResearchLimitReached:
		return ResearchLimit(cpe.ResetsAt, cpe.RetryAfterSeconds)
	}
	switch cpe.StatusCode {
	case http.StatusUnauthorized:
		return api.Err(401, "UNAUTHENTICATED", "Authentication is required")
	case http.StatusNotFound:
		return api.Err(404, "THREAD_NOT_FOUND", "The requested data was not found")
	case http.StatusPreconditionFailed:
		return api.Err(409, "REVISION_CONFLICT", "The stored row changed")
	case http.StatusTooManyRequests:
		e := api.Err(429, "QUOTA_EXHAUSTED", "The request quota is exhausted")
		e.RetryAfter = int(cpe.RetryAfterSeconds)
		return e
	}
	e := api.Err(502, "UPSTREAM_REFUSED", "A downstream service refused the request")
	e.Status = cpe.StatusCode
	api.RefusedAsSent(e, cpe.StatusCode)
	return e
}
