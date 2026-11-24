package auth

import (
	"errors"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Caller is who a request is from, recognized before its body is read.
// Caller 识别请求来自谁，在其 body 被读取之前完成识别。
func (a *Authenticator) Caller(r *http.Request, anonymous bool) (*Principal, error) {
	p, err := a.Authenticate(r, anonymous)
	if err != nil {
		return nil, err
	}
	return p, a.Recognize(r.Context(), p)
}

// RunCaller is who a request about a run already started is from. Such a
// request admits no work and spends no quota, and reaches only runs started
// under the caller's identity. A signed-out caller is known by its free key
// alone, without asking the control plane, which refuses the key once its
// day ends: holding the key a run started under is the proof the run's own
// caller gave.
// RunCaller 识别关于某个已启动 run 的请求来自谁。这类请求不接纳任何工作、
// 不消耗 quota，且只能访问在调用方身份下启动的 run。已登出的调用方仅凭其
// free key 识别，无需询问 control plane——该 key 在当日结束时会被 control
// plane 拒绝：持有 run 启动时所使用的 key，即是该 run 自身的调用方所提供的
// 凭证。
func (a *Authenticator) RunCaller(r *http.Request) (*Principal, error) {
	return a.Authenticate(r, true)
}

// WithQuota gives a refusal for an exhausted quota the caller's quota as the
// refusal leaves it. A signed-out caller's is the one its request held.
// WithQuota 在 quota 耗尽时返回一个带有调用方剩余 quota 的拒绝响应。已登出
// 调用方的 quota 即其请求所携带的那一个。
func WithQuota(p *Principal, held api.RateLimit, err error) error {
	var e *api.Error
	if p == nil || !errors.As(err, &e) || e.Code != "QUOTA_EXHAUSTED" || e.RateLimit != nil {
		return err
	}
	refused, limit := *e, Exhausted(p, held)
	refused.RateLimit = &limit
	return &refused
}

// RequestKey is the content key a request about a run names, which a run
// of a keyless thread leaves out. A missing key is checked against the run: keyless threads accept it, and
// every other thread compares the key's fingerprint.
// RequestKey 是有关 run 的请求所指定的 content key，keyless thread 的 run
// 不会携带它。缺失的 key 会与 run 进行核对：keyless thread 接受缺失的 key，
// 而其他所有 thread 会比较 key 的指纹。
func RequestKey(key []byte) (ContentKey, error) {
	if len(key) == 0 {
		return ContentKey{}, nil
	}
	return DecodeContentKey(key)
}

// ErrAnonymousCloud refuses a signed-out caller a stored chat.
// ErrAnonymousCloud 拒绝已登出的调用方访问已存储的 chat。
var ErrAnonymousCloud = api.Err(401, "UNAUTHENTICATED", "Anonymous chats are kept on the device").WithAction(api.ActionStop)
