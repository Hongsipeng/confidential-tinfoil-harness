package auth

import (
	"context"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

const (
	// renewAhead is how long before a chat token expires a run asks for the
	// next one, so a control plane that is slow to answer costs it nothing.
	// renewAhead 是 run 在 chat token 到期前多久就申请下一个 token 的时长，
	// 这样即使 control plane 应答缓慢，也不会让 run 付出任何代价。
	renewAhead = 5 * time.Minute
	// renewPause is how long a run waits before it asks for a chat token
	// again, whether the last answer was a token or a refusal.
	// renewPause 是 run 再次申请 chat token 之前等待的时长，无论上一次的应答
	// 是 token 还是拒绝。
	renewPause = 30 * time.Second
	// renewTimeout bounds one request for a chat token.
	// renewTimeout 限制单次 chat token 申请的时长。
	renewTimeout = 15 * time.Second
)

// errAccessRefused ends a run whose chat token a model or tool refused
// moments after the control plane issued it.
// errAccessRefused 结束那些 chat token 在 control plane 签发后不久即被
// model 或 tool 拒绝的 run。
var errAccessRefused = api.Err(503, "UPSTREAM_REFUSED", "Chat access was refused; try again shortly")

// Access is the chat access a run's calls carry for as long as it runs. A
// subscriber's chat token lasts minutes and a run may last longer, so the
// harness asks the control plane for the next one itself, before the one it
// holds expires: the run does not depend on its caller's client still being
// there to mint one. A free key or an API key is not renewed.
// Access 是 run 在运行期间其调用所携带的 chat 访问凭据。subscriber 的
// chat token 只持续几分钟，而 run 可能运行更久，因此 harness 会在所持有的
// token 到期之前自行向 control plane 申请下一个，这样 run 就不依赖其调用方的
// client 仍然在线来签发 token。free key 或 API key 不会被续期。
type Access struct {
	// renew trades the token held for a fresh one; nil for a key that is
	// not renewed.
	// renew 用当前持有的 token 换取一个新的 token；对于不会被续期的 key，
	// 其值为 nil。
	renew func(ctx context.Context, held Secret) (Credential, error)

	mu         sync.Mutex
	credential Credential
	// asking is closed when the request for a token now in flight ends.
	// asking 在当前进行中的 token 申请结束时被关闭。
	asking chan struct{}
	// refusal is why the last request for a token failed, and askAgain when
	// the next one may be made.
	// refusal 是上一次 token 申请失败的原因；askAgain 是下一次申请可以发起的
	// 时间。
	refusal  error
	askAgain time.Time
}

// NewAccess is the access a run starts with, which renew keeps current.
// NewAccess 是 run 启动时持有的 access，由 renew 保持其最新。
func NewAccess(credential Credential, renew func(ctx context.Context, held Secret) (Credential, error)) *Access {
	return &Access{credential: credential, renew: renew}
}

// Current is the key held now, without asking for another.
// Current 返回当前持有的 key，而不会再去申请新的。
func (a *Access) Current() Secret {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.credential.Key
}

// Key is the key a request sent now carries. A token within renewAhead of
// its expiry is renewed, and is still the one sent while it lasts. Once it
// has expired the request waits for the next one, and fails with the control
// plane's refusal instead of going out with a token that would be refused.
// Key 是当前发出的请求所携带的 key。距离到期不足 renewAhead 的 token 会被
// 续期，且在仍然有效的期间发送的仍是这一个。一旦过期，请求会等待下一个 token，
// 并以 control plane 的拒绝而失败，而不是带着一个会被拒绝的 token 发出。
func (a *Access) Key(ctx context.Context) (Secret, error) {
	for {
		a.mu.Lock()
		held, now := a.credential, api.Now()
		if a.renew == nil || held.Expires.Sub(now) > renewAhead {
			a.mu.Unlock()
			return held.Key, nil
		}
		if a.asking == nil && !now.Before(a.askAgain) {
			a.asking = make(chan struct{})
			go a.ask(ctx, held, a.asking)
		}
		asking, refusal := a.asking, a.refusal
		a.mu.Unlock()
		switch {
		case held.Expires.After(now):
			return held.Key, nil
		case asking != nil:
			select {
			case <-asking:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		case refusal != nil:
			return "", refusal
		default:
			return "", errAccessRefused
		}
	}
}

// ask requests the token after held, for every caller waiting on asking.
// It outlives the request that happened to need it first.
// ask 为所有等待 asking 的调用方申请 held 之后的下一个 token。它比恰好最先
// 需要它的那个请求存活得更久。
func (a *Access) ask(ctx context.Context, held Credential, asking chan struct{}) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), renewTimeout)
	defer cancel()
	fresh, err := a.renew(ctx, held.Key)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.asking, a.refusal, a.askAgain = nil, err, api.Now().Add(renewPause)
	if err == nil {
		// The quota the run was admitted with stays its own.
		// run 被准入时获得的 quota 保持其自身不变。
		fresh.Limit = held.Limit
		a.credential = fresh
	}
	close(asking)
}

// Refused is the key to send again after a model or tool refused key: the
// one held now if it is another, or else the next one, asked for at once. A
// key that is not renewed is returned as it is.
// Refused 返回在 model 或 tool 拒绝了 key 之后应重新发送的 key：如果当前持有
// 的已不是这个 key 就用当前这个，否则立即申请下一个。不会被续期的 key 则原样
// 返回。
func (a *Access) Refused(ctx context.Context, key Secret) (Secret, error) {
	a.mu.Lock()
	if a.renew != nil && a.credential.Key == key {
		a.credential.Expires = time.Time{}
	}
	a.mu.Unlock()
	return a.Key(ctx)
}
