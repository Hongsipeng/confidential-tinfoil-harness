package auth

import (
	"context"
	"net/http"
	"time"

	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

type callerKeyKey struct{}

// WithCallerKey has ctx's requests signed with key, the caller's; an empty
// key leaves them unsigned, since the harness has no key of its own.
// WithCallerKey 使 ctx 中的请求以 key（调用方的 key）签名；空的 key 使请求
// 保持未签名，因为 harness 自身没有 key。
func WithCallerKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, callerKeyKey{}, key)
}

// WithCaller has ctx's requests signed with the key access holds when each
// is sent, as a run's are, which may outlast the key it started with.
// WithCaller 使 ctx 中的请求在发送时以 access 所持有的 key 签名，正如 run 的
// 请求那样，该 key 可能比其启动时的 key 更持久。
func WithCaller(ctx context.Context, access *Access) context.Context {
	return context.WithValue(ctx, callerKeyKey{}, access)
}

// CallerKey is the key ctx's requests are signed with now.
// CallerKey 是当前 ctx 中的请求所用来签名的 key。
func CallerKey(ctx context.Context) string {
	switch caller := ctx.Value(callerKeyKey{}).(type) {
	case string:
		return caller
	case *Access:
		return string(caller.Current())
	}
	return ""
}

type usageKey struct{}

// WithUsage has ctx's requests report usage under the run it names.
// WithUsage 使 ctx 中的请求在它指定的 run 名下上报 usage。
func WithUsage(ctx context.Context, usage usagereporting.Context) context.Context {
	return context.WithValue(ctx, usageKey{}, usage)
}

// Usage is the usage ctx's requests report, if any.
// Usage 是 ctx 中的请求所上报的 usage（如果有的话）。
func Usage(ctx context.Context) (usagereporting.Context, bool) {
	usage, ok := ctx.Value(usageKey{}).(usagereporting.Context)
	return usage, ok
}

// CallerTransport signs every request with the caller's key and usage. A
// run's request carries the key its access holds as the request goes out,
// and one refused as unauthenticated is sent once more with the next key: a
// 401 rejects a request before it is processed, so the same body is sent
// again under the same usage.
// CallerTransport 用调用方的 key 和 usage 为每个请求签名。run 的请求在发出时
// 携带其 access 所持有的 key；被以未认证拒绝的请求会用下一个 key 再发送一次：
// 401 会在请求被处理之前将其拒绝，因此同样的 body 会在相同的 usage 下再次发送。
type CallerTransport struct {
	Inner       http.RoundTripper
	UsageSecret string
}

// RoundTrip 使用调用方的 key 与 usage 签名并发送请求；对于因未认证被拒绝的
// 请求，会用下一个 key 重发一次。
func (t *CallerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	access, _ := r.Context().Value(callerKeyKey{}).(*Access)
	if access == nil {
		return t.send(r, CallerKey(r.Context()))
	}
	key, err := access.Key(r.Context())
	if err != nil {
		if r.Body != nil {
			r.Body.Close()
		}
		return nil, err
	}
	resp, err := t.send(r, string(key))
	if err != nil || resp.StatusCode != http.StatusUnauthorized || r.Body != nil && r.GetBody == nil {
		return resp, err
	}
	next, err := access.Refused(r.Context(), key)
	if err == nil && next == key {
		return resp, nil
	}
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	again := r.Clone(r.Context())
	if r.GetBody != nil {
		if again.Body, err = r.GetBody(); err != nil {
			return nil, err
		}
	}
	resp, err = t.send(again, string(next))
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	// A key refused as it was issued is not the caller's to replace by
	// signing in again.
	// 一个在签发时就被拒绝的 key 不应由调用方通过重新登录来替换。
	if _, err := access.Refused(r.Context(), next); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// send signs r with key, and with the usage of its context as key's.
// send 用 key 为 r 签名，并将其 context 中的 usage 作为该 key 的 usage。
func (t *CallerTransport) send(r *http.Request, key string) (*http.Response, error) {
	r = r.Clone(r.Context())
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if usage, ok := Usage(r.Context()); ok {
		usage.IssuedAt = time.Now().UTC()
		if key != "" {
			usage.APIKeyHash = usagereporting.HashAPIKey(key)
		}
		if err := usagereporting.SetHeaders(r.Header, usage, t.UsageSecret); err != nil {
			return nil, err
		}
	}
	return t.Inner.RoundTrip(r)
}
