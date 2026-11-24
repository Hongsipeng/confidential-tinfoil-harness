// Package store 实现 chat 访问权限的构造与续期。
package store

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
)

// ChatAccess is the chat access the calls of a run, or of a reply read
// aloud, carry. A subscriber's chat token is renewed by the control plane
// for as long as the run or the reading lasts, as the user its client's
// session proved; a free key or an API key is carried as it is.
// ChatAccess 表示一次 run 或一次朗读的 reply 所携带的 chat 访问权限。订阅者的 chat token
// 在 run 或朗读期间由 control plane 持续续期，正如其客户端 session 已证明的用户身份；
// 免费 key 或 API key 则原样携带。
func (s *Store) ChatAccess(authenticator *auth.Authenticator, p *auth.Principal, credential auth.Credential) *auth.Access {
	if !credential.FirstParty {
		return auth.NewAccess(credential, nil)
	}
	user := p.ID
	return auth.NewAccess(credential, func(ctx context.Context, held auth.Secret) (auth.Credential, error) {
		token, err := s.Controlplane.RenewChatToken(ctx, user, string(held))
		if err != nil {
			return auth.Credential{}, accessRefused(err)
		}
		fresh, err := authenticator.ChatCredential(user, auth.Secret(token))
		if err != nil {
			return auth.Credential{}, accessRefused(err)
		}
		return fresh, nil
	})
}

// accessRefused is why chat access was not renewed, which the run or the
// reading it was for ends saying. Only an account that may no longer chat is
// told to stop: an answer the control plane could not give is worth another
// try.
// accessRefused 表示 chat 访问未能续期的原因，会作为其所属 run 或朗读结束时的说明。
// 只有不再允许聊天的账户才会被告知停止：control plane 无法给出的应答值得再试一次。
func accessRefused(err error) *api.Error {
	slog.Warn("chat access was not renewed", "error", err)
	var refused *controlplane.Error
	if errors.As(err, &refused) {
		switch refused.StatusCode {
		case http.StatusPaymentRequired:
			return api.Err(403, "UPSTREAM_REFUSED", "The Tinfoil Chat subscription has ended").WithAction(api.ActionStop)
		case http.StatusForbidden:
			return api.Err(403, "UPSTREAM_REFUSED", "This account is suspended").WithAction(api.ActionStop)
		}
	}
	return api.Err(503, "UPSTREAM_REFUSED", "Chat access is temporarily unavailable")
}
