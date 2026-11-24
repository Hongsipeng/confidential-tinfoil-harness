package auth

import (
	"context"
	"log/slog"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// FlagsTimeout bounds the flag lookup that every chat read and turn makes.
// FlagsTimeout 限制每次 chat 读取和 Turn 所进行的标记查询的时长。
var FlagsTimeout = 2 * time.Second

// A caller's flags are reused for flagsTTL, so the control plane does not
// learn of every chat read and turn, for up to maxFlagCallers callers. A chat
// flagged in that time may take one more turn.
// 调用方的 flags 在 flagsTTL 时间内被复用，因此 control plane 不会得知每一次
// chat 读取和 Turn，最多服务 maxFlagCallers 个调用方。在此期间被标记的 chat
// 还可能再执行一个 Turn。
const (
	flagsTTL       = time.Minute
	maxFlagCallers = 10000
)

// ErrChatFlagged 表示该 chat 因违反可接受使用政策被标记，且不再接受任何 Turn。
var ErrChatFlagged = api.Err(403, "CHAT_FLAGGED", "This chat was flagged under the acceptable use policy and takes no more turns")

// ChatFlags are the ids of the chats the safeguards flagged.
// ChatFlags 是 safeguards 标记的 chat 的 id 集合。
type ChatFlags map[string]bool

// flaggedChats asks the control plane which of the caller's chats the
// safeguards flagged. Only signed-in callers' chats are screened. The caller
// is named by the id their session proved, not by the session, which expires
// before a turn that waited in a queue starts. A list the control plane
// cannot give in time flags none, as the web client's polling did.
// FlaggedChats 向 control plane 查询调用方的哪些 chat 被 safeguards 标记。
// 只有已登录调用方的 chat 会被筛查。调用方以其 session 证明的 id 来标识，而不是
// 以 session 本身标识，因为 session 会在排队等待的 Turn 开始之前过期。如果
// control plane 未能及时返回列表，则视为没有任何标记，这与 web client 的轮询
// 行为一致。
func (a *Authenticator) FlaggedChats(ctx context.Context, p *Principal) ChatFlags {
	if p.Anonymous || p.ID == "" || a.readFlags == nil {
		return nil
	}
	if flags, ok := a.recentFlags().Get(p.Scope()); ok {
		return flags
	}
	ctx, cancel := context.WithTimeout(ctx, FlagsTimeout)
	defer cancel()
	chats, err := a.readFlags(ctx, p.ID)
	if err != nil {
		slog.Warn("safeguard flags unavailable", "error", err)
		return nil
	}
	flags := ChatFlags{}
	for _, chat := range chats {
		flags[chat] = true
	}
	a.recentFlags().Add(p.Scope(), flags)
	return flags
}

// recentFlags are the flags last read for each caller. Callers only read them.
// recentFlags 是每个调用方最近一次读取到的 flags。调用方仅读取它们。
func (a *Authenticator) recentFlags() *expirable.LRU[string, ChatFlags] {
	a.flagsOnce.Do(func() { a.flags = expirable.NewLRU[string, ChatFlags](maxFlagCallers, nil, flagsTTL) })
	return a.flags
}

// mark records on each thread whether the safeguards flagged it. A flag
// once stored stays.
// mark 在每个 thread 上记录该 thread 是否被 safeguards 标记。一旦写入的标记
// 将一直保留。
