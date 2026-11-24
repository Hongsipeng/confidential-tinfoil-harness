package store

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/jobs"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// A reseal brings every row the control plane holds under the caller's
// current key: rows older clients sealed without a key id, and rows an
// earlier key sealed. It runs in the background, since an account may
// hold thousands of rows, and the caller asks again to see where it
// stands.
// reseal 会把 control plane 持有的每个 row 迁移到调用者的当前 key 下：
// 包括旧客户端在无 key id 情况下封存的 row，以及更早的 key 所封存的 row。
// 它在后台运行，因为一个账户可能持有成千上万个 row，调用者可以再次询问以查看进度。

const (
	// ResealBudget bounds one reseal; a caller whose rows take longer
	// sees a partial report and asks again.
	// ResealBudget 为一次 reseal 设界；row 更多、耗时更长的调用者会看到部分报告，并可再次询问。
	ResealBudget = 10 * time.Minute
	// ResealRetention is how long an ended reseal still answers for the
	// caller, so a client polling for its end sees it rather than
	// starting another.
	// ResealRetention 是已结束的 reseal 仍为调用者保留应答的时长，
	// 这样轮询其结束的客户端能看到它，而不是又发起一次新的。
	ResealRetention = 5 * time.Minute
	// resealPage is how many rows one listing from the control plane
	// names.
	// resealPage 是 control plane 一次列表所列出的 row 数量。
	resealPage = 200
	// resealAttempts bounds the reseals of a row another writer keeps
	// changing under them.
	// resealAttempts 限制对某个 row 的 reseal 次数——该 row 可能被其他写入者不断在其下改动。
	resealAttempts = 3
)

// resealScopes is every scope the control plane holds rows of, in the
// order they are resealed.
// resealScopes 是 control plane 持有 row 的所有 scope，按其被 reseal 的顺序排列。
var resealScopes = []seal.Scope{seal.ScopeProfile, seal.ScopeChat, seal.ScopeProject, seal.ScopeProjectDocument}

// ResealReport is where a reseal stands: the rows resealed so far, the rows
// still to try, and the rows no given key opens.
// ResealReport 表示一次 reseal 的进度：已 reseal 的 row、仍待尝试的 row，以及没有任何给定 key 能打开的 row。
type ResealReport struct {
	Status    jobs.State `json:"status"`
	Migrated  int        `json:"migrated"`
	Remaining int        `json:"remaining"`
	Blocked   int        `json:"blocked"`
	Partial   bool       `json:"partial"` // the budget or the caller's credential ran out first
	// 表示是预算或调用者的凭证先耗尽。
}

// Reseal seals every row an older key sealed under target, which must be
// the caller's registered current key, in the background; the report is
// where the job stands when the call returns.
// Reseal 在后台将每个由旧 key 封存的 row 重新封存到 target 下（target 必须是调用者已注册的当前 key）；
// 报告是调用返回时该作业的进度。
func (s *Store) Reseal(ctx context.Context, p *auth.Principal, keys []auth.ContentKey, target auth.ContentKey) (ResealReport, error) {
	defer clear(target[:])
	defer func() {
		for i := range keys {
			clear(keys[i][:])
		}
	}()
	if len(keys) == 0 {
		return ResealReport{}, api.Err(400, "BAD_REQUEST", "At least one key is required")
	}
	// The control plane accepts a reseal only under the registered key,
	// so any other target would be refused row by row.
	// control plane 只接受在已注册 key 下的 reseal，因此任何其他 target 都会被逐 row 拒绝。
	if err := s.CheckRegisteredKey(ctx, p, target); err != nil {
		return ResealReport{}, err
	}
	targetKey := seal.Key{Bytes: bytes.Clone(target[:]), KeyIDHex: keyID(target)}
	// The job outlives the request, so it takes copies of the keys and
	// of who the caller is.
	// 作业会比请求存续得更久，因此它会复制各 key 以及调用者身份。
	candidates := make([]seal.Key, 0, len(keys))
	for _, key := range keys {
		candidates = append(candidates, seal.Key{Bytes: bytes.Clone(key[:]), KeyIDHex: keyID(key)})
	}
	zero := func() {
		for _, candidate := range candidates {
			seal.Zero(candidate.Bytes)
		}
		seal.Zero(targetKey.Bytes)
	}
	caller := &auth.Principal{ID: p.ID, JWT: p.JWT}
	job, started := s.Reseals.Start(ctx, p.ID, func(ctx context.Context, report func(ResealReport)) (ResealReport, error) {
		defer zero()
		return s.reseal(ctx, caller, candidates, targetKey, report)
	})
	if !started {
		zero()
	}
	status := job.Status()
	status.Report.Status = status.State
	return status.Report, nil
}

// reseal walks each scope's rows left to reseal, a page at a time, until
// none remain or a page reseals nothing, and reports after each page. The
// counts of what remains and what is blocked are the control plane's,
// adjusted for the page just done.
// reseal 逐页遍历每个 scope 尚待 reseal 的 row，直到没有剩余或某一页没有 reseal 任何内容，
// 并在每页之后报告进度。剩余与受阻的数量来自 control plane，并针对刚完成的一页做了调整。
func (s *Store) reseal(ctx context.Context, p *auth.Principal, keys []seal.Key, target seal.Key, report func(ResealReport)) (ResealReport, error) {
	rows := &Rows{Controlplane: s.Controlplane, Buckets: s.Buckets}
	var out ResealReport
	for _, scope := range resealScopes {
		remaining, blocked := 0, 0
		// A walk cut short keeps what the scope's last page counted.
		// 被中途打断的遍历会保留该 scope 最后一页所统计的结果。
		cutShort := func() (ResealReport, error) {
			out.Remaining, out.Blocked, out.Partial = out.Remaining+remaining, out.Blocked+blocked, true
			return out, nil
		}
		for {
			if ctx.Err() != nil {
				return cutShort()
			}
			list, err := s.Controlplane.ListNeedsMigration(ctx, string(scope), resealPage, string(p.JWT), p.ID)
			if err != nil {
				if ctx.Err() != nil || credentialRefused(err) {
					return cutShort()
				}
				return out, FromControlplane(err)
			}
			page := s.resealPage(ctx, rows, p, scope, list.IDs, keys, target)
			out.Migrated += page.migrated
			remaining = max(0, list.RetryableRemaining-page.migrated-page.blocked)
			blocked = list.BlockedUnmigrated + page.blocked
			report(ResealReport{Migrated: out.Migrated, Remaining: out.Remaining + remaining, Blocked: out.Blocked + blocked})
			if page.stopped {
				return cutShort()
			}
			if page.migrated == 0 || remaining == 0 {
				break
			}
		}
		out.Remaining += remaining
		out.Blocked += blocked
	}
	return out, nil
}

// resealedPage is what one page of rows came to: the rows resealed, the
// rows no key opens, and whether the page was cut short.
// resealedPage 表示一页 row 的处理结果：已 reseal 的 row、没有任何 key 能打开的 row，
// 以及该页是否被中途打断。
type resealedPage struct {
	migrated, blocked int
	stopped           bool
}

// resealPage reseals each row of a page. A row no key opens, or whose
// reseal fails, is recorded as blocked, which keeps it out of the next
// pages for a while; an ended context or a refused credential stops the
// page without recording the row it was on.
// resealPage 对一页中的每个 row 执行 reseal。没有任何 key 能打开或 reseal 失败的 row 会记为受阻，
// 从而在一段时间内不再出现在后续页中；context 结束或凭证被拒绝会中止该页，
// 且不记录当时正在处理的 row。
func (s *Store) resealPage(ctx context.Context, rows *Rows, p *auth.Principal, scope seal.Scope, ids []string, keys []seal.Key, target seal.Key) resealedPage {
	var page resealedPage
	for _, id := range ids {
		if ctx.Err() != nil {
			page.stopped = true
			return page
		}
		switch s.resealOne(ctx, rows, p, scope, id, keys, target) {
		case resealed:
			page.migrated++
		case resealBlocked:
			if ctx.Err() != nil {
				page.stopped = true
				return page
			}
			page.blocked++
			_ = s.Controlplane.RecordMigrationFailure(ctx, string(scope), id, string(p.JWT), p.ID)
		case resealStopped:
			page.stopped = true
			return page
		}
	}
	return page
}

// resealOutcome is what became of one row.
// resealOutcome 表示单个 row 的最终结果。
type resealOutcome int

const (
	resealed resealOutcome = iota
	resealBlocked
	resealStopped
)

// resealOne reads a row as it is now, opens it with whichever key seals
// it, and reseals it under target at the revision it read. A row that
// changed in between is read again; one already under target is done.
// resealOne 读取一个 row 的当前内容，用封存它的某个 key 打开，并在读取到的修订版本上
// 将其 reseal 到 target 下。中途发生变化的 row 会重新读取；已在 target 下的则视为完成。
func (s *Store) resealOne(ctx context.Context, rows *Rows, p *auth.Principal, scope seal.Scope, id string, keys []seal.Key, target seal.Key) resealOutcome {
	for attempt := 0; attempt < resealAttempts; attempt++ {
		blob, err := s.Controlplane.GetBlob(ctx, string(scope), id, string(p.JWT), p.ID)
		if err != nil {
			return resealFailure(ctx, err)
		}
		if seal.Detect(blob.Ciphertext) == seal.VersionV2 && blob.KeyID == target.KeyIDHex {
			return resealed
		}
		plaintext, ok := openRow(blob.Ciphertext, keys, scope, id, p.ID)
		if !ok {
			return resealBlocked
		}
		_, err = rows.rewrapRow(ctx, p, string(scope), id, plaintext, blob.ETag, target)
		seal.Zero(plaintext)
		switch {
		case err == nil:
			return resealed
		case controlplane.IsCode(err, controlplane.StatusStaleBlob):
			continue
		default:
			return resealFailure(ctx, err)
		}
	}
	return resealBlocked
}

// resealFailure is whether a row's failed read or write blocks the row,
// or stops the job: the context ended, or the caller's credential was
// refused, which every later call would be too.
// resealFailure 判断一个 row 读取或写入失败是使其受阻，还是中止整个作业：
// 即 context 已结束，或调用者凭证被拒绝（后续每次调用同样会被拒绝）。
func resealFailure(ctx context.Context, err error) resealOutcome {
	if ctx.Err() != nil || credentialRefused(err) {
		return resealStopped
	}
	return resealBlocked
}

// openRow opens a row with whichever of keys seals it: a row with a key
// id by that key, a legacy row by trying each.
// openRow 用 keys 中封存该 row 的那个 key 打开它：带 key id 的 row 用对应 key，
// 旧格式 row 则逐个尝试。
func openRow(ciphertext []byte, keys []seal.Key, scope seal.Scope, id, owner string) ([]byte, bool) {
	var dec seal.DecryptResult
	var err error
	switch seal.Detect(ciphertext) {
	case seal.VersionV2:
		dec, err = seal.DecryptV2(ciphertext, keys, seal.AAD{Scope: scope, ID: aadID(string(scope), id), ClerkUserID: owner})
	case seal.VersionV0, seal.VersionV1:
		dec, err = seal.DecryptLegacy(ciphertext, keys)
	default:
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	return dec.Plaintext, true
}

// credentialRefused is whether the control plane refused the caller's
// credential.
// credentialRefused 表示 control plane 是否拒绝了调用者的凭证。
func credentialRefused(err error) bool {
	var cpe *controlplane.Error
	return errors.As(err, &cpe) && (cpe.StatusCode == http.StatusUnauthorized || cpe.StatusCode == http.StatusForbidden)
}
