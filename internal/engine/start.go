package engine

import (
	"cmp"
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

// CommitTimeout bounds a finished run's writes and a queued turn's start.
// CommitTimeout 限制已结束 run 的写入以及排队 turn 的启动。
const CommitTimeout = time.Minute

// turnPlan is what a turn starts from: its thread with the turn applied, in
// the form it is stored, and the request its run makes.
// turnPlan 是 turn 的起点：已应用该 turn 的 thread（以其存储的形式），
// 以及其 run 所发出的请求。
type turnPlan struct {
	row         *domain.ThreadRow
	turn        *TurnRequest // the turn carried out: a retry is the turn it repeats
	request     *Request
	model       *inference.Model
	titleText   string
	retryTarget *retryTarget
}

// retryTarget is the widget call a retry regenerates, and the reply it is in.
// retryTarget 是 retry 重新生成的 widget 调用，以及它所在的回复。
type retryTarget struct {
	block    domain.Block
	parentID string
}

// errThreadDeleted refuses a turn on a chat deleted while the turn started.
// errThreadDeleted 拒绝在 turn 启动期间聊天被删除的 turn。
var errThreadDeleted = api.Err(404, "THREAD_NOT_FOUND", "The thread was deleted")

// prepareTurn applies a turn to its chat as read now. A stored chat's
// attachments are placed under it; the prompt reads their content. With
// create, a new chat's missing row is made.
// prepareTurn 将 turn 应用到当前读到的聊天上。已存储聊天的附件会被置于其下；
// prompt 会读取它们的内容。在 create 时，会为缺失行创建新聊天的行。
func (eng *Engine) prepareTurn(ctx context.Context, r *Run, p *auth.Principal, create bool) (*turnPlan, error) {
	s := r.Thread
	var row *domain.ThreadRow
	var profile domain.Profile
	var project *domain.Project
	var docs []domain.ProjectDocument
	// The run holds a device chat's row and an ask's. A device chat's
	// settings are the ones its client sends; an ask about a stored chat
	// reads the user's and the chat's project, as the chat does.
	// run 持有设备聊天和 ask 的行。设备聊天的设置就是其客户端发送的设置；
	// 关于已存储聊天的 ask 则读取用户的和该聊天的 project，正如该聊天本身那样。
	if s.local {
		eng.Live.mu.Lock()
		row = &domain.ThreadRow{Thread: s.row.Clone()}
		eng.Live.mu.Unlock()
	}
	if r.input.Local {
		if r.profile != nil {
			profile = *r.profile
		}
	} else {
		var err error
		profile, err = eng.Store.Profile(ctx, p, r.key)
		if err != nil {
			return nil, err
		}
		if !s.local {
			// A new chat's first turn creates its row under the client's id.
			// 新聊天的第一个 turn 会在客户端的 id 下创建其行。
			row, err = eng.Store.LoadThread(ctx, p, r.key, s.ID)
			if err != nil && (api.AsError(err).HTTP != 404 || !domain.RowIDPattern.MatchString(s.ID) || !create) {
				return nil, err
			}
			if err != nil {
				row = &domain.ThreadRow{Thread: newThread(s.ID, r.input.ProjectID, r.input.PresetID.Value)}
			}
			if row.LastRequestID == r.nonce {
				return nil, errAlreadyApplied
			}
		}
		if id := domain.ValueOr(row.ProjectID, ""); id != "" {
			stored, err := eng.Store.LoadProject(ctx, p, r.key, id)
			if err != nil {
				return nil, err
			}
			project = &stored
			docs, err = eng.Store.ProjectDocuments(ctx, p, r.key, id, store.RefuseUnreadable)
			if err != nil {
				return nil, err
			}
		}
	}
	t := &row.Thread
	turn := r.input
	if turn.Kind == "retry" {
		var err error
		if turn, err = retried(t, r.input); err != nil {
			return nil, err
		}
	}
	// A new chat starts on the user's model, web search setting and default
	// prompt preset, whose settings it takes. A first turn naming a preset,
	// or null, overrides the default, and the turn's own settings override
	// the preset's.
	// 新聊天以用户的模型、web 搜索设置和默认 prompt preset 开始，并采用这些设置。
	// 第一个 turn 若指定某个 preset 或 null，会覆盖默认值，而 turn 自身的设置则覆盖 preset 的。
	if row.ETag == "" && len(t.Messages) == 0 {
		preset := r.input.PresetID.Value
		if id := domain.ValueOr(profile.DefaultPromptPresetID, ""); !r.input.PresetID.Set && id != "" {
			preset = &id
		}
		c := eng.Models.Catalog.Get()
		t.Model = c.SelectedModel(&profile)
		t.WebSearchEnabled = domain.ValueOr(profile.WebSearchEnabled, t.WebSearchEnabled)
		c.ApplyPreset(t, &profile, preset)
	}
	attachments, err := attach.OfTurn(turn.Attachments, t)
	if err != nil {
		return nil, err
	}
	if err := applyTurn(t, turn, r.messageID, attachments); err != nil {
		return nil, err
	}
	markResearch(t, turn)
	options := r.input.Options
	t.Model = cmp.Or(options.Model, t.Model, domain.AutoModelName)
	t.WebSearchEnabled = domain.ValueOr(options.WebSearch, t.WebSearchEnabled)
	// A chat is stored only if every attachment it names is stored under the
	// chat. An ask's stored source has attachments in buckets; a keyless thread
	// carries its own. Image descriptions are stored, image bytes are not.
	// 聊天只有在它所指定的每个附件都存储在聊天之下时才会被存储。ask 的已存储来源的附件
	// 在 bucket 中；无 key 的 thread 自带附件。图片的描述会被存储，图片字节则不会。
	if !s.local {
		if err := eng.Attachments.PlaceAttachments(ctx, p, r.key, s.ID, t); err != nil {
			return nil, err
		}
	}
	if err := eng.Attachments.HydrateContext(ctx, p, t, s.keyless); err != nil {
		return nil, err
	}
	// Offloaded documents are read back for the prompt alone.
	// 被卸载的文档只为 prompt 而读回。
	working := t.Clone()
	if !s.keyless {
		if err := eng.Attachments.HydrateDocuments(ctx, p, &working); err != nil {
			return nil, err
		}
	}
	request, model, err := eng.BuildRequest(&working, &profile, project, docs, turn, r.usageID)
	if err != nil {
		return nil, err
	}
	if turn.Kind == "compact" {
		if !compactable(request.messages, turn.CompactLatest) {
			return nil, errNothingToCompactNow()
		}
	} else if err := preflightBudget(request); err != nil {
		return nil, err
	}
	plan := &turnPlan{row: row, turn: turn, request: request, model: model, titleText: titleSource(&working)}
	if r.input.Kind == "retryToolCall" {
		for _, message := range t.Messages {
			for _, block := range message.Timeline {
				if block.CallID() == r.input.ToolCallID {
					plan.retryTarget = &retryTarget{block: block, parentID: message.ID}
				}
			}
		}
		if plan.retryTarget == nil {
			return nil, api.Err(404, "THREAD_NOT_FOUND", "The widget call was not found")
		}
		if !slices.ContainsFunc(eng.Models.Catalog.Get().SelectedWidgets(r.input.Widgets, true), func(w domain.Widget) bool { return w.Name == plan.retryTarget.block.Name }) {
			return nil, api.Err(503, "TOOL_UNAVAILABLE", "This widget is not enabled for the client").WithAction(api.ActionStop)
		}
	}
	t.ContextUsage = request.usage
	t.ActiveRun, t.LastRequestID = activeRun(r.Log.ID, runBudget(request.research)), r.nonce
	// A compaction is what a retry repeats only once it fails.
	// retry 只有在 compaction 失败后才会重复一次 compaction。
	if turn.Kind != "compact" {
		t.LastRequestKind = turn.Kind
	}
	if !s.local {
		attach.StripInlineAttachments(t)
	}
	return plan, nil
}

// launch starts the chat's active turn. It commits the turn and the active
// run's marker before any model I/O, and holds no lock while it reads or
// writes; a chat being deleted or the service stopping refuses it at the end.
// launch 启动聊天当前活动的 turn。它在任何模型 I/O 之前提交该 turn 和活动 run 的标记，
// 并在读取或写入时不持有锁；正在被删除的聊天或正在停止的服务会在最后拒绝它。
func (eng *Engine) launch(ctx context.Context, r *Run) error {
	s := r.Thread
	eng.Live.mu.Lock()
	p := r.Principal
	eng.Live.mu.Unlock()
	if !s.keyless && (p.Grant == nil || !p.Grant.ExpiresAt.After(api.Now().Add(runNeeds))) {
		fresh := *p
		fresh.Grant = nil
		grant, err := eng.Store.Rows.AuthorizeRun(ctx, &fresh, r.key, r.input.ThreadID, r.input.ProjectID, runGrant)
		if err != nil {
			return err
		}
		fresh.Grant = grant
		p = &fresh
		eng.Live.mu.Lock()
		r.Principal = p
		eng.Live.mu.Unlock()
	}
	ctx = auth.WithCaller(ctx, r.access)
	ctx = auth.WithUsage(ctx, usageForRun(r))
	ctx = inference.WithUsageSink(ctx, r.out)
	if err := eng.refuseFlagged(ctx, r, p); err != nil {
		return err
	}
	var plan *turnPlan
	// A chat another writer changed since it was read takes the turn again.
	// One that writer removed was deleted, so only a first attempt creates it.
	// 自读取以来被另一个写入者修改的聊天会再次接受该 turn。被该写入者移除的聊天已被删除，
	// 因此只有首次尝试才会创建它。
	for attempt := 1; ; attempt++ {
		var err error
		if plan, err = eng.prepareTurn(ctx, r, p, attempt == 1); err != nil {
			return err
		}
		// Refused once the turn is known not to be applied already, and
		// before it is, so a research the caller may not start leaves their
		// chat as it was.
		// 在确认该 turn 尚未被应用后、且在应用之前就拒绝它，这样调用方不能发起的 research
		// 会使其聊天保持原样。
		if attempt == 1 {
			if err := eng.allowResearch(ctx, r, p, plan.request.research); err != nil {
				return err
			}
		}
		if s.local {
			break
		}
		eng.Live.mu.Lock()
		deleted := s.deleted
		eng.Live.mu.Unlock()
		if deleted {
			return errThreadDeleted
		}
		err = eng.Store.SaveThread(ctx, p, r.key, plan.row)
		if err == nil {
			break
		}
		if api.AsError(err).Code != "REVISION_CONFLICT" || attempt == store.MaxCommitAttempts {
			return err
		}
	}
	row, request, model, titleText := plan.row, plan.request, plan.model, plan.titleText
	r.screening, r.retryTarget, r.turn = eng.screening(request), plan.retryTarget, plan.turn
	request.cacheScope = cacheScope(p, s.ID, r.input.Options.CacheSecret)
	request.browser = r.browser
	request.searchBilled = r.credential.APIKey
	request.compact = func(ctx context.Context, reserved int) error { return eng.compact(ctx, r, request, reserved) }
	// A continuation streams into the reply it extends, which RUN_STARTED names.
	// continue 会流式写入它所延伸的回复，该回复由 RUN_STARTED 指定。
	if r.turn.Kind == "continue" {
		reply := row.Messages[len(row.Messages)-1]
		r.assembler.Seed(reply)
		r.out.replyID = reply.ID
	}
	shown := domain.Window(row, "", domain.MessageWindow)
	eng.Live.mu.Lock()
	if s.deleted {
		eng.Live.mu.Unlock()
		return errThreadDeleted
	}
	if eng.Live.closing {
		eng.Live.mu.Unlock()
		return errShutdown
	}
	snapshot := fittedSnapshot(shown.Messages, shown.HasOlder)
	r.row, r.running = row, true
	if len(snapshot.Messages) > 0 {
		r.firstShown = snapshot.Messages[0].ID
	}
	// Framed under the lock, so no queue change reaches the run before them.
	// 在锁保护下成帧，这样在它们之前就不会有任何队列变更到达该 run。
	r.out.emit(domain.RunStarted{ThreadID: s.ID, RunID: r.Log.ID, Instance: InstanceID(), MessageID: r.out.replyID})
	r.out.emit(snapshot)
	r.out.emit(domain.StateSnapshotEvent{Snapshot: domain.StateSnapshot{Thread: shown.ThreadSummary, Queue: queueItems(s), RateLimit: r.credential.Limit}})
	r.out.mu.Lock()
	r.out.Observe, r.out.folding = r.assembler.Apply, true
	r.out.mu.Unlock()
	titlePrincipal := *r.Principal
	eng.Live.working.Add(1)
	eng.Live.mu.Unlock()
	runCtx, cancel := context.WithTimeout(r.stopped, runBudget(request.research))
	runCtx = auth.WithCaller(runCtx, r.access)
	runCtx = auth.WithUsage(runCtx, usageForRun(r))
	runCtx = inference.WithUsageSink(runCtx, r.out)
	r.out.mu.Lock()
	r.out.thoughts = eng.thoughtSummaries(runCtx, r.out, r.assembler)
	r.out.mu.Unlock()
	r.titleDone = make(chan struct{})
	titleState := row.TitleState
	go func() {
		defer close(r.titleDone)
		defer func() { api.Rescued("title", recover()) }()
		// An ask's thread is discarded with its run.
		// ask 的 thread 会随其 run 一起被丢弃。
		if r.input.Kind != "ask" {
			titleCtx, stop := context.WithTimeout(runCtx, 30*time.Second)
			defer stop()
			eng.generateTitle(titleCtx, r, &titlePrincipal, titleState, titleText)
		}
	}()
	go func() {
		defer cancel()
		var runErr error
		defer func() {
			if err := api.Rescued("chat run", recover()); err != nil {
				runErr = err
			}
			eng.finishRun(r, runErr)
		}()
		if r.retryTarget != nil {
			runErr = eng.retryWidget(runCtx, r, request, model)
		} else if r.turn.Kind == "compact" {
			runErr = eng.compactChat(runCtx, r, request)
		} else if request.research {
			runErr = eng.research(runCtx, r, request, model)
		} else {
			runErr = eng.loop(runCtx, r.out, request, model)
		}
	}()
	return nil
}

// refuseFlagged refuses a turn on a chat the safeguards flagged, which a
// stored chat then records. An ask is a turn on the chat it asks about.
// refuseFlagged 拒绝被 safeguards 标记的聊天上的 turn，已存储的聊天随后会记录这一标记。
// ask 是对其询问的聊天的一次 turn。
func (eng *Engine) refuseFlagged(ctx context.Context, r *Run, p *auth.Principal) error {
	chat := r.Thread.ID
	if r.input.Kind == "ask" {
		chat = r.input.ThreadID
	}
	if !eng.Auth.FlaggedChats(ctx, p)[chat] {
		return nil
	}
	if !r.Thread.local && r.input.Kind != "ask" {
		_, err := eng.Store.MutateThread(ctx, p, r.key, chat, func(t *domain.Thread) error {
			t.Flagged = true
			return nil
		})
		if err != nil && api.AsError(err).HTTP != http.StatusNotFound {
			return err
		}
	}
	return auth.ErrChatFlagged
}

// activeRun marks a chat's reply as streaming until the run's deadline.
// activeRun 将聊天的回复标记为正在流式输出，直到 run 的 deadline。
func activeRun(runID string, budget time.Duration) *domain.ActiveRun {
	return &domain.ActiveRun{RunID: runID, ExpiresAt: api.Now().Add(budget).Format(time.RFC3339Nano), Instance: InstanceID()}
}

func usageForRun(r *Run) usagereporting.Context {
	return usagereporting.Context{ContextID: r.usageID, RootRequestID: r.usageID, ParentService: "harness", Depth: 1}
}

// runQueue starts queued turns until one runs or the chat's queue empties.
// A turn that cannot start ends with its own RUN_ERROR.
// runQueue 逐个启动排队的 turn，直到其中一个开始运行或聊天的队列为空。
// 无法启动的 turn 会以自己的 RUN_ERROR 结束。
func (eng *Engine) runQueue(next *Run) {
	for next != nil {
		ctx, cancel := context.WithTimeout(context.Background(), CommitTimeout)
		err := eng.launch(ctx, next)
		cancel()
		if err == nil {
			return
		}
		next = eng.endUnstarted(next, err)
	}
}

// endUnstarted ends a turn that failed to start and returns the queued turn
// to start after it.
// endUnstarted 结束启动失败的 turn，并返回它之后要启动的排队 turn。
func (eng *Engine) endUnstarted(r *Run, err error) *Run {
	eng.Metrics.FinishRun(r.admittedAt, false, true)
	eng.Live.mu.Lock()
	next := eng.advance(r, true)
	eng.Live.mu.Unlock()
	e := api.AsError(auth.WithQuota(r.Principal, r.credential.Limit, err))
	limit := domain.ValueOr(e.RateLimit, r.credential.Limit)
	r.out.emit(domain.FailedRun(r.Thread.ID, r.Log.ID, e, &domain.RunMetadata{RateLimit: &limit}))
	r.Log.Frames.close(ErrDone)
	r.release()
	close(r.Finished)
	return next
}
