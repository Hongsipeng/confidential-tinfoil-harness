package engine

import (
	"context"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// errAlreadyApplied answers a retried request whose turn its chat holds, once
// the run it started is no longer live.
// errAlreadyApplied 应答重试的请求：当它启动的 run 不再存活时，其 turn 仍由其聊天持有。
var errAlreadyApplied = api.Err(409, "ALREADY_APPLIED", "This request was already applied; reload the chat")

// MaxQueuedTurns bounds the turns one chat holds behind its running reply.
// MaxQueuedTurns 限制单个聊天在其运行中的回复后允许排队的 Turn 数量。
const MaxQueuedTurns = 32

// errQueueFull refuses a turn past a full queue. It is not the caller's
// quota, which stays as it was.
// errQueueFull 在队列已满时拒绝 Turn。它不是调用方的 quota，quota 保持不变。
var errQueueFull = api.Err(429, "QUEUE_FULL", "The turn queue is full")

// AcceptTurn 接收一个 Turn，将其作为其聊天的运行回复启动，或排在该回复之后。
func (eng *Engine) AcceptTurn(ctx context.Context, p *auth.Principal, credential auth.Credential, key auth.ContentKey, in *TurnRequest) (*Run, error) {
	defer clear(key[:])
	in.ClearKey()
	nonce, digest := in.ClientRequestID, in.digest()
	low := eng.lowOnMemory()
	var r *Run
	for r == nil {
		eng.Live.mu.Lock()
		if eng.Live.closing {
			eng.Live.mu.Unlock()
			return nil, errShutdown
		}
		if eng.Live.requests == nil {
			eng.Live.runs, eng.Live.requests = map[string]*Run{}, map[string]*Run{}
			if eng.Live.threads == nil {
				eng.Live.threads = map[string]*threadSession{}
			}
		}
		previous := eng.Live.requests[liveKey(p, nonce)]
		// A retried request still follows the run it started.
		// 重试的请求仍然跟随它最初启动的 run。
		if previous == nil && low {
			eng.Live.mu.Unlock()
			return nil, errBusy
		}
		if previous == nil {
			r = &Run{nonce: nonce, digest: digest, accepted: make(chan struct{}), Finished: make(chan struct{})}
			eng.Live.requests[liveKey(p, nonce)] = r
		}
		eng.Live.mu.Unlock()
		if previous == nil {
			break
		}
		// A retried request follows the run it started.
		// 重试的请求跟随它最初启动的 run。
		if previous.digest != digest {
			return nil, api.Err(409, "REVISION_CONFLICT", "The request nonce was already used")
		}
		// A run already accepted is followed even by a caller that has left.
		// 已被接受的 run 甚至会被已离开的调用方跟随。
		select {
		case <-previous.accepted:
		default:
			select {
			case <-previous.accepted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if previous.refused == nil {
			if !previous.Thread.keyless && previous.Thread.keyHash != key.Fingerprint() {
				return nil, api.Err(409, "REVISION_CONFLICT", "The request nonce was already used")
			}
			// A run that ended meanwhile is checked against the chat as a
			// new request is.
			// 期间已结束的 run 会像新请求一样接受聊天层面的校验。
			eng.Live.mu.Lock()
			live := eng.Live.requests[liveKey(p, nonce)] == previous
			if live {
				previous.Log.Frames.Attach()
			}
			eng.Live.mu.Unlock()
			if live {
				return previous, nil
			}
		}
	}
	if err := eng.admit(ctx, p, credential, key, in, r); err != nil {
		clear(r.key[:])
		eng.Live.mu.Lock()
		if eng.Live.requests[liveKey(p, nonce)] == r {
			delete(eng.Live.requests, liveKey(p, nonce))
		}
		eng.Live.mu.Unlock()
		r.refused = err
		close(r.accepted)
		return nil, err
	}
	if r.Queued {
		return r, nil
	}
	// A client that leaves while its turn starts leaves it to start, as a
	// queued turn does: the turn's chat may already be saved with it.
	// 当 turn 启动期间客户端离开时，会像排队的 turn 一样让它自行启动：该 turn 的聊天可能已经随之保存。
	launchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CommitTimeout)
	err := eng.launch(launchCtx, r)
	cancel()
	if err != nil {
		err = auth.WithQuota(p, r.credential.Limit, err)
		if next := eng.endUnstarted(r, err); next != nil {
			go eng.runQueue(next)
		}
		r.Log.Frames.Detach()
		r.refused = err
		close(r.accepted)
		return nil, err
	}
	close(r.accepted)
	return r, nil
}

func quotaExhausted(limit api.RateLimit) error {
	e := api.Err(429, "QUOTA_EXHAUSTED", "The daily quota is exhausted")
	e.Kind, _ = limit.Kind.(string)
	e.ResetsAt, e.RateLimit = limit.ResetsAt, &limit
	return e
}

// newThread is a chat's first form, filed under a project and preset.
// newThread 是聊天的初始形态，归属于某个 project 和 preset。
func newThread(id string, projectID, presetID *string) domain.Thread {
	return domain.Thread{ID: id, Title: "New Chat", TitleState: "placeholder", CreatedAt: api.Timestamp(), UpdatedAt: api.Timestamp(), Model: "auto", ProjectID: projectID, PresetID: presetID, WebSearchEnabled: true, Messages: []domain.Message{}}
}

// askThread is an ask's thread: it starts from the conversation it asks
// about and hides it from its client.
// askThread 是 ask 的 thread：它从所询问的对话开始，并对其客户端隐藏该对话。
func askThread(id string, source *domain.Thread) *domain.ThreadRow {
	t := newThread(id, source.ProjectID, source.PresetID)
	t.Messages, t.HiddenCount = source.Messages, len(source.Messages)
	return &domain.ThreadRow{Thread: t}
}

// admit gives a turn its chat: it starts now if the chat has no running
// reply, or waits in the chat's queue.
// admit 为 turn 分配其聊天：若聊天没有正在运行的回复则立即启动，否则在聊天的 queue 中等待。
func (eng *Engine) admit(ctx context.Context, p *auth.Principal, credential auth.Credential, key auth.ContentKey, in *TurnRequest, r *Run) error {
	defer clear(key[:])
	if credential.Limit.Exhausted() {
		return quotaExhausted(credential.Limit)
	}
	id, local := in.ThreadID, in.Local
	keyless := p.Anonymous || local
	if !keyless {
		granted, err := eng.Store.Rows.AuthorizeRun(ctx, p, key, in.ThreadID, in.ProjectID, runGrant)
		if err != nil {
			return err
		}
		copy := *p
		copy.Grant = granted
		p = &copy
	}
	var source *domain.Thread
	if in.Kind == "ask" {
		if local {
			t, err := domain.ReadDeviceThread(id, in.Thread)
			if err != nil {
				return err
			}
			source = &t
		} else {
			row, err := eng.Store.LoadThread(ctx, p, key, id)
			if err != nil {
				return err
			}
			source = &row.Thread
		}
		id = ""
	}
	// A stored chat's project is read as its first turn starts.
	// 已存储聊天的 project 在其第一个 turn 启动时被读取。
	if project := domain.ValueOr(in.ProjectID, ""); source == nil && project != "" {
		if err := api.RequireID("projectId", project); err != nil {
			return err
		}
	}
	if id == "" {
		id = domain.RowID()
	}
	// An idle local thread starts from the client's copy, which may have been
	// edited since the last turn. A queued turn continues the active run's row.
	// An ask is a new thread that hides the copy it starts from. A device
	// chat's first turn sends no thread and starts the chat under its
	// client's id.
	// 空闲的本地 thread 从客户端副本启动，该副本可能自上一个 turn 起已被编辑。
	// 排队的 turn 延续活跃 run 的行。ask 是一个隐藏其起始副本的新 thread。
	// 设备聊天的第一个 turn 不发送 thread，并以其客户端的 id 启动聊天。
	var seed *domain.ThreadRow
	switch {
	case !local || source != nil:
	case in.Thread != nil:
		t, err := domain.ReadDeviceThread(id, in.Thread)
		if err != nil {
			return err
		}
		seed = &domain.ThreadRow{Thread: t}
		if seed.LastRequestID == r.nonce {
			return errAlreadyApplied
		}
	case domain.RowIDPattern.MatchString(id):
		seed = &domain.ThreadRow{Thread: newThread(id, nil, nil)}
	}
	if local {
		r.profile = domain.LocalProfile(in.Profile)
	}
	clean := *in
	// The transcript seeds the row; holding it again per run would double a
	// local thread's memory while it waits in the queue.
	// transcript 用于初始化该行；若每个 run 再持有一次，将令本地 thread 在队列中等待时占用双倍内存。
	clean.Key, clean.Thread, clean.Profile = nil, nil, nil
	stopped, stop := context.WithCancel(context.Background())
	r.Log, r.usageID = NewLog(api.Token()), api.Token()
	r.Log.stop = stop
	// The user message a turn adds is named by the client's request.
	// turn 所添加的用户消息由客户端请求命名。
	r.stopped, r.input, r.received, r.Principal, r.key, r.messageID, r.credential, r.assembler = stopped, &clean, api.Timestamp(), p, key, r.nonce, credential, domain.NewMessageAssembler()
	r.access = eng.Store.ChatAccess(eng.Auth, p, credential)
	r.browser = newBrowserSession(in.Browser)
	if keyless {
		clear(r.key[:])
	}
	r.out = &Stream{Run: r.Log}

	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if eng.Live.closing {
		return errShutdown
	}
	s := eng.Live.threads[liveKey(p, id)]
	// An ask's run holds its row, as a device chat's does.
	// ask 的 run 持有其行，正如设备聊天那样。
	held := local || source != nil
	if s != nil && (s.deleted || s.local != held || !s.keyless && s.keyHash != key.Fingerprint()) {
		return api.Err(404, "THREAD_NOT_FOUND", "The thread is unavailable")
	}
	// A compaction is not a message to queue: it reads the chat as it is.
	// compaction 不是要排队的消息：它读取聊天当前的状态。
	if s != nil && s.Active != nil && in.Kind == "compact" {
		return api.Err(409, "RUN_IN_FLIGHT", "Wait for the reply to finish before compacting this chat").WithAction(api.ActionRetry)
	}
	if s != nil && s.Active != nil && len(s.Queue) >= MaxQueuedTurns {
		return errQueueFull
	}
	if (s == nil || s.Active == nil) && local && source == nil && seed == nil {
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	if s == nil {
		s = &threadSession{ID: id, owner: p.Scope(), keyHash: key.Fingerprint(), local: held, keyless: keyless}
		eng.Live.threads[liveKey(p, id)] = s
	}
	r.Thread = s
	eng.Live.runs[liveKey(p, r.Log.ID)] = r
	r.admittedAt = time.Now()
	eng.Metrics.AdmitRun()
	// The accepting request drains the run until it answers.
	// 接受请求会持续读取该 run 直到其应答。
	r.Log.Frames.Attach()
	if s.Active != nil {
		r.Queued = true
		s.Queue = append(s.Queue, r)
		r.out.emit(domain.StateDelta{ThreadID: id, RunID: r.Log.ID, Delta: []domain.StatePatch{{Op: "add", Path: "/queue/-", Value: r.queueItem()}}})
		queueChanged(s)
		// A queued turn is accepted as it is registered, so a stop that
		// ends it cannot come between the two, and a retried request finds
		// it either queued or ended, never missing.
		// 排队的 turn 在注册时即被接受，因此结束它的 stop 无法插在两者之间，
		// 重试的请求要么发现它已排队、要么已结束，绝不会找不到。
		close(r.accepted)
		return nil
	}
	switch {
	case seed != nil:
		s.row = seed
	case source != nil:
		s.row = askThread(id, source)
	}
	eng.makeActive(s, r)
	return nil
}
