package engine

import (
	"context"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/observability"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/summarize"
)

// Engine 是 harness 的核心：它受理 turn、构建 prompt、执行模型调用，并持有本实例的实时 run。
type Engine struct {
	Live        liveRuns
	Safeguards  *Submitter
	Attachments *attach.Service
	Auth        *auth.Authenticator
	Models      *inference.Models
	Store       *store.Store
	Metrics     *observability.Metrics
	// Summaries title chats, label reasoning and compact chats; nil when
	// the harness has none.
	// Summaries 为聊天生成标题、为推理打标签并对聊天做 compaction；当 harness 没有时其为 nil。
	Summaries *summarize.Service
	// Memory reads the enclave's memory, which new turns leave a reserve
	// of; nil admits every turn.
	// Memory 读取 enclave 的内存，新的 turn 会为其留出储备；为 nil 时接纳所有 turn。
	Memory Memory
}

// liveRuns holds what this instance is executing: each chat's running reply
// and the turns queued behind it, and nothing once they end. A run is found
// by its chat, by its own id, and by the client request that started it. mu
// guards these maps and every field of the sessions and runs they hold that
// changes after the run is published, and is never held across I/O.
// liveRuns 保存本实例正在执行的内容：每个聊天运行中的回复以及排在其后面的 turn，
// 一旦它们结束就不再保存。run 可通过其聊天、其自身 id 以及启动它的客户端请求来查找。
// mu 保护这些 map 以及它们所持有的 session 和 run 中在 run 发布之后会变化的所有字段，
// 并且绝不会跨 I/O 持有。
type liveRuns struct {
	mu       sync.Mutex
	closing  bool
	threads  map[string]*threadSession
	runs     map[string]*Run
	requests map[string]*Run
	// held is the logs of device chats' runs that ended, by run, kept for
	// clients that lost their stream.
	// held 是已结束的设备聊天 run 的日志，按 run 保存，供丢失了流的客户端使用。
	held map[string]*heldRun
	// working counts the runs that have started until their last write.
	// working 统计已开始、直到其最后一次写入为止的 run 数量。
	working sync.WaitGroup
}

// heldRunTTL is how long the log of a device chat's ended run is kept. Its
// client stores the chat the run returns, so one that lost its stream, as
// an app sent to the background does, or a page too slow to keep up with
// the reply, collects it when it follows the run again. Every device chat's
// run is held until a client following it again reads it to its end, and a
// held log is no more than its running reply's was, mostly the chat it
// showed and returned, so the enclave holds half an hour of device chats'
// replies. The memory new turns leave free counts them, so a busy enclave
// refuses new turns before held replies run it short.
// heldRunTTL 是设备聊天已结束 run 的日志被保留的时长。其客户端会存储该 run 返回的聊天，
// 因此丢失了流的客户端（如被切到后台的应用，或跟不上回复速度的页面）会在再次跟随该 run 时
// 取回它。每个设备聊天的 run 都会保留，直到再次跟随它的客户端读到末尾；保留的日志不会超过
// 其运行中回复时的规模，主要是它所展示和返回的聊天，因此 enclave 保留着半小时的设备聊天回复。
// 新的 turn 所留出的空闲内存会对它们计数，这样繁忙的 enclave 会在保留的回复耗尽内存之前
// 拒绝新的 turn。
const heldRunTTL = 30 * time.Minute

type heldRun struct {
	log             *Log
	owner, threadID string
	expiry          *time.Timer
}

// hold keeps the log of a device chat's run that is ending; live.mu is held.
// hold 保留正在结束的设备聊天 run 的日志；调用时持有 live.mu。
func (l *liveRuns) hold(r *Run) {
	key := r.Thread.owner + "/" + r.Log.ID
	if l.held == nil {
		l.held = map[string]*heldRun{}
	}
	log := r.Log
	log.Frames.hold()
	l.held[key] = &heldRun{log: log, owner: r.Thread.owner, threadID: r.Thread.ID, expiry: time.AfterFunc(heldRunTTL, func() { l.release(key, log) })}
}

// release stops keeping a held log, which is wiped once nothing drains it.
// release 停止保留一个被持有的日志，一旦没有任何流在读取它，它就会被擦除。
func (l *liveRuns) release(key string, log *Log) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if h := l.held[key]; h != nil && h.log == log {
		l.drop(key, h)
	}
}

// dropHeld stops keeping the held logs that match; live.mu is held.
// dropHeld 停止保留匹配的日志；调用时持有 live.mu。
func (l *liveRuns) dropHeld(match func(*heldRun) bool) {
	for key, h := range l.held {
		if match(h) {
			l.drop(key, h)
		}
	}
}

// drop stops keeping one held log; live.mu is held.
// drop 停止保留一个被持有的日志；调用时持有 live.mu。
func (l *liveRuns) drop(key string, h *heldRun) {
	delete(l.held, key)
	h.expiry.Stop()
	h.log.Frames.release()
}

// Counts is how many chats, runs and client requests the registry holds.
// Counts 是注册表所持有的聊天、run 与客户端请求的数量。
func (l *liveRuns) Counts() (threads, runs, requests int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.threads), len(l.runs), len(l.requests)
}

// Stats reads workload counts without exposing a caller or run identifier.
// Stats 读取工作负载计数，而不暴露任何调用方或 run 标识符。
func (l *liveRuns) Stats() (active, queued, held int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, thread := range l.threads {
		if thread.Active != nil {
			active++
		}
		queued += len(thread.Queue)
	}
	return active, queued, len(l.held)
}

// Locked runs f under the registry's lock.
// Locked 在注册表的锁保护下执行 f。
func (l *liveRuns) Locked(f func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f()
}

func liveKey(p *auth.Principal, id string) string { return p.Scope() + "/" + id }

var errRunNotFound = api.Err(404, "RUN_NOT_FOUND", "The run is not live on this instance")

// forget unregisters a run that is ending.
// forget 注销一个正在结束的 run。
func (l *liveRuns) forget(r *Run) {
	if key := r.Thread.owner + "/" + r.Log.ID; l.runs[key] == r {
		delete(l.runs, key)
	}
	if key := r.Thread.owner + "/" + r.nonce; l.requests[key] == r {
		delete(l.requests, key)
	}
}

type threadSession struct {
	ID, owner, keyHash string
	deleted            bool
	// A local thread is stored by its client, which sends its transcript
	// with each turn; an ask is stored by nobody. The run holds either's
	// row. A keyless thread, local or anonymous, never uses the content key.
	// 本地 thread 由其客户端存储，客户端会随每个 turn 发送其记录；ask 则无人存储。
	// run 持有这两者中任意一个的行。无 key 的 thread（本地或匿名的）从不使用 content key。
	local, keyless bool
	// row is a local thread's transcript, which a queued turn continues from.
	// row 是本地 thread 的记录，排队的 turn 会从它继续。
	row    *domain.ThreadRow
	Active *Run
	Queue  []*Run
}

// Run 是一次聊天回复的执行：它承载该 turn 的请求、输出流以及进行中的状态。
type Run struct {
	Thread                   *threadSession
	Log                      *Log
	out                      *Stream
	input                    *TurnRequest
	turn                     *TurnRequest // the turn as carried out, once started
	received                 string       // when the turn's request arrived
	admittedAt               time.Time
	Principal                *auth.Principal
	key                      auth.ContentKey
	credential               auth.Credential
	access                   *auth.Access // the key the run's calls carry, kept current
	nonce, digest, messageID string
	Queued, cancelled        bool
	running                  bool            // its RUN_STARTED is framed
	ending                   bool            // finishing what it produced; too late to cancel
	stopped                  context.Context // done once the run is cancelled
	accepted                 chan struct{}   // closed once the turn is queued, started, or refused
	refused                  error
	Finished                 chan struct{}
	titleDone                chan struct{}
	row                      *domain.ThreadRow // the thread as the run started and committed it
	firstShown               string            // the first message of the run's MESSAGES_SNAPSHOT
	assembler                *domain.MessageAssembler
	retryTarget              *retryTarget
	replacement              domain.ReplacedWidget
	profile                  *domain.Profile // a local thread's settings, sent by its client
	screening                *screening
	compacted                *compaction // what the run's compaction left, once it compacted
	researched               bool        // the run started a research, which its reply records
	browser                  *browserSession

	// yielded marks an active run stopped so a queued turn starts now.
	// yielded 标记一个活动 run 被停止，从而让排队的 turn 现在开始。
	yielded bool

	// usageID names the run in the usage reports its calls carry in
	// headers, which leave the enclave. The run's own id never does: for a
	// keyless run it is the only secret that admits a follower.
	// usageID 在其调用所携带的 header（会离开 enclave）的使用量报告中标识该 run。
	// run 自身的 id 绝不会这样：对于无 key 的 run，它是唯一能接纳跟随者的秘密。
	usageID string
}

// HoldsKey reports whether the run still holds its caller's content key.
// HoldsKey 报告该 run 是否仍持有其调用方的 content key。
func (r *Run) HoldsKey() bool { return r.key != (auth.ContentKey{}) }

// release drops what a run holds of its caller once nothing reads it.
// release 在没有任何东西读取 run 后，丢弃它所持有的调用方内容。
func (r *Run) release() {
	clear(r.key[:])
	r.input, r.turn, r.retryTarget, r.assembler, r.screening, r.profile = nil, nil, nil, nil, nil, nil
	r.replacement = domain.ReplacedWidget{}
	r.Principal = &auth.Principal{ID: r.Principal.ID, AnonymousID: r.Principal.AnonymousID, Anonymous: r.Principal.Anonymous}
	r.credential.Key, r.access = "", nil
}

func (eng *Engine) authorizedRun(p *auth.Principal, key auth.ContentKey, id, runID string) (*Run, error) {
	if err := api.RequireID("threadId", id); err != nil {
		return nil, err
	}
	if err := api.RequireID("runId", runID); err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	r := eng.Live.runs[liveKey(p, runID)]
	if r == nil || r.Thread.ID != id {
		return nil, errRunNotFound
	}
	if !r.Thread.keyless && r.Thread.keyHash != key.Fingerprint() {
		return nil, api.Err(409, "KEY_MISMATCH", "The run uses another key")
	}
	return r, nil
}

// makeActive gives a chat's running slot to a turn about to start; live.mu is held.
// makeActive 将聊天运行中的位置交给即将开始的 turn；调用时持有 live.mu。
func (eng *Engine) makeActive(s *threadSession, r *Run) {
	s.Active = r
}

// queueChanged tells a chat's running reply its queue; live.mu is held. A
// reply that has not started is told by its first state snapshot.
// queueChanged 将队列告知聊天运行中的回复；调用时持有 live.mu。
// 尚未开始的回复会通过其第一个状态快照被告知。
func queueChanged(s *threadSession) {
	if s.Active != nil && s.Active.running {
		s.Active.out.emit(domain.StateDelta{Delta: []domain.StatePatch{{Op: "replace", Path: "/queue", Value: queueItems(s)}}})
	}
}

func (r *Run) queueItem() domain.QueueItem {
	attachments := []domain.QueuedAttachment{}
	for _, a := range r.input.Attachments {
		attachments = append(attachments, domain.QueuedAttachment{ID: a.ID, Kind: a.Kind, FileName: a.FileName, MimeType: a.MimeType, Size: a.Size, Thumbnail: a.Thumbnail})
	}
	return domain.QueueItem{QueueID: r.Log.ID, Kind: r.input.Kind, Content: r.input.Content, Quote: r.input.Quote, Attachments: attachments, CreatedAt: r.received}
}

func queueItems(s *threadSession) []domain.QueueItem {
	out := []domain.QueueItem{}
	for _, r := range s.Queue {
		out = append(out, r.queueItem())
	}
	return out
}

// advance retires a run from its chat and returns the queued turn to start
// next, if the chat goes on; live.mu is held. A chat whose reply was stopped
// ends the turns queued behind it, so the harness keeps nothing of a chat
// once its reply stops. A chat with nothing running or queued leaves the
// registry.
// advance 让 run 从其聊天中退场，并在聊天继续时返回下一个要开始的排队 turn；
// 调用时持有 live.mu。回复被停止的聊天会结束排在其后面的 turn，因此一旦聊天的回复停止，
// harness 就不再保留该聊天的任何内容。没有运行中或排队内容的聊天会离开注册表。
func (eng *Engine) advance(r *Run, goOn bool) *Run {
	s := r.Thread
	eng.Live.forget(r)
	if s.Active == r {
		s.Active = nil
	}
	var next *Run
	if goOn && !s.deleted && !eng.Live.closing && len(s.Queue) > 0 {
		next, s.Queue = s.Queue[0], s.Queue[1:]
		eng.makeActive(s, next)
	} else {
		for _, queued := range s.Queue {
			eng.endQueued(queued, nil)
		}
		s.Queue = nil
	}
	eng.dropIdle(s)
	return next
}

// dropIdle removes a chat with nothing running or queued from the registry;
// live.mu is held.
// dropIdle 从注册表中移除没有运行中或排队内容的聊天；调用时持有 live.mu。
func (eng *Engine) dropIdle(s *threadSession) {
	if s.Active != nil || len(s.Queue) > 0 {
		return
	}
	if eng.Live.threads[s.owner+"/"+s.ID] == s {
		delete(eng.Live.threads, s.owner+"/"+s.ID)
	}
}

// endQueued ends a turn that never started: removed when why is nil, and
// failed with why otherwise. live.mu is held.
// endQueued 结束一个从未开始的 turn：当 why 为 nil 时将其移除，否则以 why 使其失败。
// 调用时持有 live.mu。
func (eng *Engine) endQueued(r *Run, why error) {
	eng.Live.forget(r)
	eng.Metrics.FinishRun(r.admittedAt, why == nil, why != nil)
	if why == nil {
		r.out.emit(domain.RunFinished{ThreadID: r.Thread.ID, RunID: r.Log.ID, Metadata: domain.RunMetadata{Cancelled: new(true)}})
	} else {
		r.out.emit(domain.FailedRun(r.Thread.ID, r.Log.ID, api.AsError(why), nil))
	}
	r.Log.Frames.close(ErrDone)
	r.release()
	close(r.Finished)
}

// instanceID names this process, which alone holds its live runs. Clients
// send it back with requests about a run, so that a deployment of several
// can route them here.
// instanceID 标识这个进程，只有它持有自己的实时 run。客户端会在关于 run 的请求中把它
// 发送回来，这样多实例的部署就能把请求路由到这里。
var instanceID = api.Token()

// InstanceID is the id of this process.
// InstanceID 是这个进程的 id。
func InstanceID() string { return instanceID }
