// Package jobs runs one background job per owner at a time and keeps its
// last report for a while after it ends, so a repeated request sees the
// job it started rather than starting another.
// Package jobs 同一时刻只为每个 owner 运行一个后台 job，并在其结束后将最后一份报告
// 保留一段时间，这样重复的请求看到的是自己已启动的 job，而不会另起一个。
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// State is where a job is in its life.
// State 表示 job 在其生命周期中所处的阶段。
type State string

const (
	// Running 表示 job 正在运行。
	Running State = "running"
	// Complete 表示 job 已成功完成。
	Complete State = "complete"
	// Failed 表示 job 已失败。
	Failed State = "failed"
)

// Status is where a job stands: its state, its latest report, the error
// that failed it, and when it started and last changed.
// Status 描述 job 的当前状况：状态、最新报告、导致失败的 error，以及启动与最后变更时间。
type Status[R any] struct {
	State     State
	Report    R
	Err       error
	StartedAt time.Time
	UpdatedAt time.Time
}

// Runner starts jobs and finds the one an owner already has.
// Runner 负责启动 job，并查找某个 owner 已有的 job。
type Runner[R any] struct {
	budget, retention time.Duration

	mu   sync.Mutex
	jobs map[string]*Job[R]
}

// New is a runner whose jobs end at the budget and stay findable for the
// retention after they end.
// New 返回一个 runner：其 job 在 budget 时截止，结束后在 retention 内仍可被找到。
func New[R any](budget, retention time.Duration) *Runner[R] {
	return &Runner[R]{budget: budget, retention: retention, jobs: map[string]*Job[R]{}}
}

// Start runs work for owner unless a job of theirs is running or ended within
// the retention, in which case that job is returned and started is false.
// work runs detached from ctx (its values kept, its cancellation ignored)
// with the budget as its deadline; it reports progress through report; a
// panic is logged with its stack and ends it as failed.
// Start 为 owner 运行 work；若其已有 job 正在运行或在 retention 内刚结束，则返回
// 该 job 且 started 为 false。work 与 ctx 解耦运行（保留其值、忽略其取消），以
// budget 为截止时间；通过 report 上报进度；发生 panic 时记录其调用栈并以失败结束。
func (r *Runner[R]) Start(ctx context.Context, owner string, work func(ctx context.Context, report func(R)) (R, error)) (*Job[R], bool) {
	return r.start(ctx, owner, work, false)
}

// Restart is Start for an owner whose last job gives way once it has
// ended: a running job is returned as Start returns it, an ended one is
// forgotten and the new one started.
// Restart 是用于“上个 job 一旦结束即可让位”的 Start：正在运行的 job 与 Start 一样
// 直接返回；已结束的 job 则被遗忘并启动新的。
func (r *Runner[R]) Restart(ctx context.Context, owner string, work func(ctx context.Context, report func(R)) (R, error)) (*Job[R], bool) {
	return r.start(ctx, owner, work, true)
}

func (r *Runner[R]) start(ctx context.Context, owner string, work func(ctx context.Context, report func(R)) (R, error), replaceEnded bool) (*Job[R], bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if job, ok := r.jobs[owner]; ok {
		if !replaceEnded || job.Status().State == Running {
			return job, false
		}
		delete(r.jobs, owner)
	}
	now := time.Now()
	job := &Job[R]{id: newID(), status: Status[R]{State: Running, StartedAt: now, UpdatedAt: now}, done: make(chan struct{})}
	// The job's context is made here, so a cancel can end it before its
	// goroutine has run.
	// job 的 context 在此创建，因此取消操作可以在其 goroutine 运行之前结束它。
	ctx, job.cancel = context.WithTimeout(context.WithoutCancel(ctx), r.budget)
	r.jobs[owner] = job
	go r.run(ctx, owner, job, work)
	return job, true
}

// Get is the job owner has, running or ended within the retention, or nil.
// Get 返回 owner 的 job（正在运行或在 retention 内已结束），否则返回 nil。
func (r *Runner[R]) Get(owner string) *Job[R] {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.jobs[owner]
}

// idBytes is the length of a job's id before hex.
// idBytes 是 job 的 id 在十六进制编码前的字节长度。
const idBytes = 16

// newID names a job so a client can tell the one it started from the
// owner's next.
// newID 为 job 生成名称，使客户端能区分自己启动的 job 与 owner 的下一个 job。
func newID() string {
	var b [idBytes]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Cancel ends owner's job, running or ended, and forgets it, so the next
// Start begins at once. A running job's work sees its context end; the
// job returned, if any, is done once that work has returned.
// Cancel 结束 owner 的 job（无论运行中还是已结束）并将其遗忘，使下一次 Start 立即
// 开始。运行中 job 的 work 会看到其 context 结束；返回的 job（若有）会在该 work
// 返回后结束。
func (r *Runner[R]) Cancel(owner string) *Job[R] {
	r.mu.Lock()
	job := r.jobs[owner]
	delete(r.jobs, owner)
	r.mu.Unlock()
	if job != nil {
		job.cancel()
	}
	return job
}

func (r *Runner[R]) run(ctx context.Context, owner string, job *Job[R], work func(context.Context, func(R)) (R, error)) {
	defer job.cancel()
	final, err := guarded(ctx, job.report, work)
	// The job ends and, without a retention, leaves the runner in one
	// step, so whoever sees it done finds the owner free.
	// job 结束；在没有 retention 时一步离开 runner，因此任何看到其完成的人都会发现
	// owner 已空闲。
	r.mu.Lock()
	job.finish(final, err)
	if r.retention <= 0 && r.jobs[owner] == job {
		delete(r.jobs, owner)
	}
	r.mu.Unlock()
	if r.retention > 0 {
		time.AfterFunc(r.retention, func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.jobs[owner] == job {
				delete(r.jobs, owner)
			}
		})
	}
}

// errPanicked ends a job whose work panicked. The panic's value may hold
// what the job was working on, so only the stack is logged.
// errPanicked 结束一个 work 发生 panic 的 job。panic 值可能包含 job 正在处理的
// 内容，因此只记录调用栈。
var errPanicked = errors.New("jobs: the job panicked")

// guarded runs work, turning a panic into its error, with the stack in
// the log. A panic returns no final report, which leaves the last one the
// work made.
// guarded 运行 work，将 panic 转化为其 error，并把调用栈写入日志。panic 不会返回
// 最终报告，从而保留 work 上次生成的报告。
func guarded[R any](ctx context.Context, report func(R), work func(context.Context, func(R)) (R, error)) (final *R, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("recovered a panic", "in", "a background job", "stack", string(debug.Stack()))
			final, err = nil, errPanicked
		}
	}()
	result, err := work(ctx, report)
	return &result, err
}

// Job is one owner's background work.
// Job 是某个 owner 的一项后台工作。
type Job[R any] struct {
	id     string
	mu     sync.Mutex
	status Status[R]
	done   chan struct{}
	cancel context.CancelFunc
}

// ID names the job, unlike any other's.
// ID 返回该 job 的名称，与其他 job 均不同。
func (j *Job[R]) ID() string { return j.id }

// Status is where the job stands now.
// Status 返回 job 当前的状态。
func (j *Job[R]) Status() Status[R] {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.status
}

// Done is closed once the job has ended.
// Done 在 job 结束后关闭。
func (j *Job[R]) Done() <-chan struct{} { return j.done }

func (j *Job[R]) report(progress R) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status.Report, j.status.UpdatedAt = progress, time.Now()
}

func (j *Job[R]) finish(final *R, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status.UpdatedAt = time.Now()
	if err != nil {
		j.status.State, j.status.Err = Failed, err
	} else {
		j.status.State = Complete
	}
	if final != nil {
		j.status.Report = *final
	}
	close(j.done)
}
