package engine

import (
	"errors"
	"sync"
	"time"
)

const (
	// A run holds its recent frames in memory for clients that follow it
	// again. Past this budget they are folded into a checkpoint, so a run's
	// share of the enclave is the chat it showed, the reply it is writing
	// and this budget.
	// run 在内存中保留其最近的帧，供再次跟随它的客户端使用。超过此预算后，帧会被折叠为
	// 一个 checkpoint，因此 run 在 enclave 中占用的就是它所展示的聊天、它正在写入的回复
	// 以及此预算。
	runLogBudget = 32 << 20
	// mergedFrameBytes bounds a frame a checkpoint merges deltas into.
	// mergedFrameBytes 限制 checkpoint 将 delta 合并进某个帧的大小时。
	mergedFrameBytes = 1 << 20
	// RunTimeout bounds a reply, and researchTimeout one that researches.
	// What a run holds for as long as it lasts is sized from them: its
	// deadline and the marker that tells other clients a reply is
	// streaming from its own, and its storage grant from the longer.
	// RunTimeout 限制一次回复，researchTimeout 限制一次做研究的回复。run 在其存续期间
	// 所持有的内容由它们决定规模：其 deadline 和告知其他客户端某个回复正在从其自身流式
	// 输出的标记，而其存储授权取两者中较长者。
	RunTimeout      = 30 * time.Minute
	researchTimeout = researchTime + researchReportTime
	longestRun      = max(RunTimeout, researchTimeout)
	// runNeeds is how long the grant of a turn about to start must last:
	// through its start, the longest run, and its last writes.
	// runNeeds 是即将开始的 turn 的授权必须持续的时长：涵盖其开始、最长的 run，
	// 以及其最后的写入。
	runNeeds = CommitTimeout + longestRun + CommitTimeout
	// runGrant is how long a run may use its chat's rows: twice what it
	// needs, so a turn that waited a while in its chat's queue starts
	// without asking again.
	// runGrant 是 run 可使用其聊天行的时长：为其所需的两倍，这样在其聊天队列中等待了一段
	// 时间的 turn 就能无需再次请求而启动。
	runGrant = 2 * runNeeds
)

// runBudget is how long a run may last.
// runBudget 是 run 可持续的时长。
func runBudget(research bool) time.Duration {
	if research {
		return researchTimeout
	}
	return RunTimeout
}

// ErrDone 表示 run 已正常结束。
var ErrDone = errors.New("run finished")

// frameKind is what a checkpoint does with a frame.
// frameKind 表示 checkpoint 对某个帧所做的处理类型。
type frameKind uint8

const (
	// frameKept is sent again as it was to a follower behind a checkpoint.
	// frameKept 按原样再次发送给位于 checkpoint 之后的跟随者。
	frameKept frameKind = iota
	// frameDelta extends a message, its reasoning or a call's arguments. A
	// checkpoint merges each run of deltas to the same one into one frame.
	// frameDelta 扩展一条消息、其推理或某次调用的参数。checkpoint 会将针对同一目标的
	// 连续 delta 合并为一个帧。
	frameDelta
	// frameActivity reports a tool's activity, which a checkpoint sends as
	// it stands in the place of the activity's first frame.
	// frameActivity 报告工具的活动，checkpoint 按其当前状态在活动首帧的位置发送它。
	frameActivity
)

type entry struct {
	data []byte
	kind frameKind
	key  string // what a delta extends, or whose activity a frame reports
}

// FrameLog 是按顺序存储 run 所发出帧的日志，供跟随的客户端读取。
type FrameLog struct {
	mu sync.Mutex
	// base is the cursor of frames[0]. The frames before it are folded into
	// checkpoint: the same events, with deltas merged and each tool's
	// activity as it stood.
	// base 是 frames[0] 的游标。它之前的帧被折叠进 checkpoint：同样是这些事件，
	// 只是 delta 被合并、每个工具的活动按当时状态保存。
	base       int
	checkpoint []entry
	frames     []entry
	bytes      int // the size of frames
	budget     int // the size of frames past which they are folded
	closed     error
	wake       chan struct{}
	// ended marks the run as ended and no longer registered, so nothing
	// more attaches; a log that is ended and that its last reader has left
	// is wiped.
	// ended 标记 run 已结束且不再注册，因此不会再有任何东西附着；已结束且其最后一个读者
	// 已离开的日志会被擦除。
	ended    bool
	draining int
	// held keeps an ended log from being wiped while the client of its
	// device chat may still collect it.
	// held 在其设备聊天客户端仍可能取回它时，防止已结束的日志被擦除。
	held bool
	// A stream sends the frames it read after the lock is released, so the
	// frames a fold drops are kept in retired until every stream that read
	// before the fold has sent them. folds counts the folds, and lent the
	// streams holding frames by the folds made before they read them.
	// 流会在锁释放后发送其读取的帧，因此 fold 所丢弃的帧会保存在 retired 中，
	// 直到在 fold 之前读取的每个流都已将它们发出。folds 统计折叠次数，lent 则记录
	// 各流按其读取之前已完成的折叠次数所持有的帧。
	folds   int
	lent    map[int]int
	retired []retiredFrames
}

// retiredFrames are the frames a fold dropped, and the folds made with it.
// retiredFrames 是某次 fold 丢弃的帧，以及这次 fold 的折叠次数。
type retiredFrames struct {
	folds  int
	frames [][]byte
}

// Readers is how many streams are draining the log.
// Readers 是正在读取该日志的流的数量。
func (l *FrameLog) Readers() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining
}

func newRunlog() *FrameLog {
	return &FrameLog{wake: make(chan struct{}), budget: runLogBudget, lent: map[int]int{}}
}

// grow appends one frame and reports whether the frames since the last
// checkpoint outgrew the budget.
// grow 追加一个帧，并报告自上次 checkpoint 以来的帧是否超出了预算。
func (l *FrameLog) grow(frame []byte, kind frameKind, key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		l.frames, l.bytes = append(l.frames, entry{frame, kind, key}), l.bytes+len(frame)
		l.signal()
	}
	return l.bytes > l.budget
}

// fold replaces every frame the log holds with a checkpoint of the same
// events: kept frames as they were, each run of deltas to the same target
// merged by merge into a frame of at most mergedFrameBytes followed by the
// run's last delta, and each tool's activity as activity reports it, in the
// place of its first frame. It wipes the frames it drops once no stream
// holds them.
// fold 将日志所持有的每个帧替换为同一组事件的 checkpoint：保留的帧按原样，
// 针对同一目标的连续 delta 由 merge 合并为一个不超过 mergedFrameBytes 的帧，
// 其后跟该段 delta 的最后一条，而每个工具的活动则按 activity 报告的内容放在其首帧的位置。
// 一旦没有流持有被丢弃的帧，它就会擦除它们。
func (l *FrameLog) fold(merge func(deltas [][]byte) []byte, activity func(key string) []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed != nil {
		return
	}
	var checkpoint, run []entry
	var dropped [][]byte
	size := 0
	// flush puts the run of deltas so far in the checkpoint: every one but
	// the last merged at the time of the first, then the last as it was, so
	// a client still sees when the run began and when it last grew, as it
	// times reasoning by them.
	// flush 将到目前为止的 delta 段放入 checkpoint：除最后一条外全部按第一条的时间合并，
	// 最后一条则保持原样，这样客户端仍能看到该段何时开始以及何时最后一次增长，
	// 因为客户端正是靠它们来计时推理的。
	flush := func() {
		if len(run) > 2 {
			merged := run[:len(run)-1]
			deltas := make([][]byte, len(merged))
			for i, e := range merged {
				deltas[i] = e.data
			}
			checkpoint = append(checkpoint, entry{merge(deltas), frameDelta, run[0].key})
			dropped = append(dropped, deltas...)
			run = run[len(run)-1:]
		}
		checkpoint = append(checkpoint, run...)
		run, size = nil, 0
	}
	placed := map[string]bool{}
	for _, e := range append(l.checkpoint, l.frames...) {
		if e.kind == frameDelta && len(run) > 0 && run[0].key == e.key && size+len(e.data) <= mergedFrameBytes {
			run, size = append(run, e), size+len(e.data)
			continue
		}
		flush()
		switch e.kind {
		case frameDelta:
			run, size = []entry{e}, len(e.data)
			continue
		case frameKept:
			checkpoint = append(checkpoint, e)
			continue
		case frameActivity:
			if !placed[e.key] {
				placed[e.key] = true
				if now := activity(e.key); now != nil {
					checkpoint = append(checkpoint, entry{now, frameActivity, e.key})
				}
			}
		}
		dropped = append(dropped, e.data)
	}
	flush()
	l.checkpoint, l.base, l.frames, l.bytes = checkpoint, l.base+len(l.frames), nil, 0
	l.folds++
	l.retired = append(l.retired, retiredFrames{l.folds, dropped})
	l.reclaim()
}

// reclaim wipes the retired frames no stream holds, those every stream
// holding frames read after their fold. l.mu is held.
// reclaim 擦除没有任何流持有的退役帧，即每个持有帧的流都在其 fold 之后读取的那些。
// 调用时持有 l.mu。
func (l *FrameLog) reclaim() {
	oldest := l.folds
	for folds := range l.lent {
		oldest = min(oldest, folds)
	}
	kept := l.retired[:0]
	for _, r := range l.retired {
		if r.folds > oldest {
			kept = append(kept, r)
			continue
		}
		for _, frame := range r.frames {
			clear(frame)
		}
	}
	clear(l.retired[len(kept):])
	l.retired = kept
}

// Attach counts a stream about to drain the log. It is called while the run
// is registered, which a run is until just before its log is ended, so the
// log still holds every frame or their checkpoint.
// Attach 统计一个即将读取日志的流。它在 run 仍注册时被调用（run 一直保持注册，
// 直到其日志结束前），因此日志仍持有每个帧或它们的 checkpoint。
func (l *FrameLog) Attach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.draining++
}

// Detach ends a stream's draining of the log.
// Detach 结束一个流对日志的读取。
func (l *FrameLog) Detach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.draining--
	l.wipe()
}

// wipe zeroes and drops the frames of an ended log that nothing drains, so
// the run's plaintext leaves memory as its last stream ends rather than when
// the collector reuses it. l.mu is held.
// wipe 将没有流读取的已结束日志的帧清零并丢弃，这样 run 的明文会在其最后一个流结束时
// 离开内存，而不是等到回收器复用它时。调用时持有 l.mu。
func (l *FrameLog) wipe() {
	if !l.ended || l.draining > 0 || l.held {
		return
	}
	for _, e := range append(l.checkpoint, l.frames...) {
		clear(e.data)
	}
	for _, r := range l.retired {
		for _, frame := range r.frames {
			clear(frame)
		}
	}
	l.checkpoint, l.frames, l.retired = nil, nil, nil
}

func (l *FrameLog) hold() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = true
}

// release ends a hold, wiping the log if its run ended and nothing drains it.
// release 结束一次保留，若其 run 已结束且没有流在读取，则擦除该日志。
func (l *FrameLog) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = false
	l.wipe()
}

func (l *FrameLog) signal() {
	close(l.wake)
	l.wake = make(chan struct{})
}

// close ends the log with err, once its run has left the registry.
// close 在其 run 已离开注册表后，以 err 结束该日志。
func (l *FrameLog) close(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed == nil {
		l.closed = err
		l.signal()
	}
	l.ended = true
	l.wipe()
}

// Read is what a follower at cursor from is sent next: the frames from it,
// and the cursor of the first of them. A follower behind the frames the log
// holds is sent their checkpoint first. The frames stay intact until the
// follower calls done, as it does once it has sent them.
// Read 是位于游标 from 处的跟随者接下来会收到的内容：从该处起的帧，以及其中第一帧的游标。
// 落后于日志所持帧的跟随者会先收到它们的 checkpoint。这些帧会保持完整，直到跟随者
// 调用 done（它在发送完这些帧后就会调用）。
func (l *FrameLog) Read(from int) (checkpoint, frames [][]byte, first int, wake <-chan struct{}, closed error, done func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from < l.base {
		for _, e := range l.checkpoint {
			checkpoint = append(checkpoint, e.data)
		}
		from = l.base
	}
	for _, e := range l.frames[min(from-l.base, len(l.frames)):] {
		frames = append(frames, e.data)
	}
	folds := l.folds
	l.lent[folds]++
	done = func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.lent[folds]--; l.lent[folds] == 0 {
			delete(l.lent, folds)
		}
		l.reclaim()
	}
	return checkpoint, frames, from, l.wake, l.closed, done
}

// sent is the cursor after the last frame the log appended.
// sent 是日志追加的最后一帧之后的游标。
func (l *FrameLog) sent() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.base + len(l.frames)
}

// Log 是一个 run 的日志：其标识、其帧以及停止其自身运行的方法。
type Log struct {
	ID     string
	Frames *FrameLog
	stop   func() // cancels the run itself
}

// NewLog 以给定 id 创建一个新的 run 日志。
func NewLog(id string) *Log {
	return &Log{ID: id, Frames: newRunlog(), stop: func() {}}
}
