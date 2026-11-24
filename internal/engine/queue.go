package engine

import (
	"context"
	"slices"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// CancelThread 取消聊天正在运行的回复，或移除一个排队的 turn。
func (eng *Engine) CancelThread(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *RunRequest) (any, error) {
	key, err := auth.RequestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := eng.authorizedRun(p, key, in.ThreadID, in.RunID)
	if err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if eng.removeQueued(r) {
		return struct{}{}, nil
	}
	if r.Thread.Active != r || r.ending {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "This run is no longer active")
	}
	r.cancelled = true
	r.Log.stop()
	return struct{}{}, nil
}

// Move the selected queued turn first, then finish the active run normally.
// 将选中的排队 turn 移到最前面，然后正常结束活动的 run。
func (eng *Engine) SendQueued(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *RunRequest) (any, error) {
	key, err := auth.RequestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := eng.authorizedRun(p, key, in.ThreadID, in.QueueID)
	if err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	s := r.Thread
	index := slices.Index(s.Queue, r)
	if index < 0 {
		eng.Live.mu.Unlock()
		return nil, api.Err(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	s.Queue = append([]*Run{r}, slices.Delete(s.Queue, index, index+1)...)
	queueChanged(s)
	// A reply already stopped and finishing still hands the chat on.
	// 已经停止、正在收尾的回复仍会把聊天交接下去。
	s.Active.yielded = true
	if !s.Active.ending {
		s.Active.cancelled = true
		s.Active.Log.stop()
	}
	eng.Live.mu.Unlock()
	return struct{}{}, nil
}

// RemoveQueued 移除聊天队列中的一个 turn。
func (eng *Engine) RemoveQueued(ctx context.Context, p *auth.Principal, _ auth.ContentKey, in *RunRequest) (any, error) {
	key, err := auth.RequestKey(in.Key)
	if err != nil {
		return nil, err
	}
	defer clear(key[:])
	r, err := eng.authorizedRun(p, key, in.ThreadID, in.QueueID)
	if err != nil {
		return nil, err
	}
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if !eng.removeQueued(r) {
		return nil, api.Err(409, "RUN_IN_FLIGHT", "The queued turn has started")
	}
	return struct{}{}, nil
}

// removeQueued ends a turn still waiting in its chat's queue, and reports
// whether it was waiting; live.mu is held.
// removeQueued 结束仍在聊天队列中等待的 turn，并报告它是否在等待；调用时持有 live.mu。
func (eng *Engine) removeQueued(r *Run) bool {
	s := r.Thread
	index := slices.Index(s.Queue, r)
	if index < 0 {
		return false
	}
	s.Queue = slices.Delete(s.Queue, index, index+1)
	r.cancelled = true
	eng.endQueued(r, nil)
	queueChanged(s)
	eng.dropIdle(s)
	return true
}

// StopThread stops a chat being deleted: its running reply is cancelled and
// each queued turn ends.
// StopThread 停止一个正在被删除的聊天：其运行中的回复被取消，每个排队的 turn 都会结束。
func (eng *Engine) StopThread(p *auth.Principal, id string) {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if s := eng.Live.threads[liveKey(p, id)]; s != nil {
		eng.stop(s)
	}
}

// StopDeviceThread stops a device chat its client deleted, as StopThread
// stops a chat, and drops the replies kept of it. A request that names a
// device chat carries no content key, so it stops nothing else.
// StopDeviceThread 停止客户端已删除的设备聊天，如同 StopThread 停止一个聊天那样，
// 并丢弃为其保留的回复。指定设备聊天的请求不携带 content key，因此它不会停止其他任何东西。
func (eng *Engine) StopDeviceThread(p *auth.Principal, id string) {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	if s := eng.Live.threads[liveKey(p, id)]; s != nil && isDevice(s) {
		eng.stop(s)
	}
	eng.Live.dropHeld(func(h *heldRun) bool { return h.owner == p.Scope() && h.threadID == id })
}

// StopDeviceThreads stops every device chat of the caller, as
// StopDeviceThread stops one, when the caller deletes them all. It returns
// how many chats it held anything of.
// StopDeviceThreads 在调用方删除其所有设备聊天时，如同 StopDeviceThread 停止一个那样
// 停止调用方的每一个设备聊天。它返回为其保留了内容的聊天数量。
func (eng *Engine) StopDeviceThreads(p *auth.Principal) int {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	chats := map[string]bool{}
	for _, s := range eng.Live.threads {
		if s.owner == p.Scope() && isDevice(s) {
			chats[s.ID] = true
			eng.stop(s)
		}
	}
	eng.Live.dropHeld(func(h *heldRun) bool {
		if h.owner != p.Scope() {
			return false
		}
		chats[h.threadID] = true
		return true
	})
	return len(chats)
}

// isDevice reports whether a chat is kept by its client's device, which
// sends it with each turn and needs no content key.
// isDevice 报告聊天是否由其客户端设备保存，设备会随每个 turn 发送它，并且不需要 content key。
func isDevice(s *threadSession) bool { return s.local && s.keyless }

// stop ends what a chat being deleted runs; live.mu is held.
// stop 结束正在被删除的聊天所运行的内容；调用时持有 live.mu。
func (eng *Engine) stop(s *threadSession) {
	s.deleted = true
	if s.Active != nil {
		s.Active.cancelled = true
		s.Active.Log.stop()
	}
	for _, r := range s.Queue {
		eng.endQueued(r, nil)
	}
	s.Queue = nil
	eng.dropIdle(s)
}
