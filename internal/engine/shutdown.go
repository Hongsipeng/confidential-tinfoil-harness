package engine

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

var errShutdown = api.Err(503, "UPSTREAM_REFUSED", "The service is shutting down")

// Stop refuses new turns, cancels every running reply, and ends every
// queued turn.
// Stop 拒绝新的 turn，取消每个运行中的回复，并结束每个排队的 turn。
func (eng *Engine) Stop() {
	eng.Live.mu.Lock()
	defer eng.Live.mu.Unlock()
	eng.Live.closing = true
	for _, s := range eng.Live.threads {
		if s.Active != nil {
			s.Active.cancelled = true
			s.Active.Log.stop()
		}
		for _, r := range s.Queue {
			eng.endQueued(r, errShutdown)
		}
		s.Queue = nil
	}
}

// Wait waits until every started run has made its last write.
// Wait 一直等待，直到每个已启动的 run 都完成了其最后一次写入。
func (eng *Engine) Wait(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		eng.Live.working.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
