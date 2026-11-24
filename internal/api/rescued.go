package api

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// Rescued logs a panic and turns it into an error worth surfacing. Without one
// of these on every goroutine the harness spawns, a panic in any single run
// takes the process, and with it every other run this enclave holds in memory.
// The panic value may contain plaintext, so only the stack is logged.
//
// It takes the recovered value rather than recovering itself, because recover
// works only in the function a defer names, which is one frame above this.
// Rescued 记录 panic 并将其转化为值得上报的错误。若 harness 启动的每个 goroutine
// 上都没有这一层防护，任何一次 run 中的 panic 都会拖垮整个进程，连同该 enclave
// 驻留内存中的其他所有 run。panic 值可能含有明文，因此只记录调用栈。
//
// 它接收已恢复的值，而不是自行 recover，因为 recover 仅在 defer 命名的函数内
// 生效，而该函数位于本函数上一帧。
func Rescued(what string, p any) error {
	if p == nil {
		return nil
	}
	slog.Error("recovered a panic", "in", what, "stack", string(debug.Stack()))
	return fmt.Errorf("%s failed", what)
}
