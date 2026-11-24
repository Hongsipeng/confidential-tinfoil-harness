package archives

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"golang.org/x/sync/semaphore"
)

// Archives are spooled to the temporary directory: a tmpfs whose size the
// enclave's configuration sets, held in the confidential VM's encrypted
// memory, which has no swap device. A request takes space as the bytes it
// spools arrive. Until it holds some, it waits while other requests hold
// it; once it does, it never waits for more, since two requests could then
// each wait for the space the other holds. One that cannot grow is told to
// retry, freeing what it holds. A request that could never fit is refused
// at once, and one whose body stops arriving is ended, freeing what it
// holds.
// 归档被暂存到临时目录：这是一个 tmpfs，其大小由 enclave 的配置决定，位于机密 VM 的加密内存中，
// 没有交换设备。请求随着其暂存字节的到来而占用空间。在尚未持有任何空间之前，它会等待其他请求释放；
// 一旦持有，就绝不再等待更多，否则两个请求可能各自等待对方占用的空间。无法继续增长时会被要求重试，
// 并释放其已占用的空间。永远放不下的请求会立即被拒绝，而 body 停止传输的请求会被终止，以释放其占用的空间。

const (
	// scratchWait bounds how long a request waits for space before it is
	// told to retry.
	// scratchWait 限制请求在被告知重试之前等待空间的最长时间。
	scratchWait = 2 * time.Minute
	// scratchIdle bounds how long a request's body may send nothing.
	// scratchIdle 限制请求 body 允许不发任何数据的时长。
	scratchIdle = 30 * time.Second
)

var (
	errArchiveTooLarge = api.Err(413, "ARCHIVE_TOO_LARGE", "The archive needs more temporary space than this service has")
	errScratchBusy     = &api.Error{HTTP: 503, Code: "TEMP_SPACE_BUSY", Message: "Other archives are using this service's temporary space; retry shortly", Action: api.ActionRetry, RetryAfter: 30}
)

// Scratch 管理归档暂存空间的分配与释放。
type Scratch struct {
	Dir        string
	size       int64
	sem        *semaphore.Weighted
	Wait, Idle time.Duration
	// waiters counts requests waiting for space.
	// waiters 统计正在等待空间的请求数。
	waiters atomic.Int32
}

// Waiting is how many requests are waiting for space.
// Waiting 返回当前正在等待空间的请求数。
func (s *Scratch) Waiting() int32 { return s.waiters.Load() }

// Empty reports whether no request holds any of the space.
// Empty 报告当前是否没有请求占用任何空间。
func (s *Scratch) Empty() bool {
	if !s.sem.TryAcquire(s.size) {
		return false
	}
	s.sem.Release(s.size)
	return true
}

// NewScratch 返回一个以 dir 为目录、size 为总空间的 Scratch。
func NewScratch(dir string, size int64) *Scratch {
	return &Scratch{Dir: dir, size: size, sem: semaphore.NewWeighted(size), Wait: scratchWait, Idle: scratchIdle}
}

// sweepScratch removes the spool files a process that ended before releasing
// them left in dir.
// sweepScratch 删除 dir 中由在释放前就结束的进程遗留的暂存文件。
func sweepScratch(dir string) error {
	left, err := filepath.Glob(filepath.Join(dir, ScratchPattern))
	if err != nil {
		return err
	}
	for _, name := range left {
		if err := os.Remove(name); err != nil {
			return err
		}
	}
	return nil
}

// ScratchPattern names the spool files, so a later process can tell what
// its predecessor left from anything else in the directory.
// ScratchPattern 是暂存文件的命名模式，使后续进程能区分前一个进程遗留的文件与目录中的其他文件。
const ScratchPattern = "harness-archive-*"

// MeasuredScratch budgets the temporary directory as large as it is, once
// the spool files a previous process left in it are removed.
// MeasuredScratch 在清理上一个进程遗留的暂存文件后，将临时目录按其实际大小纳入预算。
func MeasuredScratch() (*Scratch, error) {
	dir := os.TempDir()
	if err := sweepScratch(dir); err != nil {
		return nil, err
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return nil, err
	}
	return NewScratch(dir, int64(stat.Blocks)*int64(stat.Bsize)), nil
}

// Lease is the space one request holds: what its files take.
// Lease 是一个请求持有的空间，即其文件所占用的空间。
type Lease struct {
	ctx   context.Context
	space *Scratch
	held  int64
	files []*os.File
}

// Lease starts a request's lease, which holds nothing until its files are
// written. A request expected to need more than the whole space is refused.
// Lease 启动一个请求的租约，在其文件写入前不占用任何空间。预计需要超过全部空间的请求会被拒绝。
func (s *Scratch) Lease(ctx context.Context, expected int64) (*Lease, error) {
	if expected > s.size {
		return nil, errArchiveTooLarge
	}
	return &Lease{ctx: ctx, space: s}, nil
}

// Take 为租约占用 n 字节空间，空间不足时等待，超时则返回重试错误。
func (l *Lease) Take(n int64) error {
	if l.held+n > l.space.size {
		return errArchiveTooLarge
	}
	if l.space.sem.TryAcquire(n) {
		l.held += n
		return nil
	}
	if l.held > 0 {
		return errScratchBusy
	}
	waitCtx, cancel := context.WithTimeout(l.ctx, l.space.Wait)
	defer cancel()
	l.space.waiters.Add(1)
	err := l.space.sem.Acquire(waitCtx, n)
	l.space.waiters.Add(-1)
	if err != nil {
		if l.ctx.Err() != nil {
			return l.ctx.Err()
		}
		return errScratchBusy
	}
	l.held += n
	return nil
}

// idleBody ends a request whose body sends nothing for the space's idle
// time. The returned func lifts the limit once the body is read, so it
// cannot end the request after; left in place, it also bounds the server
// reading what a refused request still sends.
// idleBody 终止那些在 idle 时长内 body 不发任何数据的请求。返回的函数在 body 读取完后解除限制，
// 以免之后又终止该请求；若保留不动，它也会限制服务器继续读取被拒绝请求仍发送的数据。
func (s *Scratch) idleBody(w http.ResponseWriter, r *http.Request) func() {
	rc := http.NewResponseController(w)
	r.Body = &idleReader{ReadCloser: r.Body, rc: rc, idle: s.Idle}
	return func() { rc.SetReadDeadline(time.Time{}) }
}

// idleReader gives each read of a body until the idle time to return.
// idleReader 让 body 的每次读取都在 idle 时长内返回。
type idleReader struct {
	io.ReadCloser
	rc   *http.ResponseController
	idle time.Duration
}

// Read 为每次读取设置 idle 截止时间后委托给底层读取。
func (r *idleReader) Read(p []byte) (int, error) {
	r.rc.SetReadDeadline(time.Now().Add(r.idle))
	return r.ReadCloser.Read(p)
}

// Release deletes the lease's files and frees its space.
// Release 删除租约的文件并释放其空间。
func (l *Lease) Release() {
	for _, f := range l.files {
		f.Close()
		os.Remove(f.Name())
	}
	l.files = nil
	l.space.sem.Release(l.held)
	l.held = 0
}

// detach takes f out of the lease, which no longer deletes it or holds its
// space: whoever takes it releases it, after the request it was written in.
// detach 将 f 从租约中移除，租约不再删除它或为其占用空间：由取走它的一方在其写入请求结束后释放。
func (l *Lease) detach(f *scratchFile) {
	l.files = slices.DeleteFunc(l.files, func(file *os.File) bool { return file == f.file })
	l.held -= f.held
}

// scratchFile is a temporary file written through its lease. It is read
// back at offsets.
// scratchFile 是通过其租约写入的临时文件，可按偏移量读回。
type scratchFile struct {
	file  *os.File
	lease *Lease
	// size is what the file holds, and held the space taken for it, which
	// a write that fell short leaves apart.
	// size 是文件持有的字节数，held 是它占用的空间，二者在写入不足时可能不一致。
	size, held int64
}

func (l *Lease) create() (*scratchFile, error) {
	f, err := os.CreateTemp(l.space.Dir, ScratchPattern)
	if err != nil {
		return nil, err
	}
	l.files = append(l.files, f)
	return &scratchFile{file: f, lease: l}, nil
}

// Write 占用相应空间后把数据写入文件。
func (f *scratchFile) Write(p []byte) (int, error) {
	if err := f.lease.Take(int64(len(p))); err != nil {
		return 0, err
	}
	f.held += int64(len(p))
	n, err := f.file.Write(p)
	f.size += int64(n)
	return n, err
}

// release deletes a detached file and frees its space.
// release 删除已脱离租约的文件并释放其空间。
func (f *scratchFile) release() {
	f.file.Close()
	os.Remove(f.file.Name())
	f.lease.space.sem.Release(f.held)
	f.held = 0
}

// ReadAt 从指定偏移量读取文件内容。
func (f *scratchFile) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= f.size {
		return 0, io.EOF
	}
	if limit := f.size - offset; int64(len(p)) > limit {
		n, err := f.file.ReadAt(p[:limit], offset)
		if err == nil {
			err = io.EOF
		}
		return n, err
	}
	return f.file.ReadAt(p, offset)
}

// Size 返回文件当前持有的字节数。
func (f *scratchFile) Size() int64 { return f.size }

// section reads length bytes of the file from offset.
// section 返回从 offset 开始读取 length 字节的读取器。
func (f *scratchFile) section(offset, length int64) *io.SectionReader {
	return io.NewSectionReader(f, offset, length)
}
