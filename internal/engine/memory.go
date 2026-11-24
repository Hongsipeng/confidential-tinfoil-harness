package engine

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// The harness shares the enclave's memory with the sidecars beside it and
// the archives' in-memory /tmp, and the enclave has no swap. A new turn is
// refused while little memory is left, so the replies already running can
// finish rather than the enclave running out under them.
// harness 与其旁边的 sidecar 以及 archives 的内存 /tmp 共享 enclave 的内存，且 enclave
// 没有交换分区。当剩余内存很少时会拒绝新的 turn，这样已经运行中的回复可以完成，
// 而不会让 enclave 在它们之下耗尽内存。
const (
	// memoryReserveShare is the share of the enclave's memory, as one part
	// in this many, that new turns leave free.
	// memoryReserveShare 是新的 turn 所留出的空闲内存占 enclave 内存的比例（为整体的多少分之一）。
	memoryReserveShare = 8
	// busyRetryAfterSeconds is when a turn refused for memory is worth
	// sending again.
	// busyRetryAfterSeconds 是因内存而被拒绝的 turn 值得重新发送的等待秒数。
	busyRetryAfterSeconds = 15
	kibibyte              = 1024
)

var errBusy = &api.Error{HTTP: 503, Code: "SERVICE_BUSY", Message: "The chat service is busy; retry shortly", Action: api.ActionRetry, RetryAfter: busyRetryAfterSeconds}

// Memory reports the memory available in the enclave and its total, in
// bytes.
// Memory 报告 enclave 中可用的内存及其总量，单位为字节。
type Memory func() (available, total uint64, err error)

// SystemMemory reads the enclave's memory from /proc/meminfo, which counts
// every container in it.
// SystemMemory 从 /proc/meminfo 读取 enclave 的内存，该文件统计了其中的每个容器。
func SystemMemory() (available, total uint64, err error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	return readMeminfo(f)
}

// readMeminfo reads MemAvailable and MemTotal, which /proc/meminfo gives in
// kibibytes. Either missing is an error: a reading of no available memory
// would refuse every turn.
// readMeminfo 读取 MemAvailable 和 MemTotal，/proc/meminfo 以 kibibyte 为单位给出它们。
// 任一个缺失都算错误：读到没有可用内存会拒绝每个 turn。
func readMeminfo(r io.Reader) (available, total uint64, err error) {
	read := map[string]uint64{}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		if value, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
			read[fields[0]] = value * kibibyte
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, err
	}
	available, hasAvailable := read["MemAvailable:"]
	total, hasTotal := read["MemTotal:"]
	if !hasAvailable || !hasTotal || total == 0 {
		return 0, 0, errors.New("meminfo names no MemAvailable or MemTotal")
	}
	return available, total, nil
}

// lowOnMemory reports whether the enclave has too little memory left to
// start another turn. Memory it cannot read refuses nothing.
// lowOnMemory 报告 enclave 是否剩余内存过少而无法再启动一个 turn。
// 无法读取的内存不会拒绝任何内容。
func (eng *Engine) lowOnMemory() bool {
	if eng.Memory == nil {
		return false
	}
	available, total, err := eng.Memory()
	return err == nil && available < total/memoryReserveShare
}
