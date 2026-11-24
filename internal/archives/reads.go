package archives

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// An export reads each attachment a stored chat keeps from the storage
// services. A read they fail to answer is tried again a few times; one that
// still fails leaves the attachment out, as one they no longer hold is, so a
// passing fault costs an export an attachment rather than the whole export.
// A conversations export reads its images again once its chats, which name
// them, are written, so an image read then that still fails ends it.
// Most chats keep one or two attachments, so an export reads those of a page
// of chats a few at a time, ahead of the chat it writes, and writes the chats
// in the order the page lists them.
// export 会从存储服务读取已存储 chat 持有的每个 attachment。存储服务未能应答的读取会重试几次；
// 仍失败的读取与已不再持有的 attachment 一样被略过，因此一个可自行恢复的故障只会让 export 丢失
// 一个 attachment，而不是整个 export。conversations export 会在命名这些 image 的 chat 写完后再次读取
// image，因此那时仍失败的 image 读取会终止 export。大多数 chat 只持有一两个 attachment，所以 export
// 会一次提前读取一页 chat 中的几个 attachment，赶在要写的 chat 之前，并按页列出的顺序写入 chat。

const (
	// attachmentReadAttempts bounds the reads of a stored attachment the
	// storage services fail to answer; each wait is a step longer than the
	// last.
	// attachmentReadAttempts 限制对存储服务未能应答的已存储 attachment 的读取次数；每次等待都比上次更长。
	attachmentReadAttempts = 3
	attachmentRetryStep    = 500 * time.Millisecond
	// readsAhead bounds the stored attachments an export reads at once, and
	// so the ones it holds before writing them.
	// readsAhead 限制 export 一次读取的已存储 attachment 数，即它在写入前持有的数量。
	readsAhead = 4
)

// errAttachmentUnavailable is a stored attachment the storage services did
// not serve, however often they were asked.
// errAttachmentUnavailable 表示存储服务无论被询问多少次都未提供的已存储 attachment。
var errAttachmentUnavailable = api.Err(502, "UPSTREAM_REFUSED", "The attachment could not be read")

// passing is whether a storage service's failure may pass: a refusal for
// load, or a failure or timeout of the service.
// passing 判断存储服务的故障是否可自行恢复：负载过高而拒绝，或服务故障、超时。
func passing(err error) bool {
	var e *api.Error
	return errors.As(err, &e) && (e.HTTP >= http.StatusInternalServerError || e.HTTP == http.StatusTooManyRequests)
}

// readStored reads a stored attachment, again after a failure that may pass.
// One the storage services no longer hold, or that cannot be read as its
// kind, is missing; one they keep failing to serve is unavailable.
// readStored 读取已存储 attachment，遇到可恢复的故障会重试。存储服务已不再持有或无法按其 kind 读取的
// 视为缺失；存储服务始终无法提供的视为不可用。
func readStored[T any](ctx context.Context, read func() (T, error)) (T, error) {
	for attempt := 1; ; attempt++ {
		v, err := read()
		switch {
		case err == nil:
			return v, nil
		case attach.Unreachable(err):
			return v, errAttachmentMissing
		case ctx.Err() != nil || !passing(err):
			return v, err
		case attempt == attachmentReadAttempts:
			return v, errAttachmentUnavailable
		}
		wait := time.NewTimer(time.Duration(attempt) * attachmentRetryStep)
		select {
		case <-wait.C:
		case <-ctx.Done():
			wait.Stop()
			return v, ctx.Err()
		}
	}
}

// pageReads reads the stored attachments of a page of chats ahead of the
// writer, which takes them chat by chat in the order the page lists them.
// pageReads 提前读取一页 chat 的已存储 attachment，写入者按页列出的顺序逐个 chat 取用。
type pageReads struct {
	cancel   context.CancelFunc
	image    func(*domain.Attachment) ([]byte, error)
	document func(*domain.Attachment) error
	wg       sync.WaitGroup
	// reads are in the order the page lists them; one the writer took is
	// nil, so the page no longer holds its bytes.
	// reads 按页列出的顺序排列；被写入者取走的为 nil，表示页已不再持有其字节。
	reads []*storedRead
	// at finds a read by its chat's place in the page and its attachment.
	// at 通过 chat 在页中的位置及其 attachment 找到对应读取。
	at      map[readKey]int
	started int
}

type readKey struct {
	chat int
	id   string
}

// storedRead is one attachment's read: an image's bytes, or a document's
// content set on its copy.
// storedRead 是一个 attachment 的读取结果：image 的字节，或 document 在其副本上设置的内容。
type storedRead struct {
	attachment domain.Attachment
	data       []byte
	err        error
	done       chan struct{}
}

// readAhead starts reading the attachments a page of stored chats keeps in
// the cloud: each image, and each document whose content only its blob
// holds.
// readAhead 开始读取一页已存储 chat 保存在云端的 attachment：每张 image，以及内容仅由其 blob 持有的 document。
func (ar *Service) readAhead(ctx context.Context, p *auth.Principal, chats []*domain.Thread) *pageReads {
	r := ar.newPageReads(ctx, p)
	for k, t := range chats {
		for _, m := range t.Messages {
			for _, a := range m.Attachments {
				if !a.IsImage() && (a.HasContent() || a.StorageKey() == "") {
					continue
				}
				r.add(k, a)
			}
		}
	}
	r.startBefore(readsAhead)
	return r
}

// readImagesAhead starts reading stored images again, each as the only
// attachment of the chat at its place in images.
// readImagesAhead 再次开始读取已存储 image，每张都作为 images 中对应位置 chat 的唯一 attachment。
func (ar *Service) readImagesAhead(ctx context.Context, p *auth.Principal, images []domain.Attachment) *pageReads {
	r := ar.newPageReads(ctx, p)
	for k, a := range images {
		r.add(k, a)
	}
	r.startBefore(readsAhead)
	return r
}

func (ar *Service) newPageReads(ctx context.Context, p *auth.Principal) *pageReads {
	ctx, cancel := context.WithCancel(ctx)
	return &pageReads{cancel: cancel, image: ar.cloudImage(ctx, p), document: ar.cloudDocument(ctx, p), at: map[readKey]int{}}
}

// add queues the read of the kth chat's attachment a, once.
// add 将第 k 个 chat 的 attachment a 的读取入队一次。
func (r *pageReads) add(k int, a domain.Attachment) {
	key := readKey{k, a.ID}
	if _, ok := r.at[key]; ok {
		return
	}
	r.at[key] = len(r.reads)
	r.reads = append(r.reads, &storedRead{attachment: a, done: make(chan struct{})})
}

// startBefore starts every read before the nth.
// startBefore 启动第 n 个之前的每个读取。
func (r *pageReads) startBefore(n int) {
	for ; r.started < min(n, len(r.reads)); r.started++ {
		read := r.reads[r.started]
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer close(read.done)
			if read.attachment.IsImage() {
				read.data, read.err = r.image(&read.attachment)
			} else {
				read.err = r.document(&read.attachment)
			}
		}()
	}
}

// chat is how the writer reads the attachments of the page's kth chat: from
// the reads ahead of it, or directly for one they do not hold.
// chat 返回写入者读取页中第 k 个 chat 的 attachment 的方式：取自提前读取的结果，未持有的则直接读取。
func (r *pageReads) chat(k int) (func(*domain.Attachment) ([]byte, error), func(*domain.Attachment) error) {
	image := func(a *domain.Attachment) ([]byte, error) {
		read := r.take(k, a)
		if read == nil {
			return r.image(a)
		}
		return read.data, read.err
	}
	document := func(a *domain.Attachment) error {
		read := r.take(k, a)
		if read == nil {
			return r.document(a)
		}
		a.TextContent, a.Pages = read.attachment.TextContent, read.attachment.Pages
		return read.err
	}
	return image, document
}

// take hands over an attachment's read once it is done, after starting the
// reads that follow it.
// take 在启动后续读取之后，将一个已完成 attachment 的读取结果交出。
func (r *pageReads) take(k int, a *domain.Attachment) *storedRead {
	i, ok := r.at[readKey{k, a.ID}]
	if !ok || r.reads[i] == nil {
		return nil
	}
	r.startBefore(i + readsAhead)
	read := r.reads[i]
	<-read.done
	r.reads[i] = nil
	return read
}

// close ends the reads the writer did not take.
// close 终止写入者未取走的读取。
func (r *pageReads) close() {
	r.cancel()
	r.wg.Wait()
}
