package summarize

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
)

var (
	// ErrUnavailable is a summary no model could write: the task's model is
	// not in the catalog, or refused or failed every call.
	// ErrUnavailable 表示没有模型能写出该摘要：task 的模型不在 catalog 中，或每次调用都被拒绝或失败。
	ErrUnavailable = errors.New("summarize: no model could write the summary")
	// ErrInvalid is a summary that is not what its task asks for.
	// ErrInvalid 表示摘要不符合其 task 所要求的形式。
	ErrInvalid = errors.New("summarize: the summary does not have the form its task asks for")
	// ErrTooLarge is an input that does not fit its task's model.
	// ErrTooLarge 表示输入超出其 task 模型所能容纳的范围。
	ErrTooLarge = errors.New("summarize: the input does not fit the model")
)

const (
	// A call fills at most nine tenths of its model's window, since the
	// harness counts tokens the way one tokenizer does.
	// 每次调用最多占其模型窗口的十分之九，因为 harness 按某一种 tokenizer 的方式统计 token。
	windowShareTenths = 9
	// minPartTokens is the least room a part of an input needs.
	// minPartTokens 是输入的每个部分所需的最少空间。
	minPartTokens = 1024
	// documentOverhead is what the line breaks around a document's tags
	// and text cost beside them.
	// documentOverhead 是文档标签及其文本周围的换行所额外消耗的 token。
	documentOverhead = 4
	// concurrentCalls bounds the calls each role's model takes at once
	// from this instance; more wait.
	// concurrentCalls 限制本实例中各角色模型同时进行的调用数；超出则等待。
	concurrentCalls = 8
	// A failed call is tried again after retryDelay, doubling to
	// maxRetryDelay, or after the delay the model asked for.
	// 失败的调用会在 retryDelay 之后重试，延迟翻倍直至 maxRetryDelay，或按模型要求的延迟重试。
	retryDelay    = time.Second
	maxRetryDelay = 30 * time.Second
	// summaryEffort is the reasoning a model gives a summary, on a model
	// whose reasoning cannot be turned off.
	// summaryEffort 是模型为摘要投入的推理量，用于无法关闭推理的模型。
	summaryEffort = "low"
)

// Input is what a summary reads: its documents, in order, after what a
// summary of what came before them said, when one did.
// Input 是摘要读取的内容：按顺序排列的文档，前面（若有）是此前内容摘要所述。
type Input struct {
	Earlier   string
	Documents []Document
}

// Result is a summary, the model that wrote it, and how many parts its
// input was read in.
// Result 包含摘要、写出它的模型，以及其输入被拆成多少个部分读取。
type Result struct {
	Text  string
	Model string
	Parts int
}

// Service writes summaries on the harness's account, or the caller's for a
// task its caller pays for, with the model configuration names for each role.
// Service 在 harness 的账户上写摘要（对由调用方付费的 task 则在调用方账户上），并持有各角色的模型配置名称。
type Service struct {
	account inference.Account
	catalog *inference.CatalogCache
	models  map[Role]string
	slots   map[Role]chan struct{}
}

// New is a service that runs the fast role's tasks on fast and the long
// role's on long, each as the catalog describes it.
// New 返回一个 Service，它在 fast 上运行 fast 角色的 task、在 long 上运行 long 角色的 task，均按 catalog 的描述执行。
func New(account inference.Account, catalog *inference.CatalogCache, fast, long string) *Service {
	return &Service{account: account, catalog: catalog, models: map[Role]string{Fast: fast, Long: long},
		slots: map[Role]chan struct{}{Fast: make(chan struct{}, concurrentCalls), Long: make(chan struct{}, concurrentCalls)}}
}

// Summarize writes task's summary of in.
// Summarize 为 task 写出对 in 的摘要。
func (s *Service) Summarize(ctx context.Context, task *Task, in Input) (Result, error) {
	model := s.models[task.Role]
	result, err := s.summarize(ctx, task, model, in)
	if err != nil && ctx.Err() == nil {
		slog.Warn("summary failed", "task", task.Name, "model", model, "error", err)
	}
	return result, err
}

// Ready reports whether the catalog lists the model task runs on, without
// which its summaries fail.
// Ready 报告 catalog 是否列出了 task 所运行的模型；没有它，摘要就会失败。
func (s *Service) Ready(task *Task) bool {
	return s != nil && s.definition(task) != nil
}

// definition is how the catalog describes the model task runs on, or nil.
// definition 返回 catalog 对 task 所运行模型的描述，若不存在则为 nil。
func (s *Service) definition(task *Task) api.Object {
	if c := s.catalog.Get(); c != nil {
		return c.Definition(s.models[task.Role])
	}
	return nil
}

func (s *Service) summarize(ctx context.Context, task *Task, model string, in Input) (Result, error) {
	def := s.definition(task)
	if def == nil {
		return Result{}, fmt.Errorf("%w: the %s model %q is not in the catalog", ErrUnavailable, task.Role, model)
	}
	system := task.system()
	room := api.Int(def["budgetWindow"])*windowShareTenths/10 - inference.Tokens(system) - inference.Tokens(task.Closing) - int(task.MaxTokens)
	completion := inference.Completion{Task: task.Name, Model: model, System: system, MaxTokens: task.MaxTokens, Temperature: task.Temperature,
		Params: domain.ModelParams(def, false, summaryEffort), CacheScope: api.Token(), CallerPays: task.CallerPays}
	summary, docs, parts := in.Earlier, in.Documents, 0
	for parts == 0 || len(docs) > 0 {
		part, rest, err := take(docs, room-inference.Tokens(summary))
		if err == nil && len(rest) > 0 && !task.Fold {
			err = ErrTooLarge
		}
		if err != nil {
			return Result{}, err
		}
		completion.User = task.user(summary, part)
		if summary, err = s.call(ctx, task, completion); err != nil {
			return Result{}, err
		}
		docs, parts = rest, parts+1
	}
	return Result{Text: summary, Model: model, Parts: parts}, nil
}

// take is the documents that fit budget tokens, in order, and the rest. A
// first document too large on its own is split, and its first piece taken.
// take 按顺序取出能放入 budget 个 token 的文档，并返回剩余部分。若第一个文档本身就过大，则将其拆分并取第一段。
func take(docs []Document, budget int) (part, rest []Document, err error) {
	if budget < minPartTokens {
		return nil, nil, ErrTooLarge
	}
	used := 0
	for i, d := range docs {
		open, end := d.tags()
		overhead := inference.Tokens(open) + inference.Tokens(end) + documentOverhead
		cost := inference.Tokens(d.Text) + overhead
		if used+cost <= budget {
			part, used = append(part, d), used+cost
			continue
		}
		if len(part) > 0 {
			return part, docs[i:], nil
		}
		if overhead >= budget {
			return nil, nil, ErrTooLarge
		}
		pieces := split(d.Text, budget-overhead)
		first, more := d, d
		if len(pieces) < 2 {
			// Only the whitespace split leaves out made the text too large.
			// 只有 split 去掉的空白导致了文本过大。
			first.Text = strings.Join(pieces, "")
			return []Document{first}, docs[i+1:], nil
		}
		first.Text = pieces[0]
		more.Text = strings.Join(pieces[1:], "\n\n")
		more.Attrs = append(append([]Attr{}, d.Attrs...), Attr{"part", "continued"})
		return []Document{first}, append([]Document{more}, docs[i+1:]...), nil
	}
	return part, nil, nil
}

// split cuts text into pieces of at most budget tokens, between paragraphs
// where it can, between words where it cannot, and within a word larger
// than a piece, such as an encoded file, between its runes.
// split 把文本切成每段至多 budget 个 token 的片段：尽量在段落间切分，不行则在单词间切分，对于比一个片段还大的单词（如编码后的文件）则按 rune 切分。
func split(text string, budget int) []string {
	var pieces, current []string
	used := 0
	flush := func(separator string) {
		if len(current) > 0 {
			pieces = append(pieces, strings.Join(current, separator))
			current, used = nil, 0
		}
	}
	for _, paragraph := range strings.Split(text, "\n\n") {
		cost := inference.Tokens(paragraph) + 1
		if cost <= budget {
			if used+cost > budget {
				flush("\n\n")
			}
			current, used = append(current, paragraph), used+cost
			continue
		}
		flush("\n\n")
		for _, word := range strings.Fields(paragraph) {
			cost := inference.Tokens(word) + 1
			if cost > budget {
				flush(" ")
				pieces = append(pieces, chunks(word, budget-1)...)
				continue
			}
			if used+cost > budget {
				flush(" ")
			}
			current, used = append(current, word), used+cost
		}
		flush(" ")
	}
	flush("\n\n")
	return pieces
}

// chunks cuts s between runes into chunks of at most limit bytes, so of at
// most limit tokens, since no token is shorter than a byte. A chunk holds a
// rune even when the rune is longer than limit.
// chunks 在 rune 之间把 s 切成每块至多 limit 字节（因而也至多 limit 个 token，因为 token 不会短于一个字节）。即使某个 rune 比 limit 长，也会完整保留在一个块中。
func chunks(s string, limit int) []string {
	var cut []string
	limit = max(1, limit)
	for len(s) > limit {
		at := limit
		for at > 0 && !utf8.RuneStart(s[at]) {
			at--
		}
		if at == 0 {
			_, at = utf8.DecodeRuneInString(s)
		}
		cut, s = append(cut, s[:at]), s[at:]
	}
	return append(cut, s)
}

// call asks for one summary, again after a failure worth retrying, up to
// the task's attempts.
// call 请求一次摘要，在值得重试的失败后再次请求，最多不超过 task 的 attempts 次。
func (s *Service) call(ctx context.Context, task *Task, c inference.Completion) (string, error) {
	var err error
	for attempt := range max(1, task.Attempts) {
		if attempt > 0 {
			select {
			case <-time.After(delayAfter(attempt, err)):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		var summary string
		if summary, err = s.attempt(ctx, task, c); err == nil {
			return summary, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if !retryable(err) {
			return "", err
		}
	}
	return "", err
}

func (s *Service) attempt(ctx context.Context, task *Task, c inference.Completion) (string, error) {
	slots := s.slots[task.Role]
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-slots }()
	callCtx, cancel := context.WithTimeout(ctx, task.Timeout)
	defer cancel()
	done, err := s.account.Complete(callCtx, c)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return task.check(done.Text, done.FinishReason)
}

// retryable is a failure another call may not repeat: a summary that failed
// its check, a refusal for load, a call that timed out, or a connection
// that failed. Any other refusal would be refused again, and so would a
// call its caller's chat access could not be renewed for.
// retryable 表示另一次调用可能不会再重现的失败：未通过 check 的摘要、因负载而拒绝、调用超时或连接失败。其他拒绝会被再次拒绝，无法续期调用方 chat 访问的调用同样如此。
func retryable(err error) bool {
	var refused *inference.Refused
	if errors.As(err, &refused) {
		return refused.Status == 429 || refused.Status >= 500
	}
	var access *api.Error
	return !errors.As(err, &access)
}

func delayAfter(attempt int, err error) time.Duration {
	var refused *inference.Refused
	if errors.As(err, &refused) && refused.RetryAfter > 0 {
		return min(refused.RetryAfter, maxRetryDelay)
	}
	return min(retryDelay<<(attempt-1), maxRetryDelay)
}
