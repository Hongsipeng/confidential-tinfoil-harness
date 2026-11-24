package inference

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	tinfoil "github.com/tinfoilsh/tinfoil-go"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Account calls models of the inference service under the harness's own
// account, so what it asks for bills no caller, unless its caller pays.
// Account 使用 harness 自身的账户调用推理服务的模型，因此其请求不会让任何调用者付费，除非该调用者自己支付。
type Account interface {
	Complete(ctx context.Context, c Completion) (Completed, error)
}

// Completion is one request to a model on the harness's account: a system
// prompt and one user message.
// Completion 是发往 harness 账户下某个模型的一次请求：一条 system prompt 和一条用户消息。
type Completion struct {
	// Task is what asks, which the account's log of its spending names.
	// Task 是发起请求的一方，账户的开销日志会以它来命名。
	Task         string
	Model        string
	System, User string
	MaxTokens    int64
	// Temperature is the model's own unless set.
	// 若未设置，Temperature 使用模型自身的值。
	Temperature *float64
	// Params are the request members the catalog gives the model, such as
	// its reasoning settings.
	// Params 是 catalog 提供给该模型的请求字段，例如其 reasoning 设置。
	Params api.Object
	// CacheScope partitions the enclave's prompt cache: requests that share
	// one may reuse each other's prompt, and no others.
	// CacheScope 划分 enclave 的 prompt 缓存：共享同一 CacheScope 的请求可复用彼此的 prompt，其余请求则不能。
	CacheScope string
	// CallerPays bills the completion to the caller whose key the context
	// carries, as part of the run its usage names, instead of the harness.
	// CallerPays 将本次 completion 计费给上下文所携带 key 对应的调用者，作为其用量所对应 run 的一部分，而非计费给 harness。
	CallerPays bool
}

// Completed is a model's answer to a Completion.
// Completed 是模型对一次 Completion 的应答。
type Completed struct {
	Text         string
	FinishReason string
	Usage        api.Usage
}

// Refused is a completion the inference service answered with an error
// status, and how long it asked to wait before another.
// Refused 表示推理服务以错误状态码应答的一次 completion，以及它要求等待多久后才能再次发起。
type Refused struct {
	Status     int
	RetryAfter time.Duration
	err        error
}

// Error 返回 Refused 的错误描述。
func (r *Refused) Error() string {
	return fmt.Sprintf("inference: the model refused the request with status %d: %v", r.Status, r.err)
}

// Unwrap 返回底层错误。
func (r *Refused) Unwrap() error { return r.err }

type completionService interface {
	New(context.Context, openai.ChatCompletionNewParams, ...option.RequestOption) (*openai.ChatCompletion, error)
}

type modelAccount struct {
	completions completionService
	// callers takes the completions their callers pay for.
	// callers 处理由调用者自己付费的 completion。
	callers completionService
}

// NewAccount calls models with apiKey through the Tinfoil SDK's attested,
// enclave-bound transport, and those a caller pays for through router, whose
// client signs each request with the caller's key and usage.
// NewAccount 通过 Tinfoil SDK 经 attest 且绑定 enclave 的传输层使用 apiKey 调用模型；对于调用者付费的请求则经由 router 调用，其客户端会用调用者的 key 和用量对每个请求签名。
func NewAccount(apiKey string, router *Service) (Account, error) {
	secureClient, err := tinfoil.NewClientWithOptions(
		attestedTLS,
		cacheFallback,
		tinfoil.WithOpenAIOptions(
			option.WithAPIKey(apiKey),
			option.WithMaxRetries(0),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("inference: initialize the attested account client: %w", err)
	}
	return &modelAccount{completions: &secureClient.Chat.Completions, callers: callerCompletions(router)}, nil
}

// callerCompletions takes completions through router, whose client signs
// each with the caller's key and usage.
// callerCompletions 经由 router 处理 completion，其客户端会用调用者的 key 和用量对每个请求签名。
func callerCompletions(router *Service) completionService {
	client := openai.NewClient(option.WithBaseURL(router.URL+"/v1"), option.WithHTTPClient(router.Client), option.WithMaxRetries(0))
	return &client.Chat.Completions
}

func (a *modelAccount) Complete(ctx context.Context, c Completion) (Completed, error) {
	params := openai.ChatCompletionNewParams{
		Model:               c.Model,
		MaxCompletionTokens: openai.Int(c.MaxTokens),
		Messages:            []openai.ChatCompletionMessageParamUnion{openai.SystemMessage(c.System), openai.UserMessage(c.User)},
	}
	if c.Temperature != nil {
		params.Temperature = openai.Float(*c.Temperature)
	}
	options := []option.RequestOption{option.WithJSONSet("user_cache_secret", c.CacheScope)}
	for key, value := range c.Params {
		options = append(options, option.WithJSONSet(key, value))
	}
	completions := a.completions
	if c.CallerPays {
		completions = a.callers
	}
	started := time.Now()
	response, err := completions.New(ctx, params, options...)
	if err != nil {
		var refused *openai.Error
		if errors.As(err, &refused) {
			r := &Refused{Status: refused.StatusCode, err: err}
			if refused.Response != nil {
				r.RetryAfter = time.Duration(api.RetryAfter(refused.Response.Header)) * time.Second
			}
			return Completed{}, r
		}
		return Completed{}, err
	}
	if len(response.Choices) == 0 {
		return Completed{}, errors.New("inference: the model answered with no choices")
	}
	done := Completed{Text: response.Choices[0].Message.Content, FinishReason: string(response.Choices[0].FinishReason),
		Usage: api.Usage{Prompt: response.Usage.PromptTokens, Completion: response.Usage.CompletionTokens, Total: response.Usage.TotalTokens}}
	// What each completion spends, by what spent it and who pays.
	// 每次 completion 的花销，按产生方与付费方记录。
	slog.Info("account completion", "task", c.Task, "model", c.Model, "callerPays", c.CallerPays, "promptTokens", done.Usage.Prompt, "completionTokens", done.Usage.Completion, "finishReason", done.FinishReason, "duration", time.Since(started))
	return done, nil
}
