package inference

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	tinfoil "github.com/tinfoilsh/tinfoil-go"
)

const (
	defaultRequestTimeout = 60 * time.Second

	// MaxBatchSize caps how many inputs one Embed call sends. The
	// inference service accepts larger batches, but bounding it here
	// keeps request bodies and worst-case latency predictable.
	// MaxBatchSize 限制单次 Embed 调用发送的输入数量。推理服务接受更大的批次，但在此处限制可让请求体和最坏情况延迟保持可预测。
	MaxBatchSize = 64
)

type embeddingService interface {
	New(context.Context, openai.EmbeddingNewParams, ...option.RequestOption) (*openai.CreateEmbeddingResponse, error)
}

// Embedder calls the embeddings endpoint through the Tinfoil SDK's
// attested, enclave-bound transport.
// Embedder 通过 Tinfoil SDK 经 attest 且绑定 enclave 的传输层调用 embeddings 端点。
type Embedder struct {
	model      string
	embeddings embeddingService
}

// NewEmbedder 使用 apiKey 和 model 构造 Embedder。
func NewEmbedder(apiKey, model string) (*Embedder, error) {
	client := &Embedder{model: model}
	if apiKey == "" || model == "" {
		return client, nil
	}
	secureClient, err := tinfoil.NewClientWithOptions(
		attestedTLS,
		tinfoil.WithOpenAIOptions(
			option.WithAPIKey(apiKey),
			option.WithRequestTimeout(defaultRequestTimeout),
			option.WithMaxRetries(0),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("inference: initialize attested embedder: %w", err)
	}
	client.embeddings = &secureClient.Embeddings
	return client, nil
}

// Configured reports whether the client has everything it needs to
// issue embedding requests. Callers use this to gate search features
// with a clean 503 instead of a confusing upstream error.
// Configured 报告客户端是否具备发起 embedding 请求所需的全部条件。调用者用它来把关搜索功能，以干净的 503 代替令人困惑的上游错误。
func (c *Embedder) Configured() bool {
	return c != nil && c.embeddings != nil && c.model != ""
}

// Model returns the embedding model identifier. The search index
// records it so a model change invalidates stored vectors instead of
// silently mixing incomparable embedding spaces.
// Model 返回 embedding 模型标识符。搜索索引会记录它，以便模型变更时使已存储的向量失效，而不是静默混用不可比较的 embedding 空间。
func (c *Embedder) Model() string {
	if c == nil {
		return ""
	}
	return c.model
}

// Embed returns one embedding vector per input, in input order.
// Embed 按输入顺序为每个输入返回一个 embedding 向量。
func (c *Embedder) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	if !c.Configured() {
		return nil, errors.New("inference: embedder not configured")
	}
	if len(inputs) == 0 {
		return nil, errors.New("inference: no inputs")
	}
	if len(inputs) > MaxBatchSize {
		return nil, fmt.Errorf("inference: batch of %d exceeds limit %d", len(inputs), MaxBatchSize)
	}
	resp, err := c.embeddings.New(ctx, openai.EmbeddingNewParams{
		Model: c.model,
		Input: openai.EmbeddingNewParamsInputUnion{
			OfArrayOfStrings: inputs,
		},
		EncodingFormat: openai.EmbeddingNewParamsEncodingFormatFloat,
	})
	if err != nil {
		return nil, errors.New("inference: embeddings request failed")
	}
	if resp == nil {
		return nil, errors.New("inference: empty embeddings response")
	}
	if len(resp.Data) != len(inputs) {
		return nil, fmt.Errorf("inference: got %d embeddings for %d inputs", len(resp.Data), len(inputs))
	}
	out := make([][]float32, len(inputs))
	for _, d := range resp.Data {
		if d.Index < 0 || d.Index >= int64(len(inputs)) {
			return nil, fmt.Errorf("inference: embedding index %d out of range", d.Index)
		}
		if len(d.Embedding) == 0 {
			return nil, errors.New("inference: empty embedding vector")
		}
		if out[int(d.Index)] != nil {
			return nil, fmt.Errorf("inference: duplicate embedding index %d", d.Index)
		}
		vector := make([]float32, len(d.Embedding))
		for i, value := range d.Embedding {
			if math.IsNaN(value) || math.IsInf(value, 0) || math.Abs(value) > math.MaxFloat32 {
				return nil, fmt.Errorf("inference: invalid embedding value at index %d", d.Index)
			}
			vector[i] = float32(value)
		}
		out[int(d.Index)] = vector
	}
	for i, v := range out {
		if v == nil {
			return nil, fmt.Errorf("inference: missing embedding for input %d", i)
		}
	}
	return out, nil
}
