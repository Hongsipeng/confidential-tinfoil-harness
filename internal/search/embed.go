// 本文件负责调用 embedding 服务：把文本分块嵌入为向量，并做量化前的批处理与分批。
package search

import (
	"context"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

// embedBatch is how many chunks one call to the embedding service
// carries, within what the inference client accepts.
// embedBatch 是单次 embedding 服务调用所携带的分块数量，在推理客户端可接受的范围内。
const embedBatch = 32

// embedChunks prefixes each chunk with the model's task and embeds them
// in batches; wait is whether the call may wait its turn at the gate.
// embedChunks 给每个分块加上模型任务前缀并分批嵌入；wait 表示调用是否可以在 gate 处排队等待。
func (s *Service) embedChunks(ctx context.Context, owner, prefix string, chunks []string, wait bool) ([][]float32, error) {
	out := make([][]float32, 0, len(chunks))
	for start := 0; start < len(chunks); start += embedBatch {
		end := min(start+embedBatch, len(chunks))
		batch := make([]string, 0, end-start)
		for _, chunk := range chunks[start:end] {
			batch = append(batch, prefix+chunk)
		}
		vectors, err := s.embed(ctx, owner, batch, wait)
		if err != nil {
			return nil, err
		}
		out = append(out, vectors...)
	}
	return out, nil
}

// embed is one call to the embedding service, admitted by the gate and
// bounded by the foreground or the background timeout.
// embed 是单次 embedding 服务调用，需经 gate 准入，并受前台或后台超时约束。
func (s *Service) embed(ctx context.Context, owner string, inputs []string, wait bool) ([][]float32, error) {
	timeout := s.timeouts.foreground
	if wait {
		timeout = s.timeouts.background
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	release, err := s.gate.acquire(ctx, owner, len(inputs), wait)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.Embedder.Embed(ctx, inputs)
}

func quantizeAll(vectors [][]float32) []searchindex.Vector {
	if len(vectors) == 0 {
		return nil
	}
	out := make([]searchindex.Vector, len(vectors))
	for i, v := range vectors {
		out[i] = searchindex.Quantize(v)
	}
	return out
}
