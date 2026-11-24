// Package api is what every route of the harness shares: its error, the
// action a client takes about each code, the reading of request bodies and
// writing of responses, the quota shapes, the mapping of downstream refusals,
// and the JSON helpers for shapes the harness does not own.
// Package api 是 harness 各路由共享的公共部分：错误类型、客户端针对每个错误码
// 应采取的动作、请求体的读取与响应的写出、quota 结构、下游拒绝的映射，
// 以及用于处理 harness 并不拥有的 JSON 形状的辅助函数。
package api
