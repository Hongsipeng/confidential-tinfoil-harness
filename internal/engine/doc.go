// Package engine carries out turns: it admits each as its chat's running
// reply or queues it behind one, builds the prompt, streams the model's
// reply and runs its tool calls, commits what the run produced, screens it,
// and holds the live runs of this instance for clients to follow, cancel and
// queue behind. It serves its own routes.
// Package engine 负责执行各个 turn：它将每个 turn 接纳为其聊天的运行中回复，或排在某一个回复之后；
// 构建 prompt，流式输出模型的回复并执行其工具调用，提交 run 的产出，对其进行审查，
// 并持有本实例的实时 run 供客户端跟随、取消和排队。它提供自己的路由。
package engine
