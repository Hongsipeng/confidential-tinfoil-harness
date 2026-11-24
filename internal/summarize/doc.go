// Package summarize writes the summaries the harness uses, each for a Task:
// a chat's title, a label of what a reply is reasoning about, the summary
// that stands in for a compacted chat's older messages, and the notes that
// stand in for what a research read. A task names the model role it runs
// on, how its input is framed, what its summary holds and how that is
// checked. A title or label runs on the harness's own account, so it bills
// no caller; a compaction or research notes bill the caller whose run asks
// for them.
// summarize 包生成 harness 所用的各种摘要，每一种对应一个 Task：chat 的标题、回复正在推理的内容标签、替代被 compact 的 chat 中较早消息的摘要，以及替代 research 所读内容的笔记。一个 task 会指定它运行的模型角色、输入的包装方式、摘要的内容以及校验方式。标题和标签在 harness 自己的账户上运行，因此不向任何调用方计费；compaction 或 research 笔记则向发起该 run 的调用方计费。
package summarize
