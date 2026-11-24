// Package store reads and writes what the cloud keeps for a caller: the rows of
// chats, projects, documents and profiles under the caller's content key,
// the blobs of attachments, the key registry, and the grants that let a
// run or an archive write. It holds nothing between requests but the
// summaries chat lists show.
// 包 store 读写云端为调用者保存的内容：在其 content key 下加密的 chats、projects、documents 与 profiles 的 row、
// attachment 的 blob、key 注册表，以及允许 run 或 archive 写入的授权。除 chat 列表所展示的 summary 外，
// 它在请求之间不持有任何内容。
package store
