// Package seal is the cryptography of what the harness stores for a caller
// under the caller's content key, which the harness holds only for the
// request that carries it: the envelope a row is sealed in, and the formats
// older clients sealed rows in, which are still read; the id a key is
// registered under; the proof of possession a change to a row carries to the
// control plane; the key a caller's search index is sealed under; and the
// sealing of a shared chat.
// Package seal 是 harness 为调用方在其 content key 下所存储数据的加密实现，
// 该 content key 仅在携带它的请求期间由 harness 持有：包括行被密封所用的
// envelope，以及旧 client 密封行所用的、至今仍可读取的格式；key 注册时的 id；
// 对行的修改提交给 control plane 时携带的持有证明；调用方的 search index 被
// 密封所用的 key；以及 shared chat 的密封。
package seal
