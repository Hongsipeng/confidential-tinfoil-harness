// Package auth knows who a request is from and what it may use: the Clerk
// session, free key or API key that names the caller, the chat access token
// or key its model calls carry, the content key that opens its stored data, and
// which of its chats the safeguards flagged.
// Package auth 负责识别请求来自谁以及它可以使用的凭据：标识调用方的 Clerk
// session、free key 或 API key，其 model 调用所携带的 chat access token 或
// key，用于打开其已存储数据的 content key，以及其哪些 chat 被 safeguards 标记。
package auth
