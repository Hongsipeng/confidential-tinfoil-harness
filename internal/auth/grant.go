package auth

import "time"

// StorageGrant is a run's or an operation's right to the stored rows for
// a while: the chat a run is for and the project its context reads, or
// the operation, the content key it was given for, and when it ends.
// StorageGrant 是 run 或 operation 在一段时间内对已存储行的访问权限：
// run 对应的 chat 及其 context 读取的 project，或者 operation 本身、
// 为其分配的 content key 以及其结束时间。
type StorageGrant struct {
	// Operation 是操作类型："" 表示 chat 的 run；"import"、"export" 或 "project-upload" 之一。
	Operation string // "" for a chat's run; "import", "export" or "project-upload"
	ChatID    string
	ProjectID string
	// KeyID 是 content key 的十六进制 id，如 seal.KeyIDHex 所给出的那样。
	KeyID     string // hex id of the content key, as seal.KeyIDHex gives it
	ExpiresAt time.Time
}
