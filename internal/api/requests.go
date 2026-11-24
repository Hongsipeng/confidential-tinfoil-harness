package api

// IDRequest names a row by id.
// IDRequest 通过 id 命名一行。
type IDRequest struct {
	ID string `json:"id"`
}

// Validate 校验 id 是否为合法标识符。
func (in *IDRequest) Validate() error { return RequireID("id", in.ID) }

// ThreadRequest names a chat by id.
// ThreadRequest 通过 id 命名一个 chat。
type ThreadRequest struct {
	ThreadID string `json:"threadId"`
}

// Validate 校验 threadId 是否为合法标识符。
func (in *ThreadRequest) Validate() error { return RequireID("threadId", in.ThreadID) }

// DeleteRequest names a row to delete, at the revision the caller read, or
// a device chat its client deleted.
// DeleteRequest 命名要删除的行（按调用方读到的 revision），或客户端已删除的设备 chat。
type DeleteRequest struct {
	IDRequest
	Revision string `json:"revision,omitempty"`
	Local    bool   `json:"local,omitempty"`
}
