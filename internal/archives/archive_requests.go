package archives

import (
	"encoding/json"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// ShareRequest shares a stored chat by its key, or a device chat by the
// thread its client sends.
// ShareRequest 按 key 分享已存储的聊天，或按客户端发送的 thread 分享设备上的聊天。
type ShareRequest struct {
	api.KeyRequest
	ThreadID string          `json:"threadId"`
	Local    bool            `json:"local"`
	Thread   json.RawMessage `json:"thread"`
}

// Validate 校验分享请求：threadId 必填，本地 thread 还需携带 transcript 并通过校验。
func (in *ShareRequest) Validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if !in.Local {
		return nil
	}
	if !api.IsObject(in.Thread) {
		return api.Err(400, "BAD_REQUEST", "A local thread requires its transcript")
	}
	return domain.ValidateLocalThread(in.Thread)
}

type openShareRequest struct {
	ShareID  string `json:"shareId"`
	Fragment string `json:"fragment"`
}

// Validate 校验打开分享请求的 shareId。
func (in *openShareRequest) Validate() error { return api.RequireID("shareId", in.ShareID) }

type importStatusRequest struct {
	JobID string `json:"jobId"`
}

// Validate 校验导入状态请求的 jobId。
func (in *importStatusRequest) Validate() error { return api.RequireID("jobId", in.JobID) }
