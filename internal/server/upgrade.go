package server

import (
	"encoding/json"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// handleUpgrade reads chats the previous web client kept on a device, or in
// an export, into the thread format, in order. It stores nothing. A batch
// holding a chat it cannot read is refused whole, so its client can tell
// which chat that is.
// handleUpgrade 按顺序把旧版 web 客户端保存在设备上或导出文件中的聊天读取为 thread 格式。它不存储任何内容。若一批数据中包含无法读取的聊天，则整批拒绝，以便客户端识别是哪个聊天有问题。
func (h *harness) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	// A signed-out tab upgrades the old chats it holds once its free key is
	// accepted.
	// 未登录的标签页在其免费 key 被接受后，升级其持有的旧聊天。
	if _, err := h.auth.Caller(r, true); err != nil {
		api.Respond(w, nil, err)
		return
	}
	body, err := api.ReadBody(w, r, domain.MaxTurnBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	var in upgradeRequest
	if err := api.DecodeBody(body, &in); err != nil || in.Threads == nil {
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "threads must be an array"))
		return
	}
	threads := make([]domain.Thread, 0, len(*in.Threads))
	for _, item := range *in.Threads {
		if !api.IsObject(item) {
			api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Each thread must be an object"))
			return
		}
		var named api.IDRequest
		json.Unmarshal(item, &named)
		if err := named.Validate(); err != nil {
			api.Respond(w, nil, err)
			return
		}
		if err := domain.ValidateLocalThread(item); err != nil {
			api.Respond(w, nil, err)
			return
		}
		// The thread is one its client can send back with a turn.
		// 该 thread 是客户端可以随 turn 一并回发的形式。
		thread, err := domain.ReadDeviceThread(named.ID, item)
		if err != nil {
			api.Respond(w, nil, err)
			return
		}
		attach.ModelCopies(&thread)
		thread.CheckWidgets()
		thread.RecordVersion = domain.RecordVersion
		threads = append(threads, thread)
	}
	api.Respond(w, struct {
		Threads []domain.Thread `json:"threads"`
	}{threads}, nil)
}
