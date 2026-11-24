package server

import (
	"bytes"
	"cmp"
	"encoding/json"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Each route decodes its JSON body into one of these. A field of the wrong
// type refuses the request; a field the route does not read is ignored,
// except in a profile patch.
// 每个路由将其 JSON 请求体解码为这些结构体之一。字段类型错误会使请求被拒绝；路由不读取的字段会被忽略，profile patch 中的字段除外。

// listRequest 是分页列表请求。
type listRequest struct {
	Cursor    string `json:"cursor"`
	ProjectID string `json:"projectId"`
	Limit     int    `json:"limit"`
}

// windowRequest 请求一个窗口范围内的内容。
type windowRequest struct {
	ID     string `json:"id"`
	Before string `json:"before"`
	Limit  int    `json:"limit"`
}

// Validate 校验请求的 id。
func (in *windowRequest) Validate() error { return api.RequireID("id", in.ID) }

// updateThreadRequest 是更新聊天的请求。
type updateThreadRequest struct {
	domain.ThreadSource
	ID               string               `json:"id"`
	Title            *string              `json:"title"`
	Model            *string              `json:"model"`
	WebSearchEnabled *bool                `json:"webSearchEnabled"`
	Pinned           *bool                `json:"pinned"`
	ProjectID        api.Nullable[string] `json:"projectId"`
	PresetID         api.Nullable[string] `json:"presetId"`
}

// Validate 校验更新聊天请求的字段。
func (in *updateThreadRequest) Validate() error {
	if in.Local && (in.Pinned != nil || in.ProjectID.Set) {
		return api.Err(400, "BAD_REQUEST", "Pins and projects require cloud storage")
	}
	return cmp.Or(in.ProjectID.Check("projectId"), in.PresetID.Check("presetId"), api.RequireID("id", in.ID), in.ThreadSource.Validate())
}

// apply makes the settings change a chat's client asks for. Choosing another
// preset takes the settings it chooses, except those the request sets.
// apply 应用客户端请求的聊天设置变更。选择另一个 preset 会采用该 preset 的设置，但请求中显式指定的设置除外。
func (in *updateThreadRequest) apply(t *domain.Thread, c *domain.Catalog, profile *domain.Profile) {
	if in.Title != nil {
		t.Title, t.TitleState = *in.Title, "manual"
	}
	if in.PresetID.Set && domain.ValueOr(in.PresetID.Value, "") != domain.ValueOr(t.PresetID, "") {
		c.ApplyPreset(t, profile, in.PresetID.Value)
	}
	t.Model = domain.ValueOr(in.Model, t.Model)
	t.WebSearchEnabled = domain.ValueOr(in.WebSearchEnabled, t.WebSearchEnabled)
}

// messageRequest names a message of a chat; an edit sends its text.
// messageRequest 指定聊天中的一条消息；编辑操作会发送其文本。
type messageRequest struct {
	domain.ThreadSource
	ThreadID  string  `json:"threadId"`
	MessageID string  `json:"messageId"`
	Content   *string `json:"content"`
}

// Validate 校验消息请求的 threadId 与 messageId。
func (in *messageRequest) Validate() error {
	if err := api.RequireID("threadId", in.ThreadID); err != nil {
		return err
	}
	if err := api.RequireID("messageId", in.MessageID); err != nil {
		return err
	}
	return in.ThreadSource.Validate()
}

// answerRequest changes the answers the user gave in a reply's questions
// widget: each question named takes the options picked, or null to leave it
// unanswered.
// answerRequest 修改用户在回复的问题 widget 中所给的答案：每个被点名的题目采用所选选项，或传 null 表示不回答。
type answerRequest struct {
	messageRequest
	ToolCallID string               `json:"toolCallId"`
	Answers    map[string]*[]string `json:"answers"`
}

// Validate 校验答题请求。
func (in *answerRequest) Validate() error {
	if err := in.messageRequest.Validate(); err != nil {
		return err
	}
	if in.ToolCallID == "" {
		return api.Err(400, "BAD_REQUEST", "toolCallId is required")
	}
	if len(in.Answers) == 0 {
		return api.Err(400, "BAD_REQUEST", "answers must name a question")
	}
	return nil
}

// searchRequest 是搜索聊天的请求。
type searchRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

// deleteAllRequest 是批量删除的请求。
type deleteAllRequest struct {
	ProjectID *string `json:"projectId"`
	Local     bool    `json:"local"`
}

// profileUpdateRequest 是更新 profile 的请求。
type profileUpdateRequest struct {
	Patch json.RawMessage `json:"patch"`
	domain.PromptEdits
}

// Validate 校验 prompt 编辑。
func (in *profileUpdateRequest) Validate() error { return in.PromptEdits.Validate() }

// patch is the profile fields a request sets, and whether it clears the
// default prompt by setting it to null.
// patch 返回请求设置的 profile 字段，以及是否通过将默认 prompt 设为 null 来清除它。
func (in *profileUpdateRequest) patch() (domain.Profile, bool, error) {
	if in.Patch == nil {
		return domain.Profile{}, false, nil
	}
	if !api.IsObject(in.Patch) {
		return domain.Profile{}, false, api.Err(400, "BAD_REQUEST", "patch must be an object")
	}
	var patch domain.Profile
	decoder := json.NewDecoder(bytes.NewReader(in.Patch))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&patch); err != nil {
		if field, unknown := strings.CutPrefix(err.Error(), "json: unknown field "); unknown {
			return domain.Profile{}, false, api.Err(400, "BAD_REQUEST", "Unknown profile field: "+strings.Trim(field, `"`))
		}
		return domain.Profile{}, false, api.RequestError(err)
	}
	if err := patch.Validate(); err != nil {
		return domain.Profile{}, false, err
	}
	var preset struct {
		DefaultPromptPresetID api.Nullable[string] `json:"defaultPromptPresetId"`
	}
	json.Unmarshal(in.Patch, &preset)
	return patch, preset.DefaultPromptPresetID.Set && preset.DefaultPromptPresetID.Value == nil, nil
}

// profileUpgradeRequest names the settings a client keeps on its device.
// profileUpgradeRequest 表示客户端保存在其设备上的设置。
type profileUpgradeRequest struct {
	Profile json.RawMessage `json:"profile"`
}

// profileMergeRequest 是合并设备 profile 的请求。
type profileMergeRequest struct {
	profileUpgradeRequest
	ChangedFields []string `json:"changedFields"`
}

// Validate 校验 profile 必须为对象。
func (in *profileUpgradeRequest) Validate() error {
	if !api.IsObject(in.Profile) {
		return api.Err(400, "BAD_REQUEST", "profile must be an object")
	}
	return nil
}

// projectRequest 是创建或更新 project 的请求。
type projectRequest struct {
	ID                 string               `json:"id"`
	Name               *string              `json:"name"`
	Description        *string              `json:"description"`
	SystemInstructions *string              `json:"systemInstructions"`
	Color              api.Nullable[string] `json:"color"`
}

// Validate 校验 color 字段。
func (in *projectRequest) Validate() error { return in.Color.Check("color") }

// projectDocumentRequest 是操作 project 文档的请求。
type projectDocumentRequest struct {
	ProjectID  string `json:"projectId"`
	DocumentID string `json:"documentId"`
}

// Validate 校验 projectId 与 documentId。
func (in *projectDocumentRequest) Validate() error {
	if err := api.RequireID("projectId", in.ProjectID); err != nil {
		return err
	}
	return api.RequireID("documentId", in.DocumentID)
}

// upgradeRequest 是旧客户端升级聊天格式的请求。
type upgradeRequest struct {
	Threads *[]json.RawMessage `json:"threads"`
}
