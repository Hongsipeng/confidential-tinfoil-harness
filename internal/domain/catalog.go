// 本文件定义模型、preset 与 widget 的目录。
package domain

import (
	"cmp"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sync"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

//go:embed widgets.json
var widgetJSON []byte

//go:embed presets.json
var presetJSON []byte

//go:embed prompt.json
var promptJSON []byte

// chatPrompt is the system prompt every chat is built from. It is part of the
// measured image; the only text the control plane adds to it is the model's
// name from the catalog.
// chatPrompt 是每个聊天所依据的系统 prompt。它是受度量镜像的一部分；控制平面唯一附加
// 给它的文本是 catalog 中的模型名称。
type chatPrompt struct {
	SystemPrompt string `json:"systemPrompt"`
	Rules        string `json:"rules"`
	GenUI        struct {
		Header         string   `json:"header"`
		EnabledWidgets []string `json:"enabledWidgets"`
	} `json:"genUI"`
}

// BuiltInPrompt is the compiled system prompt, read once.
// BuiltInPrompt 是编译后的系统 prompt，只读取一次。
var BuiltInPrompt = sync.OnceValue(func() chatPrompt {
	var prompt chatPrompt
	if json.Unmarshal(promptJSON, &prompt) != nil || prompt.SystemPrompt == "" {
		panic("invalid compiled system prompt")
	}
	return prompt
})

// Preset is a built-in prompt the clients offer.
// Preset 是客户端提供的内置 prompt。
type Preset struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	SystemPrompt string `json:"systemPrompt"`
	Icon         string `json:"icon"`
	BuiltIn      bool   `json:"builtIn"`
}

// BuiltInPresets 返回全部内置 preset。
func BuiltInPresets() []Preset {
	var presets []Preset
	json.Unmarshal(presetJSON, &presets)
	return presets
}

// Widget 是控制平面配置的一个 UI 组件。
type Widget struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
	Hint        string          `json:"promptHint"`

	Resolved *jsonschema.Resolved // what a call's arguments are validated against
}

// WidgetCatalog is the compiled widget declarations, read once.
// WidgetCatalog 是编译后的 widget 声明，只读取一次。
var WidgetCatalog = sync.OnceValue(func() []Widget {
	var widgets []Widget
	if json.Unmarshal(widgetJSON, &widgets) != nil {
		panic("invalid compiled widgets")
	}
	for i := range widgets {
		resolved, err := resolveSchema(widgets[i].Schema)
		if err != nil {
			panic("invalid compiled widget schema: " + err.Error())
		}
		widgets[i].Resolved = resolved
	}
	return widgets
})

// Declaration 返回该 widget 的工具声明。
func (w Widget) Declaration() json.RawMessage {
	return api.Raw(api.Object{"type": "function", "function": api.Object{"name": w.Name, "description": w.Description, "parameters": w.Schema}})
}

// Catalog 是控制平面提供的模型与 widget 目录。
type Catalog struct {
	Models                      []api.Object
	Deprecated                  []api.Object
	Listed                      []string // every chat model the control plane lists, offered or not
	Prompt, Rules, WidgetHeader string
	EnabledWidgets              []string
	Presets                     []Preset
}

// Definition 按模型名返回其定义。
func (c *Catalog) Definition(name string) api.Object {
	for _, m := range c.Models {
		if api.Str(m["modelName"]) == name {
			return m
		}
	}
	return nil
}

// DisplayName is what clients call a configured model, if anything.
// DisplayName 是客户端调用某个已配置模型时使用的名称（如有）。
func (c *Catalog) DisplayName(name string) string {
	return api.Str(c.Definition(name)["name"])
}

// Vision reports whether a model reads images. The router gives an Auto turn
// that carries images to a multimodal model.
// Vision 报告模型是否能读取图像。router 会把携带图像的 Auto turn 交给多模态模型。
func (c *Catalog) Vision(name string) bool {
	return name == "auto" || api.Bool(c.Definition(name)["multimodal"], false)
}

// Offers reports whether a chat may run on a model now.
// Offers 报告某个聊天现在是否能在某模型上运行。
func (c *Catalog) Offers(name string) bool {
	if name == AutoModelName {
		return true
	}
	definition := c.Definition(name)
	return definition != nil && !autoOnly(definition)
}

// autoOnly reports whether a model is configured as not paid: it serves free
// keys behind Auto, and no chat chooses it.
// autoOnly 报告模型是否被配置为未付费：它在 Auto 背后服务于免费密钥，没有聊天会选择它。
func autoOnly(definition api.Object) bool {
	return !api.Bool(definition["paid"], true)
}

// ApplyPreset files a chat under a prompt preset, or none, and takes the
// model and web search setting a saved preset chooses. A model that is not
// offered is not taken.
// ApplyPreset 将聊天归入某个 prompt preset（或不归入任何），并采用已保存 preset 选择的
// 模型与网络搜索设置。未提供的模型不会被采用。
func (c *Catalog) ApplyPreset(t *Thread, profile *Profile, id *string) {
	t.PresetID = id
	if id == nil {
		return
	}
	saved := profile.Preset(*id)
	if saved == nil {
		return
	}
	if model := presetModel(ValueOr(saved.Model, "")); model != "" && c.Offers(model) {
		t.Model = model
	}
	t.WebSearchEnabled = ValueOr(saved.WebSearchEnabled, t.WebSearchEnabled)
}

// retired reports whether the control plane no longer lists a chat model.
// retired 报告控制平面是否已不再列出某个聊天模型。
func (c *Catalog) retired(name string) bool {
	return name != AutoModelName && c.Definition(name) == nil && !slices.Contains(c.Listed, name)
}

// AutoModelName is the model that has the router choose one per turn.
// AutoModelName 是让 router 在每一 turn 选择模型的模型名。
const AutoModelName = "auto"

// legacyAutoModels are the Auto tiers of an earlier design, which old chats
// and settings name.
// legacyAutoModels 是早期设计中的 Auto 档位，旧聊天与旧设置会用到这些名称。
var legacyAutoModels = []string{"auto-smart", "auto-fast"}

// presetModel is the model a saved prompt names: an earlier design's Auto
// tier, as an older client may still write one, is Auto.
// presetModel 是已保存 prompt 所指的模型：早期设计的 Auto 档位（旧客户端仍可能写入）
// 一律视为 Auto。
func presetModel(name string) string {
	if slices.Contains(legacyAutoModels, name) {
		return AutoModelName
	}
	return name
}

func normalizeModel(name string) string {
	if name == "" || slices.Contains(legacyAutoModels, name) {
		return AutoModelName
	}
	return name
}

// ReasoningEfforts 是模型推理力度的可选值。
var ReasoningEfforts = []string{"low", "medium", "high"}

// IntelligenceLevel is one setting of how hard Auto works, as clients label
// it alone and as the model a chat runs on.
// IntelligenceLevel 是 Auto 工作力度的一种设置，客户端用它单独标注，也用它标注聊天所运行的模型。
type IntelligenceLevel struct {
	ID           string `json:"id"`
	Label        string `json:"label"`
	DisplayLabel string `json:"displayLabel"`
}

var intelligenceLevels = []IntelligenceLevel{{ID: "instant", Label: "Instant"}, {ID: "low", Label: "Low"}, {ID: "medium", Label: "Med"}, {ID: "high", Label: "High"}, {ID: "extra", Label: "Extra"}, {ID: "max", Label: "Max"}}

// autoLabel is what clients call Auto.
// autoLabel 是客户端对 Auto 的称呼。
const autoLabel = "Auto"

// AutoLevels are Auto's levels, each labeled as the model a chat runs on.
// AutoLevels 是 Auto 的各个档位，每个都标注为聊天所运行的模型。
func AutoLevels() []IntelligenceLevel {
	levels := slices.Clone(intelligenceLevels)
	for i := range levels {
		levels[i].DisplayLabel = autoLabel + " · " + levels[i].Label
	}
	return levels
}

func intelligenceIDs() []string {
	ids := []string{}
	for _, l := range intelligenceLevels {
		ids = append(ids, l.ID)
	}
	return ids
}

// Intelligence 返回某推理力度对应的数值档位。
func Intelligence(level string) (int, error) {
	if level == "" {
		level = "high"
	}
	for i, l := range intelligenceLevels {
		if l.ID == level {
			return i * 20, nil
		}
	}
	return 0, api.Err(400, "BAD_REQUEST", "Unknown Auto intelligence level")
}

// modelConfig is what a client shows of a model the control plane configures.
// modelConfig 是客户端展示的、控制平面所配置模型的信息。
type modelConfig struct {
	ModelName       string  `json:"modelName"`
	Name            string  `json:"name"`
	NameShort       string  `json:"nameShort"`
	Image           string  `json:"image"`
	Experimental    bool    `json:"experimental"`
	Deprecated      bool    `json:"deprecated"`
	DeprecationDate *string `json:"deprecationDate"`
	ChatConfig      struct {
		DescriptionShort string `json:"descriptionShort"`
		ReasoningConfig  *struct {
			SupportsToggle bool  `json:"supportsToggle"`
			SupportsEffort bool  `json:"supportsEffort"`
			DefaultEnabled *bool `json:"defaultEnabled"`
		} `json:"reasoningConfig"`
	} `json:"chatConfig"`
}

// SessionModel is a chat model as a client offers it, every field ready to
// show. Auto is the entry that has the router choose a model for each turn.
// SessionModel 是客户端提供的聊天模型，所有字段都已就绪可直接展示。Auto 这一项表示由
// router 在每一 turn 选择模型。
type SessionModel struct {
	ID          string           `json:"id"`
	Label       string           `json:"label"`
	ShortLabel  string           `json:"shortLabel"`
	Icon        string           `json:"icon"`
	Description string           `json:"description"`
	Badges      []lifecycleBadge `json:"badges"`
	Reasoning   *reasoningView   `json:"reasoning"`
	Deprecated  bool             `json:"deprecated"`
	Auto        bool             `json:"auto,omitempty"`
}

type reasoningView struct {
	Toggle         bool `json:"toggle"`
	Effort         bool `json:"effort"`
	DefaultEnabled bool `json:"defaultEnabled"`
}

// lifecycleBadge marks a model that may go away.
// lifecycleBadge 标记可能下线的模型。
type lifecycleBadge struct {
	Kind    string `json:"kind"`
	Label   string `json:"label"`
	Tooltip string `json:"tooltip"`
}

// 以下是模型徽章的提示文案。
const (
	ExperimentalTooltip   = "Support and availability are not guaranteed. This model can be deprecated at any time."
	deprecatedTooltip     = "This model is deprecated. An offline date has not been announced."
	deprecatedDateTooltip = "This model will be taken offline on %s."
)

// DisplayModels are the models a client offers, Auto first.
// DisplayModels 是客户端提供的模型列表，Auto 排在最前。
func (c *Catalog) DisplayModels() []SessionModel {
	models := []SessionModel{{ID: AutoModelName, Label: autoLabel, ShortLabel: autoLabel, Badges: []lifecycleBadge{}, Auto: true}}
	for _, m := range append(slices.Clone(c.Models), c.Deprecated...) {
		if autoOnly(m) {
			continue
		}
		var config modelConfig
		readLenient(api.Raw(m), &config)
		display := SessionModel{ID: config.ModelName, Label: config.Name, ShortLabel: cmp.Or(config.NameShort, config.Name), Icon: config.Image, Description: config.ChatConfig.DescriptionShort,
			Deprecated: config.Deprecated, Badges: []lifecycleBadge{}}
		if reason := config.ChatConfig.ReasoningConfig; reason != nil {
			display.Reasoning = &reasoningView{Toggle: reason.SupportsToggle, Effort: reason.SupportsEffort, DefaultEnabled: ValueOr(reason.DefaultEnabled, true)}
		}
		if config.Experimental {
			display.Badges = append(display.Badges, lifecycleBadge{"experimental", "Experimental", ExperimentalTooltip})
		}
		if config.Deprecated {
			tooltip := deprecatedTooltip
			if date := ValueOr(config.DeprecationDate, ""); date != "" {
				tooltip = fmt.Sprintf(deprecatedDateTooltip, date)
			}
			display.Badges = append(display.Badges, lifecycleBadge{"deprecated", "Deprecated", tooltip})
		}
		models = append(models, display)
	}
	return models
}

// SelectedWidgets 返回请求的 widget 中已启用的那些。
func (c *Catalog) SelectedWidgets(requested []string, enabled bool) []Widget {
	result := []Widget{}
	if !enabled {
		return result
	}
	for _, widget := range WidgetCatalog() {
		if slices.Contains(c.EnabledWidgets, widget.Name) && slices.Contains(requested, widget.Name) {
			result = append(result, widget)
		}
	}
	return result
}

// reasoningSetting is a word a model's reasoning effort is written in. The
// model catalog comes from outside the enclave, and a chat template writes
// these settings into the prompt, so nothing that could be an instruction is
// sent.
// reasoningSetting 是模型推理力度所用的单词。模型 catalog 来自 enclave 之外，聊天模板会
// 把这些设置写入 prompt，因此不会发送任何可能被当作指令的内容。
var reasoningSetting = regexp.MustCompile(`^[a-z]{1,16}$`)

// ModelParams are the reasoning settings a model's configuration asks for:
// whether it thinks and how hard, and nothing else.
// ModelParams 是模型配置所需的推理设置：是否思考以及思考的力度，仅此而已。
func ModelParams(def api.Object, thinking bool, effort string) api.Object {
	params := api.Object{}
	reason := api.Obj(api.Obj(def["chatConfig"])["reasoningConfig"])
	mode := "enable"
	if api.Bool(reason["supportsToggle"], false) && !thinking {
		mode = "disable"
	}
	if effort == "" || !api.Bool(reason["supportsEffort"], false) {
		effort = "medium"
	}
	effort = api.FirstString(api.Obj(reason["effortMap"])[effort], effort)
	block := api.Obj(substituteEffort(api.Obj(api.Obj(reason["params"])["/v1/chat/completions"])[mode], effort))
	if value, ok := block["reasoning_effort"].(string); ok && reasoningSetting.MatchString(value) {
		params["reasoning_effort"] = value
	}
	switches := api.Object{}
	for k, v := range api.Obj(block["chat_template_kwargs"]) {
		switch value := v.(type) {
		case bool:
			if k == "enable_thinking" || k == "thinking" {
				switches[k] = value
			}
		case string:
			if k == "reasoning_effort" && reasoningSetting.MatchString(value) {
				switches[k] = value
			}
		}
	}
	if len(switches) > 0 {
		params["chat_template_kwargs"] = switches
	}
	return params
}
func substituteEffort(value any, effort string) any {
	switch v := value.(type) {
	case string:
		if v == "$EFFORT" {
			return effort
		}
		return v
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = substituteEffort(item, effort)
		}
		return out
	case map[string]any:
		out := api.Object{}
		for k, item := range v {
			out[k] = substituteEffort(item, effort)
		}
		return out
	case api.Object:
		out := api.Object{}
		for k, item := range v {
			out[k] = substituteEffort(item, effort)
		}
		return out
	}
	return value
}
