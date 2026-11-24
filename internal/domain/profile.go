// 本文件定义用户 profile 及其读写、校验与合并。
package domain

import (
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// Profile is a user's settings: what the clients show and a prompt reads.
// Every field is optional, so a patch sets exactly the fields it names.
// Profile 是用户的设置：客户端展示的内容以及 prompt 读取的内容。每个字段都是可选的，
// 因此 patch 只会设置其指明的字段。
type Profile struct {
	ThemeMode                        *string         `json:"themeMode,omitempty"`
	ChatFont                         *string         `json:"chatFont,omitempty"`
	Language                         *string         `json:"language,omitempty"`
	Nickname                         *string         `json:"nickname,omitempty"`
	Profession                       *string         `json:"profession,omitempty"`
	Traits                           []string        `json:"traits,omitzero"`
	AdditionalContext                *string         `json:"additionalContext,omitempty"`
	IsUsingPersonalization           *bool           `json:"isUsingPersonalization,omitempty"`
	IsUsingCustomPrompt              *bool           `json:"isUsingCustomPrompt,omitempty"`
	CustomSystemPrompt               *string         `json:"customSystemPrompt,omitempty"`
	CustomPromptPresets              []CustomPreset  `json:"customPromptPresets,omitzero"`
	FavoritePromptPresetIDs          []string        `json:"favoritePromptPresetIds,omitzero"`
	DefaultPromptPresetID            *string         `json:"defaultPromptPresetId,omitempty"`
	SelectedModel                    *string         `json:"selectedModel,omitempty"`
	AutoIntelligence                 *string         `json:"autoIntelligence,omitempty"`
	ReasoningEffort                  *string         `json:"reasoningEffort,omitempty"`
	ThinkingEnabled                  *bool           `json:"thinkingEnabled,omitempty"`
	WebSearchEnabled                 *bool           `json:"webSearchEnabled,omitempty"`
	WebSearchAvailable               *bool           `json:"webSearchAvailable,omitempty"`
	PIICheckEnabled                  *bool           `json:"piiCheckEnabled,omitempty"`
	GenUIEnabled                     *bool           `json:"genUIEnabled,omitempty"`
	PixelateSidebarChatTitlesEnabled *bool           `json:"pixelateSidebarChatTitlesEnabled,omitempty"`
	BrowserTabChatTitleEnabled       *bool           `json:"browserTabChatTitleEnabled,omitempty"`
	EnterToNewlineEnabled            *bool           `json:"enterToNewlineEnabled,omitempty"`
	ReadAloudSpeed                   *float64        `json:"readAloudSpeed,omitempty"`
	HasSeenOnboarding                *bool           `json:"hasSeenOnboarding,omitempty"`
	HasSeenWebSearchIntro            *bool           `json:"hasSeenWebSearchIntro,omitempty"`
	Dismissed                        map[string]bool `json:"dismissed,omitzero"`
}

// CustomPreset is a prompt the user saved. Its timestamps are kept as the
// client wrote them; one stored without a number for either is dated then.
// CustomPreset 是用户保存的 prompt。其时间戳按客户端写入的保留；任一字段没有数字时，
// 则按当时日期记录。
type CustomPreset struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	Description      *string         `json:"description,omitempty"`
	SystemPrompt     *string         `json:"systemPrompt,omitempty"`
	Model            *string         `json:"model,omitempty"`
	WebSearchEnabled *bool           `json:"webSearchEnabled,omitempty"`
	CreatedAt        json.RawMessage `json:"createdAt,omitempty"`
	UpdatedAt        json.RawMessage `json:"updatedAt,omitempty"`
}

// StoredProfile is the profile row.
// StoredProfile 是 profile row。
type StoredProfile struct {
	Profile
	UpdatedAt     string `json:"updatedAt,omitempty"`
	RecordVersion int    `json:"recordVersion,omitempty"`
	// PinnedChatIDs makes favorites available outside the loaded history pages.
	// PinnedChatIDs 让收藏在已加载的历史页之外也可用。
	PinnedChatIDs []string `json:"pinnedChatIds,omitzero"`
	// Others are the row's members this harness does not name, such as the
	// edit clocks the apps' own profile sync keeps. A write keeps them.
	// Others 是本 harness 未命名的 row 成员，例如应用自身 profile 同步所保留的编辑时钟。
	// 写入时会保留它们。
	Others map[string]json.RawMessage `json:"-"`
}

// storedProfileFields is every member of the row a StoredProfile names.
// storedProfileFields 是 StoredProfile 所命名的 row 的全部成员。
var storedProfileFields = jsonFields(reflect.TypeFor[StoredProfile]())

func jsonFields(t reflect.Type) map[string]bool {
	fields := map[string]bool{}
	for i := range t.NumField() {
		field := t.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		switch {
		case field.Anonymous && name == "":
			maps.Copy(fields, jsonFields(field.Type))
		case name != "" && name != "-":
			fields[name] = true
		}
	}
	return fields
}

// otherMembers are the members of a profile row a StoredProfile does not name.
// otherMembers 是 StoredProfile 未命名的 profile row 成员。
func otherMembers(data []byte) map[string]json.RawMessage {
	var members map[string]json.RawMessage
	json.Unmarshal(data, &members)
	maps.DeleteFunc(members, func(name string, _ json.RawMessage) bool { return storedProfileFields[name] })
	if len(members) == 0 {
		return nil
	}
	return members
}

// MarshalJSON writes the row with the members it does not name kept.
// MarshalJSON 写出 row，并保留其未命名的成员。
func (p StoredProfile) MarshalJSON() ([]byte, error) {
	type row StoredProfile
	data, err := json.Marshal(row(p))
	if err != nil || len(p.Others) == 0 {
		return data, err
	}
	members := map[string]json.RawMessage{}
	json.Unmarshal(data, &members)
	for name, value := range p.Others {
		if !storedProfileFields[name] {
			members[name] = value
		}
	}
	return json.Marshal(members)
}

// The apps' own profile sync merges the row field by field. It keeps an edit
// clock {v, w} for each field in fieldClocks, and trusts the clocks only while
// clockVersion is the row's revision. A write that leaves them stale makes
// those apps refuse to merge any field both they and the write changed.
// 应用自身的 profile 同步会逐字段合并 row。它在 fieldClocks 中为每个字段保留一个编辑时钟
// {v, w}，且仅在 clockVersion 等于 row 的修订号时信任这些时钟。若某次写入使它们过期，
// 应用会拒绝合并它与该写入都改动的任何字段。
const (
	fieldClocksMember  = "fieldClocks"
	clockVersionMember = "clockVersion"
	versionMember      = "version"
	clockWriter        = "harness"
	// maxClock is the largest counter those apps keep, the largest integer a
	// JavaScript number holds exactly.
	// maxClock 是那些应用所保留的最大计数，即 JavaScript 数字可精确表示的最大整数。
	maxClock = 1<<53 - 1
)

// PrepareWrite readies a changed profile to be stored over data, the row it
// was read from, as revision, the revision the write creates or 0 when it is
// not known. Its saved prompts get every member the apps' own profile sync
// requires, and each field the write changes takes an edit clock past every
// one the row holds, so that sync can merge it.
// PrepareWrite 准备好变更后的 profile，以便存到 data 之上——data 是它读取自的 row，
// revision 是写入所创建的修订号（未知时为 0）。其已保存 prompt 会补齐应用自身 profile
// 同步所需的所有成员；写入所改动的每个字段都会取得超过 row 中所有时钟的编辑时钟，使该
// 同步能够合并它。
func (p *StoredProfile) PrepareWrite(data json.RawMessage, revision int, now time.Time) {
	p.completePresets(now)
	p.UpdatedAt = now.Format(time.RFC3339Nano)
	p.RecordVersion = RecordVersion
	p.stampEdits(data, revision)
}

func (p *StoredProfile) stampEdits(data json.RawMessage, revision int) {
	type row StoredProfile
	var was, is map[string]any
	json.Unmarshal(data, &was)
	json.Unmarshal(api.Raw(row(*p)), &is)
	var changed []string
	for name := range storedProfileFields {
		if name != "updatedAt" && name != "recordVersion" && !reflect.DeepEqual(was[name], is[name]) {
			changed = append(changed, name)
		}
	}
	if p.Others == nil {
		p.Others = map[string]json.RawMessage{}
	}
	if len(changed) > 0 {
		clocks := map[string]json.RawMessage{}
		json.Unmarshal(p.Others[fieldClocksMember], &clocks)
		latest := int64(0)
		for _, raw := range clocks {
			var clock struct {
				V float64 `json:"v"`
			}
			if json.Unmarshal(raw, &clock) == nil {
				latest = max(latest, int64(clock.V))
			}
		}
		tick := api.Raw(api.Object{"v": min(latest+1, maxClock), "w": clockWriter})
		for _, name := range changed {
			clocks[name] = tick
		}
		p.Others[fieldClocksMember] = api.Raw(clocks)
	}
	if revision > 0 {
		p.Others[clockVersionMember], p.Others[versionMember] = api.Raw(revision), api.Raw(revision)
	}
}

// completePresets gives each saved prompt the members the apps' own profile
// sync requires of one, without which it refuses the whole profile: its text,
// empty when unset, and its dates as Unix milliseconds, now for one without.
// completePresets 为每个已保存 prompt 补齐应用自身 profile 同步所需的成员，否则同步会
// 拒绝整个 profile：其文本（未设置时为空），以及其日期（Unix 毫秒，无日期时用 now）。
func (p *Profile) completePresets(now time.Time) {
	for i := range p.CustomPromptPresets {
		preset := &p.CustomPromptPresets[i]
		preset.Description = new(ValueOr(preset.Description, ""))
		preset.SystemPrompt = new(ValueOr(preset.SystemPrompt, ""))
		if !isNumber(preset.CreatedAt) {
			preset.CreatedAt = api.Raw(now.UnixMilli())
		}
		if !isNumber(preset.UpdatedAt) {
			preset.UpdatedAt = preset.CreatedAt
		}
	}
}

func isNumber(raw json.RawMessage) bool {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	_, number := value.(float64)
	return number
}

// samePrompt reports whether two saved prompts read the same, text left unset
// reading as empty, as a stored prompt's does.
// samePrompt 报告两个已保存 prompt 是否读起来相同；未设置的文本按空处理，与已存储
// prompt 一致。
func samePrompt(a, b CustomPreset) bool {
	for _, preset := range []*CustomPreset{&a, &b} {
		preset.Description = new(ValueOr(preset.Description, ""))
		preset.SystemPrompt = new(ValueOr(preset.SystemPrompt, ""))
	}
	return reflect.DeepEqual(a, b)
}

// ProfileView is the profile a client reads.
// ProfileView 是客户端读取的 profile。
type ProfileView struct {
	Profile
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// maxProfileList bounds each list a profile holds.
// maxProfileList 是 profile 中每个列表的长度上限。
const maxProfileList = 500

var (
	themeModes = []string{"light", "dark", "system"}
	chatFonts  = []string{"system", "serif", "mono", "dyslexic"}
)

// The defaults of profile fields that hold a choice.
// 具有取值选择的 profile 字段的默认值。
const (
	DefaultLanguage     = "System"
	DefaultIntelligence = "high"
	defaultEffort       = "medium"
	defaultTheme        = "system"
	defaultFont         = "system"
)

// Read-aloud audio plays from MinSpeechSpeed to MaxSpeechSpeed times as fast
// as the voice speaks it, at VoiceSpeed unless the user chose otherwise.
// 朗读音频以 MinSpeechSpeed 到 MaxSpeechSpeed 倍的语速播放，用户未另选时使用 VoiceSpeed。
const (
	MinSpeechSpeed = 0.5
	MaxSpeechSpeed = 2
	VoiceSpeed     = 1
)

// DefaultProfile holds the value of every profile field a profile leaves
// unset. An empty default prompt preset is none. Thinking has no default:
// left unset, it follows each model's own.
// DefaultProfile 保存 profile 未设置时每个字段的默认值。空的默认 prompt preset 表示
// 没有。Thinking 没有默认值：未设置时跟随各模型自身的设置。
func DefaultProfile() Profile {
	return Profile{ThemeMode: new(defaultTheme), ChatFont: new(defaultFont), Language: new(DefaultLanguage), Nickname: new(""), Profession: new(""), Traits: []string{}, AdditionalContext: new(""),
		IsUsingPersonalization: new(true), IsUsingCustomPrompt: new(false), CustomSystemPrompt: new(""), CustomPromptPresets: []CustomPreset{}, FavoritePromptPresetIDs: []string{}, DefaultPromptPresetID: new(""),
		SelectedModel: new(AutoModelName), AutoIntelligence: new(DefaultIntelligence), ReasoningEffort: new(defaultEffort), WebSearchEnabled: new(true),
		WebSearchAvailable: new(true), PIICheckEnabled: new(true), GenUIEnabled: new(true), PixelateSidebarChatTitlesEnabled: new(false), BrowserTabChatTitleEnabled: new(true), EnterToNewlineEnabled: new(false),
		ReadAloudSpeed: new(float64(VoiceSpeed)), HasSeenOnboarding: new(false), HasSeenWebSearchIntro: new(false), Dismissed: map[string]bool{}}
}

// withDefaults is the profile with every field it leaves unset at its default.
// withDefaults 返回每个未设置字段都取默认值的 profile。
func (p Profile) withDefaults() Profile {
	filled := DefaultProfile()
	filled.Apply(p)
	return filled
}

// Validate refuses a profile with a value a client may not set.
// Validate 拒绝客户端不可设置的 profile 值。
func (p *Profile) Validate() error {
	checked := *p
	if invalid := checked.dropInvalid(); invalid != nil {
		return invalid
	}
	return nil
}

// dropInvalid clears each field whose value a client may not set, and
// reports the first.
// dropInvalid 清空客户端不可设置的每个字段，并报告第一个。
func (p *Profile) dropInvalid() *api.Error {
	var first *api.Error
	refuse := func(message string) {
		if first == nil {
			first = api.Err(400, "BAD_REQUEST", message)
		}
	}
	for _, choice := range []struct {
		name    string
		value   **string
		choices []string
	}{{"themeMode", &p.ThemeMode, themeModes}, {"chatFont", &p.ChatFont, chatFonts}, {"reasoningEffort", &p.ReasoningEffort, ReasoningEfforts}, {"autoIntelligence", &p.AutoIntelligence, intelligenceIDs()}} {
		if *choice.value != nil && !slices.Contains(choice.choices, **choice.value) {
			refuse("Invalid " + choice.name)
			*choice.value = nil
		}
	}
	if p.ReadAloudSpeed != nil && (*p.ReadAloudSpeed < MinSpeechSpeed || *p.ReadAloudSpeed > MaxSpeechSpeed) {
		refuse("Invalid readAloudSpeed")
		p.ReadAloudSpeed = nil
	}
	if len(p.Traits) > maxProfileList {
		refuse("traits has too many entries")
		p.Traits = nil
	}
	if len(p.FavoritePromptPresetIDs) > maxProfileList {
		refuse("favoritePromptPresetIds has too many entries")
		p.FavoritePromptPresetIDs = nil
	}
	if len(p.CustomPromptPresets) > maxProfileList {
		refuse("customPromptPresets has too many entries")
		p.CustomPromptPresets = nil
	}
	if slices.ContainsFunc(p.CustomPromptPresets, unnamedPreset) {
		refuse("A custom preset requires an id and name")
		p.CustomPromptPresets = slices.DeleteFunc(slices.Clone(p.CustomPromptPresets), unnamedPreset)
	}
	return first
}

func unnamedPreset(preset CustomPreset) bool { return preset.ID == "" || preset.Name == "" }

// Apply sets every field the patch sets.
// Apply 设置 patch 所设置的每个字段。
func (p *Profile) Apply(patch Profile) {
	to, from := reflect.ValueOf(p).Elem(), reflect.ValueOf(patch)
	for i := range from.NumField() {
		if !from.Field(i).IsZero() {
			to.Field(i).Set(from.Field(i))
		}
	}
}

// MergeDeviceProfile imports missing settings and prompts, with explicit
// device edits taking precedence. Conflicting prompt text gets its own copy.
// MergeDeviceProfile 导入缺失的设置与 prompt，以设备的显式编辑为准。冲突的 prompt 文本
// 会各自保留一份副本。
func MergeDeviceProfile(remote, local Profile, changed []string) Profile {
	merged := local
	merged.Apply(remote)
	if slices.Contains(changed, "defaultPromptPresetId") {
		merged.DefaultPromptPresetID = local.DefaultPromptPresetID
	}
	to, from := reflect.ValueOf(&merged).Elem(), reflect.ValueOf(local)
	for i := range from.NumField() {
		name, _, _ := strings.Cut(from.Type().Field(i).Tag.Get("json"), ",")
		if slices.Contains(changed, name) && !from.Field(i).IsZero() {
			to.Field(i).Set(from.Field(i))
		}
	}
	presets := slices.Clone(remote.CustomPromptPresets)
	remapped := map[string]string{}
	for _, preset := range local.CustomPromptPresets {
		index := slices.IndexFunc(presets, func(p CustomPreset) bool { return p.ID == preset.ID })
		if index >= 0 {
			kept, incoming := presets[index], preset
			kept.CreatedAt, kept.UpdatedAt, incoming.CreatedAt, incoming.UpdatedAt = nil, nil, nil, nil
			if samePrompt(kept, incoming) {
				continue
			}
			preset.ID = "user:" + uuid.NewSHA1(uuid.NameSpaceURL, api.Raw(incoming)).String()
			preset.Name += " (from this device)"
			remapped[incoming.ID] = preset.ID
			if slices.ContainsFunc(presets, func(p CustomPreset) bool { return p.ID == preset.ID }) {
				continue
			}
		}
		presets = append(presets, preset)
	}
	merged.CustomPromptPresets = presets
	merged.FavoritePromptPresetIDs = slices.Clone(remote.FavoritePromptPresetIDs)
	if slices.Contains(changed, "favoritePromptPresetIds") {
		merged.FavoritePromptPresetIDs = []string{}
	}
	for _, id := range local.FavoritePromptPresetIDs {
		if replacement := remapped[id]; replacement != "" {
			id = replacement
		}
		if !slices.Contains(merged.FavoritePromptPresetIDs, id) {
			merged.FavoritePromptPresetIDs = append(merged.FavoritePromptPresetIDs, id)
		}
	}
	if (remote.DefaultPromptPresetID == nil || slices.Contains(changed, "defaultPromptPresetId")) && local.DefaultPromptPresetID != nil {
		if replacement := remapped[*local.DefaultPromptPresetID]; replacement != "" {
			merged.DefaultPromptPresetID = &replacement
		}
	}
	return merged
}

// PromptEdits change saved prompts and favorites one at a time, on the
// profile as stored when they apply, so a client never sends back a copy of
// a whole list that it read before another device changed it.
// PromptEdits 逐个修改已保存 prompt 与收藏，基于其应用时所存储的 profile，因此客户端
// 绝不会把在另一台设备改动之前读到的整个列表副本回传。
type PromptEdits struct {
	// SavePromptPresets adds each prompt, or replaces the one with its id
	// where it stands.
	// SavePromptPresets 新增每个 prompt，或在其原位置替换具有相同 id 的那个。
	SavePromptPresets []CustomPreset `json:"savePromptPresets"`
	// UpdatePromptPresets sets the members each update names on the stored
	// prompt with its id, so a client that read the prompt before another
	// device changed it changes only what its user did.
	// UpdatePromptPresets 在每个 update 指名的已存储 prompt 上设置其指明的成员，这样在
	// 另一台设备改动前读取该 prompt 的客户端只会改动其用户所做的部分。
	UpdatePromptPresets []PresetUpdate `json:"updatePromptPresets"`
	// DeletePromptPresetIDs removes each prompt, which is then no longer a
	// favorite or the default.
	// DeletePromptPresetIDs 删除每个 prompt，它们随后不再是收藏或默认项。
	DeletePromptPresetIDs         []string `json:"deletePromptPresetIds"`
	AddFavoritePromptPresetIDs    []string `json:"addFavoritePromptPresetIds"`
	RemoveFavoritePromptPresetIDs []string `json:"removeFavoritePromptPresetIds"`
}

// PresetUpdate is a change to a saved prompt: the members it names. A null
// model or web search setting clears it.
// PresetUpdate 是对已保存 prompt 的一次变更：它指明的成员。为 null 的 model 或网络搜索
// 设置会将其清空。
type PresetUpdate struct {
	ID               string               `json:"id"`
	Name             *string              `json:"name,omitempty"`
	Description      *string              `json:"description,omitempty"`
	SystemPrompt     *string              `json:"systemPrompt,omitempty"`
	Model            api.Nullable[string] `json:"model,omitzero"`
	WebSearchEnabled api.Nullable[bool]   `json:"webSearchEnabled,omitzero"`
	UpdatedAt        json.RawMessage      `json:"updatedAt,omitempty"`
}

func (u PresetUpdate) applied(preset CustomPreset) CustomPreset {
	if u.Name != nil {
		preset.Name = *u.Name
	}
	if u.Description != nil {
		preset.Description = u.Description
	}
	if u.SystemPrompt != nil {
		preset.SystemPrompt = u.SystemPrompt
	}
	if u.Model.Set {
		preset.Model = u.Model.Value
	}
	if u.WebSearchEnabled.Set {
		preset.WebSearchEnabled = u.WebSearchEnabled.Value
	}
	if len(u.UpdatedAt) > 0 {
		preset.UpdatedAt = u.UpdatedAt
	}
	return preset
}

// Validate refuses edits that name a prompt without an id or name.
// Validate 拒绝没有 id 或名称的 prompt 的编辑。
func (e *PromptEdits) Validate() error {
	if slices.ContainsFunc(e.SavePromptPresets, unnamedPreset) {
		return api.Err(400, "BAD_REQUEST", "A custom preset requires an id and name")
	}
	for _, update := range e.UpdatePromptPresets {
		if update.ID == "" || update.Name != nil && *update.Name == "" {
			return api.Err(400, "BAD_REQUEST", "A custom preset requires an id and name")
		}
		if err := update.Model.Check("model"); err != nil {
			return err
		}
		if err := update.WebSearchEnabled.Check("webSearchEnabled"); err != nil {
			return err
		}
	}
	for _, ids := range [][]string{e.DeletePromptPresetIDs, e.AddFavoritePromptPresetIDs, e.RemoveFavoritePromptPresetIDs} {
		if slices.Contains(ids, "") {
			return api.Err(400, "BAD_REQUEST", "A prompt id is required")
		}
	}
	return nil
}

// ApplyPromptEdits applies edits to the profile, refusing them when a list
// would grow past what a profile holds.
// ApplyPromptEdits 将编辑应用到 profile，若列表增长超过 profile 的容量则拒绝。
func (p *Profile) ApplyPromptEdits(e PromptEdits) error {
	presets := slices.Clone(p.CustomPromptPresets)
	for _, preset := range e.SavePromptPresets {
		if i := slices.IndexFunc(presets, func(kept CustomPreset) bool { return kept.ID == preset.ID }); i >= 0 {
			presets[i] = preset
		} else {
			presets = append(presets, preset)
		}
	}
	// A prompt another device deleted stays deleted.
	// 另一台设备已删除的 prompt 保持删除状态。
	for _, update := range e.UpdatePromptPresets {
		if i := slices.IndexFunc(presets, func(kept CustomPreset) bool { return kept.ID == update.ID }); i >= 0 {
			presets[i] = update.applied(presets[i])
		}
	}
	presets = slices.DeleteFunc(presets, func(preset CustomPreset) bool { return slices.Contains(e.DeletePromptPresetIDs, preset.ID) })
	unfavored := slices.Concat(e.DeletePromptPresetIDs, e.RemoveFavoritePromptPresetIDs)
	favorites := slices.DeleteFunc(slices.Clone(p.FavoritePromptPresetIDs), func(id string) bool { return slices.Contains(unfavored, id) })
	for _, id := range e.AddFavoritePromptPresetIDs {
		if !slices.Contains(favorites, id) && !slices.Contains(e.DeletePromptPresetIDs, id) {
			favorites = append(favorites, id)
		}
	}
	if len(presets) > maxProfileList || len(favorites) > maxProfileList {
		return api.Err(400, "BAD_REQUEST", "The profile cannot hold that many saved prompts or favorites")
	}
	if !reflect.DeepEqual(presets, p.CustomPromptPresets) {
		p.CustomPromptPresets = presets
	}
	if !slices.Equal(favorites, p.FavoritePromptPresetIDs) {
		p.FavoritePromptPresetIDs = favorites
	}
	if id := ValueOr(p.DefaultPromptPresetID, ""); id != "" && slices.Contains(e.DeletePromptPresetIDs, id) {
		p.DefaultPromptPresetID = new("")
	}
	return nil
}

// Personalized reports whether the prompt carries the user's preferences.
// Personalized 报告 prompt 是否携带用户偏好。
func (p *Profile) Personalized() bool {
	return p.IsUsingPersonalization == nil || *p.IsUsingPersonalization
}

// Preset is a saved prompt by id.
// Preset 按 id 返回已保存 prompt。
func (p *Profile) Preset(id string) *CustomPreset {
	for i := range p.CustomPromptPresets {
		if p.CustomPromptPresets[i].ID == id {
			return &p.CustomPromptPresets[i]
		}
	}
	return nil
}

// LocalProfile is the settings a device chat's client sends with a request,
// without the fields it may not set, which never refuse the request.
// LocalProfile 是设备聊天客户端随请求发送的设置，剔除其不可设置的字段，这些字段绝不会
// 拒绝请求。
func LocalProfile(data []byte) *Profile {
	var profile Profile
	readLenient(data, &profile)
	upgradeProfile(&profile, api.Now())
	profile.dropInvalid()
	return &profile
}

// EffectiveProfile is a profile as its client reads it: a saved prompt
// keeps a model only while the control plane lists it, the selected model
// is the one new chats start on, and every field the profile leaves unset
// reads as its default.
// EffectiveProfile 是客户端读取的 profile：已保存 prompt 仅在控制平面列出该模型时保留
// 它，所选模型是新聊天起始所用的模型，profile 未设置的每个字段都读为其默认值。
func (c *Catalog) EffectiveProfile(p Profile) Profile {
	p.CustomPromptPresets = slices.Clone(p.CustomPromptPresets)
	for i := range p.CustomPromptPresets {
		model := p.CustomPromptPresets[i].Model
		if model == nil {
			continue
		}
		name := presetModel(*model)
		if c.retired(name) {
			p.CustomPromptPresets[i].Model = nil
		} else {
			p.CustomPromptPresets[i].Model = new(name)
		}
	}
	p.SelectedModel = new(c.SelectedModel(&p))
	return p.withDefaults()
}

// SelectedModel is the model new chats start on: the profile's while it is
// offered, and Auto otherwise.
// SelectedModel 是新聊天起始所用的模型：提供该模型时为 profile 中的，否则为 Auto。
func (c *Catalog) SelectedModel(p *Profile) string {
	if model := ValueOr(p.SelectedModel, ""); c.Offers(model) {
		return model
	}
	return AutoModelName
}
