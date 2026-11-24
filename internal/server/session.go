package server

import (
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// The version of protocol/openapi.yaml this harness speaks, and the oldest
// whose clients it still serves.
// 本 harness 所使用的 protocol/openapi.yaml 版本，以及其仍支持的最旧客户端版本。
const (
	protocolVersion        = 1
	minimumProtocolVersion = 1
)

// protocolView 是协议版本信息。
type protocolView struct {
	Version int `json:"version"`
	Minimum int `json:"minimum"`
}

// sessionView is what a client opens the app with.
// sessionView 是客户端打开应用时获取的会话视图。
type sessionView struct {
	Protocol        protocolView          `json:"protocol"`
	User            sessionUser           `json:"user"`
	RateLimit       api.RateLimit         `json:"rateLimit"`
	Models          []domain.SessionModel `json:"models"`
	Auto            autoView              `json:"auto"`
	DefaultModel    string                `json:"defaultModel"`
	Widgets         []string              `json:"widgets"`
	ProfileDefaults domain.Profile        `json:"profileDefaults"`
	Composer        attach.ComposerView   `json:"composer"`
	Presets         []domain.Preset       `json:"presets"`
	Features        map[string]bool       `json:"features"`
}

// sessionUser 是会话中的用户信息。
type sessionUser struct {
	ID        string `json:"id"`
	Anonymous bool   `json:"anonymous"`
}

// autoView is how hard Auto works: its default level, and each level's
// labels.
// autoView 描述 Auto 的工作力度：其默认级别以及各级别的标签。
type autoView struct {
	Default string                     `json:"default"`
	Levels  []domain.IntelligenceLevel `json:"levels"`
}
