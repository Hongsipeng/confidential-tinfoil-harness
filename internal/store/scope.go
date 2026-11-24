// Package store 定义存储操作的授权范围。
package store

import (
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/seal"
)

// keyID is the hex id a content key is registered and sealed under.
// keyID 是 content key 注册与封存所用的十六进制 id。
func keyID(key auth.ContentKey) string {
	id, _ := seal.DeriveKeyID(key[:])
	return seal.KeyIDHex(id)
}

// maxResourceID bounds the ids a grant names.
// maxResourceID 限制 grant 所命名的 id 的长度。
const maxResourceID = 256

// runIDValid is whether id can name a chat, project or document: one path
// segment the control plane stores it under.
// runIDValid 判断 id 能否命名 chat、project 或文档：即 control plane 存储它所用的单个路径段。
func runIDValid(id string) bool {
	return id != "" && id != "." && id != ".." && len(id) <= maxResourceID && !strings.ContainsAny(id, "/\x00")
}

// A storage operation, as a grant allows or refuses it.
// 存储操作，作为 grant 允许或拒绝的对象。
type storageOp int

const (
	opPush storageOp = iota
	opPull
	opList
	opRemove
	opPutAttachment
	opGetAttachment
	opImport
)

// grantScope is what a storage grant lets its holder do: a run reads and
// writes its chat and reads its project's context; an operation does what
// it is for and nothing else.
// grantScope 是 storage grant 允许其持有者做的事：run 读写其 chat 并读取其 project 的上下文；
// 某操作则只做它该做的事，别无其他。
type grantScope auth.StorageGrant

func (g *grantScope) allows(op storageOp) bool {
	if !api.Now().Before(g.ExpiresAt) {
		return false
	}
	switch g.Operation {
	case "project-upload":
		return op == opPull || op == opPush
	case "import":
		return op == opImport
	case "export":
		return op == opPull || op == opList || op == opGetAttachment
	case "":
		return op == opPush || op == opPull || op == opList || op == opPutAttachment || op == opGetAttachment
	}
	return false
}

func (g *grantScope) keyMatches(key auth.ContentKey) bool { return keyID(key) == g.KeyID }

// ownsDocument is whether id names a document of the grant's project.
// ownsDocument 判断 id 是否命名该 grant 的 project 下的某个文档。
func (g *grantScope) ownsDocument(id string) bool {
	if g.ProjectID == "" {
		return false
	}
	document, ok := strings.CutPrefix(id, g.ProjectID+"/")
	return ok && runIDValid(document)
}

func (g *grantScope) permitsPull(key auth.ContentKey, scope string, ids []string) bool {
	if !g.allows(opPull) || !g.keyMatches(key) {
		return false
	}
	if g.Operation == "export" {
		return seal.Scope(scope).Valid()
	}
	if len(ids) == 0 {
		return false
	}
	if g.Operation == "project-upload" {
		for _, id := range ids {
			if scope == "project" && id == g.ProjectID || scope == "project_document" && g.ownsDocument(id) {
				continue
			}
			return false
		}
		return true
	}
	for _, id := range ids {
		switch scope {
		case "chat":
			if id != g.ChatID {
				return false
			}
		case "profile":
			if id != seal.ProfileSingletonID {
				return false
			}
		case "project":
			if g.ProjectID == "" || id != g.ProjectID {
				return false
			}
		case "project_document":
			if !g.ownsDocument(id) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (g *grantScope) permitsPush(key auth.ContentKey, scope string, row *domain.StoredRow) bool {
	if !g.allows(opPush) || !g.keyMatches(key) {
		return false
	}
	if g.Operation == "project-upload" {
		return scope == "project_document" && g.ownsDocument(row.ID) && row.ETag == "" && row.Project != nil && *row.Project == g.ProjectID
	}
	// The grant owns the chat, including its metadata. ProjectID limits
	// context reads; the user can move the chat while a reply is running.
	// grant 拥有该 chat，包括其元数据。ProjectID 只限制上下文读取；
	// 用户可以在 reply 运行期间移动 chat。
	return g.Operation == "" && scope == "chat" && row.ID == g.ChatID
}

// permitsPutAttachment is whether the grant may store an attachment under
// chatID: a run's, under its own chat.
// permitsPutAttachment 判断 grant 是否可将 attachment 存储到 chatID 下：
// 即 run 的、位于其自身 chat 下的 attachment。
func (g *grantScope) permitsPutAttachment(chatID string) bool {
	return g.allows(opPutAttachment) && chatID == g.ChatID
}

// permitsList is whether the grant may list a scope's rows, or those of a
// project: an export lists anything, a run only its project's documents.
// permitsList 判断 grant 是否可列出某 scope 的 row，或某 project 的 row：
// export 可列出任何内容，run 只能列出其 project 的文档。
func (g *grantScope) permitsList(scope, project string) bool {
	if !g.allows(opList) {
		return false
	}
	return g.Operation == "export" || scope == "project_document" && project != "" && project == g.ProjectID
}
