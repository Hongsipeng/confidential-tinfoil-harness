package store

import (
	"context"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
)

// Change tokens tell a client whether what it stores changed, from the
// control plane's ledger alone, without opening any row.
// 变更 token 仅凭 control plane 的账本（无需打开任何 row）即可告诉客户端其所存储的内容是否发生了变化。

// changeLedger is what a client's change token for a scope is a digest
// of: the edge of the scope's ledger, as listed. A token a client holds
// stays valid only while this shape does.
// changeLedger 是客户端针对某 scope 的变更 token 所摘要的内容：即该 scope 账本的边界（按列表所示）。
// 客户端持有的 token 仅在此结构保持不变时才有效。
type changeLedger struct {
	Updates []ledgerUpdate `json:"updates"`
	Deletes []ledgerDelete `json:"deletes"`
}

type ledgerUpdate struct {
	ID        string `json:"id"`
	ETag      string `json:"etag"`
	UpdatedAt string `json:"updated_at"`
}

type ledgerDelete struct {
	ID        string `json:"id"`
	DeletedAt string `json:"deleted_at"`
}

// StorageChanges is a token per kind of row: the chats' revision, and a
// digest of the ledger's edge for the profile and for projects and their
// documents.
// StorageChanges 为每种 row 返回一个 token：chats 的修订版本，以及 profile、projects
// 及其文档的账本边界摘要。
func (s *Store) StorageChanges(ctx context.Context, p *auth.Principal) (any, error) {
	summary, err := s.Controlplane.RevisionSummary(ctx, string(p.JWT), p.ID)
	if err != nil {
		return nil, FromControlplane(err)
	}
	tokens := map[string]string{"chats": summary.CurrentRevision}
	for _, scope := range []string{"profile", "project", "project_document"} {
		direction := "desc"
		if scope == "profile" {
			direction = "asc"
		}
		list, err := s.Controlplane.ListStatus(ctx, scope, "", 1, string(p.JWT), p.ID, "", direction, false)
		if err != nil {
			return nil, FromControlplane(err)
		}
		var ledger changeLedger
		for _, update := range list.Updates {
			ledger.Updates = append(ledger.Updates, ledgerUpdate{update.ID, update.ETag, update.UpdatedAt.UTC().Format(time.RFC3339Nano)})
		}
		for _, deletion := range list.Deletes {
			ledger.Deletes = append(ledger.Deletes, ledgerDelete{deletion.ID, deletion.DeletedAt.UTC().Format(time.RFC3339Nano)})
		}
		tokens[scope] = api.HashHex(api.Raw(ledger))
	}
	return struct {
		Chats    string `json:"chats"`
		Profile  string `json:"profile"`
		Projects string `json:"projects"`
	}{tokens["chats"], tokens["profile"], api.HashHex([]byte(tokens["project"] + tokens["project_document"]))}, nil
}
