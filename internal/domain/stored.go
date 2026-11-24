// 本文件定义控制平面持有的 row 结构。
package domain

import (
	"encoding/json"
	"slices"
	"time"
)

// StoredRow is a row as the control plane holds it: sealed JSON, opened
// only by the store's readers.
// StoredRow 是控制平面持有的 row：密封的 JSON，仅由 store 的读取器打开。
type StoredRow struct {
	ID, ETag string
	Data     json.RawMessage
	// project is the project the control plane files a chat or document
	// under, apart from its sealed data; projectSet says it reported one,
	// even none.
	// project 是控制平面将聊天或文档归档到的 project（与其密封数据分开）；projectSet
	// 表示它报告过一个 project，即使是空值。
	Project    *string `json:"-"`
	ProjectSet bool    `json:"-"`
	// UpdatedAt is when the control plane last wrote the row. A row an
	// older client sealed carries no time of its own, and is read as
	// updated then.
	// UpdatedAt 是控制平面最后一次写入该 row 的时间。旧客户端密封的 row 自身不带时间，
	// 读取时按该时间视为更新。
	UpdatedAt time.Time `json:"-"`
}

// rowTime is when a row was last written, as RFC 3339, or "" when the
// control plane did not say.
// rowTime 返回 row 最后写入的时间（RFC 3339），控制平面未给出时返回 ""。
func (row *StoredRow) rowTime() string {
	if row.UpdatedAt.IsZero() {
		return ""
	}
	return row.UpdatedAt.UTC().Format(time.RFC3339Nano)
}

// Copy 返回 row 的副本。
func (row StoredRow) Copy() *StoredRow {
	row.Data = slices.Clone(row.Data)
	return &row
}

// ImportOrigin is what an imported row came from: the import's source and
// the source's own id for it, and which copy of it this is when an earlier
// copy was in the way.
// ImportOrigin 是导入 row 的来源：导入的源、源对它的自有 id，以及在已有早期副本挡路时
// 这是第几份副本。
type ImportOrigin struct {
	Format     string `json:"format"`
	Source     string `json:"sourceBackupId"`
	Kind       string `json:"kind"`
	SourceID   string `json:"sourceId"`
	Generation int    `json:"generation"`
}

// ReadImportOrigin is where a row came from when an import stored it, or
// nil for a row that was not imported or cannot be read.
// ReadImportOrigin 返回导入存储 row 时的来源；对未导入或无法读取的 row 返回 nil。
func ReadImportOrigin(row *StoredRow) *ImportOrigin {
	var stored struct {
		ImportedFrom *ImportOrigin `json:"_restore"`
	}
	if json.Unmarshal(row.Data, &stored) != nil {
		return nil
	}
	return stored.ImportedFrom
}

// rowJSON is a stored row as JSON, to check which fields it holds.
// rowJSON 是将存储 row 作为 JSON，用于检查它包含哪些字段。
