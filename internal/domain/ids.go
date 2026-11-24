// 本文件定义聊天与附件 id 的生成规则。
package domain

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// MaxReverseMillis is the instant a reverse-time chat id counts down from.
// MaxReverseMillis 是逆向时间聊天 id 起算的时刻。
const MaxReverseMillis = 9999999999999

// RowIDPattern is the id a client gives a chat it creates, as rowID makes it.
// RowIDPattern 是客户端为其创建的聊天赋予的 id 格式，与 rowID 生成的一致。
var RowIDPattern = regexp.MustCompile(`^\d{13}_[0-9a-f-]{36}$`)

// RowID 生成一个新的聊天 id。
func RowID() string {
	return fmt.Sprintf("%013d_%s", MaxReverseMillis-api.Now().UnixMilli(), uuid.NewString())
}

// EnclaveAttachmentID is the id of an attachment stored through this harness, as
// the store names its blobs.
// EnclaveAttachmentID 是通过本 harness 存储的附件的 id，与 store 命名 blob 的方式一致。
var EnclaveAttachmentID = regexp.MustCompile(`^[0-9a-f]{36}$`)
