// 本文件定义 project 及其文档的存储结构。
package domain

// Project groups chats under shared instructions and documents.
// Project 把聊天归组到共享的指令与文档之下。
type Project struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	Description        string  `json:"description"`
	SystemInstructions string  `json:"systemInstructions"`
	Color              *string `json:"color"`
	CreatedAt          string  `json:"createdAt,omitempty"`
	UpdatedAt          string  `json:"updatedAt,omitempty"`
}

// ProjectDocument is a document's extracted text, which every chat of its
// project reads.
// ProjectDocument 是文档的提取文本，其 project 中的每个聊天都会读取。
type ProjectDocument struct {
	ID              string `json:"id"`
	ProjectID       string `json:"projectId"`
	Filename        string `json:"filename"`
	ContentType     string `json:"contentType"`
	SizeBytes       int    `json:"sizeBytes"`
	Content         string `json:"content,omitempty"`
	ThumbnailBase64 string `json:"thumbnailBase64,omitempty"`
	CreatedAt       string `json:"createdAt,omitempty"`
	UpdatedAt       string `json:"updatedAt,omitempty"`
}

// StoredProject and StoredDocument are the rows this harness writes. A row
// an import made says where it came from, which no client is sent.
// StoredProject 与 StoredDocument 是本 harness 写入的 row。导入生成的 row 会标明其
// 来源，该信息不会发送给任何客户端。
type StoredProject struct {
	Project
	RecordVersion int           `json:"recordVersion"`
	ImportedFrom  *ImportOrigin `json:"_restore,omitempty"`
}

type StoredDocument struct {
	ProjectDocument
	RecordVersion int           `json:"recordVersion"`
	ImportedFrom  *ImportOrigin `json:"_restore,omitempty"`
}
