package archives

import (
	"archive/zip"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// A native backup is a ZIP archive: one JSON entry per project, project
// document and chat, each image's bytes beside its description, the
// relationships between them, and a manifest listing every entry with its
// hash. Versions 1 and 2 hold chats in the format the web app kept them in;
// version 3 holds them in the thread format. The legacy reader reads both.
// 原生 backup 是一个 ZIP 归档：每个 project、project document 和 chat 各一个 JSON 条目，
// 每张 image 的字节与其描述并列存放，还有它们之间的 relationships，以及一个列出每个条目及其
// hash 的 manifest。版本 1 和 2 以 web 应用保存 chat 的格式保存它们；版本 3 以 thread 格式保存。
// 旧版读取器两种都能读。

const (
	// NativeBackupFormat 是本服务原生 backup 的格式标识。
	NativeBackupFormat  = "tinfoil-native-backup"
	nativeBackupVersion = 3
	// ManifestPath 是 backup 中 manifest 条目的路径名。
	ManifestPath      = "manifest.json"
	relationshipsPath = "relationships.json"
)

// Limits a backup is read and written within. A backup holds any image
// uploads take.
// 备份读写受以下限制约束。备份需容纳任意 image 上传所占用的空间。
const (
	maxArchiveEntries      = 50_000
	MaxArchiveExpansion    = 2 << 30
	maxArchiveJSONEntry    = 256 << 20
	maxArchiveImage        = attach.MaxUploadBytes
	maxBackupManifestBytes = 32 << 20
)

// backupEntryLimit is the most an entry of a backup may hold.
// backupEntryLimit 是 backup 中单个条目最多可容纳的字节数。
func backupEntryLimit(name string) uint64 {
	if path.Ext(name) == ".bin" {
		return maxArchiveImage
	}
	return maxArchiveJSONEntry
}

// BackupManifest 是原生 backup 的 manifest，记录整体元数据与每个条目。
type BackupManifest struct {
	Format    string           `json:"format"`
	Version   int              `json:"version"`
	BackupID  string           `json:"backup_id"`
	CreatedAt string           `json:"created_at"`
	Complete  bool             `json:"complete"`
	Counts    BackupCounts     `json:"counts"`
	Notices   backupNotices    `json:"notices"`
	Files     []backupFile     `json:"files"`
	Omissions []BackupOmission `json:"omissions"`
	Warnings  []backupWarning  `json:"warnings"`
}

// BackupCounts 是 backup 中各类条目的数量统计。
type BackupCounts struct {
	Projects         int `json:"projects"`
	ProjectDocuments int `json:"project_documents"`
	CloudChats       int `json:"cloud_chats"`
	LocalChats       int `json:"local_chats"`
	Relationships    int `json:"relationships"`
	Images           int `json:"images"`
	Files            int `json:"files"`
}

type backupNotices struct {
	ContainsPlaintext             bool `json:"contains_plaintext"`
	DocumentsAreExtractedTextOnly bool `json:"documents_are_extracted_text_only"`
}

type backupFile struct {
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

// BackupOmission names an item a backup could not include.
// BackupOmission 记录一个 backup 无法纳入的条目。
type BackupOmission struct {
	Kind           string `json:"kind"`
	SourceID       string `json:"source_id"`
	ParentSourceID string `json:"parent_source_id,omitempty"`
	Category       string `json:"category"`
	Reason         string `json:"reason"`
}

type backupWarning struct {
	Code     string `json:"code"`
	Category string `json:"category"`
	Count    int    `json:"count"`
}

type backupRelationships struct {
	ProjectChats     []projectChatLink     `json:"projectChats"`
	ProjectDocuments []projectDocumentLink `json:"projectDocuments"`
	ChatImages       []chatImageLink       `json:"chatImages"`
}

type projectChatLink struct {
	ProjectID string `json:"projectId"`
	ChatID    string `json:"chatId"`
}

type projectDocumentLink struct {
	ProjectID  string `json:"projectId"`
	DocumentID string `json:"documentId"`
}

type chatImageLink struct {
	ChatID  string `json:"chatId"`
	ImageID string `json:"imageId"`
}

func (r *backupRelationships) count() int {
	return len(r.ProjectChats) + len(r.ProjectDocuments) + len(r.ChatImages)
}

// backupImage describes an image a backup holds and where its chat names it.
// backupImage 描述 backup 持有的一张 image，以及它在对应 chat 中的位置。
type backupImage struct {
	ID           string `json:"id"`
	ChatID       string `json:"chatId"`
	MessageIndex int    `json:"messageIndex"`
	AttachmentID string `json:"attachmentId,omitempty"`
	Page         *int   `json:"page,omitempty"`
	LegacyIndex  *int   `json:"legacyIndex,omitempty"`
	FileName     string `json:"fileName"`
	MimeType     string `json:"mimeType"`
	SizeBytes    *int64 `json:"sizeBytes,omitempty"`
	Description  string `json:"description,omitempty"`
}

// backupProject is a project as every version holds it.
// backupProject 是所有版本都以同一方式保存的 project。
type backupProject struct {
	domain.Project
	CreatedAt domain.LooseTime `json:"createdAt"`
	UpdatedAt domain.LooseTime `json:"updatedAt"`
}

// BackupDocument is a project document; its text is extractedText.
// BackupDocument 是一个 project document；其文本即 extractedText。
type BackupDocument struct {
	ID            string           `json:"id"`
	ProjectID     string           `json:"projectId"`
	Filename      string           `json:"filename"`
	ContentType   string           `json:"contentType"`
	SizeBytes     int              `json:"sizeBytes"`
	ExtractedText string           `json:"extractedText"`
	CreatedAt     domain.LooseTime `json:"createdAt"`
	UpdatedAt     domain.LooseTime `json:"updatedAt"`
}

// backupThread is a chat as version 3 holds it: the thread, with each image
// named by the image the backup holds instead of its bytes.
// backupThread 是版本 3 保存的 chat：即 thread，其中每张 image 由 backup 所持有的 image 命名，而非内联其字节。
type backupThread struct {
	domain.Thread
	Messages []backupMessage `json:"messages"`
}

type backupMessage struct {
	domain.Message
	Attachments []backupAttachment `json:"attachments,omitzero"`
}

type backupAttachment struct {
	domain.Attachment
	ImageID string       `json:"imageId,omitempty"`
	Pages   []backupPage `json:"pages,omitempty"`
}

type backupPage struct {
	Page      int    `json:"page"`
	Text      string `json:"text"`
	IsScanned bool   `json:"is_scanned"`
	ImageID   string `json:"imageId,omitempty"`
}

func idComponent(id string) string { return "id-" + hex.EncodeToString([]byte(id)) }

// BackupEntityPath 返回 project、chat 等实体在 backup 中的条目路径。
func BackupEntityPath(kind, id string) string { return kind + "/" + idComponent(id) + ".json" }

// BackupDocumentPath 返回 project document 在 backup 中的条目路径。
func BackupDocumentPath(projectID, id string) string {
	return "project_documents/" + idComponent(projectID) + "/" + idComponent(id) + ".json"
}

// BackupImagePath 返回 image 在 backup 中的条目路径。
func BackupImagePath(id, extension string) string { return "images/" + idComponent(id) + extension }

// backupPaths are the entries a native backup may hold.
// backupPaths 是原生 backup 允许包含的条目路径模式。
var backupPaths = regexp.MustCompile(`^(manifest\.json|relationships\.json|(projects|cloud_chats|local_chats)/id-[0-9a-f]+\.json|project_documents/id-[0-9a-f]+/id-[0-9a-f]+\.json|images/id-[0-9a-f]+\.(json|bin))$`)

// BackupKind is the kind the manifest must list an entry under.
// BackupKind 是 manifest 中某条目应归属的 kind。
func BackupKind(name string) string {
	if name == relationshipsPath {
		return "relationships"
	}
	kind, _, _ := strings.Cut(name, "/")
	return kind
}

// safeEntryName reports whether a ZIP entry names a file inside the archive.
// safeEntryName 判断一个 ZIP 条目是否命名了归档内的文件。
func safeEntryName(f *zip.File) bool {
	name := f.Name
	return name != "" && name == path.Clean(name) && !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") && name != ".." &&
		!strings.ContainsAny(name, "\\\x00") && f.Mode().IsRegular()
}

// readEntry reads a whole entry, refusing one larger than limit or than its
// header says.
// readEntry 读取整个条目，若其超过 limit 或超过其头部声明的大小则拒绝。
func readEntry(f *zip.File, limit int64) ([]byte, error) {
	if f.UncompressedSize64 > uint64(limit) {
		return nil, api.Err(413, "IMPORT_TOO_LARGE", "An archive entry exceeds the size limit")
	}
	r, err := f.Open()
	if err != nil {
		return nil, api.Err(400, "IMPORT_INVALID", "An archive entry cannot be read")
	}
	defer r.Close()
	b, err := io.ReadAll(io.LimitReader(r, int64(f.UncompressedSize64)+1))
	if err != nil || uint64(len(b)) != f.UncompressedSize64 {
		return nil, api.Err(400, "IMPORT_INVALID", "An archive entry does not match its size")
	}
	return b, nil
}

// archiveTime is a time as the web app wrote it in archives.
// archiveTime 是 web 应用写入归档时使用的时间格式。
func archiveTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// BackupWriter writes a backup's entries and lists them in its manifest.
// BackupWriter 写入 backup 的各个条目，并在其 manifest 中列出它们。
type BackupWriter struct {
	zip      *zip.Writer
	manifest BackupManifest
	expanded uint64
	written  map[string]bool
}

// NewBackupWriter 返回一个向 w 写入原生 backup 的 BackupWriter。
func NewBackupWriter(w io.Writer) *BackupWriter {
	return &BackupWriter{zip: zip.NewWriter(w), manifest: BackupManifest{Format: NativeBackupFormat, Version: nativeBackupVersion, Notices: backupNotices{true, true}, Files: []backupFile{}, Omissions: []BackupOmission{}}, written: map[string]bool{}}
}

// Add writes an entry, refusing one the backup already holds, as a restore
// refuses a backup that holds an entry twice.
// Add 写入一个条目，若 backup 已持有该条目则拒绝——正如 restore 会拒绝包含重复条目的 backup 一样。
func (b *BackupWriter) Add(kind, name string, data []byte) error {
	if len(b.manifest.Files)+1 >= maxArchiveEntries {
		return api.Err(413, "ARCHIVE_TOO_LARGE", "The backup holds too many items")
	}
	if b.written[name] {
		return errors.New("the backup already holds an entry it was given again")
	}
	if err := b.write(name, data, backupEntryLimit(name), kind != "images" || path.Ext(name) == ".json"); err != nil {
		return err
	}
	b.written[name] = true
	b.manifest.Files = append(b.manifest.Files, backupFile{Path: name, Kind: kind, SHA256: api.HashHex(data), SizeBytes: int64(len(data))})
	return nil
}

// write adds an entry, refusing one a restore would refuse: one past its
// limit, or one that expands the backup past the limit.
// write 添加一个条目，并拒绝 restore 同样会拒绝的情况：超过自身上限，或使 backup 解压后超过总上限。
func (b *BackupWriter) write(name string, data []byte, limit uint64, compress bool) error {
	b.expanded += uint64(len(data))
	if uint64(len(data)) > limit || b.expanded > MaxArchiveExpansion {
		return api.Err(413, "ARCHIVE_TOO_LARGE", "The backup exceeds the size a restore takes")
	}
	return writeEntry(b.zip, name, data, compress)
}

func (b *BackupWriter) addJSON(kind, name string, value any) error {
	return b.Add(kind, name, api.Raw(value))
}

func (b *BackupWriter) omit(omission BackupOmission) {
	b.manifest.Omissions = append(b.manifest.Omissions, omission)
}

// close writes the manifest last, once every entry is listed.
// close 在所有条目列完后最后写入 manifest。
func (b *BackupWriter) close(id string, created time.Time) error {
	m := &b.manifest
	m.BackupID, m.CreatedAt, m.Complete, m.Counts.Files = id, archiveTime(created), len(m.Omissions) == 0, len(m.Files)
	m.Warnings = []backupWarning{}
	omitted, detached := 0, 0
	for _, o := range m.Omissions {
		if o.Kind == "relationship" {
			detached++
		} else {
			omitted++
		}
	}
	if omitted > 0 {
		m.Warnings = append(m.Warnings, backupWarning{Code: "source_items_omitted", Category: "source_coverage", Count: omitted})
	}
	if detached > 0 {
		m.Warnings = append(m.Warnings, backupWarning{Code: "chats_detached_from_omitted_projects", Category: "relationship_adjustment", Count: detached})
	}
	if err := b.write(ManifestPath, api.Raw(m), maxBackupManifestBytes, true); err != nil {
		return err
	}
	return b.zip.Close()
}

// writeEntry adds one file to an archive, compressed unless it already is.
// writeEntry 向归档添加一个文件，除非其本身已压缩，否则会压缩。
func writeEntry(zw *zip.Writer, name string, data []byte, compress bool) error {
	header := &zip.FileHeader{Name: name, Method: zip.Store}
	if compress {
		header.Method = zip.Deflate
	}
	header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	w, err := zw.CreateHeader(header)
	if err == nil {
		_, err = w.Write(data)
	}
	return err
}

// decodeEntry reads a JSON entry of an archive into v.
// decodeEntry 读取归档的一个 JSON 条目并解码到 v。
func decodeEntry(f *zip.File, v any) error {
	b, err := readEntry(f, maxArchiveJSONEntry)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return api.Err(400, "IMPORT_INVALID", "An archive entry is not valid JSON: "+f.Name)
	}
	return nil
}
