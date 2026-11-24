package importer

import (
	"path"
	"slices"
	"strings"
)

// Index looks up the entries of an archive, so an attachment can only name
// an entry the archive holds. Its caller builds it from entry names it has
// already found safe.
// Index 查询归档的条目，使 attachment 只能命名归档中确实存在的条目。调用方用已确认安全的条目名构建它。
type Index struct {
	byName map[string]struct{}
	byBase map[string]string
	// byStem finds the entries whose basename less its extension is a file
	// id.
	// byStem 用于查找去扩展名后 basename 是文件 id 的条目。
	byStem map[string]idMatch
	// sorted holds each entry once, ordered by basename, so the entries
	// whose basename is a file id followed by a separator and more form a
	// range found by binary search.
	// sorted 按 basename 排序保存每个条目一次，使 basename 为“文件 id 后接分隔符及更多内容”的条目
	// 构成一个可通过二分查找找到的区间。
	sorted []indexEntry
}

type indexEntry struct{ base, name string }

// idMatch is the first entry a file id finds and how many it finds.
// idMatch 是文件 id 找到的第一个条目以及找到的数量。
type idMatch struct {
	name  string
	count int
}

func (m idMatch) unique() (string, bool) {
	if m.count != 1 {
		return "", false
	}
	return m.name, true
}

// addID records that id finds name.
// addID 记录 id 找到 name。
func addID(ids map[string]idMatch, id, name string) {
	m := ids[id]
	if m.count == 0 {
		m.name = name
	}
	m.count++
	ids[id] = m
}

// idSeparators may follow a file id in a media file's name.
// idSeparators 是媒体文件名中可跟在文件 id 之后的分隔符。
const idSeparators = ".-_"

// NewIndex indexes entry names. The first entry with a basename claims it,
// which is deterministic for a given archive.
// NewIndex 索引条目名。第一个使用某 basename 的条目认领该 basename，对给定归档是确定的。
func NewIndex(names []string) *Index {
	idx := &Index{byName: make(map[string]struct{}, len(names)), byBase: make(map[string]string, len(names)), byStem: map[string]idMatch{}}
	for _, n := range names {
		clean := normalizeEntry(n)
		if clean == "" {
			continue
		}
		if _, ok := idx.byName[clean]; ok {
			continue
		}
		idx.byName[clean] = struct{}{}
		base := path.Base(clean)
		if idx.byBase[base] == "" {
			idx.byBase[base] = clean
		}
		addID(idx.byStem, strings.TrimSuffix(base, path.Ext(base)), clean)
		idx.sorted = append(idx.sorted, indexEntry{base: base, name: clean})
	}
	slices.SortFunc(idx.sorted, func(a, b indexEntry) int { return strings.Compare(a.base, b.base) })
	return idx
}

// byPrefix finds the entries whose basename is id followed by a separator
// and more, counting no further than a second.
// byPrefix 查找 basename 为“id 后接分隔符及更多内容”的条目，最多数到第二个。
func (i *Index) byPrefix(id string) idMatch {
	var m idMatch
	for _, sep := range idSeparators {
		prefix := id + string(sep)
		k, _ := slices.BinarySearchFunc(i.sorted, prefix, func(e indexEntry, p string) int { return strings.Compare(e.base, p) })
		for ; k < len(i.sorted) && m.count < 2 && strings.HasPrefix(i.sorted[k].base, prefix); k++ {
			if m.count == 0 {
				m.name = i.sorted[k].name
			}
			m.count++
		}
	}
	return m
}

func normalizeEntry(name string) string {
	return strings.TrimPrefix(path.Clean("/"+strings.ReplaceAll(name, "\\", "/")), "/")
}

// Exact finds the entry a reference names by its path.
// Exact 按路径查找引用所指向的条目。
func (i *Index) Exact(ref string) (string, bool) {
	if i == nil {
		return "", false
	}
	clean := normalizeEntry(ref)
	if _, ok := i.byName[clean]; ok && clean != "" {
		return clean, true
	}
	return "", false
}

// Basename finds the entry a bare file name names.
// Basename 查找纯文件名所指向的条目。
func (i *Index) Basename(ref string) (string, bool) {
	if i == nil {
		return "", false
	}
	clean := normalizeEntry(ref)
	if clean == "" {
		return "", false
	}
	name, ok := i.byBase[path.Base(clean)]
	return name, ok
}

// ByIDPrefix finds the one entry whose basename is a file id, or starts
// with it, as ChatGPT and Claude name media files.
// ByIDPrefix 查找 basename 为文件 id、或以文件 id 开头的唯一条目，正如 ChatGPT 与 Claude 命名媒体文件的方式。
func (i *Index) ByIDPrefix(id string) (string, bool) {
	id = strings.TrimSpace(id)
	if i == nil || id == "" {
		return "", false
	}
	if name, ok := i.byBase[id]; ok {
		return name, true
	}
	if name, ok := i.byStem[id].unique(); ok {
		return name, true
	}
	return i.byPrefix(id).unique()
}

// imageTypes are the image types an export's file names imply.
// imageTypes 是导出文件名所暗示的 image 类型。
var imageTypes = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".gif":  "image/gif",
	".webp": "image/webp",
	".bmp":  "image/bmp",
	".svg":  "image/svg+xml",
}

// isImage tells by a declared type or a file name whether a reference
// names an image. Its bytes are checked when they are read.
// isImage 根据声明的类型或文件名判断引用是否指向 image。其字节在读取时再校验。
func isImage(mimeType, name string) bool {
	if strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return true
	}
	_, ok := imageTypes[strings.ToLower(path.Ext(name))]
	return ok
}

func imageType(name string) string { return imageTypes[strings.ToLower(path.Ext(name))] }
