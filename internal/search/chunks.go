// 本文件负责把已存储的聊天文本切分为嵌入所需的分块（chunk），供索引与查询使用。
package search

import (
	"encoding/json"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/searchindex"
)

const (
	// The embedding model family separates documents from queries: each
	// is tagged with its task so the two embed into their own subspaces.
	// embedding 模型家族将文档与查询区分开来：各自打上任务标记，使两者嵌入到各自子空间。
	docPrefix   = "search_document: "
	queryPrefix = "search_query: "

	// chunkChars is the size a chunk of chat text is cut to, on message
	// boundaries where possible: about 500 tokens, small enough that one
	// topic dominates the chunk's vector.
	// chunkChars 是聊天文本被切分成的分块大小，尽量按消息边界切分：约 500 token，
	// 小到足以让单个主题主导该分块的向量。
	chunkChars = 2000
)

// chatText is what of a stored chat is indexed: its title and the text of
// its messages. Other members are left alone, so a client may add to the
// shape without breaking indexing.
// chatText 是已存储聊天中被索引的部分：标题与各消息文本。其他成员保持不变，因此客户端
// 可以在不影响索引的前提下扩展其结构。
type chatText struct {
	Title    string `json:"title"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
}

// chatChunks is a chat's text in embedding-sized chunks: the title, then
// each message, packed up to chunkChars and cut on message boundaries
// where possible; a message larger than a chunk is split. At most
// searchindex.MaxChunksPerChat chunks are kept. A chat that is not chat
// shaped, or has no text, has none.
// chatChunks 把聊天文本切成嵌入大小的分块：先是标题，再是每条消息，尽量按 chunkChars
// 打包并在消息边界处切分；大于一个分块的消息会被拆分。最多保留 searchindex.MaxChunksPerChat
// 个分块。形状不是 chat 的聊天，或没有文本的聊天，则没有分块。
func chatChunks(plaintext []byte) []string {
	var chat chatText
	if err := json.Unmarshal(plaintext, &chat); err != nil {
		return nil
	}
	parts := make([]string, 0, len(chat.Messages)+1)
	if title := strings.TrimSpace(chat.Title); title != "" {
		parts = append(parts, title)
	}
	// A compaction marker's summary repeats the messages before it.
	// compaction 标记的摘要会重复其之前的消息。
	for _, m := range chat.Messages {
		if content := strings.TrimSpace(m.Content); content != "" && m.Role != domain.CompactionRole {
			parts = append(parts, content)
		}
	}
	var chunks []string
	var current strings.Builder
	// flush closes the chunk being built, and is whether there is room
	// for another.
	// flush 结束正在构建的分块，并返回是否还有空间容纳下一个。
	flush := func() bool {
		if current.Len() == 0 {
			return true
		}
		chunks = append(chunks, current.String())
		current.Reset()
		return len(chunks) < searchindex.MaxChunksPerChat
	}
	for _, part := range parts {
		for len(part) > 0 {
			if current.Len() > 0 && len(part)+1 <= chunkChars-current.Len() {
				current.WriteByte('\n')
				current.WriteString(part)
				break
			}
			if current.Len() > 0 && len(part) <= chunkChars {
				if !flush() {
					return chunks
				}
				continue
			}
			available := chunkChars - current.Len()
			if current.Len() > 0 {
				available--
			}
			piece := truncateUTF8(part, available)
			if piece == "" {
				// Less room left than the next rune needs: close the
				// chunk and try again with a fresh one.
				// 剩余空间小于下一个 rune 所需：结束当前分块，换一个新的重试。
				if !flush() {
					return chunks
				}
				continue
			}
			if current.Len() > 0 {
				current.WriteByte('\n')
			}
			current.WriteString(piece)
			part = part[len(piece):]
			if len(part) > 0 && !flush() {
				return chunks
			}
		}
	}
	flush()
	return chunks
}

// truncateUTF8 cuts s to at most n bytes without splitting a rune. A
// non-positive n is the empty string, which the chunker reads as no room
// left.
// truncateUTF8 将 s 截断为至多 n 字节且不拆分 rune。n 非正时返回空串，分块器会将其解读为
// 没有剩余空间。
func truncateUTF8(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut]
}
