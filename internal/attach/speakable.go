package attach

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// A reply is read aloud as the web client read it: its markdown without
// code blocks, images, footnotes, citation badges and bare links, keeping
// link labels, inline code and math, in pieces of about speechTarget
// characters that end on a sentence where they can.
// 回复按 Web 客户端读取的方式朗读：去掉代码块、图像、脚注、引用角标和裸链接的 markdown，保留链接文字、行内代码和数学公式，切成约 speechTarget 个字符的片段，并尽量在句子结尾处断开。
const (
	speechTargetCharacters = 300
	maxSpeechCharacters    = 600
	MaxSpeechText          = 100000
)

var markdown = goldmark.New(goldmark.WithExtensions(extension.GFM, extension.Footnote),
	goldmark.WithParserOptions(parser.WithInlineParsers(util.Prioritized(mathParser{}, 500)))).Parser()

var (
	citationBadge = regexp.MustCompile(`^\[?\d+\]?$`)
	lineBreakTag  = regexp.MustCompile(`(?i)^<br\s*/?>$`)
	blankRun      = regexp.MustCompile(`[^\S\n]+`)
	newlineRun    = regexp.MustCompile(` *\n+ *`)
)

// speakableText is a reply's markdown as it is read aloud.
// speakableText 是回复的 markdown 在朗读时所呈现的文本。
func speakableText(source string) string {
	data := []byte(source)
	spoken := readable(markdown.Parse(text.NewReader(data)), data)
	return strings.TrimSpace(newlineRun.ReplaceAllString(blankRun.ReplaceAllString(spoken, " "), "\n"))
}

func readable(n ast.Node, source []byte) string {
	switch node := n.(type) {
	case *ast.FencedCodeBlock, *ast.CodeBlock, *ast.Image, *ast.ThematicBreak, *east.FootnoteLink, *east.FootnoteBacklink, *east.Footnote, *east.FootnoteList, *east.TaskCheckBox:
		return ""
	case *ast.Text:
		value := plainText(node.Value(source))
		if node.IsRaw() {
			value = string(node.Value(source))
		}
		if node.SoftLineBreak() || node.HardLineBreak() {
			value += "\n"
		}
		return value
	case *ast.String:
		return plainText(node.Value)
	case *ast.HTMLBlock:
		return htmlBreak(strings.TrimSpace(string(node.Lines().Value(source))))
	case *ast.RawHTML:
		return htmlBreak(strings.TrimSpace(string(node.Segments.Value(source))))
	case *ast.AutoLink:
		if label := string(node.Label(source)); label != string(node.URL(source)) {
			return label
		}
		return ""
	case *ast.Link:
		label := children(node, source, "")
		if label == string(node.Destination) || citationBadge.MatchString(label) {
			return ""
		}
		return label
	case *ast.Document, *ast.Blockquote, *ast.List, *ast.ListItem, *east.Table, *east.TableHeader, *east.TableRow:
		return children(node, source, "\n")
	}
	return children(n, source, "")
}

func children(n ast.Node, source []byte, separator string) string {
	parts := []string{}
	for child := n.FirstChild(); child != nil; child = child.NextSibling() {
		parts = append(parts, readable(child, source))
	}
	return strings.Join(parts, separator)
}

func plainText(value []byte) string {
	return string(util.ResolveEntityNames(util.ResolveNumericReferences(util.UnescapePunctuations(value))))
}

// htmlBreak reads a line break tag as a new line and other markup as nothing.
// htmlBreak 把换行标签读作换行，其余标记则忽略。
func htmlBreak(tag string) string {
	if lineBreakTag.MatchString(tag) {
		return "\n"
	}
	return ""
}

// splitSpeech cuts text into pieces that take at most maxSpeechCharacters to
// read, joining sentences until a piece reaches speechTargetCharacters and
// cutting a longer sentence at a pause where it can.
// splitSpeech 把文本切分成朗读时长至多 maxSpeechCharacters 的片段：不断拼接句子直到片段达到 speechTargetCharacters，更长的句子则尽量在停顿处断开。
func splitSpeech(text string) []string {
	pieces := []string{}
	current := ""
	flush := func() {
		if piece := strings.TrimSpace(current); piece != "" {
			pieces = append(pieces, piece)
		}
		current = ""
	}
	for _, sentence := range sentences(text) {
		points := []rune(strings.TrimSpace(sentence))
		for speechLength(points) > maxSpeechCharacters {
			flush()
			end := cutSpeech(points)
			pieces = append(pieces, strings.TrimSpace(string(points[:end])))
			points = points[end:]
		}
		remainder := strings.TrimSpace(string(points))
		if remainder == "" {
			continue
		}
		if speechLength([]rune(current))+speechLength(points)+1 > maxSpeechCharacters {
			flush()
		}
		if current != "" {
			current += " "
		}
		current += remainder
		if speechLength([]rune(current)) >= speechTargetCharacters {
			flush()
		}
	}
	flush()
	return pieces
}

// cutSpeech is where a sentence too long for one piece is cut: after the
// most it can read in one, moved back to the last space or Chinese, Japanese
// or Korean pause past speechTargetCharacters where there is one.
// cutSpeech 决定一个过长而无法放入单个片段的句子在哪里断开：先取单片段能朗读的最大长度，再回退到 speechTargetCharacters 之后最近的空格或中、日、韩文的停顿处（若存在）。
func cutSpeech(points []rune) int {
	end, target, length := len(points), 0, 0
	for i, r := range points {
		if length += speechLength([]rune{r}); length > maxSpeechCharacters {
			end = i
			break
		}
		if length <= speechTargetCharacters {
			target = i
		}
	}
	for i := end; i > target; i-- {
		if unicode.IsSpace(points[i]) {
			return i
		}
		if strings.ContainsRune(widePauses, points[i-1]) {
			return i
		}
	}
	return end
}

// speechLength is how long text takes to read, in characters of Latin text:
// a Chinese, Japanese or Korean character takes about as long as two.
// speechLength 以拉丁字符为单位估算文本的朗读时长：一个中、日、韩文字符大约相当于两个拉丁字符。
func speechLength(points []rune) int {
	length := len(points)
	for _, r := range points {
		if unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul) {
			length++
		}
	}
	return length
}

// Sentences end at these marks before a space or the end, and at a run of
// the wide marks of Chinese, Japanese and Korean text, which no space
// follows, with the closing quotes and brackets after them. A long sentence
// of wide text is cut after one of widePauses.
// 句子在这些标点处结束：后跟空格或位于结尾的标点，以及中、日、韩文的全角标点（其后没有空格，且可能带收尾的引号和括号）。过长的全角文本句子会在 widePauses 之一之后断开。
const (
	sentenceEnds     = ".!?"
	wideSentenceEnds = "。！？"
	widePauses       = "，、；：。！？"
)

// sentences cuts text after each sentence's closing punctuation.
// sentences 在每个句子的收尾标点之后切分文本。
func sentences(text string) []string {
	var cut []string
	runes := []rune(text)
	start := 0
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		ends := strings.ContainsRune(sentenceEnds, r) && (i+1 == len(runes) || unicode.IsSpace(runes[i+1]))
		if strings.ContainsRune(wideSentenceEnds, r) {
			ends = true
			for i+1 < len(runes) && (strings.ContainsRune(wideSentenceEnds, runes[i+1]) || unicode.In(runes[i+1], unicode.Pe, unicode.Pf)) {
				i++
			}
		}
		if ends {
			cut = append(cut, string(runes[start:i+1]))
			start = i + 1
		}
	}
	if start < len(runes) {
		cut = append(cut, string(runes[start:]))
	}
	return cut
}

// mathNode is math between dollar signs, which is read as written.
// mathNode 表示美元符号之间的数学公式，朗读时按原样读出。
type mathNode struct {
	ast.BaseInline
}

var kindMath = ast.NewNodeKind("Math")

func (n *mathNode) Kind() ast.NodeKind { return kindMath }
func (n *mathNode) Dump(source []byte, level int) {
	ast.DumpHelper(n, source, level, nil, nil)
}

// mathParser reads math as the web client's markdown reader does: a run
// of dollar signs opens it and a run of as many closes it, across lines.
// mathParser 按 Web 客户端 markdown 阅读器的方式解析数学公式：一串美元符号开启，等长的美元符号串结束，可跨行。
type mathParser struct{}

func (mathParser) Trigger() []byte { return []byte{'$'} }

func (mathParser) Parse(_ ast.Node, block text.Reader, _ parser.Context) ast.Node {
	line, start := block.PeekLine()
	opener := 0
	for opener < len(line) && line[opener] == '$' {
		opener++
	}
	block.Advance(opener)
	l, pos := block.Position()
	node := &mathNode{}
	for {
		line, segment := block.PeekLine()
		if line == nil {
			block.SetPosition(l, pos)
			return ast.NewTextSegment(start.WithStop(start.Start + opener))
		}
		for i := 0; i < len(line); i++ {
			if line[i] != '$' {
				continue
			}
			run := i
			for i < len(line) && line[i] == '$' {
				i++
			}
			if i-run == opener {
				if inner := segment.WithStop(segment.Start + run); !inner.IsEmpty() {
					node.AppendChild(node, ast.NewRawTextSegment(inner))
				}
				block.Advance(i)
				if node.ChildCount() == 0 {
					block.SetPosition(l, pos)
					return ast.NewTextSegment(start.WithStop(start.Start + opener))
				}
				return node
			}
		}
		node.AppendChild(node, ast.NewRawTextSegment(segment))
		block.AdvanceLine()
	}
}
