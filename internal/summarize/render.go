package summarize

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

// Document is one item a summary reads: text, in a tag named by Kind whose
// attributes tell the model what it is.
// Document 是摘要读取的一项内容：由 Kind 命名的标签包裹的文本，标签的属性告知模型它是什么。
type Document struct {
	Kind  string
	Attrs []Attr
	Text  string
}

// Attr is one attribute of a document's tag.
// Attr 是文档标签的一个属性。
type Attr struct {
	Name, Value string
}

// tags are the opening and closing tags a document is read in.
// tags 返回文档被读取时所用的开始与结束标签。
func (d Document) tags() (open, end string) {
	var b strings.Builder
	b.WriteString("<" + d.Kind)
	for _, a := range d.Attrs {
		b.WriteString(" " + a.Name + `="` + attrEscaper.Replace(a.Value) + `"`)
	}
	return b.String() + ">", "</" + d.Kind + ">"
}

// earlierTag holds what a summary of the input before a part said.
// earlierTag 保存对某部分之前的输入所做摘要的内容。
const earlierTag = "previous_summary"

// sectionsOpening introduces a task's sections in its system prompt.
// sectionsOpening 在 system prompt 中引出 task 的各 section。
const sectionsOpening = `Write the summary in these sections, in this order, each under a level-two heading written exactly as here, whatever language the rest is in. Write "None" under a section with nothing in it.`

// system is the task's system prompt.
// system 是该 task 的 system prompt。
func (t *Task) system() string {
	prompt := t.Instructions
	if len(t.Sections) > 0 {
		prompt += "\n\n" + sectionsOpening
		for _, s := range t.Sections {
			prompt += "\n\n## " + s.Heading + "\n" + s.Description
		}
	}
	if t.MaxWords > 0 {
		prompt += fmt.Sprintf(" Use between %d and %d words.", t.MinWords, t.MaxWords)
	}
	return prompt
}

// user is the user message a call sends: what the parts before said, then
// the part's documents in the task's frame, then its closing. A task without
// a frame sends its documents' text as it is.
// user 是每次调用所发送的用户消息：先前的各部分内容，然后是当前部分的文档（置于 task 的 frame 中），最后是结尾。没有 frame 的 task 会原样发送其文档文本。
func (t *Task) user(earlier string, docs []Document) string {
	if t.Frame == "" {
		texts := make([]string, len(docs))
		for i, d := range docs {
			texts[i] = d.Text
		}
		return strings.Join(texts, "\n\n")
	}
	tags := []string{t.Frame, earlierTag}
	for _, d := range docs {
		tags = append(tags, d.Kind)
	}
	neutral := neutralizer(tags)
	var b strings.Builder
	if earlier != "" {
		b.WriteString("<" + earlierTag + ">\n" + neutral(earlier) + "\n</" + earlierTag + ">\n\n")
	}
	b.WriteString("<" + t.Frame + ">")
	for _, d := range docs {
		open, end := d.tags()
		b.WriteString("\n" + open + "\n" + neutral(d.Text) + "\n" + end)
	}
	b.WriteString("\n</" + t.Frame + ">")
	if t.Closing != "" {
		b.WriteString("\n\n" + t.Closing)
	}
	return b.String()
}

var attrEscaper = strings.NewReplacer("&", "&amp;", `"`, "&quot;", "<", "&lt;", ">", "&gt;")

// neutralizer escapes the opening of any of the tags in text, so text cannot
// close the tag it is read in or pose as another document, while code and
// other angle brackets stay as written.
// neutralizer 转义文本中任何标签的开头，使文本无法闭合它所处的标签或伪装成其他文档，而代码及其他尖括号保持原样。
func neutralizer(tags []string) func(string) string {
	names := make([]string, len(tags))
	for i, tag := range tags {
		names[i] = regexp.QuoteMeta(tag)
	}
	pattern := regexp.MustCompile(`(?i)<(/?(?:` + strings.Join(names, "|") + `)\b)`)
	return func(text string) string { return pattern.ReplaceAllString(text, "&lt;$1") }
}

// finishedReason is the finish reason of a model that ended its answer
// itself, rather than being cut off or filtered.
// finishedReason 是模型自行结束回答（而非被截断或过滤）时的 finish reason。
const finishedReason = "stop"

// check is the summary a call wrote, refused when the model did not end it
// itself, wrote nothing, or did not write exactly the task's sections in
// order.
// check 校验一次调用写出的摘要：当模型未自行结束、未写出内容或未按顺序写出 task 的各 section 时拒绝。
func (t *Task) check(text, finishReason string) (string, error) {
	if finishReason != finishedReason {
		return "", fmt.Errorf("%w: it ended with finish reason %q", ErrInvalid, finishReason)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("%w: it is empty", ErrInvalid)
	}
	if t.MaxLineChars > 0 && (strings.ContainsAny(text, "\r\n") || strings.Contains(text, "```") || utf8.RuneCountInString(text) > t.MaxLineChars) {
		return "", fmt.Errorf("%w: it is not one line of at most %d characters without code", ErrInvalid, t.MaxLineChars)
	}
	if len(t.Sections) == 0 {
		return text, nil
	}
	want := make([]string, len(t.Sections))
	for i, s := range t.Sections {
		want[i] = s.Heading
	}
	if got := headings(text); !slices.Equal(got, want) {
		return "", fmt.Errorf("%w: its sections are %q", ErrInvalid, got)
	}
	return text, nil
}

// headings are the level-two headings of Markdown text, in order, leaving
// out lines of fenced code, where a summary may quote Markdown verbatim.
// headings 按顺序返回 Markdown 文本的二级标题，跳过代码围栏内的行（摘要可能逐字引用 Markdown）。
func headings(text string) []string {
	var found []string
	fence := ""
	for line := range strings.Lines(text) {
		trimmed := strings.TrimSpace(line)
		switch {
		case fence != "":
			if strings.HasPrefix(trimmed, fence) {
				fence = ""
			}
		case strings.HasPrefix(trimmed, "```"), strings.HasPrefix(trimmed, "~~~"):
			fence = trimmed[:3]
		case strings.HasPrefix(line, "## "):
			found = append(found, strings.TrimSpace(strings.TrimPrefix(line, "## ")))
		}
	}
	return found
}
