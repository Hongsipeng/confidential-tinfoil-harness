package engine

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// Search and fetch results as the model reads them, ported from the model
// router (toolruntime/citations/format.go) without its Harmony cursors: every
// model here is asked for markdown links.
// 模型所读取的搜索与抓取结果，移植自模型 router（toolruntime/citations/format.go），
// 但不包含其 Harmony 游标：这里的每个模型都被要求给出 markdown 链接。

const citationInstructions = "Attach sources by embedding a clickable markdown link to the original URL directly after the sentence it supports. Format every citation exactly like this example, copying the punctuation characters verbatim: The sky is blue [Example page](https://example.com/article). Reference 1-2 sources per claim; do not reference every source on every sentence. Copy the URL character-for-character from the tool output: preserve or omit a trailing slash exactly as the tool emitted it, keep query parameters verbatim, and do not append punctuation, whitespace, zero-width characters, or any other character after the URL before the closing parenthesis. Never invent URLs, never paraphrase URLs, and never wrap the link in any other brackets, braces, or quotation marks."

// citationSource is a page a tool showed the model, which its answer may link.
// citationSource 是工具展示给模型的页面，其回答可能会链接到它。
type citationSource struct{ url, title string }

// modelCopy is a search or fetch result as cited text, with the pages it
// showed; other output is read as it is.
// modelCopy 是将搜索或抓取结果转换为引用文本（含其展示的页面）；其他输出则按原样读取。
func modelCopy(name, content string) (string, []citationSource) {
	var value api.Object
	if json.Unmarshal([]byte(content), &value) != nil || len(value) == 0 {
		return content, nil
	}
	formatted := ""
	var sources []citationSource
	switch name {
	case "web_search":
		formatted = formatSearchOutput(value["results"])
		for _, item := range api.Arr(value["results"]) {
			sources = append(sources, citationSource{strings.TrimSpace(api.Str(api.Obj(item)["url"])), strings.TrimSpace(api.Str(api.Obj(item)["title"]))})
		}
	case "web_fetch":
		formatted = cmp.Or(formatFetchOutput(value["pages"]), formatFetchFailures(value["results"]))
		for _, item := range api.Arr(value["pages"]) {
			sources = append(sources, citationSource{strings.TrimSpace(api.Str(api.Obj(item)["url"])), "Fetched page"})
		}
	}
	return cmp.Or(formatted, content), sources
}

// A URL may hold one level of balanced parentheses, as Wikipedia's Mercury_(planet) does.
// URL 中可以包含一层成对的括号，正如 Wikipedia 的 Mercury_(planet) 那样。
var markdownLinkPattern = regexp.MustCompile(`\[([^\]]+)\]\((https?://(?:[^\s()]|\([^\s()]*\))+)\)`)

// citationAnnotations marks each markdown link in text to a page the run's
// tools showed the model, as chat completions url_citation annotations over
// the link's label, in code points. A source without a title is named by its
// URL; the model's own spelling of the URL is kept.
// citationAnnotations 将文本中每个指向 run 的工具所展示页面的 markdown 链接，标注为覆盖链接
// 标签的 chat completions url_citation 注释（以码点计）。没有标题的来源以其 URL 命名；
// 保留模型自己对 URL 的拼写。
func citationAnnotations(text string, sources []citationSource) []domain.Annotation {
	byURL := map[string]citationSource{}
	for _, source := range sources {
		key := normalizeURL(source.url)
		if existing, seen := byURL[key]; key != "" && (!seen || strings.TrimSpace(existing.title) == "") {
			byURL[key] = source
		}
	}
	var annotations []domain.Annotation
	for _, match := range markdownLinkPattern.FindAllStringSubmatchIndex(text, -1) {
		link := text[match[4]:match[5]]
		source, ok := byURL[normalizeURL(link)]
		if !ok {
			continue
		}
		title := source.title
		if strings.TrimSpace(title) == "" {
			title = link
		}
		annotations = append(annotations, domain.Annotation{Type: "url_citation", URLCitation: &domain.URLCitation{URL: link, Title: title,
			StartIndex: utf8.RuneCountInString(text[:match[2]]), EndIndex: utf8.RuneCountInString(text[:match[3]])}})
	}
	return annotations
}

// trackingParams do not change which page a URL names.
// trackingParams 不会改变 URL 所指向的页面。
var trackingParams = map[string]bool{"utm_source": true, "utm_medium": true, "utm_campaign": true, "utm_term": true, "utm_content": true, "utm_id": true,
	"fbclid": true, "gclid": true, "mc_cid": true, "mc_eid": true, "ref": true, "ref_src": true}

// normalizeURL is the key a cited URL is matched to a source by, across what
// search results and model output disagree on: trailing slashes, www., case,
// tracking parameters, fragments and invisible characters. A string that does
// not parse is its own key.
// normalizeURL 是被引用的 URL 与来源进行匹配所用的 key，它抹平了搜索结果与模型输出之间
// 不一致的地方：末尾斜杠、www.、大小写、tracking 参数、片段以及不可见字符。
// 无法解析的字符串以自身作为 key。
func normalizeURL(raw string) string {
	raw = strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029' {
			return -1
		}
		return r
	}, raw))
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.TrimPrefix(strings.ToLower(parsed.Host), "www.")
	if len(parsed.Path) > 1 {
		parsed.Path = strings.TrimRight(parsed.Path, "/")
	}
	if query := parsed.Query(); len(query) > 0 {
		for key := range query {
			if trackingParams[strings.ToLower(key)] {
				query.Del(key)
			}
		}
		parsed.RawQuery = query.Encode()
	}
	parsed.Fragment, parsed.RawFragment = "", ""
	return parsed.String()
}

func formatSearchOutput(raw any) string {
	results, _ := raw.([]any)
	if len(results) == 0 {
		return "No safe search results were found."
	}
	var out strings.Builder
	for _, item := range results {
		result, ok := item.(map[string]any)
		if !ok {
			continue
		}
		fmt.Fprintf(&out, "Source: %s\n", cmp.Or(strings.TrimSpace(api.Str(result["title"])), "Search result"))
		if url := strings.TrimSpace(api.Str(result["url"])); url != "" {
			fmt.Fprintf(&out, "URL: %s\n", url)
		}
		if published := strings.TrimSpace(api.Str(result["published_date"])); published != "" {
			fmt.Fprintf(&out, "Published: %s\n", published)
		}
		if content := strings.TrimSpace(api.Str(result["content"])); content != "" {
			out.WriteString(content + "\n")
		}
		out.WriteString("\n")
	}
	return strings.TrimSpace(out.String())
}

// formatFetchOutput is empty when nothing was fetched.
// formatFetchOutput 在未抓取到任何内容时返回空。
func formatFetchOutput(raw any) string {
	var out strings.Builder
	for _, item := range api.Arr(raw) {
		page, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out.WriteString("Source: Fetched page\n")
		if url := strings.TrimSpace(api.Str(page["url"])); url != "" {
			fmt.Fprintf(&out, "URL: %s\n", url)
		}
		if content := strings.TrimSpace(api.Str(page["content"])); content != "" {
			out.WriteString(content + "\n")
		}
		out.WriteString("\n")
	}
	return strings.TrimSpace(out.String())
}

func formatFetchFailures(raw any) string {
	var lines []string
	for _, item := range api.Arr(raw) {
		result, ok := item.(map[string]any)
		if !ok || strings.TrimSpace(api.Str(result["status"])) == "completed" {
			continue
		}
		url, failure := strings.TrimSpace(api.Str(result["url"])), strings.TrimSpace(api.Str(result["error"]))
		switch {
		case url != "" && failure != "":
			lines = append(lines, fmt.Sprintf("Fetch failed for %s: %s", url, failure))
		case url != "":
			lines = append(lines, fmt.Sprintf("Fetch failed for %s.", url))
		case failure != "":
			lines = append(lines, "Fetch failed: "+failure)
		}
	}
	return strings.Join(lines, "\n")
}
