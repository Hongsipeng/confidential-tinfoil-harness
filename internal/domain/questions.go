// 本文件定义问题 widget 的解析与答案处理。
package domain

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

// QuestionsWidget is the widget whose questions the user answers in place.
// QuestionsWidget 是让用户就地回答问题的问题 widget。
const QuestionsWidget = "render_questions"

// QuestionsShown is what the model reads once it has shown its questions.
// QuestionsShown 是模型在展示其问题之后读到的内容。
const QuestionsShown = "The questions are shown to the user, who answers them in place. Their answers reach you with their next message."

// SingleSelection is how a question that takes one option says so.
// SingleSelection 表示只接受一个选项的问题。
const SingleSelection = "single"

// questions are a questions widget's arguments.
// questions 是问题 widget 的参数。
type questions struct {
	Title     string     `json:"title"`
	Questions []question `json:"questions"`
}

type question struct {
	ID        string           `json:"id"`
	Prompt    string           `json:"prompt"`
	Selection string           `json:"selection"`
	Options   []QuestionOption `json:"options"`
}

// QuestionOption is one answer a question offers.
// QuestionOption 是问题提供的一个答案选项。
type QuestionOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// readQuestions reads a questions widget's arguments, refusing ids its
// schema cannot say are unique: a client keeps each answer by them.
// readQuestions 读取问题 widget 的参数，并拒绝其模式无法保证唯一的 id：客户端靠这些 id
// 保存每个答案。
func readQuestions(arguments string) (questions, error) {
	var q questions
	if err := json.Unmarshal([]byte(arguments), &q); err != nil {
		return questions{}, err
	}
	asked := map[string]bool{}
	for _, item := range q.Questions {
		if asked[item.ID] {
			return questions{}, fmt.Errorf("question id %q is used more than once", item.ID)
		}
		asked[item.ID] = true
		offered := map[string]bool{}
		for _, o := range item.Options {
			if offered[o.ID] {
				return questions{}, fmt.Errorf("option id %q is used more than once in question %q", o.ID, item.ID)
			}
			offered[o.ID] = true
		}
	}
	return q, nil
}

// answerableQuestions are the questions of a widget call the user can
// answer: one whose arguments the widget draws.
// answerableQuestions 返回用户可回答的、某次 widget 调用的问题：即该 widget 能按参数
// 绘制的那些。
func answerableQuestions(b *Block) (questions, bool) {
	if b.Type != "tool_call" || b.Name != QuestionsWidget {
		return questions{}, false
	}
	for _, w := range WidgetCatalog() {
		if w.Name == QuestionsWidget && WidgetFailure(w, b.Arguments) == nil {
			q, err := readQuestions(b.Arguments)
			return q, err == nil
		}
	}
	return questions{}, false
}

// picked is the options of a question that ids name, in the order the
// question offers them, or why they cannot be its answer.
// picked 返回问题中由 ids 指名的选项，按问题提供的顺序排列；若不能作为答案，则返回原因。
func (q *question) picked(ids []string) ([]string, error) {
	chosen := []string{}
	for _, o := range q.Options {
		if slices.Contains(ids, o.ID) {
			chosen = append(chosen, o.ID)
		}
	}
	for _, id := range ids {
		if !slices.Contains(chosen, id) {
			return nil, api.Err(409, "REVISION_CONFLICT", "The question no longer offers that option")
		}
	}
	if q.Selection == SingleSelection && len(chosen) != 1 {
		return nil, api.Err(400, "BAD_REQUEST", "A single-choice question takes one option")
	}
	return chosen, nil
}

// AnswerQuestions changes the answers the user gave in a questions widget of
// a reply: each question named takes the options picked, or none to leave it
// unanswered. Questions not named keep their answers.
// AnswerQuestions 修改用户在回复的某个问题 widget 中给出的答案：每个被指名的题目采用
// 选中的选项，或为 nil 以保持未答。未指名的题目保留原答案。
func AnswerQuestions(m *Message, callID string, changes map[string]*[]string) error {
	// Only a reply's widgets are read for the answers the next message sends.
	// 只有回复的 widget 会被读取，以获取下一条消息所发送的答案。
	if m.Role != "assistant" {
		return api.Err(400, "BAD_REQUEST", "Only a reply takes answers")
	}
	index := slices.IndexFunc(m.Timeline, func(b Block) bool { return b.Type == "tool_call" && b.CallID() == callID })
	if index < 0 {
		return api.Err(409, "REVISION_CONFLICT", "The widget was removed")
	}
	block := &m.Timeline[index]
	if block.Name != QuestionsWidget {
		return api.Err(400, "BAD_REQUEST", "The widget takes no answers")
	}
	asked, ok := answerableQuestions(block)
	if !ok {
		return api.Err(400, "BAD_REQUEST", "The widget's questions cannot be answered")
	}
	answers := maps.Clone(block.Answers)
	if answers == nil {
		answers = map[string][]string{}
	}
	for id, ids := range changes {
		at := slices.IndexFunc(asked.Questions, func(q question) bool { return q.ID == id })
		if at < 0 {
			return api.Err(409, "REVISION_CONFLICT", "The widget no longer asks that question")
		}
		if ids == nil {
			delete(answers, id)
			continue
		}
		chosen, err := asked.Questions[at].picked(*ids)
		if err != nil {
			return err
		}
		answers[id] = chosen
	}
	block.Answers = answers
	if len(answers) == 0 {
		block.Answers = nil
	}
	return nil
}

// WidgetAnswers is what the user had answered of one questions widget when
// they sent a message. It holds the questions' text, so the message reads
// the same however the widget changes later.
// WidgetAnswers 是用户发送消息时对某个问题 widget 已做的回答。它保存题目的文本，因此
// 无论 widget 之后如何变化，消息读起来都相同。
type WidgetAnswers struct {
	ToolCallID string             `json:"toolCallId"`
	Title      string             `json:"title,omitempty"`
	Questions  []AnsweredQuestion `json:"questions"`
}

// AnsweredQuestion is a question and the options picked for it. An answered
// question may have none picked, as a checklist with nothing ticked.
// AnsweredQuestion 是一道题以及为其选中的选项。已答的题也可能没有选中任何选项，如同
// 未勾选任何项的清单。
type AnsweredQuestion struct {
	ID       string           `json:"id"`
	Prompt   string           `json:"prompt"`
	Answered bool             `json:"answered"`
	Selected []QuestionOption `json:"selected"`
}

// answersOf is what a questions widget's call holds answered now.
// answersOf 返回某次问题 widget 调用当前已答的内容。
func answersOf(b *Block) (WidgetAnswers, bool) {
	asked, ok := answerableQuestions(b)
	if !ok {
		return WidgetAnswers{}, false
	}
	answers := WidgetAnswers{ToolCallID: b.CallID(), Title: asked.Title, Questions: []AnsweredQuestion{}}
	for _, q := range asked.Questions {
		ids, answered := b.Answers[q.ID]
		item := AnsweredQuestion{ID: q.ID, Prompt: q.Prompt, Answered: answered, Selected: []QuestionOption{}}
		for _, o := range q.Options {
			if slices.Contains(ids, o.ID) {
				item.Selected = append(item.Selected, o)
			}
		}
		answers.Questions = append(answers.Questions, item)
	}
	return answers, true
}

// picks are the options picked for each answered question, by its id.
// picks 按题目 id 返回每道已答题所选中的选项。
func (w *WidgetAnswers) picks() map[string][]string {
	picks := map[string][]string{}
	for _, q := range w.Questions {
		if !q.Answered {
			continue
		}
		picks[q.ID] = []string{}
		for _, o := range q.Selected {
			picks[q.ID] = append(picks[q.ID], o.ID)
		}
	}
	return picks
}

// PendingAnswers are the answers a new message after messages sends: those
// of each questions widget whose answers changed since a message last sent
// them, or that no message has sent yet.
// PendingAnswers 是位于 messages 之后的新消息要发送的答案：自上一条消息发送以来答案发生
// 变化的每个问题 widget，或尚未有任何消息发送过答案的那些。
func PendingAnswers(messages []Message) []WidgetAnswers {
	sent := map[string]map[string][]string{}
	for _, m := range messages {
		for _, answers := range m.Answers {
			sent[answers.ToolCallID] = answers.picks()
		}
	}
	var pending []WidgetAnswers
	for i := range messages {
		if messages[i].Role != "assistant" {
			continue
		}
		for j := range messages[i].Timeline {
			current, ok := answersOf(&messages[i].Timeline[j])
			if !ok || maps.EqualFunc(current.picks(), sent[current.ToolCallID], slices.Equal) {
				continue
			}
			pending = append(pending, current)
		}
	}
	return pending
}

// KeepAnswers gives the questions widgets of a reply the answers another
// copy of it holds, as a continued reply takes those given while it ran.
// KeepAnswers 把另一份回复副本所持有的答案赋予该回复的问题 widget，如同续写的回复采用
// 其在运行期间收到的答案。
func (m *Message) KeepAnswers(from *Message) {
	given := map[string]map[string][]string{}
	for _, b := range from.Timeline {
		if b.Type == "tool_call" && b.Name == QuestionsWidget {
			given[b.CallID()] = b.Answers
		}
	}
	if len(given) == 0 {
		return
	}
	m.Timeline = clonedSlice(m.Timeline)
	for i := range m.Timeline {
		b := &m.Timeline[i]
		if answers, ok := given[b.CallID()]; ok && b.Type == "tool_call" && b.Name == QuestionsWidget {
			b.Answers = answers
		}
	}
}

// AnswersPrompt is how a message's answers read to the model.
// AnswersPrompt 是消息答案呈现给模型的方式。
func AnswersPrompt(answers []WidgetAnswers) string {
	var b strings.Builder
	b.WriteString("<question_answers>")
	for _, w := range answers {
		fmt.Fprintf(&b, "\nThe user's answers to the questions of %s call %s", QuestionsWidget, w.ToolCallID)
		if w.Title != "" {
			fmt.Fprintf(&b, " (%s)", w.Title)
		}
		b.WriteString(":")
		for i, q := range w.Questions {
			// An option is named by its id too, which tells apart options
			// that read the same.
			// 选项也以其 id 命名，以区分读起来相同的选项。
			labels := []string{}
			for _, o := range q.Selected {
				labels = append(labels, fmt.Sprintf("[%s] %s", o.ID, o.Label))
			}
			answer := strings.Join(labels, "; ")
			switch {
			case !q.Answered:
				answer = "(not answered)"
			case len(labels) == 0:
				answer = "(none selected)"
			}
			fmt.Fprintf(&b, "\n%d. %s\nAnswer: %s", i+1, q.Prompt, answer)
		}
	}
	b.WriteString("\n</question_answers>")
	return b.String()
}
