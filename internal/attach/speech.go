package attach

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	usagereporting "github.com/tinfoilsh/usage-reporting-go"
)

// Read-aloud audio is mono 16-bit 24 kHz PCM. Each piece of a message is
// synthesized apart, at most speechParallelism at once, and the pieces
// stream back in order as one response.
// 朗读音频为单声道 16-bit 24 kHz PCM。消息的每个片段单独合成，同时最多合成 speechParallelism 段，并按顺序作为一个响应流式返回。
const (
	speechModel        = "qwen3-tts"
	SpeechVoice        = "aiden"
	SpeechInstructions = "Read in a calm, natural conversational voice with a steady pace, consistent pitch range, and restrained expression. Use natural sentence pauses and maintain an even narration style."
	speechMediaType    = "audio/pcm"
	speechTimeout      = 2 * time.Minute // for each piece
	speechParallelism  = 2
	speechReadSize     = 32 << 10
	speechAttempts     = 2
	// speechBytesPerSecond is how much audio a second of speech takes.
	// speechBytesPerSecond 是每秒 speech 所产生的音频字节数。
	speechBytesPerSecond = 24000 * 2
	// A piece is read with at most speechBaseSeconds of audio, and
	// speechSecondsPerCharacter for each character it reads, about four
	// times what an English character takes and twice a Chinese one.
	// 每个片段以最多 speechBaseSeconds 的音频朗读，再加上每个字符 speechSecondsPerCharacter 的时长——约为英文字符的四倍、中文字符的两倍。
	speechBaseSeconds         = 5
	speechSecondsPerCharacter = 0.25
	// speechParentService is the service the router lets bill a reading's
	// pieces once.
	// speechParentService 是 router 允许把一次朗读的各个片段一并计费到其下的服务。
	speechParentService = "harness"
)

// HandleSpeech 处理朗读消息的 HTTP 请求。
func (at *Service) HandleSpeech(w http.ResponseWriter, r *http.Request) {
	p, err := at.Auth.Authenticate(r, true)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	credential, err := at.Auth.Inference(r.Context(), p)
	if err != nil {
		api.Respond(w, nil, auth.WithQuota(p, credential.Limit, err))
		return
	}
	body, err := api.ReadBody(w, r, domain.MaxTurnBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	in := &speechRequest{}
	defer in.ClearKey()
	if err := api.DecodeBody(body, in); err != nil {
		api.Respond(w, nil, err)
		return
	}
	if err := in.Validate(); err != nil {
		api.Respond(w, nil, err)
		return
	}
	text, err := at.speechText(r.Context(), p, in)
	in.ClearKey()
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	pieces := splitSpeech(text)
	if len(pieces) == 0 {
		api.Respond(w, nil, api.Err(400, "NOTHING_TO_READ", "There is nothing to read aloud"))
		return
	}
	router := at.Models.Services["router"]
	if router == nil {
		api.Respond(w, nil, api.Err(503, "TOOL_UNAVAILABLE", "Speech is unavailable"))
		return
	}
	// A reading asks for its pieces as it plays, so a long one outlasts the
	// chat token it started with.
	// 朗读在播放过程中按需请求各片段，因此较长的朗读可以超过启动它时所用的 chat token。
	ctx := auth.WithCaller(r.Context(), at.Store.ChatAccess(at.Auth, p, credential))
	// A reading counts as one request, as a run does: its pieces report
	// usage under it, and only the first is billed.
	// 一次朗读计作一次请求，与一次 run 相同：其片段在它之下上报 usage，且只有第一个片段被计费。
	reading := "speech_" + api.Token()
	ctx = auth.WithUsage(ctx, usagereporting.Context{ContextID: reading, RootRequestID: reading, ParentService: speechParentService, APIKeyHash: usagereporting.HashAPIKey(string(credential.Key)), Depth: 1})
	if err := streamSpeech(ctx, w, router, pieces, domain.ValueOr(in.Speed, domain.VoiceSpeed)); err != nil {
		api.Respond(w, nil, auth.WithQuota(p, credential.Limit, err))
	}
}

// speechText is what a request reads aloud: a message's markdown, or text
// a client selected, as it is spoken.
// speechText 是请求要朗读的内容：消息的 markdown，或客户端选中的文本，按朗读方式呈现。
func (at *Service) speechText(ctx context.Context, p *auth.Principal, in *speechRequest) (string, error) {
	source := domain.ValueOr(in.Text, "")
	if in.MessageID != "" {
		var thread domain.Thread
		switch {
		case in.Local:
			id := in.ThreadID
			if id == "" {
				var named struct {
					ID string `json:"id"`
				}
				json.Unmarshal(in.Thread, &named)
				id = named.ID
			}
			var err error
			if thread, err = domain.ReadDeviceThread(id, in.Thread); err != nil {
				return "", err
			}
		case p.Anonymous:
			return "", auth.ErrAnonymousCloud
		default:
			key, err := auth.DecodeContentKey(in.Key)
			if err != nil {
				return "", err
			}
			defer clear(key[:])
			row, err := at.Store.LoadThread(ctx, p, key, in.ThreadID)
			clear(key[:])
			if err != nil {
				return "", err
			}
			thread = row.Thread
		}
		index := thread.MessageIndex(in.MessageID)
		if index < 0 {
			return "", api.Err(404, "THREAD_NOT_FOUND", "The message was not found")
		}
		source = thread.Messages[index].Content
	}
	if utf8.RuneCountInString(source) > MaxSpeechText {
		return "", api.Err(413, "PAYLOAD_TOO_LARGE", "The text is too long to read aloud")
	}
	return speakableText(source), nil
}

// speechAudio is one piece's audio as it arrives, held until it is written,
// so a piece synthesized ahead never waits on the one being written.
// speechAudio 保存某片段的音频（边接收边保存，直到写入完成），这样提前合成的片段不会等待正在写入的片段。
type speechAudio struct {
	mu   sync.Mutex
	data []byte
	done bool
	err  error
	wake chan struct{} // closed and replaced whenever the audio changes
}

func newSpeechAudio() *speechAudio { return &speechAudio{wake: make(chan struct{})} }

func (a *speechAudio) add(audio []byte, done bool, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.data, a.done, a.err = append(a.data, audio...), done, err
	close(a.wake)
	a.wake = make(chan struct{})
}

// from is the audio past offset, whether the piece ended, and how.
// from 返回 offset 之后的音频、该片段是否结束以及结束原因。
func (a *speechAudio) from(offset int) ([]byte, bool, error, <-chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.data[offset:], a.done, a.err, a.wake
}

// streamSpeech writes the audio of each piece in order, played at speed. A
// piece is synthesized once a slot frees up, and its slot frees up once its
// audio is written. A failure before any audio is returned; one after
// aborts the response, so its client reads the audio as incomplete.
// streamSpeech 按顺序写入各片段的音频，并以指定 speed 播放。某片段在槽位空闲时才合成，其音频写入后槽位即释放。在返回任何音频之前发生的失败会作为错误返回；之后的失败则中止响应，使客户端读到不完整的音频。
func streamSpeech(ctx context.Context, w http.ResponseWriter, router *inference.Service, pieces []string, speed float64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	audio := make([]*speechAudio, len(pieces))
	for i := range audio {
		audio[i] = newSpeechAudio()
	}
	slots := make(chan struct{}, speechParallelism)
	go func() {
		for i, piece := range pieces {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			pieceCtx := ctx
			if usage, ok := auth.Usage(ctx); ok && i == 0 {
				usage.BillCustomerRequest = true
				pieceCtx = auth.WithUsage(ctx, usage)
			}
			go synthesize(pieceCtx, router, piece, speed, audio[i])
		}
	}()
	started := false
	flush := http.NewResponseController(w)
	for i := range pieces {
		for written := 0; ; {
			data, done, err, wake := audio[i].from(written)
			if len(data) > 0 {
				if !started {
					w.Header().Set("Content-Type", speechMediaType)
					w.Header().Set("Cache-Control", "no-store")
					w.Header().Set("X-Content-Type-Options", "nosniff")
					started = true
				}
				if _, err := w.Write(data); err != nil || flush.Flush() != nil {
					return nil
				}
				written += len(data)
				continue
			}
			if err != nil && !started {
				return err
			}
			if err != nil {
				panic(http.ErrAbortHandler)
			}
			if done {
				break
			}
			select {
			case <-wake:
			case <-ctx.Done():
				return nil
			}
		}
		audio[i] = nil
		<-slots
	}
	if !started {
		return api.Err(502, "UPSTREAM_REFUSED", "The speech service returned no audio")
	}
	return nil
}

// synthesize reads the audio the router speaks one piece of text with. A
// piece the speech service failed before any of its audio arrived is asked
// for again, up to speechAttempts times in all.
// synthesize 读取 router 朗读某段文本所产生的音频。对于在收到任何音频之前就失败的片段，会重新请求，总计最多 speechAttempts 次。
func synthesize(ctx context.Context, router *inference.Service, text string, speed float64, out *speechAudio) {
	var err error
	defer func() {
		if failure := api.Rescued("speech synthesis", recover()); failure != nil {
			err = failure
		}
		out.add(nil, true, err)
	}()
	for attempt := 1; ; attempt++ {
		var spoke, retry bool
		spoke, retry, err = speak(ctx, router, text, speed, out)
		if err == nil || spoke || !retry || attempt == speechAttempts || ctx.Err() != nil {
			return
		}
	}
}

// speak asks the router once for a piece's audio, which it adds to out as it
// arrives, at most what speechBudget allows, played at speed. It reports
// whether it added any audio, and whether a failure is one the speech
// service may not repeat.
// speak 向 router 请求一次片段的音频，音频到达时追加到 out，最多不超过 speechBudget 允许的量，并以 speed 播放。它报告是否加入了音频，以及某次失败是否为 speech 服务可能不会重现的失败。
func speak(ctx context.Context, router *inference.Service, text string, speed float64, out *speechAudio) (spoke, retry bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, speechTimeout)
	defer cancel()
	speech := api.Raw(api.Object{"model": speechModel, "voice": SpeechVoice, "instructions": SpeechInstructions, "input": text, "response_format": "pcm", "stream_format": "audio"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, router.URL+"/v1/audio/speech", bytes.NewReader(speech))
	if err != nil {
		return false, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := router.Client.Do(req)
	if err != nil {
		return false, true, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return false, resp.StatusCode >= http.StatusInternalServerError, api.WithRetryAfter(api.DownstreamError(resp.StatusCode, b), resp.Header)
	}
	if media, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); media != speechMediaType {
		return false, false, api.Err(502, "UPSTREAM_REFUSED", "The speech service returned unsupported audio")
	}
	var stretch *stretcher
	if speed != 1 {
		stretch = newStretcher(speed)
	}
	body, chunk := io.LimitReader(resp.Body, speechBudget(text)), make([]byte, speechReadSize)
	for {
		n, readErr := body.Read(chunk)
		if n > 0 {
			audio := chunk[:n]
			if stretch != nil {
				audio = stretch.push(audio)
			}
			if len(audio) > 0 {
				out.add(audio, false, nil)
				spoke = true
			}
		}
		if readErr == io.EOF {
			if stretch != nil {
				out.add(stretch.flush(), false, nil)
			}
			return spoke, false, nil
		}
		if readErr != nil {
			return spoke, true, readErr
		}
	}
}

// speechBudget is the most audio a piece is read with, in bytes: several
// times what reading it takes, so a voice that runs on in a loop stops soon
// after it should have, and the next piece is read.
// speechBudget 是朗读一个片段所用音频的最大字节数：为实际朗读所需的数倍，这样循环不停的声音也会在应当停止后不久被截断，接着朗读下一片段。
func speechBudget(text string) int64 {
	seconds := speechBaseSeconds + speechSecondsPerCharacter*float64(speechLength([]rune(text)))
	return int64(seconds * speechBytesPerSecond)
}
