package attach

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
	_ "golang.org/x/image/bmp"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

// Service 处理 chat 中除文本之外的附件：上传、文档、图像与语音。
type Service struct {
	Auth   *auth.Authenticator
	Models *inference.Models
	Store  *store.Store
	Pages  Pages
	Images Images
}

// Text is read whole into a chat, so a text file may be larger than the
// files the harness converts. No upload is read past maxTextBytes.
// 文本会被整体读入 chat，因此文本文件可能大于 harness 转换的文件。任何上传都不会读取超过 maxTextBytes 的内容。
const MaxUploadBytes = 33 << 20
const maxTextBytes = 50 << 20
const maxImagePixels = 40_000_000

// An image is kept as the copy models read, no longer on either side than
// modelImageSide, with a thumbnail no longer than thumbnailSide.
// 图像以模型读取时的副本形式保存，任一边都不超过 modelImageSide，缩略图不超过 thumbnailSide。
const (
	modelImageSide = 1536
	thumbnailSide  = 256
)

// Resizing decodes an image whole, so the images a turn sends larger than
// the copy models read hold together no more pixels than one image may.
// 缩放会整体解码图像，因此一个 Turn 发送的、大于模型读取副本的图像，其像素总数不得超过单张图像的上限。
const MaxTurnResizePixels = maxImagePixels

// An upload's form holds its file and a few short fields.
// 上传的表单包含文件以及几个短字段。
const (
	MaxFormParts = 12
	MaxFormField = 8 << 10
)

// UploadGroup is a kind of file uploads accept, named by extension, and the
// largest file of that kind. Clients pick a file's group by its extension.
// UploadGroup 是上传所接受的一类文件，按扩展名命名，并记录该类文件的大小上限。客户端根据扩展名为文件选择分组。
type UploadGroup struct {
	Accept       []string `json:"accept"`
	MaxBytes     int      `json:"maxBytes"`
	ImageMaxSide int      `json:"imageMaxSide,omitempty"`
	kind         string   // the kind of attachment an upload of the group makes
}

var (
	// TextUploads 是作为文本上传的文件分组。
	TextUploads = UploadGroup{Accept: strings.Fields(".txt .md .py .js .jsx .ts .tsx .css .json .xml .yaml .yml .toml .sh .bash .rb .java .cpp .c .h .hpp .go .rs .swift .kt .kts .r .sql .lua .pl .php .cs .csx .vb .fs .fsx .scala .dart .ex .exs .erl .hs .ml .mli .clj .cljs .groovy .gradle .m .mm .zig .nim .jl .ps1 .psm1 .bat .cmd .asm .s .proto .graphql .gql .tf .tfvars .dockerfile .makefile .cmake .tex .bib .svg .properties .lock .diff .patch .env .ini .cfg .conf .log .rtf .html .htm .xhtml .csv .tsv .qfx .qif .ofx .ifs .qbo .qbx .bai .bai2 .mt940 .sta .ics .vcf"), MaxBytes: maxTextBytes, kind: "document"}
	// ImageUploads 是作为图像上传的文件分组。
	ImageUploads    = UploadGroup{Accept: strings.Fields(".jpg .jpeg .png .gif .webp .bmp .tiff .tif"), MaxBytes: MaxUploadBytes, ImageMaxSide: modelImageSide, kind: "image"}
	documentUploads = UploadGroup{Accept: strings.Fields(".pdf .docx .pptx .xlsx"), MaxBytes: MaxUploadBytes, kind: "document"}
	audioUploads    = UploadGroup{Accept: strings.Fields(".mp3 .wav .ogg .m4a .aac .flac .webm .wma"), MaxBytes: MaxUploadBytes, kind: "audio"}
)

// A message attaches files of any group; a project keeps extracted text,
// image descriptions, and audio transcripts.
// 消息可附加任意分组的文件；project 保存提取出的文本、图像描述与音频转录。
var (
	AttachmentGroups      = []*UploadGroup{&TextUploads, &ImageUploads, &documentUploads, &audioUploads}
	projectDocumentGroups = []*UploadGroup{&TextUploads, &documentUploads, &ImageUploads, &audioUploads}
)

// A file of no type uploads take is read as text, as the web client read
// it, when its first textSniffBytes hold no NUL byte and at most
// maxInvalidTextPercent of their characters are not UTF-8.
// 对于上传不接受类型的文件，当其前 textSniffBytes 个字节不含 NUL 字节、且非 UTF-8 字符不超过 maxInvalidTextPercent 时，会像 Web 客户端那样按文本读取。
const (
	textSniffBytes        = 8192
	maxInvalidTextPercent = 5
)

func sniffsAsText(data []byte) bool {
	sample := data[:min(len(data), textSniffBytes)]
	if len(sample) == 0 || bytes.IndexByte(sample, 0) >= 0 {
		return false
	}
	chars, invalid := 0, 0
	for len(sample) > 0 {
		r, size := utf8.DecodeRune(sample)
		if r == utf8.RuneError && size == 1 {
			invalid++
		}
		chars++
		sample = sample[size:]
	}
	return invalid*100 <= chars*maxInvalidTextPercent
}

func (g *UploadGroup) accepts(filename string) bool {
	return slices.Contains(g.Accept, strings.ToLower(filepath.Ext(filename)))
}

// attachmentGroup is the group a message's file belongs to, if any.
// attachmentGroup 返回消息中文件所属的分组（如果存在）。
func attachmentGroup(filename string) *UploadGroup {
	for _, g := range AttachmentGroups {
		if g.accepts(filename) {
			return g
		}
	}
	return nil
}

// maxContent is the most an upload of the group holds: a text file's text
// and an image's bytes are the file, and a converted file's content is what
// the converter answers with.
// maxContent 是该分组上传内容的上限：文本文件的文本和图像的字节就是文件本身，转换后文件的内容则以转换器返回的为准。
func (g *UploadGroup) maxContent() int {
	if g == &TextUploads || g == &ImageUploads {
		return g.MaxBytes
	}
	return maxTextBytes
}

// What a client's composer does before anything reaches the harness: a
// paste longer than pasteAsFileChars becomes a text file, and dictation is
// sent to be transcribed in recordings of about dictationSegmentSeconds,
// each ended at a pause, since a long recording can keep the speech model
// talking past its deadline. A client that sends dictation as one recording
// stops it after dictationMaxSeconds.
// 客户端 composer 在内容到达 harness 之前的行为：超过 pasteAsFileChars 的粘贴会变成文本文件；听写会分成约 dictationSegmentSeconds 一段的录音去转写，每段在停顿处结束，因为过长的录音会让 speech 模型超出其期限。将听写作为单段录音发送的客户端会在 dictationMaxSeconds 后停止。
const (
	pasteAsFileChars        = 3000
	dictationMaxSeconds     = 10 * 60
	dictationSegmentSeconds = 60
)

// ComposerView is what a client's composer accepts: the groups of files
// uploads take, for a message and for a project.
// ComposerView 是客户端 composer 所接受的内容：消息和 project 各自可上传的文件分组。
type ComposerView struct {
	Attachments             []UploadGroup `json:"attachments"`
	ProjectDocuments        []UploadGroup `json:"projectDocuments"`
	PasteAsFileChars        int           `json:"pasteAsFileChars"`
	DictationMaxSeconds     int           `json:"dictationMaxSeconds"`
	DictationSegmentSeconds int           `json:"dictationSegmentSeconds"`
}

// Composer 返回客户端 composer 用于接受上传的配置视图。
func Composer() ComposerView {
	shown := func(groups []*UploadGroup) []UploadGroup {
		out := []UploadGroup{}
		for _, g := range groups {
			out = append(out, *g)
		}
		return out
	}
	return ComposerView{
		Attachments:             shown(AttachmentGroups),
		ProjectDocuments:        shown(projectDocumentGroups),
		PasteAsFileChars:        pasteAsFileChars,
		DictationMaxSeconds:     dictationMaxSeconds,
		DictationSegmentSeconds: dictationSegmentSeconds,
	}
}

// HandleUpload 处理附件上传的 HTTP 请求。
func (at *Service) HandleUpload(w http.ResponseWriter, r *http.Request) {
	p, err := at.Auth.Authenticate(r, r.URL.Path == "/v1/transcriptions" || r.URL.Path == "/v1/attachments/upload")
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	cred, err := at.Auth.Inference(r.Context(), p)
	if err != nil {
		api.Respond(w, nil, auth.WithQuota(p, cred.Limit, err))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTextBytes+(1<<20))
	reader, err := r.MultipartReader()
	if err != nil {
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Expected multipart form data"))
		return
	}
	fields := map[string]string{}
	var key auth.ContentKey
	defer clear(key[:])
	var keyField []byte
	defer func() { clear(keyField) }()
	var file []byte
	filename := ""
	for count := 0; ; count++ {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= MaxFormParts {
			api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Invalid multipart upload"))
			return
		}
		if part.FormName() == "file" {
			if filename != "" || part.FileName() == "" {
				api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Expected one named file"))
				return
			}
			if r.URL.Path == "/v1/projects/documents/upload" {
				key, err = auth.DecodeBase64Key(keyField)
				clear(keyField)
				keyField = nil
				if err != nil {
					api.Respond(w, nil, err)
					return
				}
				p.Grant, err = at.Store.Rows.AuthorizeStorage(r.Context(), p, key, "project-upload", fields["projectId"])
				if err != nil {
					api.Respond(w, nil, err)
					return
				}
			}
			filename = filepath.Base(part.FileName())
			file, err = io.ReadAll(io.LimitReader(part, maxTextBytes+1))
			if err != nil || len(file) > maxTextBytes {
				api.Respond(w, nil, api.Err(413, "ATTACHMENT_TOO_LARGE", "The upload exceeds the size limit"))
				return
			}
		} else {
			value, err := api.ReadWipeable(io.LimitReader(part, MaxFormField+1))
			if err != nil || len(value) > MaxFormField {
				clear(value)
				api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Multipart field is too large"))
				return
			}
			if _, exists := fields[part.FormName()]; exists {
				clear(value)
				api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "Duplicate multipart field"))
				return
			}
			if part.FormName() == "key" {
				keyField = value
				fields["key"] = ""
			} else {
				fields[part.FormName()] = string(value)
				clear(value)
			}
		}
		part.Close()
	}
	defer clear(file)
	if filename == "" || len(file) == 0 {
		api.Respond(w, nil, api.Err(400, "BAD_REQUEST", "A nonempty file is required"))
		return
	}
	ctx := auth.WithCallerKey(r.Context(), string(cred.Key))
	if r.URL.Path == "/v1/transcriptions" {
		text, err := at.transcribe(ctx, filename, file)
		api.Respond(w, api.Object{"text": text}, auth.WithQuota(p, cred.Limit, err))
		return
	}
	descriptor, err := at.Preprocess(ctx, filename, file)
	if err != nil {
		api.Respond(w, nil, auth.WithQuota(p, cred.Limit, err))
		return
	}
	// An upload is only converted. Its client sends the descriptor back with
	// the turn that attaches it, and that turn stores it with the chat.
	// 上传只会被转换。客户端在附加它的那个 Turn 中把 descriptor 一并发回，由该 Turn 将其存入 chat。
	if r.URL.Path == "/v1/attachments/upload" {
		descriptor.ID = "att_" + api.Token()
		api.Respond(w, struct {
			Attachment domain.Attachment `json:"attachment"`
		}{descriptor}, nil)
		return
	}
	if r.URL.Path == "/v1/projects/documents/upload" {
		projectID := fields["projectId"]
		if err := api.RequireID("projectId", projectID); err != nil {
			api.Respond(w, nil, err)
			return
		}
		if _, err := at.Store.LoadProject(ctx, p, key, projectID); err != nil {
			api.Respond(w, nil, err)
			return
		}
		if descriptor.IsImage() {
			descriptor.TextContent, err = at.describeImage(ctx, descriptor.Base64)
			if err != nil {
				api.Respond(w, nil, auth.WithQuota(p, cred.Limit, err))
				return
			}
		}
		if descriptor.TextContent == "" {
			api.Respond(w, nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "Project documents require extracted text"))
			return
		}
		doc, err := at.Store.CreateProjectDocument(ctx, p, key, domain.ProjectDocument{ID: domain.RowID(), ProjectID: projectID, Filename: filename, ContentType: descriptor.MimeType, SizeBytes: len(file), Content: descriptor.TextContent})
		api.Respond(w, doc, err)
		return
	}
}

var errUnsupportedType = api.Err(415, "ATTACHMENT_UNSUPPORTED", "Unsupported file type")

// fitsUpload refuses a descriptor no upload makes: one of a file uploads do
// not take, an image without its bytes, or holding more than an upload of
// its file's group. A turn names an image its chat holds by id alone.
// fitsUpload 拒绝无法由上传产生的 descriptor：上传不接受类型的文件、缺少字节的图像，或内容超过其文件分组上传上限。一个 Turn 仅用 id 指代其 chat 已持有的图像。
func (d *Sent) fitsUpload() error {
	group := attachmentGroup(d.FileName)
	// A file of no type uploads take was read as text.
	// 上传不接受类型的文件会被当作文本读取。
	if group == nil && d.Kind == TextUploads.kind && d.Base64 == "" && len(d.Pages) == 0 {
		group = &TextUploads
	}
	if group == nil || group.kind != d.Kind {
		return errUnsupportedType
	}
	if group == &ImageUploads && d.Base64 == "" {
		return api.Err(400, "BAD_REQUEST", "An image attachment requires its bytes")
	}
	content := len(d.TextContent) + base64.StdEncoding.DecodedLen(len(d.Base64))
	for _, page := range d.Pages {
		content += len(page.Text) + len(page.Image)
	}
	if content > group.maxContent() {
		return api.Err(413, "ATTACHMENT_TOO_LARGE", "The attachment exceeds the size limit for its type")
	}
	return nil
}

// stored is the form of an attachment a turn sends whole that its chat
// stores: an upload's descriptor, and an image's bytes no larger than the
// copy an upload makes for models.
// stored 是一个 Turn 完整发送、并由其 chat 存储的 attachment 形态：上传的 descriptor，以及不大于上传为模型生成的副本的图像字节。
func (d *Sent) stored() (domain.Attachment, error) {
	a := domain.Attachment{ID: d.ID, Kind: d.Kind, FileName: d.FileName, MimeType: d.MimeType, Size: d.Size, Thumbnail: domain.ValueOr(d.Thumbnail, ""), TextContent: d.TextContent, Description: d.Description, Pages: d.Pages, Base64: d.Base64}
	if !a.IsImage() {
		return a, nil
	}
	data, err := base64.StdEncoding.DecodeString(a.Base64)
	if err != nil {
		return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	defer clear(data)
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	// An image within the bounds, as the copy an upload makes is, is kept.
	// 在界限内的图像（即上传生成的副本）会被原样保留。
	if max(cfg.Width, cfg.Height) <= modelImageSide {
		return a, nil
	}
	resized, thumb, err := ResizeImage(data)
	if err != nil {
		return domain.Attachment{}, err
	}
	a.Base64, a.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	return a, nil
}

// ResizePixels is how many pixels storing the attachment decodes to resize
// it: an image's, when it is larger than the copy models read.
// ResizePixels 是存储该 attachment 时为缩放而解码的像素数：当图像大于模型读取的副本时，即该图像的像素数。
func (d *Sent) ResizePixels() int64 {
	if d.Kind != "image" || d.Base64 == "" {
		return 0
	}
	cfg, _, err := image.DecodeConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(d.Base64)))
	if err != nil || max(cfg.Width, cfg.Height) <= modelImageSide {
		return 0
	}
	return int64(cfg.Width) * int64(cfg.Height)
}

// Preprocess 将上传的文件预处理为 attachment descriptor。
func (at *Service) Preprocess(ctx context.Context, filename string, data []byte) (domain.Attachment, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	group := attachmentGroup(filename)
	if group == nil && sniffsAsText(data) {
		group = &TextUploads
	}
	if group == nil {
		return domain.Attachment{}, errUnsupportedType
	}
	descriptor := domain.Attachment{Kind: group.kind, FileName: filename, MimeType: http.DetectContentType(data), Size: new(int64(len(data)))}
	if len(data) > group.MaxBytes {
		return domain.Attachment{}, api.Err(413, "ATTACHMENT_TOO_LARGE", "The file exceeds the size limit for its type")
	}
	switch group {
	case &TextUploads:
		if bytes.ContainsRune(data, 0) {
			return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "Text uploads must contain UTF-8 text")
		}
		// As the web client read text, what is not UTF-8 reads as U+FFFD,
		// which can make the text longer than the file.
		// 与 Web 客户端读取文本的方式一致，非 UTF-8 内容会读成 U+FFFD，这可能使文本比文件更长。
		descriptor.TextContent = strings.ToValidUTF8(string(data), string(utf8.RuneError))
		if len(descriptor.TextContent) > group.maxContent() {
			return domain.Attachment{}, api.Err(413, "ATTACHMENT_TOO_LARGE", "The file exceeds the size limit for its type")
		}
		if m := mime.TypeByExtension(ext); m != "" {
			descriptor.MimeType = m
		}
	case &ImageUploads:
		resized, thumb, err := ResizeImage(data)
		if err != nil {
			return domain.Attachment{}, err
		}
		descriptor.Base64, descriptor.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	case &documentUploads:
		if ext == ".pdf" && !bytes.HasPrefix(data, []byte("%PDF-")) || ext != ".pdf" && !bytes.HasPrefix(data, []byte("PK\x03\x04")) {
			return domain.Attachment{}, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The file contents do not match the document type")
		}
		result, err := at.Convert(ctx, filename, data)
		if err != nil {
			return domain.Attachment{}, err
		}
		// The converter answers with the document, or with it nested.
		// 转换器返回文档本身，或嵌套在其中的文档。
		type conversion struct {
			Markdown string                `json:"md_content"`
			Pages    []domain.DocumentPage `json:"pages"`
		}
		var converted struct {
			conversion
			Document *conversion `json:"document"`
		}
		var mistyped *json.UnmarshalTypeError
		if err := json.Unmarshal(result, &converted); err != nil && !errors.As(err, &mistyped) {
			return domain.Attachment{}, api.Err(502, "UPSTREAM_REFUSED", "Invalid document response")
		}
		document := converted.conversion
		if converted.Document != nil {
			document = *converted.Document
		}
		descriptor.TextContent = document.Markdown
		if len(document.Pages) > 0 {
			descriptor.Pages = document.Pages
		}
	case &audioUploads:
		text, err := at.transcribe(ctx, filename, data)
		if err != nil {
			return domain.Attachment{}, err
		}
		descriptor.TextContent = text
	}
	return descriptor, nil
}

// ResizeImage 将图像缩放为模型副本与缩略图两种尺寸。
func ResizeImage(data []byte) ([]byte, []byte, error) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width)*int64(cfg.Height) > maxImagePixels {
		return nil, nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image cannot be decoded within the pixel limit")
	}
	im, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, nil, api.Err(415, "ATTACHMENT_UNSUPPORTED", "The image could not be decoded")
	}
	encode := func(maxSide int) ([]byte, error) {
		width, height := cfg.Width, cfg.Height
		if max(width, height) > maxSide {
			if width >= height {
				height = max(1, height*maxSide/width)
				width = maxSide
			} else {
				width = max(1, width*maxSide/height)
				height = maxSide
			}
		}
		dst := image.NewRGBA(image.Rect(0, 0, width, height))
		draw.Draw(dst, dst.Bounds(), image.White, image.Point{}, draw.Src)
		draw.CatmullRom.Scale(dst, dst.Bounds(), im, im.Bounds(), draw.Over, nil)
		var b bytes.Buffer
		err := jpeg.Encode(&b, dst, &jpeg.Options{Quality: 85})
		return b.Bytes(), err
	}
	resized, err := encode(modelImageSide)
	if err != nil {
		return nil, nil, err
	}
	thumb, err := encode(thumbnailSide)
	return resized, thumb, err
}

// The document service queues conversions itself and answers a full queue
// with a delay, which a conversion waits out while it fits the deadline.
// 文档服务自行排队处理转换，队列已满时会以延迟回应；转换会在不超出 deadline 的前提下等待该延迟。
var ConversionDeadline = 10 * time.Minute

// Convert 调用文档服务把上传的文件转换为 Markdown。
func (at *Service) Convert(ctx context.Context, filename string, data []byte) ([]byte, error) {
	router := at.Models.Services["router"]
	if router == nil {
		return nil, api.Err(503, "TOOL_UNAVAILABLE", "Document conversion is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, ConversionDeadline)
	defer cancel()
	deadline, _ := ctx.Deadline()
	for {
		result, err := multipartCall(ctx, router, "/v1/convert/file?mode=images", "files", filename, data, nil)
		delay := capacityDelay(err)
		if delay == 0 || !time.Now().Add(delay).Before(deadline) {
			return result, err
		}
		wait := time.NewTimer(delay)
		select {
		case <-wait.C:
		case <-ctx.Done():
			wait.Stop()
			return nil, err
		}
	}
}

// capacityDelay is how long a capacity rejection asks to wait, or zero when the
// refusal is final. An exhausted quota needs the user rather than a retry.
// capacityDelay 是容量不足的拒绝所要求的等待时长；若拒绝是最终性的则返回 0。quota 耗尽需要用户处理，而非重试。
func capacityDelay(err error) time.Duration {
	var e *api.Error
	if !errors.As(err, &e) || e.Code == "QUOTA_EXHAUSTED" || e.Status != http.StatusTooManyRequests && e.Status != http.StatusServiceUnavailable {
		return 0
	}
	return time.Duration(e.RetryAfter) * time.Second
}

func multipartCall(ctx context.Context, service *inference.Service, path, fileField, name string, data []byte, fields map[string]string) ([]byte, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			return nil, err
		}
	}
	part, err := writer.CreateFormFile(fileField, name)
	if err != nil {
		return nil, err
	}
	if _, err := part.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", service.URL+path, &buffer)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if key := auth.CallerKey(ctx); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := service.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxTextBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxTextBytes {
		return nil, api.Err(413, "ATTACHMENT_TOO_LARGE", "The converted document exceeds the text limit")
	}
	if resp.StatusCode/100 != 2 {
		return nil, api.WithRetryAfter(api.DownstreamError(resp.StatusCode, b), resp.Header)
	}
	return b, nil
}

// A recording not transcribed by TranscriptionDeadline fails, so a speech
// model that keeps talking does not hold its caller. A dictation segment
// transcribes in seconds.
// 未在 TranscriptionDeadline 内完成转写的录音会失败，这样持续输出的 speech 模型不会一直占用调用方。一段听写录音以秒为单位完成转写。
var TranscriptionDeadline = 2 * time.Minute

func (at *Service) transcribe(ctx context.Context, filename string, data []byte) (string, error) {
	if len(data) > audioUploads.MaxBytes {
		return "", api.Err(413, "ATTACHMENT_TOO_LARGE", "The recording exceeds the size limit")
	}
	if !audioUploads.accepts(filename) {
		return "", api.Err(415, "ATTACHMENT_UNSUPPORTED", "Unsupported recording type")
	}
	// The router reads the model from the form, as the web client's SDK sends it.
	// router 从表单中读取模型，与 Web 客户端 SDK 的发送方式一致。
	router := at.Models.Services["router"]
	if router == nil {
		return "", api.Err(503, "TOOL_UNAVAILABLE", "Transcription is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, TranscriptionDeadline)
	defer cancel()
	b, err := multipartCall(ctx, router, "/v1/audio/transcriptions", "file", filename, data, map[string]string{"model": "voxtral-small-24b", "response_format": "text"})
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "", api.Err(http.StatusGatewayTimeout, "UPSTREAM_REFUSED", "The recording took too long to transcribe")
	}
	if err != nil {
		return "", err
	}
	var response struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(b, &response) == nil {
		return response.Text, nil
	}
	return string(b), nil
}

// attachment finds an attachment of the named thread for its client to download.
// attachment 查找指定 thread 中的某个 attachment，供其客户端下载。
func (at *Service) attachment(ctx context.Context, p *auth.Principal, key auth.ContentKey, id, threadID string) (*domain.Attachment, []byte, error) {
	if p.Anonymous {
		return nil, nil, api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	if key == (auth.ContentKey{}) {
		return nil, nil, api.Err(401, "KEY_REQUIRED", "A content key is required")
	}
	var descriptor *domain.Attachment
	if threadID != "" {
		row, err := at.Store.LoadThread(ctx, p, key, threadID)
		if err != nil {
			return nil, nil, err
		}
		descriptor = row.FindAttachment(id)
	}
	if descriptor == nil {
		return nil, nil, api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
	}
	b, err := at.Bytes(ctx, p, descriptor)
	return descriptor, b, err
}

// Bytes is an attachment's content: the copy it carries, or its blob.
// Bytes 返回 attachment 的内容：它自身携带的副本，或其 blob。
func (at *Service) Bytes(ctx context.Context, p *auth.Principal, a *domain.Attachment) ([]byte, error) {
	if a.Base64 != "" {
		return base64.StdEncoding.DecodeString(a.Base64)
	}
	return at.Store.GetAttachment(ctx, p, a.ID, a.StorageKey())
}

// OpaqueContentType is the type every downloaded attachment is sent as.
// OpaqueContentType 是所有下载的 attachment 响应所使用的 Content-Type。
const OpaqueContentType = "application/octet-stream"

// HandleAttachmentGet 处理附件下载的 HTTP 请求。
func (at *Service) HandleAttachmentGet(w http.ResponseWriter, r *http.Request) {
	p, err := at.Auth.Caller(r, true)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	body, err := api.ReadBody(w, r, api.MaxRequestBody)
	defer clear(body)
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	public := r.URL.Path == "/v1/attachments/get-public"
	var in attachmentRequest
	defer in.ClearKey()
	if err := api.DecodeBody(body, &in); err != nil {
		api.Respond(w, nil, err)
		return
	}
	if err := in.Validate(); err != nil {
		api.Respond(w, nil, err)
		return
	}
	var descriptor *domain.Attachment
	var data []byte
	if public {
		encodedKey := []byte(in.AttKey)
		key, keyErr := auth.DecodeBase64Key(encodedKey)
		clear(encodedKey)
		clear(key[:])
		if keyErr != nil {
			api.Respond(w, nil, keyErr)
			return
		}
		// A share's client names the attachment and the key its link carries.
		// 分享的客户端用链接所携带的 key 来指代 attachment。
		descriptor = &domain.Attachment{ID: in.ID, EncryptionKey: in.AttKey, Kind: cmp.Or(in.Kind, in.Type), FileName: in.FileName, MimeType: in.MimeType, TextContent: in.TextContent, Pages: in.Pages}
		data, err = at.Bytes(r.Context(), &auth.Principal{Anonymous: true}, descriptor)
	} else {
		key, keyErr := auth.RequestKey(in.Key)
		in.ClearKey()
		if keyErr != nil {
			api.Respond(w, nil, keyErr)
			return
		}
		defer clear(key[:])
		descriptor, data, err = at.attachment(r.Context(), p, key, in.ID, in.ThreadID)
		clear(key[:])
	}
	if err != nil {
		api.Respond(w, nil, err)
		return
	}
	defer clear(data)
	if documentRequest(descriptor, data) {
		// A stored document keeps its extracted content, not the uploaded file.
		// 已存储的文档保留其提取出的内容，而非上传的原始文件。
		content := Content(descriptor)
		if content.empty() {
			if content, err = DecodeDocument(data); err != nil {
				api.Respond(w, nil, err)
				return
			}
		}
		clear(data)
		data = []byte(DocumentText(content))
	}
	// EHBP seals the body but not the headers, so the response names neither
	// the file nor its type; the client reads both from the chat it holds.
	// EHBP 会封装响应体但不会封装响应头，因此响应中既不标明文件名也不标明其类型；客户端从它所持有的 chat 中读取这两者。
	w.Header().Set("Content-Type", OpaqueContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Write(data)
}

// A shared chat's client may not say what an attachment is. A document blob is
// JSON, and no supported image format starts with a brace.
// 共享 chat 的客户端可能不会说明 attachment 的类型。文档 blob 是 JSON，而任何受支持的图像格式都不以花括号开头。
func documentRequest(a *domain.Attachment, data []byte) bool {
	switch a.Kind {
	case "image":
		return false
	case "":
		trimmed := bytes.TrimSpace(data)
		return len(trimmed) > 0 && trimmed[0] == '{'
	}
	return true
}

// OfTurn resolves the attachments a turn names: an upload sent
// whole, or by id one the thread already holds, as an edit sends it.
// OfTurn 解析一个 Turn 所指的 attachments：完整发送的上传，或以 id 指代该 thread 已持有的（编辑消息时的发送方式）。
func OfTurn(attachments []Sent, t *domain.Thread) ([]domain.Attachment, error) {
	result := []domain.Attachment{}
	for i := range attachments {
		sent := &attachments[i]
		if sent.whole() {
			stored, err := sent.stored()
			if err != nil {
				return nil, err
			}
			result = append(result, stored)
			continue
		}
		held := t.FindAttachment(sent.ID)
		if held == nil {
			return nil, api.Err(404, "THREAD_NOT_FOUND", "The attachment was not found")
		}
		result = append(result, *held)
	}
	return result, nil
}

// A local transcript may omit older images' bytes to stay small. The model
// then reads such an image through its description, or not at all.
// 本地 transcript 为保持体积可能省略较旧图像的字节。此时模型通过其描述来读取该图像，或干脆不读。
func (at *Service) HydrateContext(ctx context.Context, p *auth.Principal, t *domain.Thread, keyless bool) error {
	vision := at.Models.Catalog.Get().Vision(t.Model)
	return t.EachAttachment(func(a *domain.Attachment) error {
		if !a.IsImage() || !vision && a.Description != "" {
			return nil
		}
		// Without its bytes, only its thumbnail was kept.
		// 没有字节时，只保留了它的缩略图。
		if a.Base64 == "" && (keyless || a.StorageKey() == "") {
			return nil
		}
		data, err := at.Bytes(ctx, p, a)
		if Unreachable(err) {
			return nil
		}
		if err != nil {
			return err
		}
		resized, _, err := ResizeImage(data)
		clear(data)
		if err != nil {
			return err
		}
		encoded := base64.StdEncoding.EncodeToString(resized)
		clear(resized)
		if vision {
			a.Base64, a.InferenceMimeType = encoded, "image/jpeg"
			return nil
		}
		response, err := at.describeImage(ctx, encoded)
		if err != nil {
			return err
		}
		a.Description = response
		return nil
	})
}

const imageDescriptionTokens = 2048

func (at *Service) describeImage(ctx context.Context, encoded string) (string, error) {
	for _, m := range at.Models.Offer.Get() {
		if m.Vision {
			return at.Models.Completion(ctx, m, api.Object{"model": m.Name, "messages": []api.Object{{"role": "user", "content": []api.Object{{"type": "text", "text": "Describe this image faithfully for a text-only assistant. Include visible text and details relevant to understanding it."}, {"type": "image_url", "image_url": api.Object{"url": "data:image/jpeg;base64," + encoded}}}}}, "max_tokens": imageDescriptionTokens})
		}
	}
	return "", api.Err(404, "MODEL_UNAVAILABLE", "No vision model can describe the image")
}
