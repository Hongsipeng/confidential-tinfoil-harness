package attach

import (
	"encoding/base64"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

// ModelCopies gives each image of an upgraded chat the copy and thumbnail an
// upload makes in place of its original, which its client keeps from the old
// chat.
// ModelCopies 为升级后的 chat 中的每张图像，用上传生成的副本和缩略图替换其原图——原图由其客户端从旧 chat 中保留。
func ModelCopies(t *domain.Thread) {
	t.EachAttachment(func(a *domain.Attachment) error {
		if a.IsImage() && a.Base64 != "" {
			ModelImageCopy(a)
		}
		return nil
	})
}

// ModelImageCopy gives an image the copy and thumbnail an upload makes in
// place of its original. An image that does not decode keeps no bytes, since
// no model reads it.
// ModelImageCopy 用上传生成的副本和缩略图替换图像的原始内容。无法解码的图像不会保留任何字节，因为没有模型会读取它。
func ModelImageCopy(a *domain.Attachment) {
	original, err := base64.StdEncoding.DecodeString(a.Base64)
	a.Base64 = ""
	if err != nil {
		return
	}
	resized, thumb, err := ResizeImage(original)
	clear(original)
	if err == nil {
		a.Base64, a.Thumbnail = base64.StdEncoding.EncodeToString(resized), base64.StdEncoding.EncodeToString(thumb)
	}
}
