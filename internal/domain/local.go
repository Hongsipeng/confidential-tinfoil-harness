// 本文件定义设备本地 thread 的处理。
package domain

// A local transcript carries its attachments inline, so a turn may be larger
// than other requests. Uploads already accept files of up to 50 MiB.
// 本地转录记录会把附件内联携带，因此单个 turn 可能比其他请求更大。上传本就接受最大
// 50 MiB 的文件。
const MaxTurnBody = 64 << 20

// DeviceThread is a local thread as its client stores it after a run. Image
// bytes and document page images are left out, so a scanned document cannot
// fill the run's log: the client holds them and puts them back by id.
// DeviceThread 是客户端在一次 run 之后存储的本地 thread。图像字节与文档页面图像会被
// 省略，以免扫描文档塞满 run 的日志：客户端持有它们，并按 id 放回。
func DeviceThread(t *Thread) Thread {
	thread := t.Clone()
	thread.RecordVersion = RecordVersion
	thread.CheckWidgets()
	thread.EachAttachment(func(a *Attachment) error {
		a.Base64, a.InferenceMimeType = "", ""
		for i := range a.Pages {
			a.Pages[i].Image = ""
		}
		return nil
	})
	return thread
}
