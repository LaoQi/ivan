package qq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/qqbot"
)

func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSendFileUsesType4(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(directEvent("m-1", "u-1", "hi"))
	doc := writeTemp(t, "report.csv", []byte("a,b\n1,2\n"))
	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "报告",
		Media: []bus.MediaRef{{Path: doc, MIME: "text/csv", Name: "report.csv"}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	calls := f.calls()
	if len(calls) != 3 {
		t.Fatalf("期望 文本+上传+发送 共 3 次，得到 %d", len(calls))
	}
	if calls[1]["file_type"] != float64(qqbot.FileFile) {
		t.Fatalf("csv 应按文件类型上传（file_type=4），得到 %+v", calls[1]["file_type"])
	}
	if calls[1]["file_name"] != "report.csv" {
		t.Fatalf("上传应带真实文件名: %+v", calls[1]["file_name"])
	}
	if calls[2]["msg_type"] != float64(qqbot.MsgTypeMedia) {
		t.Fatalf("附件消息 msg_type 应为 7")
	}
}

func TestSendFileTypeDetection(t *testing.T) {
	cases := []struct {
		name string
		mime string
		want qqbot.FileType
	}{
		{"a.png", "image/png", qqbot.FileImage},
		{"a.mp4", "video/mp4", qqbot.FileVideo},
		{"a.silk", "audio/silk", qqbot.FileVoice},
		{"a.pdf", "", qqbot.FileFile},
		{"a.zip", "application/octet-stream", qqbot.FileFile},
	}
	for _, tc := range cases {
		f := newFakeAPI(t)
		ch := newTestChannel(t, f, nil)
		ch.translate(directEvent("m-1", "u-1", "hi"))
		path := writeTemp(t, tc.name, []byte("data"))
		if err := ch.Send(context.Background(), bus.OutboundMessage{
			Channel: Name, ChatID: "u-1", ReplyTo: "m-1",
			Media: []bus.MediaRef{{Path: path, MIME: tc.mime}},
		}); err != nil {
			t.Fatalf("%s Send: %v", tc.name, err)
		}
		calls := f.calls()
		if len(calls) != 2 {
			t.Fatalf("%s 期望 上传+发送 2 次，得到 %d", tc.name, len(calls))
		}
		if calls[0]["file_type"] != float64(tc.want) {
			t.Fatalf("%s 期望 file_type=%d，得到 %+v", tc.name, tc.want, calls[0]["file_type"])
		}
	}
}

func TestSendFileOverLimitRejected(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) {
		c.MediaMaxBytes = 8
		c.FileMaxBytes = 8
	})
	ch.translate(directEvent("m-1", "u-1", "hi"))
	doc := writeTemp(t, "big.txt", []byte("0123456789"))
	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1",
		Media: []bus.MediaRef{{Path: doc, MIME: "text/plain"}},
	})
	if err == nil {
		t.Fatal("超限应报错")
	}
	if !strings.Contains(err.Error(), "上限") {
		t.Fatalf("错误信息应说明上限: %v", err)
	}
	if n := len(f.calls()); n != 0 {
		t.Fatalf("超限不应有任何请求，得到 %d", n)
	}
}

func TestSendRejectsWhenQuotaInsufficient(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(directEvent("m-1", "u-1", "hi"))
	doc := writeTemp(t, "a.txt", []byte("x"))
	media := make([]bus.MediaRef, 0, 5)
	for i := 0; i < 5; i++ {
		media = append(media, bus.MediaRef{Path: doc, MIME: "text/plain"})
	}
	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "一堆文件", Media: media,
	})
	if err == nil {
		t.Fatal("额度不足应整条拒绝")
	}
	if !strings.Contains(err.Error(), "额度不足") {
		t.Fatalf("错误应说明额度不足: %v", err)
	}
	if n := len(f.calls()); n != 0 {
		t.Fatalf("额度不足时不应发出任何消息（避免半成品），得到 %d 次调用", n)
	}
}

func TestSendNonRegularFileRejected(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(directEvent("m-1", "u-1", "hi"))
	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1",
		Media: []bus.MediaRef{{Path: t.TempDir(), MIME: "text/plain"}},
	})
	if err == nil {
		t.Fatal("目录不应被当附件发送")
	}
}

func TestAttachmentMIMEFixups(t *testing.T) {
	if got := attachmentMIME(qqbot.Attachment{ContentType: "voice", VoiceWavURL: "http://x/a.wav"}, "x-0.wav"); got != "audio/wav" {
		t.Fatalf("voice+wav 应映射为 audio/wav，得到 %s", got)
	}
	if got := attachmentMIME(qqbot.Attachment{ContentType: "voice"}, "x-0.bin"); got != "audio/silk" {
		t.Fatalf("无 wav 的 voice 应映射为 audio/silk，得到 %s", got)
	}
	if got := attachmentMIME(qqbot.Attachment{ContentType: "file"}, "x-0.pdf"); got != "application/pdf" {
		t.Fatalf("file 应按扩展名补 MIME，得到 %s", got)
	}
	if got := attachmentMIME(qqbot.Attachment{ContentType: "image/png"}, "x-0.png"); got != "image/png" {
		t.Fatalf("真实 MIME 应原样保留，得到 %s", got)
	}
}

func TestDownloadOversizedAttachmentNotStored(t *testing.T) {
	body := strings.Repeat("A", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	f := newFakeAPI(t)
	dir := t.TempDir()
	ch := newTestChannel(t, f, func(c *Config) {
		c.MediaDir = dir
		c.MediaMaxBytes = 10
		c.FileMaxBytes = 10
	})
	ev := directEvent("m-big", "u-1", "看图")
	ev.Attachments = []qqbot.Attachment{{URL: srv.URL + "/big.bin", ContentType: "file", Filename: "big.bin"}}
	msg, ok := ch.translate(ev)
	if !ok {
		t.Fatal("应接受带附件消息")
	}
	if len(msg.Media) != 1 {
		t.Fatalf("期望 1 个附件占位，得到 %d", len(msg.Media))
	}
	ref := msg.Media[0]
	if ref.Path != "" {
		t.Fatalf("超限附件不应落盘，得到 %s", ref.Path)
	}
	if ref.Bytes == 0 {
		t.Fatal("超限占位应带字节数")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("超限附件不应在 media 目录留文件: %v", entries)
	}
}

func TestDownloadVoiceAttachment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("WAVDATA"))
	}))
	defer srv.Close()

	f := newFakeAPI(t)
	dir := t.TempDir()
	ch := newTestChannel(t, f, func(c *Config) { c.MediaDir = dir })
	ev := directEvent("m-voice", "u-1", "语音")
	ev.Attachments = []qqbot.Attachment{{ContentType: "voice", VoiceWavURL: srv.URL + "/v.wav", Size: 7}}
	msg, ok := ch.translate(ev)
	if !ok {
		t.Fatal("应接受语音消息")
	}
	if len(msg.Media) != 1 {
		t.Fatalf("期望 1 个附件，得到 %d", len(msg.Media))
	}
	ref := msg.Media[0]
	if ref.Path == "" || ref.MIME != "audio/wav" {
		t.Fatalf("语音附件元数据错误: %+v", ref)
	}
	if _, err := os.Stat(ref.Path); err != nil {
		t.Fatalf("语音附件未落盘: %v", err)
	}
}

func TestNoticeFilesSendsTextAndMedia(t *testing.T) {
	f := newFakeAPI(t)
	dir := t.TempDir()
	ch := newTestChannel(t, f, func(c *Config) { c.StatePath = filepath.Join(dir, "channel.json") })
	ch.initState()
	if _, err := ch.state.claim("u-1"); err != nil {
		t.Fatal(err)
	}
	doc := writeTemp(t, "out.txt", []byte("hello"))
	results := ch.NoticeFiles(context.Background(), "给你文件", []bus.MediaRef{{Path: doc, MIME: "text/plain", Name: "out.txt"}})
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("附件通知失败: %+v", results)
	}
	calls := f.calls()
	if len(calls) != 3 {
		t.Fatalf("期望 文本+上传+发送 共 3 次，得到 %d", len(calls))
	}
	if calls[1]["file_type"] != float64(qqbot.FileFile) {
		t.Fatalf("通知附件应按判定类型上传: %+v", calls[1])
	}
	if _, ok := calls[2]["msg_id"]; ok {
		t.Fatalf("通知附件是主动消息，不应带 msg_id: %+v", calls[2])
	}
}

func TestNoticeFilesReportsFailure(t *testing.T) {
	f := newFakeAPI(t)
	dir := t.TempDir()
	ch := newTestChannel(t, f, func(c *Config) { c.StatePath = filepath.Join(dir, "channel.json") })
	ch.initState()
	if _, err := ch.state.claim("u-1"); err != nil {
		t.Fatal(err)
	}
	results := ch.NoticeFiles(context.Background(), "", []bus.MediaRef{{Path: filepath.Join(dir, "nope.txt"), MIME: "text/plain"}})
	if len(results) != 1 {
		t.Fatalf("期望 1 条回执，得到 %d", len(results))
	}
	if results[0].Err == nil {
		t.Fatal("文件不存在应如实回报失败")
	}
}

func TestNoticeFilesNoTargets(t *testing.T) {
	f := newFakeAPI(t)
	dir := t.TempDir()
	ch := newTestChannel(t, f, func(c *Config) { c.StatePath = filepath.Join(dir, "channel.json") })
	if got := ch.NoticeFiles(context.Background(), "x", nil); len(got) != 0 {
		t.Fatalf("无目标应返回空结果，得到 %+v", got)
	}
}
