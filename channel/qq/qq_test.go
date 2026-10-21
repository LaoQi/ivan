package qq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/qqbot"
)

type fakeAPI struct {
	srv          *httptest.Server
	mu           sync.Mutex
	sent         []map[string]any
	paths        []string
	auth         []string
	failMarkdown bool
}

func newFakeAPI(t *testing.T) *fakeAPI {
	f := &fakeAPI{}
	mux := http.NewServeMux()
	mux.HandleFunc("/app/getAppAccessToken", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok-1","expires_in":"3600"}`))
	})
	mux.HandleFunc("/v2/users/", f.handle)
	mux.HandleFunc("/v2/groups/", f.handle)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) handle(w http.ResponseWriter, r *http.Request) {
	body := map[string]any{}
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.sent = append(f.sent, body)
	f.paths = append(f.paths, r.URL.Path)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	failMD := f.failMarkdown
	f.mu.Unlock()
	if failMD {
		if mt, _ := body["msg_type"].(float64); int(mt) == qqbot.MsgTypeMarkdown {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"err_code":40034011,"message":"无效的markdown内容"}`))
			return
		}
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/files"):
		_, _ = w.Write([]byte(`{"file_uuid":"fu-1","file_info":"fi-1","ttl":3600}`))
	default:
		_, _ = w.Write([]byte(`{"id":"out-1","timestamp":"2026-10-20T00:00:00Z"}`))
	}
}

func (f *fakeAPI) calls() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.sent...)
}

func newTestChannel(t *testing.T, f *fakeAPI, mutate func(*Config)) *Channel {
	t.Helper()
	cfg := Config{
		AppID:     "app-1",
		AppSecret: "sec-1",
		APIBase:   f.srv.URL,
		TokenURL:  f.srv.URL + "/app/getAppAccessToken",
		Timeout:   5 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	ch, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ch
}

func directEvent(id, openid, content string) qqbot.MessageEvent {
	return qqbot.MessageEvent{
		Kind:       qqbot.EventC2CMessage,
		ID:         id,
		Content:    content,
		UserOpenID: openid,
		Author:     qqbot.MessageAuthor{UserOpenID: openid},
		ReceivedAt: time.Now(),
	}
}

func groupEvent(id, group, member, content string) qqbot.MessageEvent {
	return qqbot.MessageEvent{
		Kind:        qqbot.EventGroupAt,
		ID:          id,
		Content:     content,
		GroupOpenID: group,
		UserOpenID:  member,
		Author:      qqbot.MessageAuthor{MemberOpenID: member},
		ReceivedAt:  time.Now(),
	}
}

func TestTranslateDirect(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	msg, ok := ch.translate(directEvent("m-1", "u-1", " 你好 "))
	if !ok {
		t.Fatal("应接受消息")
	}
	if msg.Channel != Name || msg.ChatID != "u-1" || msg.ChatType != bus.ChatDirect {
		t.Fatalf("映射错误: %+v", msg)
	}
	if msg.MessageID != "m-1" || msg.Content != "你好" || msg.Mentioned {
		t.Fatalf("字段错误: %+v", msg)
	}
	if msg.Raw["msg_id"] != "m-1" {
		t.Fatalf("Raw 缺 msg_id")
	}
	if got := bus.SessionKey(msg); got != "qq:c2c:u-1" {
		t.Fatalf("会话键期望 qq:c2c:u-1，得到 %s", got)
	}
}

func TestTranslateGroup(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	msg, ok := ch.translate(groupEvent("m-2", "g-1", "mem-1", "hi"))
	if !ok {
		t.Fatal("应接受消息")
	}
	if msg.ChatID != "g-1" || msg.ChatType != bus.ChatGroup || !msg.Mentioned {
		t.Fatalf("群消息映射错误: %+v", msg)
	}
	if msg.SenderID != "mem-1" {
		t.Fatalf("sender 期望 mem-1，得到 %s", msg.SenderID)
	}
	if got := bus.SessionKey(msg); got != "qq:group:g-1" {
		t.Fatalf("会话键期望 qq:group:g-1，得到 %s", got)
	}
}

func TestTranslateWhitelist(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) {
		c.AllowFrom = []string{"u-ok"}
		c.AllowGroups = []string{"g-ok"}
	})
	if _, ok := ch.translate(directEvent("m", "u-ok", "hi")); !ok {
		t.Fatal("白名单内单聊应接受")
	}
	if _, ok := ch.translate(directEvent("m", "u-bad", "hi")); ok {
		t.Fatal("白名单外单聊应拒绝")
	}
	if _, ok := ch.translate(groupEvent("m", "g-ok", "x", "hi")); !ok {
		t.Fatal("白名单内群应接受")
	}
	if _, ok := ch.translate(groupEvent("m", "g-bad", "x", "hi")); ok {
		t.Fatal("白名单外群应拒绝")
	}
}

func TestTranslateMissingChatID(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ev := qqbot.MessageEvent{Kind: qqbot.EventC2CMessage, ID: "m"}
	if _, ok := ch.translate(ev); ok {
		t.Fatal("缺少会话标识应拒绝")
	}
}

func TestSendTextPassive(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(directEvent("m-1", "u-1", "hi"))

	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "回复",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	calls := f.calls()
	if len(calls) != 1 {
		t.Fatalf("期望 1 次调用，得到 %d", len(calls))
	}
	if calls[0]["msg_id"] != "m-1" {
		t.Fatalf("应带被动回复 msg_id，得到 %v", calls[0]["msg_id"])
	}
	if n, _ := calls[0]["msg_seq"].(float64); int(n) != 1 {
		t.Fatalf("msg_seq 期望 1，得到 %v", calls[0]["msg_seq"])
	}
}

func TestSendSeqIncrementsPerMessageID(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(directEvent("m-1", "u-1", "hi"))

	for i := 0; i < 4; i++ {
		if err := ch.Send(context.Background(), bus.OutboundMessage{
			Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "x",
		}); err != nil {
			t.Fatalf("第 %d 次发送失败: %v", i+1, err)
		}
	}
	calls := f.calls()
	for i, c := range calls {
		if n, _ := c["msg_seq"].(float64); int(n) != i+1 {
			t.Fatalf("第 %d 次 msg_seq 期望 %d，得到 %v", i+1, i+1, c["msg_seq"])
		}
	}
	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "x",
	})
	if err == nil {
		t.Fatal("超过 4 次应报错")
	}
	if len(f.calls()) != 4 {
		t.Fatalf("超限后不应再发送，实际 %d", len(f.calls()))
	}
}

func TestSendSeqIsolatedPerMessage(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(directEvent("m-1", "u-1", "a"))
	ch.translate(directEvent("m-2", "u-1", "b"))

	for _, id := range []string{"m-1", "m-2", "m-1"} {
		if err := ch.Send(context.Background(), bus.OutboundMessage{
			Channel: Name, ChatID: "u-1", ReplyTo: id, Content: "x",
		}); err != nil {
			t.Fatalf("发送 %s 失败: %v", id, err)
		}
	}
	calls := f.calls()
	got := map[string][]int{}
	for _, c := range calls {
		id, _ := c["msg_id"].(string)
		n, _ := c["msg_seq"].(float64)
		got[id] = append(got[id], int(n))
	}
	if len(got["m-1"]) != 2 || got["m-1"][0] != 1 || got["m-1"][1] != 2 {
		t.Fatalf("m-1 的 msg_seq 应为 1,2，得到 %v", got["m-1"])
	}
	if len(got["m-2"]) != 1 || got["m-2"][0] != 1 {
		t.Fatalf("m-2 的 msg_seq 应为 1，得到 %v", got["m-2"])
	}
}

func TestSendGroupWindow(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(groupEvent("m-g", "g-1", "mem", "hi"))
	for i := 0; i < 5; i++ {
		if err := ch.Send(context.Background(), bus.OutboundMessage{
			Channel: Name, ChatID: "g-1", ReplyTo: "m-g", Content: "x",
		}); err != nil {
			t.Fatalf("群第 %d 次发送失败: %v", i+1, err)
		}
	}
	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "g-1", ReplyTo: "m-g", Content: "x",
	}); err == nil {
		t.Fatal("群超过 5 次应报错")
	}
}

func TestSendProactiveWithoutReplyTo(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.AllowProactive = true })
	ch.translate(directEvent("m-1", "u-1", "hi"))
	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", Content: "主动消息",
	}); err != nil {
		t.Fatalf("主动消息发送失败: %v", err)
	}
	calls := f.calls()
	if _, ok := calls[0]["msg_id"]; ok {
		t.Fatalf("主动消息不应带 msg_id: %+v", calls[0])
	}
}

func TestSendUnknownTarget(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	err := ch.Send(context.Background(), bus.OutboundMessage{Channel: Name, Content: "x"})
	if err == nil {
		t.Fatal("缺少 ChatID 应报错")
	}
}

func TestSendImageUploadsThenSends(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	ch.translate(directEvent("m-1", "u-1", "hi"))

	dir := t.TempDir()
	img := filepath.Join(dir, "a.png")
	if err := os.WriteFile(img, []byte("\x89PNG-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "看图",
		Media: []bus.MediaRef{{Path: img, MIME: "image/png"}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	calls := f.calls()
	if len(calls) != 3 {
		t.Fatalf("期望 文本+上传+图片 共 3 次调用，得到 %d", len(calls))
	}
	if calls[1]["file_type"] != float64(1) {
		t.Fatalf("上传应带 file_type=1")
	}
	if calls[2]["msg_type"] != float64(qqbot.MsgTypeMedia) {
		t.Fatalf("图片消息 msg_type 应为 7")
	}
}

func TestDownloadAttachment(t *testing.T) {
	fileSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("PNGDATA"))
	}))
	defer fileSrv.Close()

	f := newFakeAPI(t)
	dir := t.TempDir()
	ch := newTestChannel(t, f, func(c *Config) { c.MediaDir = dir })
	ev := directEvent("m-img", "u-1", "看图")
	ev.Attachments = []qqbot.Attachment{{URL: fileSrv.URL + "/x.png", ContentType: "image/png"}}
	msg, ok := ch.translate(ev)
	if !ok {
		t.Fatal("应接受带附件消息")
	}
	if len(msg.Media) != 1 {
		t.Fatalf("期望 1 个附件，得到 %d", len(msg.Media))
	}
	ref := msg.Media[0]
	if ref.MIME != "image/png" || ref.Bytes != 7 {
		t.Fatalf("附件元数据错误: %+v", ref)
	}
	data, err := os.ReadFile(ref.Path)
	if err != nil {
		t.Fatalf("附件未落盘: %v", err)
	}
	if string(data) != "PNGDATA" {
		t.Fatalf("附件内容错误: %q", string(data))
	}
	if !strings.HasPrefix(ref.Path, dir) {
		t.Fatalf("附件应落在 media 目录: %s", ref.Path)
	}
}

func TestMaxLenDefault(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	if ch.MaxLen() != DefaultMaxLen {
		t.Fatalf("MaxLen 默认应为 %d，得到 %d", DefaultMaxLen, ch.MaxLen())
	}
	ch2 := newTestChannel(t, f, func(c *Config) { c.MaxMessageLen = 100 })
	if ch2.MaxLen() != 100 {
		t.Fatalf("MaxLen 应为 100，得到 %d", ch2.MaxLen())
	}
}

func TestNewRequiresCredentials(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("缺凭证应报错")
	}
}

func TestMediaNamesDoNotCollide(t *testing.T) {
	ids := []string{
		"ROBOT1.0_aaaaaaaaaaaaaaaaaaaaaa!",
		"ROBOT1.0_bbbbbbbbbbbbbbbbbbbbbb!",
	}
	seen := map[string]bool{}
	for i, id := range ids {
		ev := directEvent(id, "u-1", "图")
		ev.Attachments = []qqbot.Attachment{{URL: "http://example.invalid/x.png", ContentType: "image/png"}}
		got := mediaName(ev.ID, ev.Attachments[0], 0)
		if seen[got] {
			t.Fatalf("第 %d 个附件文件名与之前碰撞: %s", i+1, got)
		}
		seen[got] = true
		if strings.Contains(got, "ROBOT1.0") {
			t.Fatalf("文件名不应沿用 msg id 前缀: %s", got)
		}
	}
	if mediaName(ids[0], qqbot.Attachment{ContentType: "image/png"}, 0) == mediaName(ids[1], qqbot.Attachment{ContentType: "image/png"}, 0) {
		t.Fatal("不同消息 id 应得到不同文件名")
	}
}

type streamCall struct {
	path string
	body map[string]any
}

func TestStreamOpenDirectOnly(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Stream = true })
	if _, ok := ch.translate(directEvent("m-1", "u-1", "hi")); !ok {
		t.Fatal("translate 失败")
	}
	h, err := ch.StreamOpen(context.Background(), bus.OutboundMessage{ChatID: "u-1", ReplyTo: "m-1"})
	if err != nil {
		t.Fatalf("StreamOpen: %v", err)
	}
	if h == nil {
		t.Fatal("未返回流句柄")
	}
	if err := h.Update(context.Background(), "正文", true); err != nil {
		t.Fatalf("Update: %v", err)
	}
	calls := f.calls()
	if len(calls) != 2 {
		t.Fatalf("请求数 %d，期望 2", len(calls))
	}
	if calls[0]["msg_seq"] != float64(1) || calls[0]["msg_id"] != "m-1" {
		t.Fatalf("首片被动回复字段异常: %+v", calls[0])
	}
	if calls[1]["input_state"] != float64(10) {
		t.Fatalf("结束片未带 input_state=10: %+v", calls[1])
	}
	paths := f.paths
	if paths[0] != "/v2/users/u-1/stream_messages" {
		t.Fatalf("路径异常: %s", paths[0])
	}
}

func TestStreamOpenRejectsGroupAndDisabled(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Stream = true })
	if _, ok := ch.translate(groupEvent("m-1", "g-1", "u-1", "hi")); !ok {
		t.Fatal("translate 失败")
	}
	if _, err := ch.StreamOpen(context.Background(), bus.OutboundMessage{ChatID: "g-1"}); err == nil {
		t.Fatal("群聊应拒绝流式")
	}
	off := newTestChannel(t, f, func(c *Config) { c.Stream = false })
	if _, err := off.StreamOpen(context.Background(), bus.OutboundMessage{ChatID: "u-1"}); err == nil {
		t.Fatal("关闭流式后应报错")
	}
}

func TestTypingRespectsQuotaReserve(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Typing = true })
	if _, ok := ch.translate(directEvent("m-9", "u-9", "hi")); !ok {
		t.Fatal("translate 失败")
	}
	msg := bus.OutboundMessage{ChatID: "u-9", ReplyTo: "m-9"}
	for i := 0; i < typingReserve; i++ {
		if err := ch.Typing(context.Background(), msg); err != nil {
			t.Fatalf("第 %d 次输入状态失败: %v", i+1, err)
		}
	}
	if err := ch.Typing(context.Background(), msg); err == nil {
		t.Fatal("额度不足时应跳过输入状态")
	}
	calls := f.calls()
	if len(calls) != typingReserve {
		t.Fatalf("输入状态请求数 %d，期望 %d", len(calls), typingReserve)
	}
	if calls[0]["msg_type"] != float64(qqbot.MsgTypeInput) {
		t.Fatalf("msg_type 异常: %+v", calls[0])
	}
	if calls[0]["msg_seq"] != float64(1) {
		t.Fatalf("输入状态未占用被动序号: %+v", calls[0])
	}
}

func TestStreamMaxLenDefault(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, nil)
	if got := ch.StreamMaxLen(); got != DefaultStreamLen {
		t.Fatalf("StreamMaxLen=%d，期望 %d", got, DefaultStreamLen)
	}
}

func TestSendMarkdownWhenEnabled(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = true })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1",
		Content: "## 结论\n\n- 第一点\n- 第二点\n",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	calls := f.calls()
	if len(calls) != 1 {
		t.Fatalf("期望 1 次调用，得到 %d", len(calls))
	}
	if mt, _ := calls[0]["msg_type"].(float64); int(mt) != qqbot.MsgTypeMarkdown {
		t.Fatalf("应走 markdown，msg_type=%v", calls[0]["msg_type"])
	}
	if _, ok := calls[0]["content"]; ok {
		t.Fatalf("markdown 消息不应带 content: %+v", calls[0])
	}
	md, _ := calls[0]["markdown"].(map[string]any)
	if md == nil || !strings.Contains(md["content"].(string), "## 结论") {
		t.Fatalf("markdown 内容错误: %+v", calls[0]["markdown"])
	}
}

func TestSendPlainWhenNoMarkdownSyntax(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = true })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "纯文本回复，没有语法",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if mt, _ := f.calls()[0]["msg_type"].(float64); int(mt) != qqbot.MsgTypeText {
		t.Fatalf("纯文本应走 msg_type=0，得到 %v", f.calls()[0]["msg_type"])
	}
}

func TestSendMarkdownDisabledFallsBackToText(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = false })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "## 标题",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if mt, _ := f.calls()[0]["msg_type"].(float64); int(mt) != qqbot.MsgTypeText {
		t.Fatalf("关闭 markdown 后应走文本，得到 %v", f.calls()[0]["msg_type"])
	}
}

func TestSendMarkdownRejectedRetriesAsText(t *testing.T) {
	f := newFakeAPI(t)
	f.failMarkdown = true
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = true })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	if err := ch.Send(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "**加粗** 内容",
	}); err != nil {
		t.Fatalf("markdown 被拒后应退回文本且不报错: %v", err)
	}
	calls := f.calls()
	if len(calls) != 2 {
		t.Fatalf("期望 markdown + 文本两次调用，得到 %d", len(calls))
	}
	if mt, _ := calls[0]["msg_type"].(float64); int(mt) != qqbot.MsgTypeMarkdown {
		t.Fatalf("第一次应为 markdown，得到 %v", calls[0]["msg_type"])
	}
	if mt, _ := calls[1]["msg_type"].(float64); int(mt) != qqbot.MsgTypeText {
		t.Fatalf("第二次应退回文本，得到 %v", calls[1]["msg_type"])
	}
	if s1, _ := calls[0]["msg_seq"].(float64); int(s1) != 1 {
		t.Fatalf("首次序号应为 1，得到 %v", calls[0]["msg_seq"])
	}
	if s2, _ := calls[1]["msg_seq"].(float64); int(s2) != 2 {
		t.Fatalf("退回重发应用下一个序号，得到 %v", calls[1]["msg_seq"])
	}
}

func TestStreamOpenUsesMarkdownContentType(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = true; c.Stream = true })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	if _, err := ch.StreamOpen(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "1. 第一点\n2. 第二点",
	}); err != nil {
		t.Fatalf("StreamOpen: %v", err)
	}
	body := f.calls()[0]
	if ct, _ := body["content_type"].(string); ct != qqbot.ContentTypeMarkdown {
		t.Fatalf("流式应声明 markdown，得到 %q", body["content_type"])
	}
}

func TestSplitOutboundMarkdownClosesFence(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = true; c.MaxMessageLen = 40 })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	content := "说明：\n\n```go\n" + strings.Repeat("fmt.Println(\"hello\")\n", 6) + "```\n\n完。"
	parts := ch.SplitOutbound(content)
	if len(parts) < 2 {
		t.Fatalf("应切成多片，得到 %d", len(parts))
	}
	for i, p := range parts {
		if !hasCompleteFence(p) {
			t.Fatalf("第 %d 片围栏未闭合:\n%s", i, p)
		}
	}
}

func TestStreamHandleClosesOpenFence(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = true; c.Stream = true })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	h, err := ch.StreamOpen(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "```go\nfmt.Println(1)",
	})
	if err != nil {
		t.Fatalf("StreamOpen: %v", err)
	}
	if err := h.Update(context.Background(), "```go\nfmt.Println(1)", false); err != nil {
		t.Fatalf("Update: %v", err)
	}
	calls := f.calls()
	last := calls[len(calls)-1]
	if raw, _ := last["content_raw"].(string); strings.Count(raw, "```")%2 != 0 {
		t.Fatalf("流式中间态应补闭合围栏，得到 %q", raw)
	}
	if ct, _ := last["content_type"].(string); ct != qqbot.ContentTypeMarkdown {
		t.Fatalf("content_type 期望 markdown，得到 %q", last["content_type"])
	}
}

func TestStreamHandleKeepsPlainText(t *testing.T) {
	f := newFakeAPI(t)
	ch := newTestChannel(t, f, func(c *Config) { c.Markdown = true; c.Stream = true })
	ch.translate(directEvent("m-1", "u-1", "hi"))

	if _, err := ch.StreamOpen(context.Background(), bus.OutboundMessage{
		Channel: Name, ChatID: "u-1", ReplyTo: "m-1", Content: "纯文本",
	}); err != nil {
		t.Fatalf("StreamOpen: %v", err)
	}
	if ct, _ := f.calls()[0]["content_type"].(string); ct != qqbot.ContentTypeText {
		t.Fatalf("无语法时应为 text，得到 %q", f.calls()[0]["content_type"])
	}
}
