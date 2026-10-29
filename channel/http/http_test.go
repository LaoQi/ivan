package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
)

type recorder struct {
	mu        sync.Mutex
	sent      []string
	err       error
	failOne   string
	noTarget  bool
	announced []string
}

func (r *recorder) announce(content string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.announced = append(r.announced, content)
}

func (r *recorder) broadcasts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

func (r *recorder) announces() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.announced...)
}

func (r *recorder) broadcast(ctx context.Context, content string) []TargetResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.noTarget {
		return nil
	}
	r.sent = append(r.sent, content)
	if r.err != nil {
		return []TargetResult{
			{Name: "qq:u1", Channel: "qq", Result: "failed", Error: r.err.Error()},
			{Name: "qq:u2", Channel: "qq", Result: "failed", Error: r.err.Error()},
		}
	}
	if r.failOne != "" {
		return []TargetResult{
			{Name: "qq:u1", Channel: "qq", Result: "sent"},
			{Name: "qq:u2", Channel: "qq", Result: "failed", Error: r.failOne},
		}
	}
	return []TargetResult{
		{Name: "qq:u1", Channel: "qq", Result: "sent"},
		{Name: "qq:u2", Channel: "qq", Result: "sent"},
	}
}

func (r *recorder) contents() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

func newTestChannel(t *testing.T, rec *recorder, submit func(bus.InboundMessage) bool) *Channel {
	t.Helper()
	if rec == nil {
		rec = &recorder{}
	}
	if submit == nil {
		submit = func(bus.InboundMessage) bool { return true }
	}
	c := New(Config{
		Listen:           "127.0.0.1:0",
		MaxBodyBytes:     65536,
		NotifyPerMinute:  30,
		RequestPerMinute: 5,
		Broadcast:        rec.broadcast,
		Notice:           func(ctx context.Context, content string) { rec.announce(content) },
		Submit:           submit,
	})
	if err := c.Start(context.Background(), bus.New(4, 4)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func (c *Channel) url(t *testing.T, path string) string {
	t.Helper()
	c.mu.Lock()
	addr := c.ln.Addr().String()
	c.mu.Unlock()
	return "http://" + addr + path
}

func doJSON(t *testing.T, method, url string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	switch v := body.(type) {
	case string:
		buf.WriteString(v)
	case nil:
	default:
		if err := json.NewEncoder(&buf).Encode(v); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if body != nil {
		if _, ok := body.(string); ok {
			req.Header.Set("Content-Type", "text/plain")
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestNotifyBroadcast(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{
		"title": "构建失败", "body": "job #123 exit 1", "source": "ci",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%v", resp.StatusCode, out)
	}
	if out["status"] != "delivered" {
		t.Fatalf("status = %v", out["status"])
	}
	trs, _ := out["targets"].([]any)
	if len(trs) != 2 || trs[0].(map[string]any)["name"] != "qq:u1" || trs[0].(map[string]any)["result"] != "sent" {
		t.Fatalf("targets = %v", out["targets"])
	}
	contents := rec.contents()
	if len(contents) != 1 {
		t.Fatalf("广播次数 = %d, want 1", len(contents))
	}
	if !strings.Contains(contents[0], "构建失败") || !strings.Contains(contents[0], "job #123 exit 1") {
		t.Fatalf("正文格式不对: %q", contents[0])
	}
	if !strings.Contains(contents[0], "— ci") {
		t.Fatalf("缺少来源脚注: %q", contents[0])
	}
}

func TestNotifyPartialAndAllFailed(t *testing.T) {
	rec := &recorder{failOne: "平台拒收"}
	c := newTestChannel(t, rec, nil)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "x", "source": "ci"})
	if resp.StatusCode != http.StatusOK || out["status"] != "partial" {
		t.Fatalf("部分失败应 200 partial: %d %v", resp.StatusCode, out)
	}
	trs, _ := out["targets"].([]any)
	first := trs[0].(map[string]any)
	second := trs[1].(map[string]any)
	if first["result"] != "sent" || second["result"] != "failed" {
		t.Fatalf("逐目标结果不对: %v", trs)
	}
	if !strings.Contains(fmt.Sprint(second["error"]), "平台拒收") {
		t.Fatalf("失败原因未回报: %v", second)
	}

	rec.err = errors.New("全挂")
	resp, out = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "y", "source": "ci"})
	if resp.StatusCode != http.StatusServiceUnavailable || out["status"] != "failed" {
		t.Fatalf("全失败应 503 failed: %d %v", resp.StatusCode, out)
	}
}

func TestNotifyPlainTextAndEmpty(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), "磁盘快满了")
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(fmt.Sprint(out["message"]), "source") {
		t.Fatalf("纯文本无 source 应 400: %d %v", resp.StatusCode, out)
	}
	resp, _ = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "磁盘快满了", "source": "ops"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("纯文本带 source 应受理: %d", resp.StatusCode)
	}
	contents := rec.contents()
	if len(contents) != 1 || !strings.Contains(contents[0], "磁盘快满了") {
		t.Fatalf("纯文本正文未投递: %v", contents)
	}
	resp, out = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"title": "  ", "body": " ", "source": "ops"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("空内容应 400: %d %v", resp.StatusCode, out)
	}
	resp, out = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"title": "t", "body": "b"})
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(fmt.Sprint(out["message"]), "source") {
		t.Fatalf("缺 source 应 400: %d %v", resp.StatusCode, out)
	}
	if c.Stats().NotifyRejected == 0 {
		t.Fatal("缺 source 未计数")
	}
}

// 对外不做去重：同样内容重复请求就重复投递（去重曾是 dedupe_key 契约，已按用户口径移除）。
func TestNotifyNoDedupe(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	payload := map[string]string{"body": "重复告警", "source": "ci"}
	for i := 0; i < 2; i++ {
		_, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), payload)
		if out["status"] != "delivered" {
			t.Fatalf("第 %d 次应投递: %v", i+1, out)
		}
	}
	if n := len(rec.contents()); n != 2 {
		t.Fatalf("不做去重，应投递两次, 实际 %d", n)
	}
}

// 老调用方可能仍在传已移除的字段：应被忽略而不是报错（字段删除不该打断上游）。
func TestNotifyIgnoresRemovedFields(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{
		"title": "t", "body": "b", "source": "ci",
		"severity": "error", "dedupe_key": "k",
	})
	if resp.StatusCode != http.StatusOK || out["status"] != "delivered" {
		t.Fatalf("旧字段应被忽略: %d %v", resp.StatusCode, out)
	}
	if _, ok := out["deduped"]; ok {
		t.Fatalf("回执不应再有 deduped 字段: %v", out)
	}
	if got := rec.contents(); len(got) != 1 || strings.Contains(got[0], "[error]") {
		t.Fatalf("不应再渲染级别前缀: %q", got)
	}
}

func TestNotifyRateLimit(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	c.cfg.NotifyPerMinute = 2
	for i := 0; i < 2; i++ {
		resp, _ := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": fmt.Sprintf("n%d", i), "source": "ci"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("第 %d 条应放行: %d", i+1, resp.StatusCode)
		}
	}
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "n3", "source": "ci"})
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("超限应 429: %d %v", resp.StatusCode, out)
	}
	if c.Stats().NotifyRejected == 0 {
		t.Fatal("超限未计数")
	}
}

func TestNotifyBodyTooLargeAndTruncation(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	long := strings.Repeat("啊", 12000)
	resp, _ := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": long, "source": "ci"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("超长正文应截断受理: %d", resp.StatusCode)
	}
	contents := rec.contents()
	if len(contents) != 1 {
		t.Fatalf("广播次数 = %d", len(contents))
	}
	if bus.RuneLen(contents[0]) > bodyLimit+100 {
		t.Fatalf("正文未截断: %d 字符", bus.RuneLen(contents[0]))
	}
	if c.Stats().NotifyTruncated != 1 {
		t.Fatalf("截断未计数: %d", c.Stats().NotifyTruncated)
	}

	// 超过 body 上限：413/400 皆可，但必须拒绝
	huge := strings.Repeat("x", 70000)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": huge, "source": "ci"})
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("超上限请求体不应受理: %v", out)
	}
}

func TestRequestAcceptedReturnsSession(t *testing.T) {
	rec := &recorder{}
	var got bus.InboundMessage
	var mu sync.Mutex
	c := newTestChannel(t, rec, func(m bus.InboundMessage) bool {
		mu.Lock()
		got = m
		mu.Unlock()
		return true
	})
	c.cfg.Query = func(string) (RequestState, bool) { return RequestState{}, false }
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathRequest), map[string]string{
		"source": "ci", "identity": "build-bot@runner-1", "purpose": "部署 staging", "text": "部署 release-2.3.1",
	})
	if resp.StatusCode != http.StatusAccepted || out["status"] != "accepted" {
		t.Fatalf("提交应立即受理: %d %v", resp.StatusCode, out)
	}
	if out["session"] != "http:ci" || out["id"] == "" {
		t.Fatalf("应返回会话键与请求 id: %v", out)
	}
	mu.Lock()
	msg := got
	mu.Unlock()
	if msg.Channel != Name || msg.SenderID != "build-bot@runner-1" || msg.SenderName != "ci" {
		t.Fatalf("入站来源信息不对: %+v", msg)
	}
	for _, want := range []string{"[外部服务请求]", "来源=ci", "身份=build-bot@runner-1", "目的=部署 staging", "部署 release-2.3.1"} {
		if !strings.Contains(msg.Content, want) {
			t.Fatalf("正文缺少 %q: %q", want, msg.Content)
		}
	}
	if msg.Raw["purpose"] != "部署 staging" || msg.Raw["source"] != "ci" {
		t.Fatalf("Raw 未带来源/目的: %+v", msg.Raw)
	}
}

func TestRequestQueryStates(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	c.cfg.Query = func(session string) (RequestState, bool) {
		switch session {
		case "http:ci":
			return RequestState{Running: true}, true
		case "http:ops":
			return RequestState{Reply: "已部署，冒烟通过", FinishedAt: time.Unix(1770000000, 0)}, true
		case "http:bad":
			return RequestState{Failed: true, Reply: "error: 上游 500", FinishedAt: time.Unix(1770000000, 0)}, true
		default:
			return RequestState{}, false
		}
	}
	resp, out := doJSON(t, http.MethodGet, c.url(t, PathRequest)+"?session=http:ci", nil)
	if resp.StatusCode != http.StatusOK || out["status"] != "running" {
		t.Fatalf("在跑会话应 running: %d %v", resp.StatusCode, out)
	}
	resp, out = doJSON(t, http.MethodGet, c.url(t, PathRequest)+"?session=http:ops", nil)
	if resp.StatusCode != http.StatusOK || out["status"] != "done" || out["reply"] != "已部署，冒烟通过" {
		t.Fatalf("完成会话应 done+reply: %d %v", resp.StatusCode, out)
	}
	if out["finished_at"] == nil {
		t.Fatalf("应带完成时间: %v", out)
	}
	resp, out = doJSON(t, http.MethodGet, c.url(t, PathRequest)+"?session=http:bad", nil)
	if resp.StatusCode != http.StatusOK || out["status"] != "error" || out["reply"] != "error: 上游 500" {
		t.Fatalf("失败会话应 error: %d %v", resp.StatusCode, out)
	}
	resp, out = doJSON(t, http.MethodGet, c.url(t, PathRequest)+"?session=http:none", nil)
	if resp.StatusCode != http.StatusNotFound || out["status"] != "unknown" {
		t.Fatalf("未知会话应 404 unknown: %d %v", resp.StatusCode, out)
	}
	resp, out = doJSON(t, http.MethodGet, c.url(t, PathRequest), nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("缺 session 应 400: %d %v", resp.StatusCode, out)
	}
}

func TestRequestValidationAndSessionOverride(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	for _, in := range []map[string]string{
		{"purpose": "p", "text": "t"},
		{"source": "s", "text": "t"},
		{"source": "s", "purpose": "p"},
	} {
		resp, out := doJSON(t, http.MethodPost, c.url(t, PathRequest), in)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("缺字段应 400: %v %v", in, out)
		}
	}
	var (
		mu  sync.Mutex
		got bus.InboundMessage
	)
	c.cfg.RequestSession = "cli:default"
	c.cfg.Submit = func(m bus.InboundMessage) bool {
		mu.Lock()
		got = m
		mu.Unlock()
		return true
	}
	_, out := doJSON(t, http.MethodPost, c.url(t, PathRequest), map[string]string{"source": "ci", "purpose": "p", "text": "t"})
	if out["session"] != "cli:default" {
		t.Fatalf("会话覆盖未生效: %v", out)
	}
	mu.Lock()
	session := got.SessionKey
	mu.Unlock()
	if session != "cli:default" {
		t.Fatalf("入站会话键 = %q, want cli:default", session)
	}
}

func TestRequestBusy(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	c.cfg.Submit = func(bus.InboundMessage) bool { return false }
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathRequest), map[string]string{"source": "ci", "purpose": "p", "text": "t"})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("总线满应 503: %d %v", resp.StatusCode, out)
	}
	if c.Stats().RequestRejected == 0 {
		t.Fatal("拒绝未计数")
	}
}

func TestRequestRateLimitAndMethod(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	c.cfg.RequestPerMinute = 1
	_, _ = doJSON(t, http.MethodPost, c.url(t, PathRequest), map[string]string{"source": "a", "purpose": "p", "text": "t"})
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathRequest), map[string]string{"source": "b", "purpose": "p", "text": "t"})
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("超限应 429: %d %v", resp.StatusCode, out)
	}
	resp, _ = doJSON(t, http.MethodGet, c.url(t, PathNotify), nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /notify 应 405: %d", resp.StatusCode)
	}
	resp, _ = doJSON(t, http.MethodPut, c.url(t, PathRequest), nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /request 应 405: %d", resp.StatusCode)
	}
	resp, _ = doJSON(t, http.MethodGet, c.url(t, PathHealth), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz 应 200: %d", resp.StatusCode)
	}
}

func TestSendAlwaysUnroutable(t *testing.T) {
	c := newTestChannel(t, &recorder{}, nil)
	err := c.Send(context.Background(), bus.OutboundMessage{Channel: Name, ChatID: "any"})
	if err == nil || !strings.Contains(err.Error(), "无待回复请求") {
		t.Fatalf("出站应恒为无目标（尽力而为落空）: %v", err)
	}
}

func TestFormatNoticeNoSeverityPrefix(t *testing.T) {
	got, trunc := formatNotice("标题", "", "正文")
	if trunc || got != "标题\n\n正文" {
		t.Fatalf("标题+正文渲染不对: %q", got)
	}
	if got, _ := formatNotice("", "", ""); got != "" {
		t.Fatalf("空内容应为空: %q", got)
	}
	// 不做分级：正文里不会出现任何 severity 前缀
	got, _ = formatNotice("t", "", "")
	if got != "t" {
		t.Fatalf("不应加任何级别前缀: %q", got)
	}
	// 来源以脚注形式追加
	got, _ = formatNotice("t", "ci", "b")
	if got != "t\n\nb\n\n— ci" {
		t.Fatalf("来源脚注不对: %q", got)
	}
}

func TestNotifyNoDeliveryTarget(t *testing.T) {
	rec := &recorder{noTarget: true}
	c := newTestChannel(t, rec, nil)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"title": "t", "body": "b", "source": "ci"})
	if resp.StatusCode != http.StatusServiceUnavailable || out["status"] != "failed" {
		t.Fatalf("无可投递目标应 503 failed: %d %v", resp.StatusCode, out)
	}
	if c.Stats().NotifyFailed != 1 {
		t.Fatalf("应计 failed: %+v", c.Stats())
	}
	if _, err := c.SendNotice(context.Background(), NoticeBody{Title: "t", Body: "b", Source: "ci"}); err == nil {
		t.Fatalf("本地入口应同样明确报错: %v", err)
	}
}

func TestStatsCounters(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	_, _ = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "a", "source": "ci"})
	_, _ = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "b", "source": "ci"})
	_, _ = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "b", "source": "ci"})
	st := c.Stats()
	if !st.Running || st.NotifyAccepted != 3 || st.NotifyDelivered != 3 {
		t.Fatalf("统计不对: %+v", st)
	}
	if !strings.HasPrefix(st.Listen, "127.0.0.1:") || strings.HasSuffix(st.Listen, ":0") {
		t.Fatalf("listen 应为实际地址: %q", st.Listen)
	}
}

// 受理后立刻告诉用户「有外部请求、准备做什么」，且不消耗模型回合（是 Announce 而非广播投递）。
func TestRequestAnnouncesStart(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathRequest), map[string]string{
		"source": "ci", "identity": "build-bot@runner-1",
		"purpose": "部署回报", "text": "把 release-2.3.1 部署到 staging 并跑冒烟测试",
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("状态码 = %d, body=%v", resp.StatusCode, out)
	}
	notes := rec.announces()
	if len(notes) != 1 {
		t.Fatalf("受理时应发一条开始通知，实际 %d 条: %v", len(notes), notes)
	}
	got := notes[0]
	for _, want := range []string{"收到外部请求", "ci", "build-bot@runner-1", "部署回报", "release-2.3.1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("开始通知缺少 %q: %q", want, got)
		}
	}
	if !strings.Contains(got, out["id"].(string)) {
		t.Fatalf("开始通知应带请求 id: %q", got)
	}
	if len(rec.broadcasts()) != 0 {
		t.Fatalf("开始通知不应走通知广播管线: %v", rec.broadcasts())
	}
}

// 未接 Notice 钩子时不应崩（老装配/测试可省）。
func TestRequestAnnounceWithoutHook(t *testing.T) {
	c := New(Config{Listen: "127.0.0.1:0", MaxBodyBytes: 65536, NotifyPerMinute: 30, RequestPerMinute: 5,
		Broadcast: (&recorder{}).broadcast, Submit: func(bus.InboundMessage) bool { return true }})
	if err := c.Start(t.Context(), bus.New(4, 4)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(t.Context()) })
	resp, _ := doJSON(t, http.MethodPost, c.url(t, PathRequest), map[string]string{
		"source": "ci", "purpose": "p", "text": "t",
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("状态码 = %d", resp.StatusCode)
	}
}
