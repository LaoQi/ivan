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
	"sync/atomic"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
)

type recorder struct {
	mu   sync.Mutex
	sent []bus.OutboundMessage
	err  error
}

func (r *recorder) deliver(ctx context.Context, m bus.OutboundMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.sent = append(r.sent, m)
	return nil
}

func (r *recorder) messages() []bus.OutboundMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bus.OutboundMessage(nil), r.sent...)
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
		DedupeSeconds:    60,
		NotifyPerMinute:  30,
		RequestPerMinute: 5,
		Targets: []Target{
			{Name: "main", Channel: "qq", ChatID: "u1"},
			{Name: "backup", Channel: "qq", ChatID: "u2"},
		},
		Deliver: rec.deliver,
		Submit:  submit,
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

func TestNotifyFanoutToAllTargets(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{
		"title": "构建失败", "body": "job #123 exit 1", "severity": "error", "source": "ci",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%v", resp.StatusCode, out)
	}
	if out["status"] != "delivered" {
		t.Fatalf("status = %v", out["status"])
	}
	msgs := rec.messages()
	if len(msgs) != 2 {
		t.Fatalf("投递条数 = %d, want 2", len(msgs))
	}
	if msgs[0].Channel != "qq" || msgs[0].ChatID != "u1" || msgs[1].ChatID != "u2" {
		t.Fatalf("目标不对: %+v", msgs)
	}
	if msgs[0].Kind != bus.KindNotice {
		t.Fatalf("Kind = %q, want notice", msgs[0].Kind)
	}
	if !strings.Contains(msgs[0].Content, "[error] 构建失败") || !strings.Contains(msgs[0].Content, "job #123 exit 1") {
		t.Fatalf("正文格式不对: %q", msgs[0].Content)
	}
	if !strings.Contains(msgs[0].Content, "— ci") {
		t.Fatalf("缺少来源脚注: %q", msgs[0].Content)
	}
}

func TestNotifyPartialAndAllFailed(t *testing.T) {
	var failAll atomic.Bool
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	c.cfg.Deliver = func(ctx context.Context, m bus.OutboundMessage) error {
		if failAll.Load() {
			return errors.New("全挂")
		}
		if m.ChatID == "u2" {
			return errors.New("平台拒收")
		}
		return rec.deliver(ctx, m)
	}
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "x"})
	if resp.StatusCode != http.StatusOK || out["status"] != "partial" {
		t.Fatalf("部分失败应 200 partial: %d %v", resp.StatusCode, out)
	}
	trs, _ := out["targets"].([]any)
	if len(trs) != 2 {
		t.Fatalf("targets = %v", out["targets"])
	}
	first := trs[0].(map[string]any)
	second := trs[1].(map[string]any)
	if first["result"] != "sent" || second["result"] != "failed" {
		t.Fatalf("逐目标结果不对: %v", trs)
	}
	if !strings.Contains(fmt.Sprint(second["error"]), "平台拒收") {
		t.Fatalf("失败原因未回报: %v", second)
	}

	failAll.Store(true)
	resp, out = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "y"})
	if resp.StatusCode != http.StatusServiceUnavailable || out["status"] != "failed" {
		t.Fatalf("全失败应 503 failed: %d %v", resp.StatusCode, out)
	}
}

func TestNotifyPlainTextAndEmpty(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	resp, _ := doJSON(t, http.MethodPost, c.url(t, PathNotify), "磁盘快满了")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("纯文本应受理: %d", resp.StatusCode)
	}
	msgs := rec.messages()
	if len(msgs) != 2 || !strings.Contains(msgs[0].Content, "磁盘快满了") {
		t.Fatalf("纯文本正文未投递: %+v", msgs)
	}
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"title": "  ", "body": " "})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("空内容应 400: %d %v", resp.StatusCode, out)
	}
}

func TestNotifyDedupe(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	payload := map[string]string{"body": "重复告警", "source": "ci", "dedupe_key": "job-1"}
	_, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), payload)
	if out["status"] != "delivered" {
		t.Fatalf("首次应投递: %v", out)
	}
	_, out = doJSON(t, http.MethodPost, c.url(t, PathNotify), payload)
	if out["status"] != "deduped" {
		t.Fatalf("重复应去重: %v", out)
	}
	if n := len(rec.messages()); n != 2 {
		t.Fatalf("去重后不应再投递, 投递数 = %d", n)
	}
	// 不同 source 的同名 key 不算重复
	payload["source"] = "other"
	_, out = doJSON(t, http.MethodPost, c.url(t, PathNotify), payload)
	if out["status"] != "delivered" {
		t.Fatalf("不同 source 不应去重: %v", out)
	}
	// 无 key 不去重
	_, out = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "no key"})
	_, out2 := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "no key"})
	if out["status"] != "delivered" || out2["status"] != "delivered" {
		t.Fatalf("无 dedupe_key 不应去重: %v %v", out, out2)
	}
}

func TestNotifyRateLimit(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	c.cfg.NotifyPerMinute = 2
	for i := 0; i < 2; i++ {
		resp, _ := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": fmt.Sprintf("n%d", i)})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("第 %d 条应放行: %d", i+1, resp.StatusCode)
		}
	}
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "n3"})
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
	resp, _ := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": long})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("超长正文应截断受理: %d", resp.StatusCode)
	}
	msgs := rec.messages()
	if len(msgs) != 2 {
		t.Fatalf("投递条数 = %d", len(msgs))
	}
	if bus.RuneLen(msgs[0].Content) > bodyLimit+100 {
		t.Fatalf("正文未截断: %d 字符", bus.RuneLen(msgs[0].Content))
	}
	if c.Stats().NotifyTruncated != 1 {
		t.Fatalf("截断未计数: %d", c.Stats().NotifyTruncated)
	}

	// 超过 body 上限：413/400 皆可，但必须拒绝
	huge := strings.Repeat("x", 70000)
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": huge})
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

func TestFormatNoticeSeverityAndTitleOnly(t *testing.T) {
	got, trunc := formatNotice("标题", "", "", "正文")
	if trunc || got != "标题\n\n正文" {
		t.Fatalf("info 不应加前缀: %q", got)
	}
	got, _ = formatNotice("", "warn", "", "")
	if got != "" {
		t.Fatalf("空内容应为空: %q", got)
	}
	got, _ = formatNotice("t", "WARN", "", "")
	if got != "[WARN] t" {
		t.Fatalf("severity 前缀不对: %q", got)
	}
}

func TestNotifyWithoutTargets(t *testing.T) {
	c := New(Config{
		Listen:           "127.0.0.1:0",
		NotifyPerMinute:  30,
		RequestPerMinute: 5,
		Deliver:          func(context.Context, bus.OutboundMessage) error { return nil },
		Submit:           func(bus.InboundMessage) bool { return true },
	})
	if err := c.Start(context.Background(), bus.New(4, 4)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	resp, out := doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"title": "t", "body": "b"})
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(fmt.Sprint(out["message"]), "未配置通知目标") {
		t.Fatalf("无 targets 应明确失败: %d %v", resp.StatusCode, out)
	}
	if c.Stats().NotifyFailed != 1 {
		t.Fatalf("应计 failed: %+v", c.Stats())
	}
	if _, err := c.SendNotice(context.Background(), NoticeBody{Title: "t"}); err == nil ||
		!strings.Contains(err.Error(), "未配置通知目标") {
		t.Fatalf("本地入口应同样明确报错: %v", err)
	}
}

func TestStatsCounters(t *testing.T) {
	rec := &recorder{}
	c := newTestChannel(t, rec, nil)
	_, _ = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "a"})
	_, _ = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "b", "dedupe_key": "k"})
	_, _ = doJSON(t, http.MethodPost, c.url(t, PathNotify), map[string]string{"body": "b", "dedupe_key": "k"})
	st := c.Stats()
	if !st.Running || st.Targets != 2 || st.NotifyAccepted != 2 || st.NotifyDelivered != 2 || st.NotifyDeduped != 1 {
		t.Fatalf("统计不对: %+v", st)
	}
	if !strings.HasPrefix(st.Listen, "127.0.0.1:") || strings.HasSuffix(st.Listen, ":0") {
		t.Fatalf("listen 应为实际地址: %q", st.Listen)
	}
}
