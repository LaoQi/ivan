package runner

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/internal/fakellm"
)

type noticeRig struct {
	*rig
	mu    sync.Mutex
	calls []string
	sent  int
	total int
	names []string
}

func (n *noticeRig) notice(ctx context.Context, content string) (int, int, string) {
	n.mu.Lock()
	n.calls = append(n.calls, content)
	n.mu.Unlock()
	return n.sent, n.total, strings.Join(n.names, ", ")
}

func sendToolRig(t *testing.T, sent, total int, names ...string) *noticeRig {
	t.Helper()
	srv := fakellm.New(fakellm.Step{Content: "done"})
	t.Cleanup(srv.Close)
	rg := newRig(t, srv)
	n := &noticeRig{rig: rg, sent: sent, total: total, names: names}
	rg.r.opts.Notice = n.notice
	return n
}

func TestSendMessageToolBroadcast(t *testing.T) {
	n := sendToolRig(t, 2, 2, "qq:u1", "qq:u2")
	inst := mustAcquire(t, n.r, "http:ci")
	tool := n.r.sendMessageTool(inst)
	got := tool.Invoke(context.Background(), `{"text":"构建失败已确认，请指示"}`).Text
	if !strings.HasPrefix(got, "ok:") || !strings.Contains(got, "2 个目标") {
		t.Fatalf("广播应成功: %q", got)
	}
	if len(n.calls) != 1 || n.calls[0] != "构建失败已确认，请指示" {
		t.Fatalf("通知出口调用不对: %v", n.calls)
	}
}

func TestSendMessageToolReportsPartialFailure(t *testing.T) {
	n := sendToolRig(t, 1, 2, "qq:u1")
	inst := mustAcquire(t, n.r, "http:ci")
	got := n.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"hi"}`).Text
	if !strings.HasPrefix(got, "partial:") || !strings.Contains(got, "1/2") || !strings.Contains(got, "qq:u1") {
		t.Fatalf("部分失败应如实汇总: %q", got)
	}
}

func TestSendMessageToolAllFail(t *testing.T) {
	n := sendToolRig(t, 0, 2)
	inst := mustAcquire(t, n.r, "http:ci")
	got := n.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"hi"}`).Text
	if !strings.HasPrefix(got, "error:") || !strings.Contains(got, "2 个目标") {
		t.Fatalf("全部失败应报错: %q", got)
	}
}

func TestSendMessageToolNoOutlet(t *testing.T) {
	n := sendToolRig(t, 0, 0)
	inst := mustAcquire(t, n.r, "http:ci")
	got := n.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"hi"}`).Text
	if !strings.Contains(got, "没有实现通知出口的通道") {
		t.Fatalf("无通知出口应明确报错: %q", got)
	}
	n.r.opts.Notice = nil
	got = n.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"hi"}`).Text
	if !strings.Contains(got, "没有通知出口") {
		t.Fatalf("回调缺失应明确报错: %q", got)
	}
}

func TestSendMessageToolValidation(t *testing.T) {
	n := sendToolRig(t, 1, 1, "qq:u1")
	inst := mustAcquire(t, n.r, "http:ci")
	tool := n.r.sendMessageTool(inst)
	if got := tool.Invoke(context.Background(), `{"text":"  "}`).Text; !strings.Contains(got, "text 不能为空") {
		t.Fatalf("空正文应拒绝: %q", got)
	}
	if got := tool.Invoke(context.Background(), `{bad json}`).Text; !strings.Contains(got, "解析") {
		t.Fatalf("坏 JSON 应报解析错误: %q", got)
	}
}

func TestSessionStateTracksOutcome(t *testing.T) {
	rg := newRig(t, fakellm.New(fakellm.Step{Content: "最终答复"}))
	rg.r.opts.Notify = nil
	_, err := rg.r.DispatchTurn(context.Background(), inbound("http:ci", "req-1", "[外部服务请求] 来源=ci 目的=测试\n正文"))
	if err != nil {
		t.Fatalf("回合应成功: %v", err)
	}
	running, o, known := rg.r.SessionState("http:ci")
	if running || !known || o.Failed || o.Reply != "最终答复" {
		t.Fatalf("结果表不对: running=%v known=%v o=%+v", running, known, o)
	}
	_, _, known = rg.r.SessionState("http:none")
	if known {
		t.Fatal("未知会话应 unknown")
	}
}

// 外部请求（Channel=http）回合结束时回报结果；普通会话不打扰用户。
func TestExternalTurnReportsOnFinish(t *testing.T) {
	n := sendToolRig(t, 1, 1, "qq:u1")
	msg := bus.InboundMessage{
		Channel:    "http",
		ChatID:     "req-1",
		MessageID:  "req-1",
		Content:    "[外部服务请求] 来源=ci 目的=回报\n跑一下 pwd",
		SessionKey: "http:ci",
	}
	if _, err := n.r.DispatchTurn(context.Background(), msg); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	notes := n.calls
	if len(notes) != 1 {
		t.Fatalf("外部请求结束应回报一条，实际 %d: %v", len(notes), notes)
	}
	got := notes[0]
	for _, want := range []string{"外部请求 req-1", "处理完成", "用时"} {
		if !strings.Contains(got, want) {
			t.Fatalf("结束回报缺少 %q: %q", want, got)
		}
	}
}

func TestExternalTurnFailureReported(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Status: 400})
	t.Cleanup(srv.Close)
	rg := newRigTweak(t, srv, func(c *config.Config) { c.MaxRetries = 0 })
	n := &noticeRig{rig: rg, sent: 1, total: 1, names: []string{"qq:u1"}}
	rg.r.opts.Notice = n.notice
	msg := bus.InboundMessage{
		Channel:    "http",
		ChatID:     "req-2",
		MessageID:  "req-2",
		Content:    "[外部服务请求] 来源=ci 目的=回报\n干活",
		SessionKey: "http:ci",
	}
	if _, err := n.r.DispatchTurn(context.Background(), msg); err == nil {
		t.Fatal("应失败")
	}
	if len(n.calls) != 1 || !strings.Contains(n.calls[0], "失败") {
		t.Fatalf("失败也应回报且标明失败: %v", n.calls)
	}
}

func TestNonExternalTurnNotReported(t *testing.T) {
	n := sendToolRig(t, 1, 1, "qq:u1")
	msg := bus.InboundMessage{
		Channel:    "qq",
		ChatID:     "u1",
		MessageID:  "m1",
		Content:    "你好",
		SessionKey: "qq:c2c:u1",
	}
	if _, err := n.r.DispatchTurn(context.Background(), msg); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	if len(n.calls) != 0 {
		t.Fatalf("普通会话不应收到结束回报: %v", n.calls)
	}
}
