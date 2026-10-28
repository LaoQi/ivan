package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func sendToolRig(t *testing.T, targets ...string) (*rig, *[]bus.OutboundMessage) {
	t.Helper()
	srv := fakellm.New(fakellm.Step{Content: "done"})
	t.Cleanup(srv.Close)
	rg := newRig(t, srv)
	for _, name := range targets {
		rg.cfg.HTTP.Targets = append(rg.cfg.HTTP.Targets, config.HTTPTargetConfig{Name: name, Channel: "qq", ChatID: "chat-" + name})
	}
	sent := &[]bus.OutboundMessage{}
	rg.r.opts.NotifyStrict = func(ctx context.Context, m bus.OutboundMessage) error {
		*sent = append(*sent, m)
		return nil
	}
	return rg, sent
}

func TestSendMessageToolFanoutToAllTargets(t *testing.T) {
	rg, sent := sendToolRig(t, "main", "ops")
	inst := rg.r.acquire("http:ci")
	tool := rg.r.sendMessageTool(inst)
	got := tool.Invoke(context.Background(), `{"text":"构建失败已确认，请指示"}`).Text
	if !strings.HasPrefix(got, "ok:") || !strings.Contains(got, "2 个目标") {
		t.Fatalf("广播应成功: %q", got)
	}
	if len(*sent) != 2 {
		t.Fatalf("投递条数 = %d", len(*sent))
	}
	for i, m := range *sent {
		if m.Channel != "qq" || m.Kind != bus.KindNotice || m.Content != "构建失败已确认，请指示" {
			t.Fatalf("投递 %d 不对: %+v", i, m)
		}
	}
}

func TestSendMessageToolReportsPartialFailure(t *testing.T) {
	rg, _ := sendToolRig(t, "main", "cli-req")
	rg.r.opts.NotifyStrict = func(ctx context.Context, m bus.OutboundMessage) error {
		if m.ChatID == "chat-main" {
			return nil
		}
		return context.DeadlineExceeded
	}
	inst := rg.r.acquire("http:ci")
	got := rg.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"hi"}`).Text
	if !strings.HasPrefix(got, "partial:") || !strings.Contains(got, "1/2") || !strings.Contains(got, "cli-req") {
		t.Fatalf("部分失败应如实汇总: %q", got)
	}
}

func TestSendMessageToolAllFail(t *testing.T) {
	rg, _ := sendToolRig(t, "main")
	rg.r.opts.NotifyStrict = func(context.Context, bus.OutboundMessage) error {
		return context.DeadlineExceeded
	}
	inst := rg.r.acquire("http:ci")
	got := rg.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"hi"}`).Text
	if !strings.HasPrefix(got, "error:") || !strings.Contains(got, "main") {
		t.Fatalf("全部失败应报错并含原因: %q", got)
	}
}

func TestSendMessageToolFallsBackToNotify(t *testing.T) {
	rg, sent := sendToolRig(t, "main")
	rg.r.opts.NotifyStrict = nil
	rg.r.opts.Notify = func(ctx context.Context, m bus.OutboundMessage) error {
		*sent = append(*sent, m)
		return nil
	}
	inst := rg.r.acquire("http:ci")
	got := rg.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"走普通钩子"}`).Text
	if !strings.HasPrefix(got, "ok:") || len(*sent) != 1 {
		t.Fatalf("无严格钩子时应回落 Notify: %q %d", got, len(*sent))
	}
}

func TestSendMessageToolValidation(t *testing.T) {
	rg, _ := sendToolRig(t, "main")
	inst := rg.r.acquire("http:ci")
	tool := rg.r.sendMessageTool(inst)
	if got := tool.Invoke(context.Background(), `{"text":"  "}`).Text; !strings.Contains(got, "text 不能为空") {
		t.Fatalf("空正文应拒绝: %q", got)
	}
	if got := tool.Invoke(context.Background(), `{bad json}`).Text; !strings.Contains(got, "解析") {
		t.Fatalf("坏 JSON 应报解析错误: %q", got)
	}
}

func TestSendMessageToolNoTargets(t *testing.T) {
	rg, _ := sendToolRig(t)
	inst := rg.r.acquire("http:ci")
	got := rg.r.sendMessageTool(inst).Invoke(context.Background(), `{"text":"hi"}`).Text
	if !strings.Contains(got, "http.targets 未配置") {
		t.Fatalf("无目标应明确报错: %q", got)
	}
}

func TestSessionStateTracksOutcome(t *testing.T) {
	rg := newRig(t, fakellm.New(fakellm.Step{Content: "最终答复"}))
	rg.r.opts.Notify = nil
	rg.r.opts.NotifyStrict = nil
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
