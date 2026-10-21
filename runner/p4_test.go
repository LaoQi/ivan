package runner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	chlog "github.com/LaoQi/ivan/channel/log"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func bareConfig(t *testing.T, baseURL string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.BaseURL = baseURL
	cfg.APIKey = "test-key"
	cfg.Model = "mock-model"
	cfg.ApiProtocol = "chat"
	cfg.UserAgent = "ivan/test"
	cfg.ConfigPath = filepath.Join(dir, "config.yaml")
	cfg.SessionMode = "global"
	cfg.Runner.Stream = false
	return cfg
}

func newP4Rig(t *testing.T, cfg *config.Config, paused func() bool) *Runner {
	t.Helper()
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard), Paused: paused})
	if err != nil {
		t.Fatalf("runner.New: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func handoffStep(summary string, cont bool) fakellm.Step {
	args, _ := json.Marshal(map[string]any{"summary": summary, "continue": cont})
	return fakellm.Step{
		Tools: []fakellm.ToolCall{{Name: "next_session", Args: string(args)}},
		Usage: fakellm.Usage{PromptTokens: 220, CompletionTokens: 5, TotalTokens: 225},
	}
}

func lastMessage(t *testing.T, srv *fakellm.Server) string {
	t.Helper()
	reqs := srv.Requests()
	if len(reqs) == 0 {
		t.Fatal("没有请求")
	}
	msgs := reqs[len(reqs)-1].Messages
	if len(msgs) == 0 {
		t.Fatal("请求没有消息")
	}
	return string(msgs[len(msgs)-1])
}

func TestContextThresholdTriggersHandoffHint(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "先做了一半", Usage: fakellm.Usage{PromptTokens: 200, CompletionTokens: 10, TotalTokens: 210}},
		handoffStep("已完成一半，下一步继续做", false),
		fakellm.Step{Content: "交接完成"},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100
	cfg.Runner.HandoffForceTokens = 1000
	r := newP4Rig(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "做个长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3（提示轮 + 交接工具回合）", calls)
	}
	reqs := srv.Requests()
	hinted := string(reqs[1].Messages[len(reqs[1].Messages)-1])
	if !strings.Contains(hinted, "建议交接阈值") || !strings.Contains(hinted, "next_session") {
		t.Fatalf("提示轮没有带上建议交接提示: %s", hinted)
	}
	if first := string(reqs[0].Messages[len(reqs[0].Messages)-1]); strings.Contains(first, "系统提示") {
		t.Fatalf("首轮不应带提示: %s", first)
	}
	st := r.Status()
	if st.Hinted != 1 || st.Handoffs != 1 {
		t.Fatalf("计数异常: hinted=%d handoffs=%d", st.Hinted, st.Handoffs)
	}
	if session, ok := r.Session("cli:default"); !ok || session == "" {
		t.Fatal("交接后未更新会话")
	}
}

func TestContextThresholdInjectsHintOncePerTask(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "第一段", Usage: fakellm.Usage{PromptTokens: 200, CompletionTokens: 10, TotalTokens: 210}},
		fakellm.Step{Content: "模型没交接", Usage: fakellm.Usage{PromptTokens: 220, CompletionTokens: 10, TotalTokens: 230}},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100
	cfg.Runner.HandoffForceTokens = 1000
	r := newP4Rig(t, cfg, nil)
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 2 {
		t.Fatalf("提示每任务只注入一次，请求数 %d", calls)
	}
}

func TestContextBelowThresholdDoesNotHint(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok", Usage: fakellm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 180000
	cfg.Runner.HandoffForceTokens = 256000
	r := newP4Rig(t, cfg, nil)
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "hi")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 1 {
		t.Fatalf("未超阈值不应多跑一轮: %d", calls)
	}
}

func TestContextThresholdSkipsWhenPaused(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "做了一半", Usage: fakellm.Usage{PromptTokens: 200, CompletionTokens: 10, TotalTokens: 210}})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100
	cfg.Runner.HandoffForceTokens = 1000
	r := newP4Rig(t, cfg, func() bool { return true })
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "做个长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 1 {
		t.Fatalf("暂停时不应注入交接提示: %d", calls)
	}
	if st := r.Status(); st.Hinted != 0 || !st.Paused {
		t.Fatalf("状态异常: hinted=%d paused=%v", st.Hinted, st.Paused)
	}
}

func TestPauseStopsAutoContinue(t *testing.T) {
	srv := fakellm.New(handoffStep("交接", true), fakellm.Step{Content: "收尾"}, fakellm.Step{Content: "不该跑到"})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.AutoContinue = true
	r := newP4Rig(t, cfg, func() bool { return true })
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 2 {
		t.Fatalf("暂停时不应自动续跑，请求数 %d（期望 2：交接工具回合）", calls)
	}
	if st := r.Status(); st.Continued != 0 || st.Handoffs != 1 {
		t.Fatalf("计数异常: continued=%d handoffs=%d", st.Continued, st.Handoffs)
	}
}

func TestAutoContinueStillWorks(t *testing.T) {
	srv := fakellm.New(handoffStep("交接", true), fakellm.Step{Content: "收尾"}, fakellm.Step{Content: "第二段"})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.AutoContinue = true
	r := newP4Rig(t, cfg, func() bool { return false })
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3（交接工具回合 + 续跑一轮）", calls)
	}
	if st := r.Status(); st.Continued != 1 {
		t.Fatalf("continued=%d，期望 1", st.Continued)
	}
}

func TestDailyBudgetStopsContinueAndPersists(t *testing.T) {
	srv := fakellm.New(handoffStep("交接", true), fakellm.Step{Content: "收尾"}, fakellm.Step{Content: "不该跑到"})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.AutoContinue = true
	cfg.Runner.MaxTokensPerDay = 100
	first := newP4Rig(t, cfg, nil)
	if err := first.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 2 {
		t.Fatalf("预算用尽后不应续跑，请求数 %d（期望 2：交接工具回合）", calls)
	}
	if st := first.Status(); st.TokensToday < 100 || st.Continued != 0 {
		t.Fatalf("预算计数异常: tokens=%d continued=%d", st.TokensToday, st.Continued)
	}
	_ = first.Close()

	second := newP4Rig(t, cfg, nil)
	if got := second.TokensToday(); got < 100 {
		t.Fatalf("重启后当日用量丢失: %d", got)
	}
	if allow, reason := second.Budget(); allow || !strings.Contains(reason, "预算") {
		t.Fatalf("预算应跨进程累计: allow=%v reason=%q", allow, reason)
	}
}

func TestCancelTurn(t *testing.T) {
	reached := make(chan struct{}, 1)
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case reached <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	defer func() {
		close(stop)
		srv.Close()
	}()
	cfg := bareConfig(t, srv.URL+"/v1")
	r := newP4Rig(t, cfg, nil)

	done := make(chan error, 1)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:default", "r1", "慢任务")) }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("请求未到达假服务端")
	}
	if !r.CancelTurn("cli:default") {
		t.Fatal("CancelTurn 应返回 true")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("取消后应返回错误")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("取消后回合未结束")
	}
	if r.CancelTurn("cli:default") {
		t.Fatal("空闲会话不应报告取消成功")
	}
}

func TestSystemPromptCarriesMemoryAndGoals(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok"})
	defer srv.Close()
	rg := newRig(t, srv)
	if err := rg.r.Dispatch(context.Background(), inbound("cli:default", "r1", "hi")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	reqs := srv.Requests()
	if len(reqs) == 0 || len(reqs[0].Messages) == 0 {
		t.Fatal("没有捕获到请求")
	}
	sys := string(reqs[0].Messages[0])
	memory := filepath.Join(rg.cfg.MemoryDir(), "cli-default.md")
	goals := filepath.Join(rg.cfg.AgentDir("cli-default"), "GOALS.md")
	for _, want := range []string{memory, goals, "会话键: cli:default", "记忆文件"} {
		if !strings.Contains(sys, want) {
			t.Fatalf("系统提示缺少 %q:\n%s", want, sys)
		}
	}
}

func TestForceTierInjectsImperativeAndStops(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "还在做", Usage: fakellm.Usage{PromptTokens: 250, CompletionTokens: 10, TotalTokens: 260}},
		fakellm.Step{Content: "还是没交接", Usage: fakellm.Usage{PromptTokens: 260, CompletionTokens: 10, TotalTokens: 270}},
		fakellm.Step{Content: "不该跑到"},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100
	cfg.Runner.HandoffForceTokens = 200
	r := newP4Rig(t, cfg, nil)
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 2 {
		t.Fatalf("强制档只注入一次并停下，请求数 %d，期望 2", calls)
	}
	reqs := srv.Requests()
	forced := string(reqs[1].Messages[len(reqs[1].Messages)-1])
	if !strings.Contains(forced, "强制交接阈值") || !strings.Contains(forced, "必须立即调用 next_session") {
		t.Fatalf("强制档提示文案异常: %s", forced)
	}
	st := r.Status()
	if st.Forced != 1 || st.Hinted != 0 {
		t.Fatalf("计数异常: forced=%d hinted=%d", st.Forced, st.Hinted)
	}
	if st.HandoffForce != 200 || st.HandoffHint != 100 {
		t.Fatalf("状态阈值异常: hint=%d force=%d", st.HandoffHint, st.HandoffForce)
	}
}

func TestHintThenForceEscalation(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "第一段", Usage: fakellm.Usage{PromptTokens: 150, CompletionTokens: 10, TotalTokens: 160}},
		fakellm.Step{Content: "第二段", Usage: fakellm.Usage{PromptTokens: 250, CompletionTokens: 10, TotalTokens: 260}},
		fakellm.Step{Content: "第三段", Usage: fakellm.Usage{PromptTokens: 270, CompletionTokens: 10, TotalTokens: 280}},
		fakellm.Step{Content: "不该跑到"},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100
	cfg.Runner.HandoffForceTokens = 200
	cfg.Runner.AutoContinue = true
	r := newP4Rig(t, cfg, nil)
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3（建议→强制→停止）", calls)
	}
	reqs := srv.Requests()
	second := string(reqs[1].Messages[len(reqs[1].Messages)-1])
	third := string(reqs[2].Messages[len(reqs[2].Messages)-1])
	if !strings.Contains(second, "建议交接阈值") {
		t.Fatalf("第二轮应为建议档: %s", second)
	}
	if !strings.Contains(third, "强制交接阈值") {
		t.Fatalf("第三轮应升级为强制档: %s", third)
	}
	if st := r.Status(); st.Hinted != 1 || st.Forced != 1 {
		t.Fatalf("计数异常: hinted=%d forced=%d", st.Hinted, st.Forced)
	}
}

func TestForceAfterHandoffReportsDiagnostic(t *testing.T) {
	srv := fakellm.New(
		handoffStep("交接一次", true),
		fakellm.Step{Content: "新会话里回答", Usage: fakellm.Usage{PromptTokens: 250, CompletionTokens: 10, TotalTokens: 260}},
		fakellm.Step{Content: "还是没交接", Usage: fakellm.Usage{PromptTokens: 270, CompletionTokens: 10, TotalTokens: 280}},
		fakellm.Step{Content: "不该跑到"},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100
	cfg.Runner.HandoffForceTokens = 200
	cfg.Runner.AutoContinue = true
	r := newP4Rig(t, cfg, nil)
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 4 {
		t.Fatalf("请求数 %d，期望 4（交接后重置两档，最多再提示一次）", calls)
	}
	st := r.Status()
	if st.Handoffs != 1 || st.Forced != 1 {
		t.Fatalf("计数异常: handoffs=%d forced=%d", st.Handoffs, st.Forced)
	}
}
