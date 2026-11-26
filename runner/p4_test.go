package runner

import (
	"bytes"
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

func newP4RigWithLog(t *testing.T, cfg *config.Config, paused func() bool) (*Runner, *bytes.Buffer) {
	t.Helper()
	buf := new(bytes.Buffer)
	r, err := New(Options{Cfg: cfg, Human: chlog.New(buf), Paused: paused})
	if err != nil {
		t.Fatalf("runner.New: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r, buf
}

func allMessages(req fakellm.Request) string {
	var b strings.Builder
	for _, m := range req.Messages {
		b.Write(m)
		b.WriteByte('\n')
	}
	return b.String()
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

func TestHandoffHintInjectedInsideTurn(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "先做一半", Tools: []fakellm.ToolCall{{ID: "c1", Name: "run_shell", Args: `{"command":"echo hi"}`}},
			Usage: fakellm.Usage{PromptTokens: 150000, CompletionTokens: 10, TotalTokens: 150010}},
		handoffStep("已完成一半，下一步继续做", false),
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	r := newP4Rig(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "做个长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3（首轮、带提示的工具轮、交接工具的收尾轮）", calls)
	}
	reqs := srv.Requests()
	if strings.Contains(allMessages(reqs[0]), "建议交接阈值") {
		t.Fatalf("首轮不应带提示: %s", allMessages(reqs[0]))
	}
	last := string(reqs[1].Messages[len(reqs[1].Messages)-1])
	if !strings.Contains(last, "建议交接阈值") || !strings.Contains(last, "next_session") {
		t.Fatalf("工具轮后的请求应带上建议交接提示: %s", last)
	}
	if st := r.Status(); st.Hinted != 1 || st.Handoffs != 1 {
		t.Fatalf("计数异常: hinted=%d handoffs=%d", st.Hinted, st.Handoffs)
	}
	if session, ok := r.Session("cli:default"); !ok || session == "" {
		t.Fatal("交接后未更新会话")
	}
}

func TestHandoffHintOncePerDispatch(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "第一段", Tools: []fakellm.ToolCall{{ID: "c1", Name: "run_shell", Args: `{"command":"echo a"}`}},
			Usage: fakellm.Usage{PromptTokens: 150000, CompletionTokens: 10, TotalTokens: 150010}},
		fakellm.Step{Content: "第二段", Tools: []fakellm.ToolCall{{ID: "c2", Name: "run_shell", Args: `{"command":"echo b"}`}},
			Usage: fakellm.Usage{PromptTokens: 152000, CompletionTokens: 10, TotalTokens: 152010}},
		fakellm.Step{Content: "收尾", Usage: fakellm.Usage{PromptTokens: 154000, CompletionTokens: 10, TotalTokens: 154010}},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	r := newP4Rig(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	reqs := srv.Requests()
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3", calls)
	}
	if n := strings.Count(allMessages(reqs[2]), "建议交接阈值"); n != 1 {
		t.Fatalf("一次推进只注入一次提示，第三次请求里出现 %d 次", n)
	}
	if st := r.Status(); st.Hinted != 1 {
		t.Fatalf("hinted=%d，期望 1", st.Hinted)
	}
}

func TestHandoffHintBelowThreshold(t *testing.T) {
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
		t.Fatalf("未超阈值不应多发请求: %d", calls)
	}
	if st := r.Status(); st.Hinted != 0 {
		t.Fatalf("hinted=%d，期望 0", st.Hinted)
	}
}

func TestHandoffHintSkippedWhenPaused(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "做了一半", Tools: []fakellm.ToolCall{{ID: "c1", Name: "run_shell", Args: `{"command":"echo hi"}`}},
			Usage: fakellm.Usage{PromptTokens: 150000, CompletionTokens: 10, TotalTokens: 150010}},
		fakellm.Step{Content: "收尾"},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	r, buf := newP4RigWithLog(t, cfg, func() bool { return true })
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "做个长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	reqs := srv.Requests()
	if calls := srv.Calls(); calls != 2 {
		t.Fatalf("暂停时不应多花请求: %d", calls)
	}
	if strings.Contains(allMessages(reqs[1]), "建议交接阈值") {
		t.Fatal("暂停时不应注入交接提示")
	}
	if st := r.Status(); st.Hinted != 0 || !st.Paused {
		t.Fatalf("状态异常: hinted=%d paused=%v", st.Hinted, st.Paused)
	}
	if !strings.Contains(buf.String(), "跳过上下文交接提示") {
		t.Fatalf("缺少暂停事件: %s", buf.String())
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

func TestForceTierResetsSession(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "还在做", Usage: fakellm.Usage{PromptTokens: 250000, CompletionTokens: 10, TotalTokens: 250010}},
		fakellm.Step{Content: "摘要：已完成 A，下一步 B", Usage: fakellm.Usage{PromptTokens: 999, CompletionTokens: 20, TotalTokens: 9999}},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	r, buf := newP4RigWithLog(t, cfg, nil)
	before, _ := r.Session("cli:default")

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	reqs := srv.Requests()
	if calls := srv.Calls(); calls != 2 {
		t.Fatalf("请求数 %d，期望 2（回合 + 摘要请求）", calls)
	}
	if len(reqs[1].Tools) != 0 {
		t.Fatalf("摘要请求不应带工具，实际 %d 个", len(reqs[1].Tools))
	}
	last := lastMessage(t, srv)
	if !strings.Contains(last, "强制交接阈值") || !strings.Contains(last, "交接摘要") {
		t.Fatalf("摘要请求应带 ivan 的指令: %s", last)
	}
	for i := range reqs {
		if strings.Contains(allMessages(reqs[i]), "必须立即调用 next_session") {
			t.Fatalf("第 %d 次请求仍带旧的强制指令文案", i+1)
		}
	}
	st := r.Status()
	if st.Forced != 1 || st.Handoffs != 1 || st.Hinted != 0 {
		t.Fatalf("计数异常: forced=%d handoffs=%d hinted=%d", st.Forced, st.Handoffs, st.Hinted)
	}
	if st.TokensToday != 250010 {
		t.Fatalf("摘要请求用量不应计入预算: tokens_today=%d", st.TokensToday)
	}
	after, _ := r.Session("cli:default")
	if after == "" || after == before {
		t.Fatalf("强制交接后会话应轮转: %q → %q", before, after)
	}
	if !strings.Contains(buf.String(), "已强制交接") {
		t.Fatalf("缺少 handoff_force 事件: %s", buf.String())
	}
}

func TestForceTierContinuesAfterReset(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "还在做", Usage: fakellm.Usage{PromptTokens: 250000, CompletionTokens: 10, TotalTokens: 250010}},
		fakellm.Step{Content: "摘要：先把结论记下来", Usage: fakellm.Usage{PromptTokens: 999, CompletionTokens: 20, TotalTokens: 9999}},
		fakellm.Step{Content: "新会话里继续", Usage: fakellm.Usage{PromptTokens: 5000, CompletionTokens: 10, TotalTokens: 5010}},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	cfg.Runner.AutoContinue = true
	r := newP4Rig(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3（回合 + 摘要 + 续跑一轮）", calls)
	}
	st := r.Status()
	if st.Forced != 1 || st.Handoffs != 1 || st.Continued != 1 {
		t.Fatalf("计数异常: forced=%d handoffs=%d continued=%d", st.Forced, st.Handoffs, st.Continued)
	}
}

func TestForceHandoffFailureStops(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "还在做", Usage: fakellm.Usage{PromptTokens: 250000, CompletionTokens: 10, TotalTokens: 250010}},
		fakellm.Step{Content: "", Usage: fakellm.Usage{PromptTokens: 999, CompletionTokens: 0, TotalTokens: 999}},
		fakellm.Step{Content: "下一条消息的回答", Usage: fakellm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	r, buf := newP4RigWithLog(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 2 {
		t.Fatalf("摘要失败后不应再发请求: %d", calls)
	}
	if st := r.Status(); st.Forced != 0 || st.Handoffs != 0 {
		t.Fatalf("复位失败不应计成功: forced=%d handoffs=%d", st.Forced, st.Handoffs)
	}
	if !strings.Contains(buf.String(), "强制交接失败") {
		t.Fatalf("缺少失败事件: %s", buf.String())
	}

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r2", "下一条")); err != nil {
		t.Fatalf("Dispatch 2: %v", err)
	}
	reqs := srv.Requests()
	if !strings.Contains(allMessages(reqs[2]), "还在做") {
		t.Fatal("复位失败后会话不应轮转，第二条消息应仍在同一会话里")
	}
}

func TestHintThenForceEscalation(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "第一段", Tools: []fakellm.ToolCall{{ID: "c1", Name: "run_shell", Args: `{"command":"echo a"}`}},
			Usage: fakellm.Usage{PromptTokens: 150000, CompletionTokens: 10, TotalTokens: 150010}},
		fakellm.Step{Content: "第二段", Usage: fakellm.Usage{PromptTokens: 250000, CompletionTokens: 10, TotalTokens: 250010}},
		fakellm.Step{Content: "摘要：阶段一到此", Usage: fakellm.Usage{PromptTokens: 999, CompletionTokens: 20, TotalTokens: 9999}},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	r := newP4Rig(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	reqs := srv.Requests()
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3（建议档注入 + 强制复位）", calls)
	}
	if second := string(reqs[1].Messages[len(reqs[1].Messages)-1]); !strings.Contains(second, "建议交接阈值") {
		t.Fatalf("第二次请求应为建议档提示: %s", second)
	}
	st := r.Status()
	if st.Hinted != 1 || st.Forced != 1 || st.Handoffs != 1 {
		t.Fatalf("计数异常: hinted=%d forced=%d handoffs=%d", st.Hinted, st.Forced, st.Handoffs)
	}
}

func TestForceResetOnceThenDiagnostic(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "还在做", Usage: fakellm.Usage{PromptTokens: 250000, CompletionTokens: 10, TotalTokens: 250010}},
		fakellm.Step{Content: "摘要：交接一次", Usage: fakellm.Usage{PromptTokens: 999, CompletionTokens: 20, TotalTokens: 9999}},
		fakellm.Step{Content: "还是超", Usage: fakellm.Usage{PromptTokens: 260000, CompletionTokens: 10, TotalTokens: 260010}},
	)
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.HandoffHintTokens = 100000
	cfg.Runner.HandoffForceTokens = 200000
	cfg.Runner.AutoContinue = true
	r, buf := newP4RigWithLog(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "长任务")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls := srv.Calls(); calls != 3 {
		t.Fatalf("请求数 %d，期望 3（回合 + 摘要 + 续跑一轮即停，不再来第二次复位）", calls)
	}
	st := r.Status()
	if st.Forced != 1 || st.Handoffs != 1 {
		t.Fatalf("计数异常: forced=%d handoffs=%d", st.Forced, st.Handoffs)
	}
	if !strings.Contains(buf.String(), "新会话基线即超阈值") {
		t.Fatalf("缺少诊断文案: %s", buf.String())
	}
}
