package runner

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	chlog "github.com/LaoQi/ivan/channel/log"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func testConfig(t *testing.T, srv *fakellm.Server) *config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.BaseURL = srv.BaseURL()
	cfg.APIKey = "test-key"
	cfg.Model = "mock-model"
	cfg.ApiProtocol = "chat"
	cfg.UserAgent = "ivan/test"
	cfg.ConfigPath = filepath.Join(dir, "config.yaml")
	cfg.SessionMode = "global"
	return cfg
}

type rig struct {
	cfg   *config.Config
	bus   *bus.Bus
	human *bytes.Buffer
	log   *data.EventLog
	r     *Runner
}

func newRig(t *testing.T, srv *fakellm.Server) *rig {
	t.Helper()
	cfg := testConfig(t, srv)
	log, err := data.NewEventLog(cfg.EventsDir(), 64)
	if err != nil {
		t.Fatalf("NewEventLog: %v", err)
	}
	raw := &bytes.Buffer{}
	b := bus.New(8, 8)
	r, err := New(Options{Cfg: cfg, Log: log, Human: chlog.New(raw), Bus: b})
	if err != nil {
		t.Fatalf("runner.New: %v", err)
	}
	t.Cleanup(func() {
		r.Close()
		log.Close()
	})
	return &rig{cfg: cfg, bus: b, human: raw, log: log, r: r}
}

func inbound(session, chatID, text string) bus.InboundMessage {
	return bus.InboundMessage{
		Channel:    bus.ChannelCLI,
		ChatID:     chatID,
		ChatType:   bus.ChatDirect,
		MessageID:  chatID,
		SenderID:   "local",
		Content:    text,
		SessionKey: session,
		ReceivedAt: time.Now(),
	}
}

func (rg *rig) mustSession(t *testing.T, key string) string {
	t.Helper()
	session, ok := rg.r.Session(key)
	if !ok {
		t.Fatalf("会话 %s 不在池中", key)
	}
	return session
}

func (rg *rig) dispatch(t *testing.T, msg bus.InboundMessage) string {
	t.Helper()
	if err := rg.r.Dispatch(context.Background(), msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	select {
	case out := <-rg.bus.Outbound():
		if out.ChatID != msg.ChatID {
			t.Fatalf("出站 ChatID=%s，期望 %s", out.ChatID, msg.ChatID)
		}
		if out.ReplyTo != msg.MessageID {
			t.Fatalf("出站 ReplyTo=%s，期望 %s", out.ReplyTo, msg.MessageID)
		}
		return out.Content
	case <-time.After(5 * time.Second):
		t.Fatal("没有出站消息")
		return ""
	}
}

func TestDispatchPublishesOutboundAndRecordsMapping(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "你好，我是 ivan"})
	defer srv.Close()
	rg := newRig(t, srv)
	if got := rg.dispatch(t, inbound("cli:default", "r1", "在吗")); !strings.Contains(got, "你好") {
		t.Fatalf("出站内容异常: %q", got)
	}
	state, err := data.LoadState(rg.cfg.StatePath())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	st, ok := state.Get("cli:default")
	if !ok || st.SessionID == "" {
		t.Fatalf("未记录会话映射: %+v", st)
	}
	wantWS := rg.cfg.AgentDir("cli-default")
	if st.Workspace != wantWS {
		t.Fatalf("工作区 %s，期望 %s", st.Workspace, wantWS)
	}
	if _, ok := state.Get("cli:other"); ok {
		t.Fatal("不应记录未使用的会话键")
	}
	if !strings.Contains(rg.human.String(), "turn_end") || !strings.Contains(rg.human.String(), "session=cli:default") {
		t.Fatalf("人类可读日志缺回合记录: %s", rg.human.String())
	}
	stt := rg.r.Status()
	if stt.Turns != 1 || stt.Failures != 0 || stt.Agents != 1 || stt.InFlight != 0 {
		t.Fatalf("状态异常: %+v", stt)
	}
}

func TestDispatchKeepsHistoryPerKey(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "第一次回复"},
		fakellm.Step{Content: "第二次回复"},
		fakellm.Step{Content: "另一个键回复"},
	)
	defer srv.Close()
	rg := newRig(t, srv)
	rg.dispatch(t, inbound("cli:default", "r1", "第一问"))
	first, _ := rg.r.Session("cli:default")
	rg.dispatch(t, inbound("cli:default", "r2", "第二问"))
	second, _ := rg.r.Session("cli:default")
	if first == "" || first != second {
		t.Fatalf("同键应复用会话: %q → %q", first, second)
	}
	ag, ok := rg.r.Agent("cli:default")
	if !ok {
		t.Fatal("实例不在池中")
	}
	if n := len(ag.History()); n < 4 {
		t.Fatalf("同键历史未累积: %d 条", n)
	}
	rg.dispatch(t, inbound("cli:other", "r3", "换个人问"))
	otherAg, ok := rg.r.Agent("cli:other")
	if !ok {
		t.Fatal("新键实例不在池中")
	}
	if otherAg.SessionFile() == ag.SessionFile() {
		t.Fatalf("不同键撞同一会话文件: %s", otherAg.SessionFile())
	}
	if want := rg.cfg.AgentDir("cli-other"); otherAg.Workspace() != want {
		t.Fatalf("新键工作区 %s，期望 %s", otherAg.Workspace(), want)
	}
	state, _ := data.LoadState(rg.cfg.StatePath())
	if state.Len() != 2 {
		t.Fatalf("映射数 %d，期望 2", state.Len())
	}
}

func TestResumeAfterRestart(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "第一段"}, fakellm.Step{Content: "第二段"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	run := func(text string) (string, int) {
		log, err := data.NewEventLog(cfg.EventsDir(), 32)
		if err != nil {
			t.Fatalf("NewEventLog: %v", err)
		}
		defer log.Close()
		b := bus.New(4, 4)
		r, err := New(Options{Cfg: cfg, Log: log, Human: chlog.New(&bytes.Buffer{}), Bus: b})
		if err != nil {
			t.Fatalf("runner.New: %v", err)
		}
		defer r.Close()
		if err := r.Dispatch(context.Background(), inbound("cli:default", "r", text)); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		out := <-b.Outbound()
		ag, _ := r.Agent("cli:default")
		return out.Content, len(ag.History())
	}
	first, n1 := run("第一问")
	second, n2 := run("第二问")
	if first != "第一段" || second != "第二段" {
		t.Fatalf("回复异常: %q %q", first, second)
	}
	if n2 <= n1 {
		t.Fatalf("重启后历史未恢复: %d → %d", n1, n2)
	}
	reqs := srv.Requests()
	if len(reqs) != 2 {
		t.Fatalf("请求数 %d，期望 2", len(reqs))
	}
	if !bytes.Contains(reqs[1].Messages[len(reqs[1].Messages)-1], []byte("第二问")) {
		t.Fatal("第二轮请求未带上新问题")
	}
	joined := ""
	for _, m := range reqs[1].Messages {
		joined += string(m)
	}
	if !strings.Contains(joined, "第一段") {
		t.Fatalf("重启后未带上历史: %s", joined)
	}
}

func TestErrorTurnPublishesErrorKind(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Status: 500})
	defer srv.Close()
	rg := newRig(t, srv)
	err := rg.r.Dispatch(context.Background(), inbound("cli:default", "r1", "会失败"))
	if err == nil {
		t.Fatal("失败回合应返回错误")
	}
	out := <-rg.bus.Outbound()
	if out.Kind != bus.KindError {
		t.Fatalf("出站类型 %q，期望 error", out.Kind)
	}
	if !strings.Contains(out.Content, "error") {
		t.Fatalf("错误文本异常: %q", out.Content)
	}
	if rg.r.Status().Failures != 1 {
		t.Fatalf("失败计数异常: %+v", rg.r.Status())
	}
}

func TestHandoffUpdatesMapping(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "next_session", Args: `{"summary":"阶段一完成","continue":false}`}}},
		fakellm.Step{Content: "本轮收尾"},
	)
	defer srv.Close()
	rg := newRig(t, srv)
	rg.dispatch(t, inbound("cli:default", "r1", "长任务"))
	state, _ := data.LoadState(rg.cfg.StatePath())
	st, _ := state.Get("cli:default")
	if st.SessionID == "" || st.SessionID != rg.mustSession(t, "cli:default") {
		t.Fatalf("交接后映射未更新: %+v", st)
	}
	if rg.r.Status().Handoffs != 1 || rg.r.Status().Continued != 0 {
		t.Fatalf("交接计数异常: %+v", rg.r.Status())
	}
	if !strings.Contains(rg.human.String(), "handoff") {
		t.Fatalf("日志缺 handoff: %s", rg.human.String())
	}
}

func TestAutoContinueRespectsTurnLimit(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "next_session", Args: `{"summary":"继续","continue":true}`}}},
		fakellm.Step{Content: "第一轮收尾"},
		fakellm.Step{Content: "第二轮"},
		fakellm.Step{Content: "第三轮"},
	)
	defer srv.Close()
	rg := newRig(t, srv)
	rg.cfg.Runner.AutoContinue = true
	rg.cfg.Runner.MaxTurnsPerTask = 2
	got := rg.dispatch(t, inbound("cli:default", "r1", "长任务"))
	if !strings.Contains(got, "第一轮收尾") || !strings.Contains(got, "第二轮") {
		t.Fatalf("续跑文本未聚合: %q", got)
	}
	st := rg.r.Status()
	if st.Continued != 1 || st.Turns != 2 {
		t.Fatalf("续跑轮次未受限: %+v", st)
	}
}

func TestEmptyContentRejected(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "不应到达"})
	defer srv.Close()
	rg := newRig(t, srv)
	if err := rg.r.Dispatch(context.Background(), inbound("cli:default", "r1", "   ")); err == nil {
		t.Fatal("空内容应被拒绝")
	}
	if srv.Calls() != 0 {
		t.Fatal("空内容不应触发请求")
	}
}

func TestPoolEvictionSkipsRunningInstance(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok"})
	defer srv.Close()
	rg := newRig(t, srv)
	a := rg.r.acquire("cli:a")
	b := rg.r.acquire("cli:b")
	a.lastUsed = time.Now().Add(-2 * time.Hour)
	b.lastUsed = time.Now()

	a.mu.Lock()
	if n := rg.r.reap(time.Minute); n != 0 {
		a.mu.Unlock()
		t.Fatalf("在跑的实例不应被回收: %d", n)
	}
	a.mu.Unlock()
	if n := rg.r.reap(time.Minute); n != 1 {
		t.Fatalf("闲置实例应被回收: %d", n)
	}
	if len(rg.r.pool) != 1 {
		t.Fatalf("池大小 %d，期望 1", len(rg.r.pool))
	}
}

func TestMaxAgentsEvictsIdle(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok"})
	defer srv.Close()
	rg := newRig(t, srv)
	rg.cfg.Runner.MaxAgents = 2
	rg.r.acquire("cli:a").lastUsed = time.Now().Add(-time.Hour)
	rg.r.acquire("cli:b").lastUsed = time.Now().Add(-time.Minute)
	rg.r.acquire("cli:c")
	if len(rg.r.pool) > 2 {
		t.Fatalf("池超上限: %d", len(rg.r.pool))
	}
	if _, ok := rg.r.pool["cli:a"]; ok {
		t.Fatal("应淘汰最久未用的实例")
	}
}

func TestRunConsumesBusAndStops(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "总线上跑通"})
	defer srv.Close()
	rg := newRig(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = rg.r.Run(ctx)
		close(done)
	}()
	if !rg.bus.Publish(inbound("cli:default", "r1", "你好")) {
		t.Fatal("投递失败")
	}
	select {
	case out := <-rg.bus.Outbound():
		if !strings.Contains(out.Content, "总线上跑通") {
			t.Fatalf("出站内容异常: %q", out.Content)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("回合未完成")
	}
	rg.bus.CloseInbound()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("入站流关闭后 Run 未返回")
	}
}

func TestDispatchAfterClose(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "x"})
	defer srv.Close()
	rg := newRig(t, srv)
	if err := rg.r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err := rg.r.Dispatch(context.Background(), inbound("cli:default", "r1", "hi"))
	if err == nil || !strings.Contains(err.Error(), "已关闭") {
		t.Fatalf("关闭后应拒绝: %v", err)
	}
}
