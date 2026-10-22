package probe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/battlelab/agent"
	"github.com/LaoQi/battlelab/tools"

	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "ivan-probe-home-*")
	if err != nil {
		panic(err)
	}
	os.Setenv("HOME", home)
	ws, err := os.MkdirTemp("", "ivan-probe-ws-*")
	if err != nil {
		panic(err)
	}
	if err := os.Chdir(ws); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(home)
	os.RemoveAll(ws)
	os.Exit(code)
}

type collector struct {
	mu        sync.Mutex
	events    []agent.Event
	text      strings.Builder
	toolEnds  []string
	responses []agent.ResponseInfo
	turns     []agent.TurnInfo
}

func (c *collector) sink(e agent.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	switch e.Kind {
	case agent.EventContent:
		c.text.WriteString(e.Text)
	case agent.EventToolEnd:
		c.toolEnds = append(c.toolEnds, e.Result.Text)
	case agent.EventResponse:
		c.responses = append(c.responses, e.Response)
	case agent.EventTurnEnd:
		c.turns = append(c.turns, e.Turn)
	}
}

func (c *collector) Sink() agent.EventSink { return c.sink }

func (c *collector) Text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.text.String()
}

func (c *collector) ToolEnds() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.toolEnds))
	copy(out, c.toolEnds)
	return out
}

func (c *collector) Responses() []agent.ResponseInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]agent.ResponseInfo, len(c.responses))
	copy(out, c.responses)
	return out
}

func (c *collector) Kinds() []agent.EventKind {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]agent.EventKind, 0, len(c.events))
	for _, e := range c.events {
		out = append(out, e.Kind)
	}
	return out
}

func newCfg(t *testing.T, srv *fakellm.Server) *agent.Config {
	t.Helper()
	dir := t.TempDir()
	return &agent.Config{
		BaseURL:          srv.BaseURL(),
		APIKey:           "probe",
		Model:            "mock-model",
		Temperature:      0.7,
		ApiProtocol:      "chat",
		SessionMode:      "global",
		UserAgent:        "ivan/probe",
		DataDir:          dir,
		ConfigPath:       filepath.Join(dir, "config.yaml"),
		ArchiveThreshold: 64,
		ArchiveKeep:      16,
	}
}

func newAgentCfg(t *testing.T, cfg *agent.Config, extra ...agent.Tool) *agent.Agent {
	t.Helper()
	var ag *agent.Agent
	list, err := tools.Standard(tools.Options{
		Home: os.Getenv("HOME"),
		Workspace: func() string {
			if ag != nil {
				if ws := ag.Workspace(); ws != "" {
					return ws
				}
			}
			wd, _ := os.Getwd()
			return wd
		},
		ImageMaxBytes: agent.DefaultImageMaxBytes,
		ImageResize:   true,
		ImageDetail:   "low",
	})
	if err != nil {
		t.Fatalf("tools.Standard: %v", err)
	}
	list = append(list, extra...)
	ag, err = agent.New(cfg, agent.WithSystemPrompt("probe system"), agent.WithTools(list...))
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	return ag
}

func switchTo(t *testing.T, ag *agent.Agent, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := ag.SwitchWorkspace(dir); err != nil {
		t.Fatalf("SwitchWorkspace: %v", err)
	}
}

func newAgent(t *testing.T, srv *fakellm.Server, extra ...agent.Tool) *agent.Agent {
	t.Helper()
	return newAgentCfg(t, newCfg(t, srv), extra...)
}

func TestR1ShellWithoutTTY(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{
			Tools: []fakellm.ToolCall{{ID: "c1", Name: "run_shell", Args: `{"command":"echo hello-ivan"}`}},
			Usage: fakellm.Usage{PromptTokens: 11, CompletionTokens: 5, TotalTokens: 16},
		},
		fakellm.Step{Content: "已执行", Usage: fakellm.Usage{PromptTokens: 21, CompletionTokens: 4, TotalTokens: 25}},
	)
	defer srv.Close()
	ag := newAgent(t, srv)
	c := &collector{}
	if err := ag.Ask(context.Background(), "执行 echo", c.Sink()); err != nil {
		t.Fatalf("Ask: %v", err)
	}
	ends := c.ToolEnds()
	if len(ends) == 0 {
		t.Fatal("没有 tool_end 事件")
	}
	if !strings.Contains(ends[0], "hello-ivan") {
		t.Fatalf("工具结果未包含命令输出: %q", ends[0])
	}
	if !strings.Contains(c.Text(), "已执行") {
		t.Fatalf("回合文本未生成: %q", c.Text())
	}
}

func TestR2InteractiveFailsFast(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "run_shell", Args: `{"command":"cat","interactive":true}`}}},
		fakellm.Step{Content: "结束"},
	)
	defer srv.Close()
	ag := newAgent(t, srv)
	c := &collector{}
	done := make(chan error, 1)
	go func() { done <- ag.Ask(context.Background(), "交互命令", c.Sink()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Ask: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("interactive 命令悬挂（未在无终端下快速失败）")
	}
	ends := c.ToolEnds()
	if len(ends) == 0 || !strings.Contains(ends[0], "interactive 不支持") {
		t.Fatalf("interactive 结果未报错: %q", ends)
	}
}

func TestR3HandoffAutoContinue(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "next_session", Args: `{"summary":"阶段一完成","continue":true}`}}},
		fakellm.Step{Content: "本轮收尾"},
		fakellm.Step{Content: "新会话继续干"},
	)
	defer srv.Close()
	ag := newAgent(t, srv)
	c := &collector{}
	if err := ag.Ask(context.Background(), "开始长任务", c.Sink()); err != nil {
		t.Fatalf("Ask: %v", err)
	}
	h, ok := ag.TakeHandoff()
	if !ok {
		t.Fatal("没有交接快照")
	}
	if !h.Continue {
		t.Fatal("continue:true 未生效")
	}
	if h.OldID == "" || h.NewID == "" || h.OldID == h.NewID {
		t.Fatalf("会话未切换: %+v", h)
	}
	if ag.SessionID() != h.NewID {
		t.Fatalf("SessionID=%s，期望 %s", ag.SessionID(), h.NewID)
	}
	c2 := &collector{}
	if err := ag.Continue(context.Background(), c2.Sink()); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if !strings.Contains(c2.Text(), "新会话继续干") {
		t.Fatalf("续跑未产生输出: %q", c2.Text())
	}
	if got := srv.Calls(); got != 3 {
		t.Fatalf("请求数 %d，期望 3（Ask 工具轮+收尾轮，Continue 一轮）", got)
	}
	history := ag.History()
	if len(history) < 2 || history[0].Role != "user" {
		t.Fatalf("新会话 history 形态异常: %+v", history)
	}
}

func TestR4SessionResume(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "第一轮回复"})
	defer srv.Close()
	cfg := newCfg(t, srv)
	ag1 := newAgentCfg(t, cfg)
	c := &collector{}
	if err := ag1.Ask(context.Background(), "你好", c.Sink()); err != nil {
		t.Fatalf("Ask: %v", err)
	}
	id, file := ag1.SessionID(), ag1.SessionFile()
	msgs := len(ag1.History())
	if id == "" || file == "" {
		t.Fatal("会话未落盘")
	}
	ag2 := newAgentCfg(t, cfg)
	if err := ag2.LoadSession(id); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if got := len(ag2.History()); got != msgs {
		t.Fatalf("恢复后消息数 %d，期望 %d", got, msgs)
	}
	if ag2.SessionFile() != file {
		t.Fatalf("恢复后文件 %s，期望 %s", ag2.SessionFile(), file)
	}
}

func TestR5ConcurrentAgents(t *testing.T) {
	srvA := fakellm.New(fakellm.Step{Content: "并发回复 A"})
	srvB := fakellm.New(fakellm.Step{Content: "并发回复 B"})
	defer srvA.Close()
	defer srvB.Close()
	cfg := newCfg(t, srvA)
	a1 := newAgentCfg(t, cfg)
	cfg2 := *cfg
	cfg2.BaseURL = srvB.BaseURL()
	a2 := newAgentCfg(t, &cfg2)
	switchTo(t, a1, filepath.Join(cfg.DataDir, "ws-a"))
	switchTo(t, a2, filepath.Join(cfg.DataDir, "ws-b"))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	cs := []*collector{{}, {}}
	ags := []*agent.Agent{a1, a2}
	for i := range ags {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ags[i].Ask(context.Background(), "并发提问", cs[i].Sink())
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("实例 %d Ask: %v", i, err)
		}
	}
	if a1.SessionFile() == a2.SessionFile() {
		t.Fatalf("两个实例落到同一会话文件: %s", a1.SessionFile())
	}
	wants := []string{"并发回复 A", "并发回复 B"}
	for i, c := range cs {
		if !strings.Contains(c.Text(), wants[i]) {
			t.Fatalf("实例 %d 输出异常: %q", i, c.Text())
		}
	}
}

func TestR6EventLogAndRestore(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Reasoning: "想一下", Content: "答案"},
	)
	defer srv.Close()
	cfg := newCfg(t, srv)
	ag := newAgentCfg(t, cfg)
	log, err := data.NewEventLog(filepath.Join(cfg.DataDir, "events"), 32)
	if err != nil {
		t.Fatalf("NewEventLog: %v", err)
	}
	c := &collector{}
	sink := agent.Sinks(c.Sink(), log.Emit)
	if err := ag.Ask(context.Background(), "提问", sink); err != nil {
		t.Fatalf("Ask: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if log.Dropped() != 0 {
		t.Fatalf("事件被丢弃 %d 条", log.Dropped())
	}
	if log.Written() == 0 {
		t.Fatal("事件未落盘")
	}
	matches, _ := filepath.Glob(filepath.Join(cfg.DataDir, "events", "*.jsonl"))
	if len(matches) == 0 {
		t.Fatal("找不到事件文件")
	}
	raw, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("读事件文件: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) < 3 {
		t.Fatalf("事件行数过少: %d", len(lines))
	}
	if !strings.Contains(string(raw), `"kind":"turn_end"`) {
		t.Fatalf("事件文件缺少 turn_end: %s", raw)
	}
	if !strings.Contains(c.Text(), "答案") {
		t.Fatalf("事件文本还原失败: %q", c.Text())
	}
}

func TestR7WorkspaceSwitch(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "run_shell", Args: `{"command":"pwd"}`}}},
		fakellm.Step{Content: "看到了"},
	)
	defer srv.Close()
	ag := newAgent(t, srv)
	target := t.TempDir()
	if err := ag.SwitchWorkspace(target); err != nil {
		t.Fatalf("SwitchWorkspace: %v", err)
	}
	if ag.Workspace() != target {
		t.Fatalf("工作区 %s，期望 %s", ag.Workspace(), target)
	}
	c := &collector{}
	if err := ag.Ask(context.Background(), "打印当前目录", c.Sink()); err != nil {
		t.Fatalf("Ask: %v", err)
	}
	ends := c.ToolEnds()
	if len(ends) == 0 || !strings.Contains(ends[0], filepath.Base(target)) {
		t.Fatalf("命令未在新工作区执行: %q", ends)
	}
}

func TestR8UsageAndContext(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "hi", Usage: fakellm.Usage{PromptTokens: 100, CompletionTokens: 10, TotalTokens: 110}},
	)
	defer srv.Close()
	ag := newAgent(t, srv)
	c := &collector{}
	if err := ag.Ask(context.Background(), "在吗", c.Sink()); err != nil {
		t.Fatalf("Ask: %v", err)
	}
	resps := c.Responses()
	if len(resps) == 0 || resps[0].ContextTokens != 100 {
		t.Fatalf("ContextTokens 异常: %+v", resps)
	}
	st := ag.Stats()
	if st.TotalTokens != 110 || st.Est == 0 {
		t.Fatalf("Stats 异常: %+v", st)
	}
	if len(c.turns) != 1 || c.turns[0].Failed {
		t.Fatalf("TurnInfo 异常: %+v", c.turns)
	}
}

func TestR9ListSessionsSharedWorkspace(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok"})
	defer srv.Close()
	cfg := newCfg(t, srv)
	a1 := newAgentCfg(t, cfg)
	a2 := newAgentCfg(t, cfg)
	switchTo(t, a1, filepath.Join(cfg.DataDir, "ws-a"))
	switchTo(t, a2, filepath.Join(cfg.DataDir, "ws-b"))
	for i, ag := range []*agent.Agent{a1, a2} {
		if err := ag.Ask(context.Background(), "提问", (&collector{}).Sink()); err != nil {
			t.Fatalf("实例 %d Ask: %v", i, err)
		}
	}
	for i, ag := range []*agent.Agent{a1, a2} {
		list, err := ag.ListSessions()
		if err != nil {
			t.Fatalf("ListSessions: %v", err)
		}
		if len(list) != 1 {
			t.Fatalf("实例 %d 会话数 %d，期望 1（工作区隔离）", i, len(list))
		}
	}
}

func TestR11SharedWorkspaceCollision(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok"})
	defer srv.Close()
	cfg := newCfg(t, srv)
	a1 := newAgentCfg(t, cfg)
	a2 := newAgentCfg(t, cfg)
	if err := a1.Ask(context.Background(), "提问", (&collector{}).Sink()); err != nil {
		t.Fatalf("a1 Ask: %v", err)
	}
	if err := a2.Ask(context.Background(), "提问", (&collector{}).Sink()); err != nil {
		t.Fatalf("a2 Ask: %v", err)
	}
	list, err := a1.ListSessions()
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if a1.SessionFile() == a2.SessionFile() {
		t.Logf("R11 观察：共享工作区下两实例同秒创建 → 撞同一会话文件 %s（tanya nextID 为秒级时间戳）", a1.SessionFile())
	}
	if len(list) != 1 {
		t.Fatalf("共享工作区会话数 %d，期望 1（碰撞后互相覆盖）", len(list))
	}
}

type turnTool struct{ state *string }

func (t turnTool) Name() string { return "turn_context" }

func (t turnTool) Definition() agent.ToolDef {
	return agent.NewToolDef("turn_context", "读取当前回合上下文（探针用）", `{"type":"object","properties":{}}`)
}

func (t turnTool) Invoke(ctx context.Context, argsJSON string) agent.ToolResult {
	return agent.ToolResult{Text: *t.state}
}

func TestR10PerTurnContextIsolation(t *testing.T) {
	srvA := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "turn_context", Args: `{}`}}},
		fakellm.Step{Content: "A 完成"},
	)
	srvB := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "turn_context", Args: `{}`}}},
		fakellm.Step{Content: "B 完成"},
	)
	defer srvA.Close()
	defer srvB.Close()
	cfg := newCfg(t, srvA)
	stateA, stateB := "session-A", "session-B"
	a1 := newAgentCfg(t, cfg, turnTool{state: &stateA})
	cfg2 := *cfg
	cfg2.BaseURL = srvB.BaseURL()
	a2 := newAgentCfg(t, &cfg2, turnTool{state: &stateB})

	cs := []*collector{{}, {}}
	ags := []*agent.Agent{a1, a2}
	var wg sync.WaitGroup
	for i := range ags {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = ags[i].Ask(context.Background(), "读上下文", cs[i].Sink())
		}(i)
	}
	wg.Wait()
	want := []string{"session-A", "session-B"}
	for i, c := range cs {
		ends := c.ToolEnds()
		if len(ends) == 0 || !strings.Contains(ends[0], want[i]) {
			t.Fatalf("实例 %d 读到错误上下文: %q", i, ends)
		}
	}
}
