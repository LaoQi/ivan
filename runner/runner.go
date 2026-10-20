package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LaoQi/tanya/agent"

	"github.com/LaoQi/ivan/bus"
	chlog "github.com/LaoQi/ivan/channel/log"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
)

type Options struct {
	Cfg    *config.Config
	Log    *data.EventLog
	Human  *chlog.Sink
	State  *data.State
	Bus    *bus.Bus
	Stream io.Writer
}

type Runner struct {
	cfg  *config.Config
	opts Options

	poolMu sync.Mutex
	pool   map[string]*instance
	sem    chan struct{}

	startedAt time.Time
	turns     atomic.Uint64
	failures  atomic.Uint64
	handoffs  atomic.Uint64
	continued atomic.Uint64
	dropped   atomic.Uint64
	inFlight  atomic.Int64
	closed    atomic.Bool

	wg          sync.WaitGroup
	turnCtx     context.Context
	cancelTurns context.CancelFunc
}

type AgentStatus struct {
	Key         string  `json:"key"`
	Session     string  `json:"session,omitempty"`
	Workspace   string  `json:"workspace"`
	Turns       uint64  `json:"turns"`
	IdleSeconds float64 `json:"idle_seconds"`
	Running     bool    `json:"running"`
}

type Status struct {
	StartedAt       time.Time     `json:"started_at"`
	Turns           uint64        `json:"turns"`
	Failures        uint64        `json:"failures"`
	Handoffs        uint64        `json:"handoffs"`
	Continued       uint64        `json:"continued"`
	DroppedOutbound uint64        `json:"dropped_outbound"`
	InFlight        int64         `json:"in_flight"`
	Agents          int           `json:"agents"`
	MaxAgents       int           `json:"max_agents"`
	MaxConcurrent   int           `json:"max_concurrent"`
	Sessions        int           `json:"sessions"`
	AgentList       []AgentStatus `json:"agent_list"`
}

func New(opts Options) (*Runner, error) {
	if opts.Cfg == nil {
		return nil, errors.New("缺少配置")
	}
	if opts.State == nil {
		st, err := data.LoadState(opts.Cfg.StatePath())
		if err != nil {
			return nil, err
		}
		opts.State = st
	}
	r := &Runner{
		cfg:       opts.Cfg,
		opts:      opts,
		pool:      map[string]*instance{},
		sem:       make(chan struct{}, opts.Cfg.Daemon.MaxConcurrent),
		startedAt: time.Now(),
	}
	r.turnCtx, r.cancelTurns = context.WithCancel(context.Background())
	if idle := opts.Cfg.IdleTimeout(); idle > 0 {
		go r.reapLoop(idle)
	}
	return r, nil
}

func (r *Runner) Dispatch(ctx context.Context, msg bus.InboundMessage) error {
	if r.closed.Load() {
		return errors.New("runner 已关闭")
	}
	if strings.TrimSpace(msg.Content) == "" && len(msg.Media) == 0 {
		return errors.New("入站消息内容为空")
	}
	key := bus.SessionKey(msg)
	inst := r.acquire(key)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.lastUsed = time.Now()
	inst.turns++
	r.inFlight.Add(1)
	defer r.inFlight.Add(-1)

	t := &task{r: r, inst: inst, msg: msg, key: key}
	err := t.run(ctx)

	out := bus.OutboundMessage{
		Channel: msg.Channel,
		ChatID:  msg.ChatID,
		ReplyTo: msg.MessageID,
		Content: t.text(),
		Kind:    bus.KindReply,
	}
	if err != nil {
		out.Kind = bus.KindError
		if strings.TrimSpace(out.Content) == "" {
			out.Content = "error: " + briefErr(err)
		}
	}
	if r.opts.Bus != nil && !r.opts.Bus.Post(out) {
		r.dropped.Add(1)
	}
	return err
}

func (r *Runner) Run(ctx context.Context) error {
	if r.opts.Bus == nil {
		return errors.New("未接总线，无法运行消费循环")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-r.opts.Bus.Inbound():
			if !ok {
				return nil
			}
			r.wg.Add(1)
			go func(m bus.InboundMessage) {
				defer r.wg.Done()
				select {
				case r.sem <- struct{}{}:
				case <-r.turnCtx.Done():
					return
				}
				defer func() { <-r.sem }()
				if err := r.Dispatch(r.turnCtx, m); err != nil {
					r.human(chlog.Event{Kind: "error", Session: bus.SessionKey(m), Channel: m.Channel, Err: briefErr(err), Failed: true})
				}
			}(msg)
		}
	}
}

func (r *Runner) Wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	if timeout <= 0 {
		<-done
		return true
	}
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (r *Runner) CancelTurns() { r.cancelTurns() }

func (r *Runner) Close() error {
	r.closed.Store(true)
	r.cancelTurns()
	r.Wait(0)
	return nil
}

func (r *Runner) Session(key string) (string, bool) {
	r.poolMu.Lock()
	defer r.poolMu.Unlock()
	inst, ok := r.pool[key]
	if !ok || inst.ag == nil {
		return "", false
	}
	return inst.ag.SessionID(), true
}

func (r *Runner) Agent(key string) (*agent.Agent, bool) {
	r.poolMu.Lock()
	defer r.poolMu.Unlock()
	inst, ok := r.pool[key]
	if !ok || inst.ag == nil {
		return nil, false
	}
	return inst.ag, true
}

func (r *Runner) Status() Status {
	st := Status{
		StartedAt:       r.startedAt,
		Turns:           r.turns.Load(),
		Failures:        r.failures.Load(),
		Handoffs:        r.handoffs.Load(),
		Continued:       r.continued.Load(),
		DroppedOutbound: r.dropped.Load(),
		InFlight:        r.inFlight.Load(),
		MaxAgents:       r.cfg.Runner.MaxAgents,
		MaxConcurrent:   r.cfg.Daemon.MaxConcurrent,
		Sessions:        r.opts.State.Len(),
	}
	r.poolMu.Lock()
	defer r.poolMu.Unlock()
	st.Agents = len(r.pool)
	st.AgentList = make([]AgentStatus, 0, len(r.pool))
	for key, inst := range r.pool {
		as := AgentStatus{
			Key:         key,
			Workspace:   inst.ws,
			Turns:       inst.turns,
			IdleSeconds: time.Since(inst.lastUsed).Seconds(),
			Running:     !inst.mu.TryLock(),
		}
		if !as.Running {
			inst.mu.Unlock()
		}
		if inst.ag != nil {
			as.Session = inst.ag.SessionID()
		}
		st.AgentList = append(st.AgentList, as)
	}
	return st
}

func (r *Runner) logEvent(e agent.Event) {
	if r.opts.Log == nil {
		return
	}
	r.opts.Log.Emit(e)
}

func (r *Runner) human(e chlog.Event) {
	if r.opts.Human == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	_ = r.opts.Human.Emit(e)
}

func (r *Runner) notice(format string, args ...any) {
	if r.opts.Human == nil {
		return
	}
	_ = r.opts.Human.Notice(format, args...)
}

type task struct {
	r    *Runner
	inst *instance
	msg  bus.InboundMessage
	key  string

	mu      sync.Mutex
	content strings.Builder
	tokens  int
	context int
}

func promptOf(msg bus.InboundMessage) string {
	text := strings.TrimSpace(msg.Content)
	if len(msg.Media) == 0 {
		return text
	}
	var b strings.Builder
	if text != "" {
		b.WriteString(text)
		b.WriteString("\n\n")
	}
	fmt.Fprintf(&b, "用户发送了 %d 个附件（本地已缓存，可用 read_image 查看）：\n", len(msg.Media))
	for i, m := range msg.Media {
		label := m.Name
		if label == "" {
			label = fmt.Sprintf("附件%d", i+1)
		}
		fmt.Fprintf(&b, "- %s（%s，%d 字节）：%s\n", label, m.MIME, m.Bytes, m.Path)
	}
	return strings.TrimSpace(b.String())
}

func (t *task) run(ctx context.Context) error {
	defer func() {
		if ag := t.inst.ag; ag != nil {
			t.r.saveMapping(t.key, ag.SessionID(), t.inst.ws)
		}
	}()
	for turn := 0; ; turn++ {
		ag, err := t.r.agentFor(t.inst)
		if err != nil {
			t.r.failures.Add(1)
			return err
		}
		sink := agent.Sinks(t.sink, t.r.logEvent)
		t.r.human(chlog.Event{Kind: "turn_start", Session: t.key, Channel: t.msg.Channel, Text: turnLabel(turn)})
		if turn == 0 {
			err = ag.Ask(ctx, promptOf(t.msg), sink)
		} else {
			err = ag.Continue(ctx, sink)
		}
		t.r.turns.Add(1)
		if err != nil {
			t.r.failures.Add(1)
			return err
		}
		h, ok := ag.TakeHandoff()
		if !ok {
			return nil
		}
		t.r.handoffs.Add(1)
		t.r.saveMapping(t.key, ag.SessionID(), t.inst.ws)
		t.r.human(chlog.Event{Kind: "handoff", Session: t.key, Channel: t.msg.Channel, Handoff: true, Text: h.OldID + " → " + h.NewID})
		if !t.r.cfg.Runner.AutoContinue || !h.Continue {
			return nil
		}
		if max := t.r.cfg.Runner.MaxTurnsPerTask; max > 0 && turn+1 >= max {
			t.r.human(chlog.Event{Kind: "budget", Session: t.key, Channel: t.msg.Channel, Text: fmt.Sprintf("续跑轮次已达上限 %d，停下等下次消息", max)})
			return nil
		}
		if max := t.r.cfg.Runner.MaxTokensPerTask; max > 0 && t.usedTokens() >= max {
			t.r.human(chlog.Event{Kind: "budget", Session: t.key, Channel: t.msg.Channel, Text: fmt.Sprintf("token 预算已达上限 %d，停下等下次消息", max)})
			return nil
		}
		t.r.continued.Add(1)
	}
}

func (t *task) sink(e agent.Event) {
	switch e.Kind {
	case agent.EventContent:
		t.mu.Lock()
		t.content.WriteString(e.Text)
		t.mu.Unlock()
		if w := t.r.opts.Stream; w != nil {
			fmt.Fprint(w, e.Text)
		}
	case agent.EventResponse:
		t.mu.Lock()
		t.context = e.Response.ContextTokens
		if e.Response.Usage != nil {
			t.tokens += e.Response.Usage.TotalTokens
		}
		t.mu.Unlock()
	case agent.EventToolStart:
		t.renderToolStart(e)
		t.r.human(chlog.Event{Kind: "tool_start", Session: t.key, Channel: t.msg.Channel, Tool: e.ToolName, Text: e.ToolArgs})
	case agent.EventToolEnd:
		t.renderToolEnd(e)
		t.r.human(chlog.Event{Kind: "tool_end", Session: t.key, Channel: t.msg.Channel, Tool: e.ToolName, Text: e.Result.Text})
	case agent.EventTurnEnd:
		t.renderTurnEnd(e)
		t.r.human(chlog.Event{Kind: "turn_end", Session: t.key, Channel: t.msg.Channel, Dur: e.Turn.Duration, Context: t.contextTokens(), Total: t.usedTokens(), Failed: e.Turn.Failed})
	}
}

func (t *task) renderToolStart(e agent.Event) {
	w := t.r.opts.Stream
	if w == nil {
		return
	}
	fmt.Fprintf(w, "\n▸ %s %s\n", e.ToolName, oneLine(e.ToolArgs, 200))
}

func (t *task) renderToolEnd(e agent.Event) {
	w := t.r.opts.Stream
	if w == nil {
		return
	}
	if text := strings.TrimSpace(e.Result.Text); text != "" {
		fmt.Fprintf(w, "%s\n", oneLine(text, 400))
	}
}

func (t *task) renderTurnEnd(e agent.Event) {
	w := t.r.opts.Stream
	if w == nil {
		return
	}
	ag := t.inst.ag
	session := ""
	if ag != nil {
		session = sessionName(ag.Stats().Session)
	}
	if e.Turn.Failed {
		fmt.Fprintf(w, "error: 回合失败 %s\n", e.Turn.Duration.Round(1e6))
	}
	fmt.Fprintf(w, "[回合 %.1fs · 上下文 %d · 累计 %d tokens · 会话 %s]\n",
		e.Turn.Duration.Seconds(), t.contextTokens(), t.usedTokens(), session)
}

func (t *task) text() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.content.String()
}

func (t *task) contextTokens() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.context
}

func (t *task) usedTokens() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tokens
}

func turnLabel(turn int) string {
	if turn == 0 {
		return ""
	}
	return fmt.Sprintf("续跑第 %d 轮", turn)
}

func briefErr(err error) string {
	if err == nil {
		return ""
	}
	s := strings.Join(strings.Fields(err.Error()), " ")
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}

func sessionName(path string) string {
	if path == "" {
		return "(未落盘)"
	}
	base := path
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	return strings.TrimSuffix(base, ".jsonl")
}

func oneLine(s string, limit int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > limit {
		s = s[:limit] + "…"
	}
	return s
}
