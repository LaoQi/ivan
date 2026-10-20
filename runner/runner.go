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
	"github.com/LaoQi/ivan/schedule"
)

type Options struct {
	Cfg    *config.Config
	Log    *data.EventLog
	Human  *chlog.Sink
	State  *data.State
	Bus    *bus.Bus
	Stream io.Writer
	Notify func(ctx context.Context, msg bus.OutboundMessage) error
	Paused func() bool
}

type Turn struct {
	Session     string
	Tokens      int
	Context     int
	NotifyError string
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
	hinted    atomic.Uint64
	cancelled atomic.Uint64
	dropped   atomic.Uint64
	inFlight  atomic.Int64
	closed    atomic.Bool

	wg          sync.WaitGroup
	turnCtx     context.Context
	cancelTurns context.CancelFunc

	schedMu sync.RWMutex
	sched   *schedule.Scheduler
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
	Hinted          uint64        `json:"hinted"`
	TokensToday     int           `json:"tokens_today"`
	MaxTokensPerDay int           `json:"max_tokens_per_day"`
	Paused          bool          `json:"paused"`
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
	_, err := r.DispatchTurn(ctx, msg)
	return err
}

func (r *Runner) DispatchTurn(ctx context.Context, msg bus.InboundMessage) (Turn, error) {
	if r.closed.Load() {
		return Turn{}, errors.New("runner 已关闭")
	}
	if strings.TrimSpace(msg.Content) == "" && len(msg.Media) == 0 {
		return Turn{}, errors.New("入站消息内容为空")
	}
	key := bus.SessionKey(msg)
	inst := r.acquire(key)
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.lastUsed = time.Now()
	inst.turns++
	inst.setTarget(replyChannel(msg), msg.ChatID, key)
	r.inFlight.Add(1)
	defer r.inFlight.Add(-1)

	runCtx, cancel := context.WithCancel(ctx)
	inst.setCancel(cancel)
	defer func() {
		cancel()
		inst.setCancel(nil)
	}()

	t := &task{r: r, inst: inst, msg: msg, key: key}
	err := t.run(runCtx)
	if n := t.usedTokens(); n > 0 {
		r.chargeTokens(n)
	}

	turn := Turn{Tokens: t.usedTokens(), Context: t.contextTokens()}
	out := bus.OutboundMessage{
		Channel: replyChannel(msg),
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
	if ag := inst.ag; ag != nil {
		turn.Session = ag.SessionID()
	}
	if out.Channel != msg.Channel {
		turn.NotifyError = r.deliver(ctx, out)
	} else if r.opts.Bus != nil && !r.opts.Bus.Post(out) {
		r.dropped.Add(1)
		turn.NotifyError = "出站队列已满或已关闭"
	}
	return turn, err
}

func replyChannel(msg bus.InboundMessage) string {
	if ch := strings.TrimSpace(msg.ReplyChannel); ch != "" {
		return ch
	}
	return msg.Channel
}

func (r *Runner) deliver(ctx context.Context, msg bus.OutboundMessage) string {
	if r.opts.Notify == nil {
		return "未接出站投递钩子"
	}
	if err := r.opts.Notify(ctx, msg); err != nil {
		return briefErr(err)
	}
	return ""
}

func (r *Runner) CancelTurn(key string) bool {
	r.poolMu.Lock()
	inst := r.pool[key]
	r.poolMu.Unlock()
	if inst == nil {
		return false
	}
	cancel := inst.cancelFunc()
	if cancel == nil {
		return false
	}
	r.cancelled.Add(1)
	cancel()
	r.note("cancel", key)
	return true
}

func (r *Runner) paused() bool {
	return r.opts.Paused != nil && r.opts.Paused()
}

func (r *Runner) chargeTokens(n int) {
	if n <= 0 || r.opts.State == nil {
		return
	}
	if _, err := r.opts.State.AddTokens(n); err != nil {
		r.notice("写入当日 token 用量失败: %v", err)
	}
}

func (r *Runner) TokensToday() int {
	if r.opts.State == nil {
		return 0
	}
	return r.opts.State.TokensToday()
}

func (r *Runner) budgetExceeded(extra int) (bool, string) {
	max := r.cfg.Runner.MaxTokensPerDay
	if max <= 0 || r.opts.State == nil {
		return false, ""
	}
	used := r.opts.State.TokensToday() + extra
	if used >= max {
		return true, fmt.Sprintf("当日 token 预算 %d 已用尽（已用 %d）", max, used)
	}
	return false, ""
}

func (r *Runner) Budget() (bool, string) {
	if ok, reason := r.budgetExceeded(0); ok {
		return false, reason
	}
	return true, ""
}

func (r *Runner) SetScheduler(s *schedule.Scheduler) {
	r.schedMu.Lock()
	defer r.schedMu.Unlock()
	r.sched = s
}

func (r *Runner) Scheduler() *schedule.Scheduler {
	r.schedMu.RLock()
	defer r.schedMu.RUnlock()
	return r.sched
}

func (r *Runner) TriggerJob(ctx context.Context, t schedule.Trigger) schedule.Outcome {
	if r.closed.Load() {
		return schedule.Outcome{Err: errors.New("runner 已关闭")}
	}
	select {
	case r.sem <- struct{}{}:
	case <-r.turnCtx.Done():
		return schedule.Outcome{Err: errors.New("runner 正在停机")}
	case <-ctx.Done():
		return schedule.Outcome{Err: ctx.Err()}
	}
	defer func() { <-r.sem }()
	msg := bus.InboundMessage{
		Channel:      bus.ChannelSchedule,
		ReplyChannel: t.Channel,
		ChatID:       t.ChatID,
		ChatType:     bus.ChatDirect,
		SenderID:     "schedule",
		SenderName:   t.Name,
		Content:      t.Prompt,
		SessionKey:   t.SessionKey,
		ReceivedAt:   time.Now(),
		Raw:          map[string]string{"trigger": "schedule", "job_id": t.JobID},
	}
	turn, err := r.DispatchTurn(ctx, msg)
	return schedule.Outcome{Tokens: turn.Tokens, NotifyErr: turn.NotifyError, Err: err}
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
				if _, err := r.DispatchTurn(r.turnCtx, m); err != nil {
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
		Hinted:          r.hinted.Load(),
		TokensToday:     r.TokensToday(),
		MaxTokensPerDay: r.cfg.Runner.MaxTokensPerDay,
		Paused:          r.paused(),
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

func (r *Runner) note(kind, text string) {
	if r.opts.Log == nil {
		return
	}
	r.opts.Log.Note(kind, text)
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
	hinted := false
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
		if h, ok := ag.TakeHandoff(); ok {
			t.r.handoffs.Add(1)
			t.r.saveMapping(t.key, ag.SessionID(), t.inst.ws)
			t.r.human(chlog.Event{Kind: "handoff", Session: t.key, Channel: t.msg.Channel, Handoff: true, Text: h.OldID + " → " + h.NewID})
			t.r.note("handoff", t.key+" "+h.OldID+" → "+h.NewID)
			if !t.r.cfg.Runner.AutoContinue || !h.Continue {
				return nil
			}
			if t.r.paused() {
				t.r.human(chlog.Event{Kind: "paused", Session: t.key, Channel: t.msg.Channel, Tool: "next_session", Text: "暂停中，交接后不自动续跑"})
				t.r.note("paused", t.key+" 交接后不自动续跑")
				return nil
			}
			if t.stopForBudget(turn) {
				return nil
			}
			t.r.continued.Add(1)
			continue
		}
		if hinted || !t.pressure() {
			return nil
		}
		if t.r.paused() {
			t.r.human(chlog.Event{Kind: "paused", Session: t.key, Channel: t.msg.Channel, Tool: "next_session", Text: "暂停中，跳过上下文阈值交接"})
			t.r.note("paused", t.key+" 跳过上下文阈值交接")
			return nil
		}
		if t.stopForBudget(turn) {
			return nil
		}
		hinted = true
		if err := ag.AppendNotice(handoffHint); err != nil {
			t.r.notice("注入交接提示失败 key=%s: %v", t.key, err)
			return nil
		}
		t.r.hinted.Add(1)
		hint := fmt.Sprintf("上下文 %d tokens 已达阈值 %.0f%%，提示模型交接", t.contextTokens(), thresholdRatio(t.r.cfg)*100)
		t.r.human(chlog.Event{Kind: "handoff_hint", Session: t.key, Channel: t.msg.Channel, Tool: "next_session", Text: hint})
		t.r.note("handoff_hint", t.key+" "+hint)
	}
}

const handoffHint = "系统提示：本会话上下文已接近窗口上限，请立即调用 next_session 交接进度（summary 写清已完成内容、关键结论与下一步），然后结束本回合。"

func (t *task) stopForBudget(turn int) bool {
	if max := t.r.cfg.Runner.MaxTurnsPerTask; max > 0 && turn+1 >= max {
		t.blocked(fmt.Sprintf("续跑轮次已达上限 %d，停下等下次消息", max))
		return true
	}
	if max := t.r.cfg.Runner.MaxTokensPerTask; max > 0 && t.usedTokens() >= max {
		t.blocked(fmt.Sprintf("token 预算已达上限 %d，停下等下次消息", max))
		return true
	}
	if exceeded, reason := t.r.budgetExceeded(t.usedTokens()); exceeded {
		t.blocked(reason + "，停下等下次消息")
		return true
	}
	return false
}

func (t *task) blocked(text string) {
	t.r.human(chlog.Event{Kind: "budget", Session: t.key, Channel: t.msg.Channel, Text: text})
	t.r.note("budget", t.key+" "+text)
}

func thresholdRatio(cfg *config.Config) float64 {
	ratio := cfg.Runner.HandoffThreshold
	if ratio <= 0 || ratio > 1 {
		return 0.7
	}
	return ratio
}

func (t *task) pressure() bool {
	window := t.r.cfg.Runner.ContextWindowTokens
	if window <= 0 {
		return false
	}
	used := t.contextTokens()
	if used <= 0 {
		if ag := t.inst.ag; ag != nil {
			st := ag.Stats()
			if st.HasContext {
				used = st.ContextTokens
			} else {
				used = st.Est
			}
		}
	}
	if used <= 0 {
		return false
	}
	return float64(used) >= float64(window)*thresholdRatio(t.r.cfg)
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
