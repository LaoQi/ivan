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

	startedAt   time.Time
	turns       atomic.Uint64
	failures    atomic.Uint64
	handoffs    atomic.Uint64
	continued   atomic.Uint64
	hinted      atomic.Uint64
	forced      atomic.Uint64
	interrupted atomic.Uint64
	cancelled   atomic.Uint64
	dropped     atomic.Uint64
	inFlight    atomic.Int64
	closed      atomic.Bool

	wg          sync.WaitGroup
	turnCtx     context.Context
	cancelTurns context.CancelFunc

	schedMu sync.RWMutex
	sched   *schedule.Scheduler

	pendingMu sync.Mutex
	pending   map[string]string
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
	Forced          uint64        `json:"forced"`
	Interrupted     uint64        `json:"interrupted"`
	HandoffHint     int           `json:"handoff_hint_tokens"`
	HandoffForce    int           `json:"handoff_force_tokens"`
	TokensToday     int           `json:"tokens_today"`
	MaxTokensPerDay int           `json:"max_tokens_per_day"`
	MemoryMaxBytes  int           `json:"memory_max_bytes"`
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
	r.recoverInterrupted()
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
	r.beginTurn(key, msg)
	defer r.endTurn(key)
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
	if s := t.stream; s != nil {
		turn.NotifyError = briefErr(s.finish(out))
		return turn, err
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

func (r *Runner) beginTurn(key string, msg bus.InboundMessage) {
	if r.opts.State == nil {
		return
	}
	rt := data.RunningTurn{Channel: msg.Channel, ChatID: msg.ChatID}
	if st, ok := r.opts.State.Get(key); ok {
		rt.Session = st.SessionID
	}
	if err := r.opts.State.BeginTurn(key, rt); err != nil {
		r.notice("写入回合标记失败 key=%s: %v", key, err)
	}
}

func (r *Runner) endTurn(key string) {
	if r.opts.State == nil {
		return
	}
	if err := r.opts.State.EndTurn(key); err != nil {
		r.notice("清理回合标记失败 key=%s: %v", key, err)
	}
	r.trimMemory(key)
}

func (r *Runner) recoverInterrupted() {
	if r.opts.State == nil {
		return
	}
	left, err := r.opts.State.DrainRunning()
	if err != nil {
		r.notice("清理残留回合标记失败: %v", err)
		return
	}
	for key, rt := range left {
		r.interrupted.Add(1)
		when := rt.StartedAt.Local().Format("2006-01-02 15:04:05")
		text := fmt.Sprintf("回合执行中进程退出：会话 %s（来源 %s/%s，开始于 %s），该回合结果未知", key, rt.Channel, rt.ChatID, when)
		r.human(chlog.Event{Kind: "turn_interrupted", Session: key, Channel: rt.Channel, Text: text})
		r.note("turn_interrupted", text)
		r.setPending(key, fmt.Sprintf(interruptedNotice, rt.Channel, when))
	}
}

func (r *Runner) setPending(key, text string) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	if r.pending == nil {
		r.pending = map[string]string{}
	}
	r.pending[key] = text
}

func (r *Runner) takePending(key string) string {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	text := r.pending[key]
	delete(r.pending, key)
	return text
}

func (r *Runner) trimMemory(key string) {
	max := r.cfg.Runner.MemoryMaxBytes
	if max <= 0 {
		return
	}
	path := r.memoryPath(key)
	removed, err := data.TrimFileTail(path, max)
	if err != nil {
		r.notice("裁剪记忆文件失败 %s: %v", path, err)
		return
	}
	if removed <= 0 {
		return
	}
	text := fmt.Sprintf("记忆文件超过 %d 字节，已裁剪 %d 字节（保留最近内容）", max, removed)
	r.human(chlog.Event{Kind: "memory_trim", Session: key, Text: text})
	r.note("memory_trim", key+" "+text)
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
	prompt := t.Prompt
	if file := strings.TrimSpace(t.File); file != "" {
		content, path, err := r.ReadTaskFile(file)
		switch {
		case err != nil:
			r.notice("任务提示词文件不可读，改用作业内 prompt job=%s file=%s: %v", t.JobID, path, err)
			r.note("job_task_file", t.JobID+" 任务文件不可读（回退作业内 prompt）: "+briefErr(err))
		case content == "":
			r.notice("任务提示词文件为空，改用作业内 prompt job=%s file=%s", t.JobID, path)
			r.note("job_task_file", t.JobID+" 任务文件为空（回退作业内 prompt）")
		default:
			prompt = content
		}
	}
	msg := bus.InboundMessage{
		Channel:      bus.ChannelSchedule,
		ReplyChannel: t.Channel,
		ChatID:       t.ChatID,
		ChatType:     bus.ChatDirect,
		SenderID:     "schedule",
		SenderName:   t.Name,
		Content:      prompt,
		SessionKey:   t.SessionKey,
		ReceivedAt:   time.Now(),
		Raw:          map[string]string{"trigger": "schedule", "job_id": t.JobID, "task_file": t.File},
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
		Forced:          r.forced.Load(),
		Interrupted:     r.interrupted.Load(),
		HandoffHint:     r.cfg.Runner.HandoffHintTokens,
		HandoffForce:    r.cfg.Runner.HandoffForceTokens,
		TokensToday:     r.TokensToday(),
		MaxTokensPerDay: r.cfg.Runner.MaxTokensPerDay,
		MemoryMaxBytes:  r.cfg.Runner.MemoryMaxBytes,
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
	tool    string
	stream  *streamState
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
	t.streamStart()
	hinted, forced, hops := false, false, 0
	for turn := 0; ; turn++ {
		ag, err := t.r.agentFor(t.inst)
		if err != nil {
			t.r.failures.Add(1)
			return err
		}
		sink := agent.Sinks(t.sink, t.r.logEvent)
		t.r.human(chlog.Event{Kind: "turn_start", Session: t.key, Channel: t.msg.Channel, Text: turnLabel(turn)})
		if turn == 0 {
			if text := t.r.takePending(t.key); text != "" {
				if err := ag.AppendNotice(text); err != nil {
					t.r.notice("注入中断提示失败 key=%s: %v", t.key, err)
				}
			}
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
			hops++
			hinted, forced = false, false
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
		level := t.pressure()
		if level == pressureNone {
			return nil
		}
		if level == pressureHint && hinted {
			return nil
		}
		if level == pressureForce && forced {
			t.forcedStop(hops)
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
		notice := ""
		kind := ""
		switch level {
		case pressureHint:
			hinted = true
			t.r.hinted.Add(1)
			notice = fmt.Sprintf(handoffHintNotice, t.contextTokens(), t.r.cfg.Runner.HandoffHintTokens)
			kind = "handoff_hint"
		case pressureForce:
			forced = true
			t.r.forced.Add(1)
			notice = fmt.Sprintf(handoffForceNotice, t.contextTokens(), t.r.cfg.Runner.HandoffForceTokens)
			kind = "handoff_force"
		}
		if err := ag.AppendNotice(notice); err != nil {
			t.r.notice("注入交接提示失败 key=%s: %v", t.key, err)
			return nil
		}
		text := fmt.Sprintf("上下文 %d tokens 达阈值 %d，已%s交接", t.contextTokens(), t.levelTokens(level), map[bool]string{true: "强制", false: "建议"}[level == pressureForce])
		t.r.human(chlog.Event{Kind: kind, Session: t.key, Channel: t.msg.Channel, Tool: "next_session", Text: text})
		t.r.note(kind, t.key+" "+text)
	}
}

const interruptedNotice = "系统提示：上一次回合在执行中因进程退出被中断（来源 %s，开始于 %s），其结果未知。继续之前先核对工作区与实际状态的进展，不要假定上一步已完成。"

const handoffHintNotice = "系统提示：本会话上下文已到 %d tokens（建议交接阈值 %d）。建议在本阶段收尾后调用 next_session 交接进度（summary 写清已完成内容、关键结论与下一步）；若目标马上就能完成，可先把当前步骤做完再交接。"

const handoffForceNotice = "系统提示：本会话上下文已到 %d tokens，达到强制交接阈值 %d。必须立即调用 next_session 完成交接，本回合不要再做其他工作，也不要继续读文件或跑命令。"

type handoffPressure int

const (
	pressureNone handoffPressure = iota
	pressureHint
	pressureForce
)

func (t *task) levelTokens(level handoffPressure) int {
	if level == pressureForce {
		return t.r.cfg.Runner.HandoffForceTokens
	}
	return t.r.cfg.Runner.HandoffHintTokens
}

func (t *task) forcedStop(hops int) {
	text := fmt.Sprintf("上下文 %d tokens 达强制交接阈值 %d，模型仍未交接，本任务已停止（下次消息继续）", t.contextTokens(), t.r.cfg.Runner.HandoffForceTokens)
	if hops > 0 {
		text = fmt.Sprintf("上下文 %d tokens 仍达强制交接阈值 %d（本任务已交接 %d 次，新会话基线即超阈值），已停止；请调高 handoff_*_tokens 或拆分任务",
			t.contextTokens(), t.r.cfg.Runner.HandoffForceTokens, hops)
	}
	t.r.human(chlog.Event{Kind: "handoff_forced", Session: t.key, Channel: t.msg.Channel, Tool: "next_session", Text: text})
	t.r.note("handoff_forced", t.key+" "+text)
}

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

func (t *task) pressure() handoffPressure {
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
		return pressureNone
	}
	if force := t.r.cfg.Runner.HandoffForceTokens; force > 0 && used >= force {
		return pressureForce
	}
	if hint := t.r.cfg.Runner.HandoffHintTokens; hint > 0 && used >= hint {
		return pressureHint
	}
	return pressureNone
}

func (t *task) sink(e agent.Event) {
	switch e.Kind {
	case agent.EventContent:
		t.mu.Lock()
		t.content.WriteString(e.Text)
		t.mu.Unlock()
		t.syncStream()
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
		t.mu.Lock()
		t.tool = e.ToolName
		t.mu.Unlock()
		t.syncStream()
		t.renderToolStart(e)
		t.r.human(chlog.Event{Kind: "tool_start", Session: t.key, Channel: t.msg.Channel, Tool: e.ToolName, Text: e.ToolArgs})
	case agent.EventToolEnd:
		t.mu.Lock()
		t.tool = ""
		t.mu.Unlock()
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
