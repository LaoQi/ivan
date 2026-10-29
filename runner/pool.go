package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/LaoQi/battlelab/agent"
	"github.com/LaoQi/battlelab/tools"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/prompt"
)

type instance struct {
	key      string
	ws       string
	mu       sync.Mutex
	ag       *agent.Agent
	lastUsed time.Time
	turns    uint64

	targetMu   sync.Mutex
	channel    string
	chatID     string
	sessionKey string

	cancelMu sync.Mutex
	cancel   context.CancelFunc

	queueMu sync.Mutex
	queue   []queuedTurn
	worker  bool
}

type queuedTurn struct {
	ctx  context.Context
	msg  bus.InboundMessage
	done chan TurnResult
}

type TurnResult struct {
	Turn Turn
	Err  error
}

func (i *instance) enqueue(r *Runner, q queuedTurn, limit int) bool {
	i.queueMu.Lock()
	if limit > 0 && len(i.queue) >= limit {
		i.queueMu.Unlock()
		return false
	}
	i.queue = append(i.queue, q)
	start := !i.worker
	if start {
		i.worker = true
	}
	i.queueMu.Unlock()
	if start {
		go i.drain(r)
	}
	return true
}

func (i *instance) dequeue() (queuedTurn, bool) {
	i.queueMu.Lock()
	defer i.queueMu.Unlock()
	if len(i.queue) == 0 {
		i.worker = false
		return queuedTurn{}, false
	}
	q := i.queue[0]
	i.queue = i.queue[1:]
	return q, true
}

func (i *instance) pending() int {
	i.queueMu.Lock()
	defer i.queueMu.Unlock()
	return len(i.queue)
}

func (i *instance) drain(r *Runner) {
	for {
		q, ok := i.dequeue()
		if !ok {
			return
		}
		turn, err := r.runQueued(q.ctx, i, q.msg)
		q.done <- TurnResult{Turn: turn, Err: err}
		close(q.done)
	}
}

func (i *instance) setCancel(fn context.CancelFunc) {
	i.cancelMu.Lock()
	defer i.cancelMu.Unlock()
	i.cancel = fn
}

func (i *instance) cancelFunc() context.CancelFunc {
	i.cancelMu.Lock()
	defer i.cancelMu.Unlock()
	return i.cancel
}

type chatTarget struct {
	Channel    string
	ChatID     string
	SessionKey string
}

func (i *instance) setTarget(channel, chatID, sessionKey string) {
	i.targetMu.Lock()
	defer i.targetMu.Unlock()
	i.channel, i.chatID, i.sessionKey = channel, chatID, sessionKey
}

func (i *instance) target() chatTarget {
	i.targetMu.Lock()
	defer i.targetMu.Unlock()
	return chatTarget{Channel: i.channel, ChatID: i.chatID, SessionKey: i.sessionKey}
}

func (r *Runner) acquire(key string) *instance {
	r.poolMu.Lock()
	if inst, ok := r.pool[key]; ok {
		inst.lastUsed = time.Now()
		r.poolMu.Unlock()
		return inst
	}
	if max := r.cfg.Runner.MaxAgents; max > 0 && len(r.pool) >= max {
		r.evictLocked(0)
	}
	inst := &instance{key: key, ws: r.cfg.AgentDir(bus.KeySlug(key)), lastUsed: time.Now()}
	r.pool[key] = inst
	r.poolMu.Unlock()
	return inst
}

func (r *Runner) evictLocked(idle time.Duration) int {
	now := time.Now()
	var (
		oldest    *instance
		oldestKey string
	)
	for key, inst := range r.pool {
		if idle > 0 && now.Sub(inst.lastUsed) < idle {
			continue
		}
		if !inst.mu.TryLock() {
			continue
		}
		inst.mu.Unlock()
		if oldest == nil || inst.lastUsed.Before(oldest.lastUsed) {
			oldest, oldestKey = inst, key
		}
	}
	if oldest == nil {
		return 0
	}
	delete(r.pool, oldestKey)
	return 1
}

func (r *Runner) reapLoop(idle time.Duration) {
	interval := idle / 2
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	if interval > 5*time.Minute {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.turnCtx.Done():
			return
		case <-ticker.C:
			if n := r.reap(idle); n > 0 {
				r.notice("回收闲置会话实例 %d 个 idle=%s", n, idle)
			}
		}
	}
}

func (r *Runner) reap(idle time.Duration) int {
	r.poolMu.Lock()
	defer r.poolMu.Unlock()
	return r.evictLocked(idle)
}

func (r *Runner) agentFor(inst *instance) (*agent.Agent, error) {
	if inst.ag != nil {
		return inst.ag, nil
	}
	ag, err := r.buildAgent(inst)
	if err != nil {
		return nil, err
	}
	inst.ag = ag
	return ag, nil
}

func (r *Runner) buildAgent(inst *instance) (*agent.Agent, error) {
	if err := os.MkdirAll(inst.ws, 0o700); err != nil {
		return nil, fmt.Errorf("创建工作区失败 %s: %w", inst.ws, err)
	}
	home, _ := os.UserHomeDir()
	var ag *agent.Agent
	list, err := tools.Standard(tools.Options{
		ShellOverride: os.Getenv("IVAN_SHELL"),
		Home:          home,
		Workspace:     func() string { return currentWorkspace(ag, inst.ws) },
		LookPath:      exec.LookPath,
		ImageMaxBytes: r.cfg.ImageMaxBytes,
		ImageResize:   r.cfg.ImageResize,
		ImageDetail:   r.cfg.ImageDetail,
	})
	if err != nil {
		return nil, err
	}
	if r.Scheduler() != nil {
		list = append(list, r.scheduleTool(inst))
	}
	list = append(list, r.sendMessageTool(inst))
	ag, err = agent.New(&r.cfg.Config,
		agent.WithSystemPrompt(prompt.System(prompt.Info{
			Workspace: inst.ws,
			DataDir:   r.cfg.DataDir,
			Session:   inst.key,
			Memory:    r.memoryPath(inst.key),
			Goals:     filepath.Join(inst.ws, "GOALS.md"),
		})),
		agent.WithTools(list...),
		agent.WithGlobalAgentsPath(r.cfg.GlobalAgentsPath()),
	)
	if err != nil {
		return nil, err
	}
	if ag.Workspace() != inst.ws {
		stray, _ := ag.SessionDir()
		if err := ag.SwitchWorkspace(inst.ws); err != nil {
			return nil, err
		}
		cleanStrayDirs(stray)
	}
	if st, ok := r.opts.State.Get(inst.key); ok && st.SessionID != "" {
		if err := ag.LoadSession(st.SessionID); err != nil {
			r.notice("会话 %s 的映射 %s 无法恢复（%v），已回退为新会话", inst.key, st.SessionID, err)
			if derr := r.opts.State.Delete(inst.key); derr != nil {
				r.notice("清理失效映射失败: %v", derr)
			}
		}
	}
	return ag, nil
}

func cleanStrayDirs(sessionDir string) {
	if sessionDir == "" {
		return
	}
	base := filepath.Dir(sessionDir)
	os.Remove(sessionDir)
	os.Remove(filepath.Join(base, "archive"))
	os.Remove(base)
}

func currentWorkspace(ag *agent.Agent, fallback string) string {
	if ag != nil {
		if ws := ag.Workspace(); ws != "" {
			return ws
		}
	}
	return fallback
}

func (r *Runner) memoryPath(key string) string {
	return filepath.Join(r.cfg.MemoryDir(), bus.KeySlug(key)+".md")
}

func (r *Runner) saveMapping(key, sessionID, ws string) {
	if sessionID == "" {
		return
	}
	if cur, ok := r.opts.State.Get(key); ok && cur.SessionID == sessionID && cur.Workspace == ws {
		return
	}
	if err := r.opts.State.Set(key, data.SessionState{SessionID: sessionID, Workspace: ws}); err != nil {
		r.notice("写入会话映射失败 key=%s: %v", key, err)
	}
}
