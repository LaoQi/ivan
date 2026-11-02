package runner

import (
	"fmt"
	"sort"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/data"
)

type SessionStatus struct {
	Key       string `json:"key"`
	Session   string `json:"session,omitempty"`
	Workspace string `json:"workspace"`
	Pooled    bool   `json:"pooled"`
	Running   bool   `json:"running"`
	Queued    int    `json:"queued,omitempty"`
}

type SessionRotation struct {
	Key     string `json:"key"`
	Pooled  bool   `json:"pooled"`
	Rotated bool   `json:"rotated"`
	OldID   string `json:"old_id,omitempty"`
	NewID   string `json:"new_id,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

func (r *Runner) Sessions() []SessionStatus {
	snap := r.opts.State.Snapshot()
	seen := make(map[string]SessionStatus, len(snap))
	for key, st := range snap {
		seen[key] = SessionStatus{Key: key, Session: st.SessionID, Workspace: st.Workspace}
	}
	r.poolMu.Lock()
	for key, inst := range r.pool {
		cur, ok := seen[key]
		if !ok {
			cur = SessionStatus{Key: key}
		}
		cur.Workspace = inst.ws
		cur.Pooled = true
		cur.Running = !inst.mu.TryLock()
		if !cur.Running {
			inst.mu.Unlock()
		}
		cur.Queued = inst.pending()
		if inst.ag != nil {
			if id := inst.ag.SessionID(); id != "" {
				cur.Session = id
			}
		}
		seen[key] = cur
	}
	r.poolMu.Unlock()
	out := make([]SessionStatus, 0, len(seen))
	for _, st := range seen {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (r *Runner) RotateSession(key string) (SessionRotation, error) {
	key = bus.OneLine(key, 128)
	if key == "" {
		return SessionRotation{}, fmt.Errorf("缺少会话键")
	}
	r.poolMu.Lock()
	inst := r.pool[key]
	r.poolMu.Unlock()
	out := SessionRotation{Key: key}
	if inst == nil {
		out.Reason = "当前没有活跃实例，下一条消息将自然开启新会话"
		return out, nil
	}
	out.Pooled = true
	if !inst.idleLocked() {
		return out, fmt.Errorf("会话 %s 有回合在跑或消息在排队，先 ivan cancel 或等回合结束", key)
	}
	ag, err := r.agentFor(inst)
	if err != nil {
		return out, err
	}
	oldID := ag.SessionID()
	ag.NewSession()
	newID := ag.SessionID()
	if newID == "" {
		return out, fmt.Errorf("会话 %s 未落盘，无法轮换", key)
	}
	out.Rotated = true
	out.OldID = oldID
	out.NewID = newID
	if err := r.opts.State.Set(key, data.SessionState{SessionID: newID, Workspace: inst.ws}); err != nil {
		return out, fmt.Errorf("写回会话映射失败: %w", err)
	}
	return out, nil
}

func (r *Runner) WorkspaceOf(key string) string {
	r.poolMu.Lock()
	inst := r.pool[key]
	r.poolMu.Unlock()
	if inst != nil {
		return inst.ws
	}
	if st, ok := r.opts.State.Get(key); ok {
		return st.Workspace
	}
	return ""
}
