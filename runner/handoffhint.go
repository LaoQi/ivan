package runner

import (
	"context"
	"fmt"
	"sync"

	"github.com/LaoQi/battlelab/agent"

	chlog "github.com/LaoQi/ivan/channel/log"
)

const handoffHintNotice = "系统提示：本会话上下文已到 %d tokens（建议交接阈值 %d）。建议在本阶段收尾后调用 next_session 交接进度（summary 写清已完成内容、关键结论与下一步）；若目标马上就能完成，可先把当前步骤做完再交接。"

// contextUsed 是两档阈值共用的上下文口径：max(上游实报 prompt tokens, 本地估算)。
func contextUsed(st agent.Stats) int {
	if st.ContextTokens > st.Est {
		return st.ContextTokens
	}
	return st.Est
}

// handoffHint 是建议档交接提醒钩子（BeforeRequestHook + SessionHook）。
//
// 提醒在一次入站消息的推进过程内只注入一次（runner 在推进开始时 arm、结束时 disarm）：
// 达阈就往 history 追加一条事实行，本回合接下来的请求自然带上它，不再额外多发一轮。
// 会话边界（交接/新建/载入/轮换）清掉已提示标记——新会话上下文基线不同，建议档重新计。
type handoffHint struct {
	r    *Runner
	inst *instance

	mu       sync.Mutex
	armed    bool
	fired    bool
	pausedAt bool
}

func (r *Runner) newHandoffHint(inst *instance) *handoffHint {
	return &handoffHint{r: r, inst: inst}
}

func (h *handoffHint) arm() {
	h.mu.Lock()
	h.armed, h.fired, h.pausedAt = true, false, false
	h.mu.Unlock()
}

func (h *handoffHint) disarm() {
	h.mu.Lock()
	h.armed = false
	h.mu.Unlock()
}

func (h *handoffHint) BeforeRequest(ctx context.Context, in agent.RequestInfo) {
	h.mu.Lock()
	skip := !h.armed || h.fired
	h.mu.Unlock()
	if skip {
		return
	}
	tokens := h.r.cfg.Runner.HandoffHintTokens
	if tokens <= 0 {
		return
	}
	used := contextUsed(in.Stats())
	if used < tokens {
		return
	}
	if h.r.paused() {
		h.pausedSkip()
		return
	}
	a, ok := agent.FromContext(ctx)
	if !ok {
		return
	}
	h.mu.Lock()
	h.fired = true
	h.mu.Unlock()
	if err := a.AppendNotice(fmt.Sprintf(handoffHintNotice, used, tokens)); err != nil {
		h.r.notice("注入交接提示失败 key=%s: %v", h.inst.key, err)
		return
	}
	h.r.hinted.Add(1)
	text := fmt.Sprintf("上下文 %d tokens 达建议交接阈值 %d，已在回合内注入交接提示", used, tokens)
	h.r.human(chlog.Event{Kind: "handoff_hint", Session: h.inst.key, Channel: h.inst.target().Channel, Tool: "next_session", Context: used, Text: text})
	h.r.note("handoff_hint", h.inst.key+" "+text)
}

func (h *handoffHint) SessionEvent(ctx context.Context, in agent.SessionEvent) {
	h.mu.Lock()
	h.fired = false
	h.mu.Unlock()
}

func (h *handoffHint) pausedSkip() {
	h.mu.Lock()
	logged := h.pausedAt
	h.pausedAt = true
	h.mu.Unlock()
	if logged {
		return
	}
	h.r.human(chlog.Event{Kind: "paused", Session: h.inst.key, Channel: h.inst.target().Channel, Tool: "next_session", Text: "暂停中，跳过上下文交接提示"})
	h.r.note("paused", h.inst.key+" 暂停中，跳过上下文交接提示")
}
