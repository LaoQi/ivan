package runner

import (
	"context"
	"fmt"
	"sync"

	"github.com/LaoQi/battlelab/agent"

	chlog "github.com/LaoQi/ivan/channel/log"
)

// handoffHintNotice 是回合内辅助提示：只提醒，不打断当前回合，模型可先把手上这步做完。
const handoffHintNotice = "系统提示：本会话上下文已到 %d tokens（建议交接阈值 %d）。建议在本阶段收尾后调用 next_session 交接进度（summary 写清已完成内容、关键结论与下一步）；若目标马上就能完成，可先把当前步骤做完再交接。"

// handoffDirectiveNotice 是回合末的交接指令：回合已经结束，直接要求开始交接。
const handoffDirectiveNotice = "系统提示：本会话上下文已到 %d tokens（建议交接阈值 %d）。现在开始交接进度：调用 next_session，summary 写清已完成内容、关键结论与下一步。"

// contextUsed 是两档阈值共用的上下文口径：max(上游实报 prompt tokens, 本地估算)。
func contextUsed(st agent.Stats) int {
	if st.ContextTokens > st.Est {
		return st.ContextTokens
	}
	return st.Est
}

// handoffHint 是回合内辅助提示钩子（BeforeRequestHook + SessionHook），每个会话实例一个
// （acquire 造、buildAgent 经 WithHooks 注册）。
//
// 一次入站消息的推进过程内（task.run arm/disarm）至多注入一条；fired 同时被回合末的
// handoffDirective 用作"本次推进已提醒过"，避免同一次推进里再补一条辅助提示。
// 会话边界（交接/新建/载入/轮换）清 fired：新会话上下文基线不同，重新计。
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

// mark 记下"本次推进已经提醒过"，供回合末下令复用同一标记。
func (h *handoffHint) mark() {
	h.mu.Lock()
	h.fired = true
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
		h.r.notice("注入交接提醒失败 key=%s: %v", h.inst.key, err)
		return
	}
	h.r.hinted.Add(1)
	text := fmt.Sprintf("上下文 %d tokens 达建议交接阈值 %d，已在回合内注入交接提醒", used, tokens)
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
	h.r.human(chlog.Event{Kind: "paused", Session: h.inst.key, Channel: h.inst.target().Channel, Tool: "next_session", Text: "暂停中，跳过交接提醒"})
	h.r.note("paused", h.inst.key+" 暂停中，跳过交接提醒")
}

// handoffDirective 在回合出口发一条交接指令：写进历史末尾，随后由调用方直接开一回合。
//
// 回合已经结束（模型把手上这轮做完了），此时下令不再打断任何工作。返回 true 表示
// 指令已注入、应 continue 开新一回合让模型执行交接；暂停/预算/写盘失败都返回 false
// （本回合就停，不白开一回合）。
func (t *task) handoffDirective(ag *agent.Agent, used, turn int) bool {
	hint := t.r.cfg.Runner.HandoffHintTokens
	if hint <= 0 || used < hint {
		return false
	}
	if t.r.paused() {
		t.r.human(chlog.Event{Kind: "paused", Session: t.key, Channel: t.msg.Channel, Tool: "next_session", Text: "暂停中，跳过交接指令"})
		t.r.note("paused", t.key+" 暂停中，跳过交接指令")
		return false
	}
	if t.stopForBudget(turn) {
		return false
	}
	if err := ag.AppendNotice(fmt.Sprintf(handoffDirectiveNotice, used, hint)); err != nil {
		t.r.notice("注入交接指令失败 key=%s: %v", t.key, err)
		return false
	}
	t.inst.hint.mark()
	t.r.directives.Add(1)
	text := fmt.Sprintf("回合末上下文 %d tokens 达建议交接阈值 %d，已发出交接指令并开新回合", used, hint)
	t.r.human(chlog.Event{Kind: "handoff_directive", Session: t.key, Channel: t.msg.Channel, Tool: "next_session", Context: used, Text: text})
	t.r.note("handoff_directive", t.key+" "+text)
	return true
}
