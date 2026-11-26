package runner

import (
	"context"
	"fmt"

	"github.com/LaoQi/battlelab/agent"

	chlog "github.com/LaoQi/ivan/channel/log"
)

// forceHandoffInstruction 是 ivan 交给模型的摘要指令（库不自带任何文案）。
const forceHandoffInstruction = "本会话上下文已达强制交接阈值，需要为后续会话留一份交接摘要。请整理：已完成的内容与关键结论、改动过的文件及其路径、未完成事项与下一步该做什么。只输出摘要正文，不要调用工具，不要客套话。"

// forceHandoff 在回合间隙做一次外部强制复位：按指令取摘要 → 旧会话落地 → 轮转新会话。
// 摘要请求的用量库侧不计入 Stats()，这里也不记入预算（一次推进至多一次，且在调用前已过预算闸门）。
// 返回 true 表示复位成立且应继续推进（auto_continue 开）。
func (t *task) forceHandoff(ctx context.Context, ag *agent.Agent, used int) bool {
	h, err := ag.ForceHandoff(ctx, agent.ForceHandoffOptions{
		Instruction: forceHandoffInstruction,
		Continue:    t.r.cfg.Runner.AutoContinue,
	})
	if err != nil {
		t.forceFailed(used, err)
		return false
	}
	t.r.handoffs.Add(1)
	t.r.forced.Add(1)
	t.r.saveMapping(t.key, ag.SessionID(), t.inst.ws)
	text := fmt.Sprintf("上下文 %d tokens 达强制交接阈值 %d，已强制交接 %s → %s", used, t.r.cfg.Runner.HandoffForceTokens, h.OldID, h.NewID)
	t.r.human(chlog.Event{Kind: "handoff_force", Session: t.key, Channel: t.msg.Channel, Handoff: true, Tool: "force_handoff", Context: used, Text: text})
	t.r.note("handoff_force", t.key+" "+text)
	return t.r.cfg.Runner.AutoContinue
}

func (t *task) forceFailed(used int, err error) {
	text := fmt.Sprintf("上下文 %d tokens 达强制交接阈值 %d，强制交接失败（%s），本次推进已停止；会话与文件未变",
		used, t.r.cfg.Runner.HandoffForceTokens, briefErr(err))
	t.r.human(chlog.Event{Kind: "handoff_forced", Session: t.key, Channel: t.msg.Channel, Tool: "force_handoff", Context: used, Failed: true, Err: briefErr(err), Text: text})
	t.r.note("handoff_forced", t.key+" "+text)
}

func (t *task) forceStopped(used int) {
	text := fmt.Sprintf("上下文 %d tokens 仍达强制交接阈值 %d（刚强制交接完，新会话基线即超阈值），已停止；请调高 handoff_*_tokens 或拆分这次推进",
		used, t.r.cfg.Runner.HandoffForceTokens)
	t.r.human(chlog.Event{Kind: "handoff_forced", Session: t.key, Channel: t.msg.Channel, Tool: "force_handoff", Context: used, Text: text})
	t.r.note("handoff_forced", t.key+" "+text)
}
