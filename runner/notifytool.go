package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LaoQi/battlelab/agent"
)

const sendMessageDesc = `发一条消息给用户（主动出站，尽力而为广播）。
- 用在需要主动通知用户的场合：外部服务请求需要用户确认、长任务阶段性结论、异常告警。
- 不选择出口：交给各通道的默认通知出口（通道自己决定发给谁），投递汇总如实返回。
- 返回 ok（全部成功）/ partial（部分成功）/ error（全部失败或没有可投递的通知出口）。`

const sendMessageParams = `{
  "type": "object",
  "properties": {
    "text": {"type": "string", "description": "消息正文（纯文本，简洁、结论先行）"}
  },
  "required": ["text"]
}`

type sendMessageArgs struct {
	Text string `json:"text"`
}

func (r *Runner) sendMessageTool(inst *instance) agent.Tool {
	return agent.NewTool("send_message", sendMessageDesc, sendMessageParams,
		func(ctx context.Context, argsJSON string) agent.ToolResult {
			return agent.ToolResult{Text: r.sendMessageRun(ctx, argsJSON)}
		})
}

func (r *Runner) sendMessageRun(ctx context.Context, argsJSON string) string {
	var a sendMessageArgs
	if err := json.Unmarshal([]byte(argsJSON), &a); err != nil {
		return fmt.Sprintf(agent.MsgParseArgs, err)
	}
	text := strings.TrimSpace(a.Text)
	if text == "" {
		return "error: text 不能为空"
	}
	if r.opts.Notice == nil {
		return "error: 当前进程没有通知出口，无法发送消息"
	}
	sent, total, names := r.opts.Notice(ctx, text)
	switch {
	case total == 0:
		return "error: 没有实现通知出口的通道，无法通知用户"
	case sent == 0:
		return fmt.Sprintf("error: 全部通知出口投递失败（%d 个目标）", total)
	case sent < total:
		return fmt.Sprintf("partial: 已发 %d/%d 个目标（成功：%s）", sent, total, names)
	default:
		return fmt.Sprintf("ok: 已广播到 %d 个目标（%s）", sent, names)
	}
}
