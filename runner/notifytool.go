package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/LaoQi/battlelab/agent"

	"github.com/LaoQi/ivan/bus"
)

const sendMessageDesc = `发一条消息给用户（主动出站，尽力而为广播）。
- 用在需要主动通知用户的场合：外部服务请求需要用户确认、长任务阶段性结论、异常告警。
- 不选择出口：广播到配置 http.targets 登录的全部目标，投递汇总如实返回。
- 返回 ok（全部成功）/ partial（部分成功）/ error（全部失败，正文含每个目标的失败原因）。`

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
	targets := r.cfg.HTTP.Targets
	if len(targets) == 0 {
		return "error: 没有可用的输出目标（http.targets 未配置），无法通知用户"
	}
	deliver := r.opts.NotifyStrict
	if deliver == nil {
		deliver = r.opts.Notify
	}
	if deliver == nil {
		return "error: 当前进程没有出站投递通道，无法发送消息"
	}
	var okNames, fails []string
	for _, t := range targets {
		msg := bus.OutboundMessage{
			Channel: t.Channel,
			ChatID:  t.ChatID,
			Content: text,
			Kind:    bus.KindNotice,
		}
		dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := deliver(dctx, msg)
		cancel()
		if err == nil {
			okNames = append(okNames, t.Name)
		} else {
			fails = append(fails, fmt.Sprintf("%s: %s", t.Name, briefErr(err)))
		}
	}
	switch {
	case len(fails) == 0:
		return fmt.Sprintf("ok: 已广播到 %d 个目标（%s）", len(okNames), strings.Join(okNames, ", "))
	case len(okNames) == 0:
		return "error: 全部目标投递失败：" + strings.Join(fails, "；")
	default:
		return fmt.Sprintf("partial: 已发 %d/%d 个目标（成功：%s；失败：%s）",
			len(okNames), len(targets), strings.Join(okNames, ", "), strings.Join(fails, "；"))
	}
}
