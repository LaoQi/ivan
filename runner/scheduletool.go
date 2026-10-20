package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/LaoQi/tanya/agent"

	"github.com/LaoQi/ivan/schedule"
)

const scheduleToolDesc = `管理计划任务（提醒、定时汇报、周期性检查）。
- 用户说"提醒我""过一会儿""X 分钟后""每天/每小时"时必须调用本工具创建作业，不要只在回复里承诺或自己等待。
- 时间一律用相对秒数：at_seconds=600 表示 10 分钟后；every_seconds=86400 表示每天一次（最小 60）。绝对时间才用 when。
- add 必须给 prompt：到点时会作为用户指令发给你自己的内容，写清楚要做什么、要汇报什么。
- 作业结果自动发回当前会话，不能也不需要指定目标通道。
- list 查看 jobs 的 id 与下次时间；remove 用 job_id 取消；run_now 立即执行一次且不改原计划。`

const scheduleToolParams = `{
  "type": "object",
  "properties": {
    "action": {"type": "string", "enum": ["add", "list", "update", "remove", "run_now"], "description": "操作类型"},
    "job_id": {"type": "string", "description": "作业 id（update/remove/run_now 必填，从 list 获取）"},
    "name": {"type": "string", "description": "作业名，便于辨认"},
    "prompt": {"type": "string", "description": "到点时发给自己的指令（add 必填）"},
    "at_seconds": {"type": "number", "description": "多少秒后触发一次，如 600 表示 10 分钟后"},
    "every_seconds": {"type": "number", "description": "每隔多少秒触发一次，最小 60，如 86400 表示每天"},
    "when": {"type": "string", "description": "一次性作业的绝对时间：RFC3339 或 \"2006-01-02 15:04\"（本地时区）"},
    "enabled": {"type": "boolean", "description": "update 时启用/停用作业"},
    "delete_after_run": {"type": "boolean", "description": "一次性作业执行后自动删除，默认 true"}
  },
  "required": ["action"]
}`

const (
	minEverySeconds = int64(60)
	maxAtSeconds    = int64(365 * 24 * 3600)
	localTimeLayout = "2006-01-02 15:04"
)

type scheduleArgs struct {
	Action         string  `json:"action"`
	JobID          string  `json:"job_id"`
	Name           string  `json:"name"`
	Prompt         string  `json:"prompt"`
	AtSeconds      float64 `json:"at_seconds"`
	EverySeconds   float64 `json:"every_seconds"`
	When           string  `json:"when"`
	Enabled        *bool   `json:"enabled"`
	DeleteAfterRun *bool   `json:"delete_after_run"`
}

func (r *Runner) scheduleTool(inst *instance) agent.Tool {
	return agent.NewTool("schedule", scheduleToolDesc, scheduleToolParams,
		func(ctx context.Context, argsJSON string) agent.ToolResult {
			return agent.ToolResult{Text: r.scheduleToolRun(ctx, inst, argsJSON)}
		})
}

func (r *Runner) scheduleToolRun(ctx context.Context, inst *instance, argsJSON string) string {
	var a scheduleArgs
	if err := json.Unmarshal([]byte(argsJSON), &a); err != nil {
		return fmt.Sprintf(agent.MsgParseArgs, err)
	}
	sched := r.Scheduler()
	if sched == nil {
		return "error: 计划任务未启用（当前进程没有调度器）"
	}
	switch strings.ToLower(strings.TrimSpace(a.Action)) {
	case "add":
		return scheduleAdd(sched, inst, a)
	case "list":
		return scheduleList(sched)
	case "update":
		return scheduleUpdate(sched, a)
	case "remove":
		return scheduleRemove(sched, a)
	case "run_now":
		return scheduleRunNow(ctx, sched, a)
	case "":
		return "error: 缺少 action（add/list/update/remove/run_now）"
	default:
		return fmt.Sprintf("error: 未知 action %q（add/list/update/remove/run_now）", a.Action)
	}
}

func scheduleAdd(sched *schedule.Scheduler, inst *instance, a scheduleArgs) string {
	prompt := strings.TrimSpace(a.Prompt)
	if prompt == "" {
		return "error: add 需要 prompt（到点时要执行的指令）"
	}
	spec, err := buildSchedule(a, time.Now())
	if err != nil {
		return "error: " + err.Error()
	}
	tg := inst.target()
	if strings.TrimSpace(tg.Channel) == "" || strings.TrimSpace(tg.SessionKey) == "" {
		return "error: 当前会话没有可回投的目标，无法创建作业"
	}
	del := spec.Kind == schedule.KindAt
	if a.DeleteAfterRun != nil {
		del = *a.DeleteAfterRun
	}
	job, err := sched.Add(schedule.Job{
		Name:           strings.TrimSpace(a.Name),
		Enabled:        true,
		Schedule:       spec,
		DeleteAfterRun: del,
		Payload: schedule.Payload{
			Prompt:     prompt,
			Channel:    tg.Channel,
			ChatID:     tg.ChatID,
			SessionKey: tg.SessionKey,
		},
	})
	if err != nil {
		return "error: 创建作业失败: " + err.Error()
	}
	return fmt.Sprintf("已创建作业 %s（%s），下次触发 %s，结果发回 %s",
		job.ID, describeSchedule(job.Schedule), formatMS(job.State.NextRunAtMS), job.Payload.SessionKey)
}

func scheduleList(sched *schedule.Scheduler) string {
	jobs, err := sched.List()
	if err != nil {
		return "error: 读取作业失败: " + err.Error()
	}
	if len(jobs) == 0 {
		return "当前没有计划任务。"
	}
	sort.Slice(jobs, func(i, k int) bool {
		a, b := jobs[i].State.NextRunAtMS, jobs[k].State.NextRunAtMS
		if a == 0 || b == 0 {
			return b == 0 && a != 0
		}
		return a < b
	})
	var b strings.Builder
	fmt.Fprintf(&b, "共 %d 个计划任务：\n", len(jobs))
	for _, j := range jobs {
		line := fmt.Sprintf("- %s | %s | %s | 下次 %s | 上次 %s | 跑了 %d 次",
			j.ID, j.Label(), describeSchedule(j.Schedule), formatMS(j.State.NextRunAtMS), statusText(j.State), j.State.Runs)
		if !j.Enabled {
			line += " | 已停用"
		}
		if j.State.LastError != "" {
			line += " | 错误: " + oneLine(j.State.LastError, 80)
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func scheduleUpdate(sched *schedule.Scheduler, a scheduleArgs) string {
	id := strings.TrimSpace(a.JobID)
	if id == "" {
		return "error: update 需要 job_id"
	}
	job, err := sched.Update(id, func(j *schedule.Job) error {
		if name := strings.TrimSpace(a.Name); name != "" {
			j.Name = name
		}
		if prompt := strings.TrimSpace(a.Prompt); prompt != "" {
			j.Payload.Prompt = prompt
		}
		if a.Enabled != nil {
			j.Enabled = *a.Enabled
		}
		if a.DeleteAfterRun != nil {
			j.DeleteAfterRun = *a.DeleteAfterRun
		}
		if a.AtSeconds > 0 || a.EverySeconds > 0 || strings.TrimSpace(a.When) != "" {
			spec, err := buildSchedule(a, time.Now())
			if err != nil {
				return err
			}
			j.Schedule = spec
			if spec.Kind == schedule.KindAt {
				j.DeleteAfterRun = true
			}
		}
		return nil
	})
	if err != nil {
		return "error: 更新作业失败: " + err.Error()
	}
	return fmt.Sprintf("已更新作业 %s（%s），下次触发 %s", job.ID, describeSchedule(job.Schedule), formatMS(job.State.NextRunAtMS))
}

func scheduleRemove(sched *schedule.Scheduler, a scheduleArgs) string {
	id := strings.TrimSpace(a.JobID)
	if id == "" {
		return "error: remove 需要 job_id"
	}
	if err := sched.Remove(id); err != nil {
		return "error: 取消作业失败: " + err.Error()
	}
	return "已取消作业 " + id
}

func scheduleRunNow(ctx context.Context, sched *schedule.Scheduler, a scheduleArgs) string {
	id := strings.TrimSpace(a.JobID)
	if id == "" {
		return "error: run_now 需要 job_id"
	}
	job, err := sched.RunNow(ctx, id)
	if err != nil {
		return "error: 立即执行失败: " + err.Error()
	}
	return fmt.Sprintf("已执行作业 %s（%s），原计划不变，下次触发 %s", job.ID, job.Label(), formatMS(job.State.NextRunAtMS))
}

func buildSchedule(a scheduleArgs, now time.Time) (schedule.Schedule, error) {
	when := strings.TrimSpace(a.When)
	hasAt := a.AtSeconds > 0
	hasWhen := when != ""
	hasEvery := a.EverySeconds > 0
	switch {
	case hasAt && hasWhen:
		return schedule.Schedule{}, fmt.Errorf("at_seconds 与 when 只能给一个")
	case hasAt && hasEvery, hasWhen && hasEvery:
		return schedule.Schedule{}, fmt.Errorf("一次性时间与 every_seconds 只能给一个")
	}
	if hasEvery {
		if int64(a.EverySeconds) < minEverySeconds {
			return schedule.Schedule{}, fmt.Errorf("every_seconds 不能小于 %d（用 at_seconds 做一次性提醒）", minEverySeconds)
		}
		return schedule.Schedule{Kind: schedule.KindEvery, EveryMS: int64(a.EverySeconds * 1000)}, nil
	}
	switch {
	case hasAt:
		if int64(a.AtSeconds) > maxAtSeconds {
			return schedule.Schedule{}, fmt.Errorf("at_seconds 过大（上限 %d 秒）", maxAtSeconds)
		}
		at := now.Add(time.Duration(a.AtSeconds * float64(time.Second)))
		return schedule.Schedule{Kind: schedule.KindAt, AtMS: schedule.MS(at)}, nil
	case hasWhen:
		at, err := parseWhen(when, now)
		if err != nil {
			return schedule.Schedule{}, err
		}
		return schedule.Schedule{Kind: schedule.KindAt, AtMS: schedule.MS(at)}, nil
	default:
		return schedule.Schedule{}, fmt.Errorf("需要 at_seconds（多少秒后）或 every_seconds（每多少秒）或 when（绝对时间）")
	}
}

func parseWhen(s string, now time.Time) (time.Time, error) {
	layouts := []string{time.RFC3339, "2006-01-02 15:04:05", localTimeLayout, "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			if t.Before(now) {
				return time.Time{}, fmt.Errorf("when 指定的时间已过去: %s", s)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析 when=%q（用 RFC3339 或 \"2006-01-02 15:04\"）", s)
}

func describeSchedule(s schedule.Schedule) string {
	switch s.Kind {
	case schedule.KindAt:
		return "一次性 " + formatMS(s.AtMS)
	case schedule.KindEvery:
		return "每 " + humanEvery(s.EveryMS)
	}
	return string(s.Kind)
}

func humanEvery(ms int64) string {
	d := time.Duration(ms) * time.Millisecond
	switch {
	case ms%86400000 == 0:
		return fmt.Sprintf("%d 天", ms/86400000)
	case ms%3600000 == 0:
		return fmt.Sprintf("%d 小时", ms/3600000)
	case ms%60000 == 0:
		return fmt.Sprintf("%d 分钟", ms/60000)
	default:
		return d.String()
	}
}

func statusText(s schedule.State) string {
	if s.LastStatus == "" {
		return "未跑过"
	}
	out := s.LastStatus
	if s.LastRunAtMS > 0 {
		out += " " + formatMS(s.LastRunAtMS)
	}
	return out
}

func formatMS(ms int64) string {
	if ms <= 0 {
		return "—"
	}
	return schedule.FromMS(ms).In(time.Local).Format(localTimeLayout) + " (" + weekday(schedule.FromMS(ms).In(time.Local)) + ")"
}

func weekday(t time.Time) string {
	return []string{"周日", "周一", "周二", "周三", "周四", "周五", "周六"}[int(t.Weekday())]
}
