package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/LaoQi/battlelab/agent"

	"github.com/LaoQi/ivan/schedule"
)

const scheduleToolDesc = `管理计划任务（提醒、定时汇报、周期性检查）。
- 用户说"提醒我""过一会儿""X 分钟后""每天/每小时"时必须调用本工具创建作业，不要只在回复里承诺或自己等待。
- 时间一律用相对秒数：at_seconds=600 表示 10 分钟后；every_seconds=3600 表示每 3600 秒一次（最小 60）。绝对时间才用 when。
- "每天/每周固定时刻"用 daily_at（如 daily_at="20:00" 表示每天 20:00），它按自然日锚定、不会漂移，优于 every_seconds=86400；需要别的时区就同时给 timezone（默认本机时区）。
- add 必须给 prompt：到点时会作为用户指令发给你自己的内容，写清楚要做什么、要汇报什么。
- 每次创建作业都会把 prompt 落成任务文件（<data_dir>/tasks/<job_id>.md），触发时优先读该文件；人可以直接改文件细化作息内容，无需重建作业。
- 作业结果自动发回当前会话，不能也不需要指定目标通道。
- list 查看 jobs 的 id 与下次时间；remove 用 job_id 取消；run_now 立即执行一次且不改原计划。`

const scheduleToolParams = `{
  "type": "object",
  "properties": {
    "action": {"type": "string", "enum": ["add", "list", "update", "remove", "run_now"], "description": "操作类型"},
    "job_id": {"type": "string", "description": "作业 id（update/remove/run_now 必填，从 list 获取）"},
    "name": {"type": "string", "description": "作业名，便于辨认"},
    "prompt": {"type": "string", "description": "到点时发给自己的指令（add 必填）"},
    "task_file": {"type": "string", "description": "任务提示词文件（相对 data_dir/tasks 或绝对路径）；给了就替代 prompt，便于人改"},
    "at_seconds": {"type": "number", "description": "多少秒后触发一次，如 600 表示 10 分钟后"},
    "every_seconds": {"type": "number", "description": "每隔多少秒触发一次（固定间隔，最小 60），如 3600 表示每小时"},
    "daily_at": {"type": "string", "description": "每天固定时刻触发，格式 HH:MM 或 HH:MM:SS（本地时区，配合 timezone 可换时区），如 \"20:00\" 表示每天 20:00"},
    "timezone": {"type": "string", "description": "daily_at 使用的时区（IANA 名，如 Asia/Shanghai），默认本机时区"},
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
	TaskFile       string  `json:"task_file"`
	AtSeconds      float64 `json:"at_seconds"`
	EverySeconds   float64 `json:"every_seconds"`
	DailyAt        string  `json:"daily_at"`
	Timezone       string  `json:"timezone"`
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
		return scheduleAdd(r, sched, inst, a)
	case "list":
		return scheduleList(sched)
	case "update":
		return scheduleUpdate(r, sched, a)
	case "remove":
		return scheduleRemove(r, sched, a)
	case "run_now":
		return scheduleRunNow(ctx, sched, a)
	case "":
		return "error: 缺少 action（add/list/update/remove/run_now）"
	default:
		return fmt.Sprintf("error: 未知 action %q（add/list/update/remove/run_now）", a.Action)
	}
}

func scheduleAdd(r *Runner, sched *schedule.Scheduler, inst *instance, a scheduleArgs) string {
	prompt := strings.TrimSpace(a.Prompt)
	file := strings.TrimSpace(a.TaskFile)
	if prompt == "" && file == "" {
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
	id := r.NewJobID()
	payload := schedule.Payload{
		Prompt:     prompt,
		Channel:    tg.Channel,
		ChatID:     tg.ChatID,
		SessionKey: tg.SessionKey,
	}
	if file == "" {
		path, err := r.WriteTaskFile(id, prompt)
		if err != nil {
			return "error: 写入任务文件失败: " + err.Error()
		}
		payload.File = path
	} else {
		payload.File = r.taskFilePath(file)
		if _, _, err := r.ReadTaskFile(file); err != nil {
			return "error: 任务文件不可读: " + err.Error()
		}
	}
	job, err := sched.Add(schedule.Job{
		ID:             id,
		Name:           strings.TrimSpace(a.Name),
		Enabled:        true,
		Schedule:       spec,
		DeleteAfterRun: del,
		Payload:        payload,
	})
	if err != nil {
		r.deleteTaskFile(id)
		return "error: 创建作业失败: " + err.Error()
	}
	return fmt.Sprintf("已创建作业 %s（%s），下次触发 %s，结果发回 %s，任务文件 %s",
		job.ID, describeSchedule(job.Schedule), formatMS(job.State.NextRunAtMS), job.Payload.SessionKey, job.Payload.File)
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
		if j.Payload.File != "" {
			line += " | 任务文件 " + j.Payload.File
		}
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

func scheduleUpdate(r *Runner, sched *schedule.Scheduler, a scheduleArgs) string {
	id := strings.TrimSpace(a.JobID)
	if id == "" {
		return "error: update 需要 job_id"
	}
	var written string
	job, err := sched.Update(id, func(j *schedule.Job) error {
		if name := strings.TrimSpace(a.Name); name != "" {
			j.Name = name
		}
		if prompt := strings.TrimSpace(a.Prompt); prompt != "" {
			j.Payload.Prompt = prompt
			if j.Payload.File != "" {
				path, err := r.WriteTaskFile(j.ID, prompt)
				if err != nil {
					return err
				}
				written = path
				j.Payload.File = path
			}
		}
		if file := strings.TrimSpace(a.TaskFile); file != "" {
			if _, _, err := r.ReadTaskFile(file); err != nil {
				return err
			}
			j.Payload.File = r.taskFilePath(file)
		}
		if a.Enabled != nil {
			j.Enabled = *a.Enabled
		}
		if a.DeleteAfterRun != nil {
			j.DeleteAfterRun = *a.DeleteAfterRun
		}
		if a.AtSeconds > 0 || a.EverySeconds > 0 || strings.TrimSpace(a.When) != "" || strings.TrimSpace(a.DailyAt) != "" {
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
	text := fmt.Sprintf("已更新作业 %s（%s），下次触发 %s", job.ID, describeSchedule(job.Schedule), formatMS(job.State.NextRunAtMS))
	if written != "" {
		text += "，任务文件 " + written
	}
	return text
}

func scheduleRemove(r *Runner, sched *schedule.Scheduler, a scheduleArgs) string {
	id := strings.TrimSpace(a.JobID)
	if id == "" {
		return "error: remove 需要 job_id"
	}
	job, ok, err := sched.Get(id)
	if err != nil {
		return "error: 读取作业失败: " + err.Error()
	}
	if !ok {
		return "error: 作业不存在: " + id
	}
	if err := sched.Remove(id); err != nil {
		return "error: 取消作业失败: " + err.Error()
	}
	if file := strings.TrimSpace(job.Payload.File); file != "" && strings.HasPrefix(file, r.cfg.TasksDir()+string(filepath.Separator)) {
		r.deleteTaskFile(id)
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
	hasDaily := strings.TrimSpace(a.DailyAt) != ""
	given := 0
	for _, ok := range []bool{hasAt, hasWhen, hasEvery, hasDaily} {
		if ok {
			given++
		}
	}
	if given > 1 {
		return schedule.Schedule{}, fmt.Errorf("at_seconds / when / every_seconds / daily_at 只能给一个")
	}
	if hasDaily {
		atTimeMS, err := parseDailyAt(a.DailyAt)
		if err != nil {
			return schedule.Schedule{}, err
		}
		if _, err := schedule.Location(a.Timezone); err != nil {
			return schedule.Schedule{}, err
		}
		return schedule.Schedule{Kind: schedule.KindDaily, AtTimeMS: atTimeMS, TZ: strings.TrimSpace(a.Timezone)}, nil
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
		return schedule.Schedule{}, fmt.Errorf("需要 at_seconds（多少秒后）、every_seconds（每多少秒）、daily_at（每天几点）或 when（绝对时间）")
	}
}

func parseDailyAt(s string) (int64, error) {
	v := strings.TrimSpace(s)
	for _, layout := range []string{"15:04", "15:04:05"} {
		if t, err := time.ParseInLocation(layout, v, time.Local); err == nil {
			return int64(t.Hour())*3600000 + int64(t.Minute())*60000 + int64(t.Second())*1000, nil
		}
	}
	return 0, fmt.Errorf("无法解析 daily_at=%q（用 HH:MM 或 HH:MM:SS）", s)
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
	case schedule.KindDaily:
		return "每天 " + clockOf(s.AtTimeMS) + tzSuffix(s.TZ)
	}
	return string(s.Kind)
}

func clockOf(ms int64) string {
	d := time.Duration(ms) * time.Millisecond
	if ms%60000 == 0 {
		return fmt.Sprintf("%02d:%02d", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%02d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}

func tzSuffix(tz string) string {
	if strings.TrimSpace(tz) == "" {
		return ""
	}
	return "（" + tz + "）"
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
