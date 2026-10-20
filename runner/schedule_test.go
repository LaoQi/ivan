package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/internal/fakellm"
	"github.com/LaoQi/ivan/schedule"
)

func withScheduler(t *testing.T, rg *rig) *schedule.Scheduler {
	t.Helper()
	sched, err := schedule.New(schedule.NewMemStore(), schedule.DispatcherFunc(rg.r.TriggerJob), schedule.Config{})
	if err != nil {
		t.Fatalf("schedule.New: %v", err)
	}
	rg.r.SetScheduler(sched)
	return sched
}

func TestScheduleToolInheritsSessionTarget(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "好的"})
	defer srv.Close()
	rg := newRig(t, srv)
	sched := withScheduler(t, rg)
	if got := rg.dispatch(t, inbound("cli:default", "req-1", "10 分钟后提醒我喝水")); !strings.Contains(got, "好的") {
		t.Fatalf("出站内容异常: %q", got)
	}
	inst := rg.r.pool["cli:default"]
	if inst == nil {
		t.Fatal("实例未建立")
	}
	tool := rg.r.scheduleTool(inst)
	ctx := context.Background()

	out := tool.Invoke(ctx, `{"action":"add","name":"喝水","at_seconds":600,"prompt":"提醒我喝水"}`).Text
	if !strings.Contains(out, "已创建作业") {
		t.Fatalf("创建失败: %q", out)
	}
	jobs, err := sched.List()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("作业列表异常: %v %v", jobs, err)
	}
	j := jobs[0]
	if j.Payload.Channel != bus.ChannelCLI || j.Payload.ChatID != "req-1" || j.Payload.SessionKey != "cli:default" {
		t.Fatalf("目标会话未继承: %+v", j.Payload)
	}
	if j.Schedule.Kind != schedule.KindAt || j.State.NextRunAtMS == 0 {
		t.Fatalf("调度配置异常: %+v", j)
	}
	if !j.DeleteAfterRun {
		t.Fatal("一次性作业默认应执行后自删")
	}

	list := tool.Invoke(ctx, `{"action":"list"}`).Text
	if !strings.Contains(list, j.ID) || !strings.Contains(list, "喝水") {
		t.Fatalf("列表输出异常: %q", list)
	}

	if out := tool.Invoke(ctx, `{"action":"update","job_id":"`+j.ID+`","name":"喝水提醒"}`).Text; !strings.Contains(out, "已更新") {
		t.Fatalf("更新失败: %q", out)
	}
	if got, _, _ := sched.Get(j.ID); got.Name != "喝水提醒" {
		t.Fatalf("名称未更新: %q", got.Name)
	}

	if out := tool.Invoke(ctx, `{"action":"remove","job_id":"`+j.ID+`"}`).Text; !strings.Contains(out, "已取消") {
		t.Fatalf("取消失败: %q", out)
	}
	if jobs, _ := sched.List(); len(jobs) != 0 {
		t.Fatalf("取消后仍有作业: %+v", jobs)
	}
}

func TestScheduleToolValidation(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok"})
	defer srv.Close()
	rg := newRig(t, srv)
	withScheduler(t, rg)
	rg.dispatch(t, inbound("cli:default", "req-1", "hi"))
	inst := rg.r.pool["cli:default"]
	tool := rg.r.scheduleTool(inst)
	cases := []struct {
		args string
		want string
	}{
		{`{"action":"add","prompt":"x"}`, "需要 at_seconds"},
		{`{"action":"add","at_seconds":10}`, "需要 prompt"},
		{`{"action":"add","prompt":"x","every_seconds":10}`, "不能小于 60"},
		{`{"action":"add","prompt":"x","at_seconds":10,"every_seconds":600}`, "只能给一个"},
		{`{"action":"add","prompt":"x","when":"2000-01-01 00:00"}`, "已过去"},
		{`{"action":"remove","job_id":"nope"}`, "作业不存在"},
		{`{"action":"bogus"}`, "未知 action"},
	}
	for _, c := range cases {
		got := tool.Invoke(context.Background(), c.args).Text
		if !strings.Contains(got, c.want) {
			t.Fatalf("args=%s 输出 %q，期望包含 %q", c.args, got, c.want)
		}
	}
}

func TestTriggerJobDeliversBackToChannel(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "提醒：该喝水了", Usage: fakellm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}})
	defer srv.Close()
	rg := newRig(t, srv)
	var sent []bus.OutboundMessage
	rg.r.opts.Notify = func(ctx context.Context, m bus.OutboundMessage) error {
		sent = append(sent, m)
		return nil
	}
	withScheduler(t, rg)
	out := rg.r.TriggerJob(context.Background(), schedule.Trigger{
		JobID:      "job_1",
		Name:       "喝水",
		Prompt:     "提醒我喝水",
		Channel:    bus.ChannelQQ,
		ChatID:     "u1",
		SessionKey: "qq:c2c:u1",
	})
	if out.Err != nil {
		t.Fatalf("TriggerJob: %v", out.Err)
	}
	if len(sent) != 1 {
		t.Fatalf("投递次数 %d，期望 1", len(sent))
	}
	msg := sent[0]
	if msg.Channel != bus.ChannelQQ || msg.ChatID != "u1" {
		t.Fatalf("回投目标异常: %+v", msg)
	}
	if msg.ReplyTo != "" {
		t.Fatalf("定时消息不应带被动回复 id: %q", msg.ReplyTo)
	}
	if !strings.Contains(msg.Content, "喝水") {
		t.Fatalf("内容异常: %q", msg.Content)
	}
	if out.Tokens <= 0 {
		t.Fatalf("未回传 token: %+v", out)
	}
	select {
	case extra := <-rg.bus.Outbound():
		t.Fatalf("定时结果不应再走总线: %+v", extra)
	default:
	}
	inst := rg.r.pool["qq:c2c:u1"]
	if inst == nil {
		t.Fatal("定时作业未建立实例")
	}
	if tg := inst.target(); tg.Channel != bus.ChannelQQ || tg.ChatID != "u1" || tg.SessionKey != "qq:c2c:u1" {
		t.Fatalf("定时回合的目标未记录: %+v", tg)
	}
}

func TestTriggerJobNotifyErrorRecorded(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "报告"})
	defer srv.Close()
	rg := newRig(t, srv)
	rg.r.opts.Notify = func(context.Context, bus.OutboundMessage) error {
		return fmt.Errorf("通道未注册 %q", "qq")
	}
	withScheduler(t, rg)
	out := rg.r.TriggerJob(context.Background(), schedule.Trigger{
		JobID: "job_2", Prompt: "汇报", Channel: bus.ChannelQQ, ChatID: "u2", SessionKey: "qq:c2c:u2",
	})
	if out.Err != nil {
		t.Fatalf("作业执行失败不应被通知失败带偏: %v", out.Err)
	}
	if !strings.Contains(out.NotifyErr, "通道未注册") {
		t.Fatalf("通知失败未回报: %q", out.NotifyErr)
	}
}

func TestScheduleToolWithoutScheduler(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok"})
	defer srv.Close()
	rg := newRig(t, srv)
	rg.dispatch(t, inbound("cli:default", "req-1", "hi"))
	inst := rg.r.pool["cli:default"]
	out := rg.r.scheduleTool(inst).Invoke(context.Background(), `{"action":"list"}`).Text
	if !strings.Contains(out, "未启用") {
		t.Fatalf("缺少调度器时应明确报错: %q", out)
	}
}

func TestParseWhenAndScheduleSummary(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.Local)
	spec, err := buildSchedule(scheduleArgs{When: "2026-10-21 09:00", Prompt: "x"}, now)
	if err != nil {
		t.Fatalf("buildSchedule: %v", err)
	}
	if spec.Kind != schedule.KindAt || spec.AtMS != time.Date(2026, 10, 21, 9, 0, 0, 0, time.Local).UnixMilli() {
		t.Fatalf("when 解析异常: %+v", spec)
	}
	if _, err := buildSchedule(scheduleArgs{When: "2026-10-19 09:00"}, now); err == nil {
		t.Fatal("过去时间应报错")
	}
	if got := describeSchedule(schedule.Schedule{Kind: schedule.KindEvery, EveryMS: 86400000}); !strings.Contains(got, "1 天") {
		t.Fatalf("周期描述异常: %q", got)
	}
	b, _ := json.Marshal(schedule.Job{ID: "x"})
	if !strings.Contains(string(b), `"id":"x"`) {
		t.Fatalf("作业序列化异常: %s", b)
	}
}

func TestScheduleToolDailyWritesTaskFile(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "好的"})
	defer srv.Close()
	rg := newRig(t, srv)
	sched := withScheduler(t, rg)
	if got := rg.dispatch(t, inbound("cli:default", "req-1", "每天 20 点汇报")); !strings.Contains(got, "好的") {
		t.Fatalf("出站内容异常: %q", got)
	}
	inst := rg.r.pool["cli:default"]
	tool := rg.r.scheduleTool(inst)
	ctx := context.Background()

	out := tool.Invoke(ctx, `{"action":"add","name":"日报","daily_at":"20:00","prompt":"跑日报脚本并汇报"}`).Text
	if !strings.Contains(out, "已创建作业") || !strings.Contains(out, "任务文件") {
		t.Fatalf("创建失败: %q", out)
	}
	jobs, _ := sched.List()
	if len(jobs) != 1 {
		t.Fatalf("作业数异常: %d", len(jobs))
	}
	j := jobs[0]
	if j.Schedule.Kind != schedule.KindDaily || j.Schedule.AtTimeMS != 20*3600*1000 {
		t.Fatalf("调度类型异常: %+v", j.Schedule)
	}
	if j.DeleteAfterRun {
		t.Fatal("周期作业不应执行后自删")
	}
	if want := rg.cfg.TaskPath(j.ID); j.Payload.File != want {
		t.Fatalf("任务文件路径 %q，期望 %q", j.Payload.File, want)
	}
	raw, err := os.ReadFile(j.Payload.File)
	if err != nil {
		t.Fatalf("任务文件未落盘: %v", err)
	}
	if string(raw) != "跑日报脚本并汇报" {
		t.Fatalf("任务文件内容异常: %q", raw)
	}
	if st, _ := os.Stat(j.Payload.File); st.Mode().Perm() != 0o600 {
		t.Fatalf("任务文件权限应为 0600: %v", st.Mode().Perm())
	}

	// 人改文件 → 触发时以文件内容为准
	if err := os.WriteFile(j.Payload.File, []byte("人改过的日报指令"), 0o600); err != nil {
		t.Fatalf("改文件: %v", err)
	}
	if _, err := sched.RunNow(ctx, j.ID); err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	last := lastMessage(t, srv)
	if !strings.Contains(last, "人改过的日报指令") {
		t.Fatalf("触发时未使用任务文件内容: %s", last)
	}

	// 任务文件丢了 → 回退作业内 prompt
	if err := os.Remove(j.Payload.File); err != nil {
		t.Fatalf("删文件: %v", err)
	}
	if _, err := sched.RunNow(ctx, j.ID); err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	last = lastMessage(t, srv)
	if !strings.Contains(last, "跑日报脚本并汇报") {
		t.Fatalf("文件缺失时应回退作业内 prompt: %s", last)
	}

	// remove 清理任务文件
	if err := os.WriteFile(j.Payload.File, []byte("x"), 0o600); err != nil {
		t.Fatalf("重建文件: %v", err)
	}
	if out := tool.Invoke(ctx, `{"action":"remove","job_id":"`+j.ID+`"}`).Text; !strings.Contains(out, "已取消作业") {
		t.Fatalf("取消失败: %q", out)
	}
	if _, err := os.Stat(j.Payload.File); !os.IsNotExist(err) {
		t.Fatalf("取消作业后应清理任务文件: %v", err)
	}
}

func TestScheduleToolDailyArgsValidation(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "好的"})
	defer srv.Close()
	rg := newRig(t, srv)
	sched := withScheduler(t, rg)
	if got := rg.dispatch(t, inbound("cli:default", "req-1", "hi")); !strings.Contains(got, "好的") {
		t.Fatalf("出站异常: %q", got)
	}
	tool := rg.r.scheduleTool(rg.r.pool["cli:default"])
	ctx := context.Background()
	cases := []struct {
		args string
		want string
	}{
		{`{"action":"add","prompt":"x","daily_at":"25:00"}`, "无法解析 daily_at"},
		{`{"action":"add","prompt":"x","daily_at":"20:00","timezone":"Nowhere/Unknown"}`, "无法解析时区"},
		{`{"action":"add","prompt":"x","daily_at":"20:00","every_seconds":60}`, "只能给一个"},
		{`{"action":"add","daily_at":"20:00"}`, "需要 prompt"},
		{`{"action":"add","prompt":"x","daily_at":"20:00","task_file":"/tmp/不存在.md"}`, "任务文件不可读"},
	}
	for _, c := range cases {
		if got := tool.Invoke(ctx, c.args).Text; !strings.Contains(got, c.want) {
			t.Fatalf("args=%s 期望含 %q，实际 %q", c.args, c.want, got)
		}
	}
	if jobs, err := sched.List(); err != nil || len(jobs) != 0 {
		t.Fatalf("非法参数不应建作业: %+v %v", jobs, err)
	}
}
