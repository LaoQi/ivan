package daemon

import (
	"testing"
	"time"

	"github.com/LaoQi/ivan/channel/cli"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/internal/fakellm"
	"github.com/LaoQi/ivan/schedule"
	sqlitestore "github.com/LaoQi/ivan/schedule/sqlite"
)

func seedJob(t *testing.T, cfg *config.Config, j schedule.Job) {
	t.Helper()
	store, err := sqlitestore.Open(cfg.JobsDBPath())
	if err != nil {
		t.Fatalf("打开作业库: %v", err)
	}
	defer store.Close()
	if err := store.Put(j); err != nil {
		t.Fatalf("写入作业: %v", err)
	}
}

func readJob(t *testing.T, cfg *config.Config, id string) (schedule.Job, bool) {
	t.Helper()
	store, err := sqlitestore.Open(cfg.JobsDBPath())
	if err != nil {
		t.Fatalf("打开作业库: %v", err)
	}
	defer store.Close()
	j, ok, err := store.Get(id)
	if err != nil {
		t.Fatalf("读取作业: %v", err)
	}
	return j, ok
}

func TestScheduleJobTriggersAndPersists(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "该喝水了", Usage: fakellm.Usage{TotalTokens: 7}})
	defer srv.Close()
	cfg := testConfig(t, srv)
	now := time.Now()
	seedJob(t, cfg, schedule.Job{
		ID:          "job_e2e",
		Name:        "喝水",
		Enabled:     true,
		Schedule:    schedule.Schedule{Kind: schedule.KindEvery, EveryMS: int64(time.Second / time.Millisecond)},
		Payload:     schedule.Payload{Prompt: "提醒我喝水", Channel: cli.Name, ChatID: "tty", SessionKey: "cli:default"},
		CreatedAtMS: now.UnixMilli(),
		State:       schedule.State{NextRunAtMS: now.Add(300 * time.Millisecond).UnixMilli()},
	})

	r := start(t, cfg)
	deadline := time.Now().Add(15 * time.Second)
	var ran schedule.Job
	for time.Now().Before(deadline) {
		j, ok := readJob(t, cfg, "job_e2e")
		if ok && j.State.Runs >= 1 {
			ran = j
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ran.State.Runs < 1 {
		t.Fatalf("作业未触发: %+v", ran)
	}
	if ran.State.LastStatus != schedule.StatusOK {
		t.Fatalf("作业状态 %q，期望 ok（err=%q）", ran.State.LastStatus, ran.State.LastError)
	}
	if ran.State.NextRunAtMS <= time.Now().UnixMilli()-1000 {
		t.Fatalf("下一次未推进: %d", ran.State.NextRunAtMS)
	}
	if ran.State.LastNotifyError != "" {
		t.Fatalf("无目标按 unmatched 计数，不应算通知失败: %q", ran.State.LastNotifyError)
	}
	unmatched := 0
	for _, c := range r.d.mgr.Status() {
		if c.Name == cli.Name {
			unmatched = c.Unmatched
		}
	}
	if unmatched == 0 {
		t.Fatal("cli 目标没有待接连接，出站应计 unmatched")
	}

	st := r.d.Status()
	if st.Schedule == nil {
		t.Fatal("status 未包含 schedule 段")
	}
	if st.Schedule.Jobs != 1 || st.Schedule.Triggers == 0 {
		t.Fatalf("status schedule 异常: %+v", *st.Schedule)
	}
	if st.ScheduleError != "" {
		t.Fatalf("schedule 错误: %s", st.ScheduleError)
	}

	if raw, err := r.d.Control(cli.OpJobsList, ""); err != nil {
		t.Fatalf("jobs/list: %v", err)
	} else if jobs, ok := raw.([]schedule.Job); !ok || len(jobs) != 1 {
		t.Fatalf("jobs/list 返回异常: %T %+v", raw, raw)
	}

	if _, err := r.d.Control(cli.OpPause, ""); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if !r.d.paused() {
		t.Fatal("暂停开关未生效")
	}
	if _, err := r.d.Control(cli.OpResume, ""); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if r.d.paused() {
		t.Fatal("恢复开关未生效")
	}

	r.stop(t)

	again, ok := readJob(t, cfg, "job_e2e")
	if !ok {
		t.Fatal("重启前作业丢失")
	}
	if again.State.Runs < 1 || again.State.NextRunAtMS == 0 {
		t.Fatalf("停机后作业状态异常: %+v", again.State)
	}

	srv2 := fakellm.New(fakellm.Step{Content: "又到点了"})
	defer srv2.Close()
	cfg.BaseURL = srv2.BaseURL()
	r2 := start(t, cfg)
	if st := r2.d.Status(); st.Schedule == nil || st.Schedule.Jobs != 1 {
		t.Fatalf("重启后作业未恢复: %+v", st.Schedule)
	}
	r2.stop(t)
}

func TestScheduleDisabledAndPauseControl(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "x"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	cfg.Schedule.Enabled = false
	d, err := New(cfg, "test", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Release()
	if d.sched != nil || d.jobs != nil {
		t.Fatal("关闭开关后不应装配调度器")
	}
	if _, err := d.Control(cli.OpJobsList, ""); err == nil {
		t.Fatal("未启用时 jobs/list 应报错")
	}
}

func TestModelCreatesJobThroughTool(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{Name: "schedule", Args: `{"action":"add","name":"喝水","at_seconds":2,"prompt":"提醒我喝水","delete_after_run":false}`}}, Usage: fakellm.Usage{TotalTokens: 5}},
		fakellm.Step{Content: "好的，等会儿提醒你喝水"},
		fakellm.Step{Content: "到点了：该喝水了", Usage: fakellm.Usage{TotalTokens: 6}},
	)
	defer srv.Close()
	cfg := testConfig(t, srv)
	r := start(t, cfg)
	defer r.stop(t)

	reply, err := cli.Send(cfg.SocketPath(), "2 秒后提醒我喝水", "", 10*time.Second)
	if err != nil || reply != "好的，等会儿提醒你喝水" {
		t.Fatalf("send 往返失败: %q %v", reply, err)
	}
	jobs, err := r.d.jobs.List()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("模型未建出作业: %+v %v", jobs, err)
	}
	job := jobs[0]
	if job.Payload.Channel != cli.Name || job.Payload.SessionKey != "cli:default" || job.Payload.ChatID == "" {
		t.Fatalf("作业目标未继承当前会话: %+v", job.Payload)
	}
	if job.Schedule.Kind != schedule.KindAt {
		t.Fatalf("作业类型异常: %+v", job.Schedule)
	}

	deadline := time.Now().Add(15 * time.Second)
	var ran schedule.Job
	for time.Now().Before(deadline) {
		cur, ok, err := r.d.jobs.Get(job.ID)
		if err != nil {
			t.Fatalf("读取作业: %v", err)
		}
		if ok && cur.State.Runs >= 1 {
			ran = cur
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ran.State.Runs < 1 {
		t.Fatalf("模型创建的作业未触发: %+v", ran.State)
	}
	if ran.State.LastStatus != schedule.StatusOK {
		t.Fatalf("触发结果 %q（err=%q）", ran.State.LastStatus, ran.State.LastError)
	}
	if st := r.d.Status(); st.Schedule == nil || st.Schedule.Triggers < 1 {
		t.Fatalf("status 未反映触发: %+v", st.Schedule)
	}
}
