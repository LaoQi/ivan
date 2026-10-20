package schedule

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fakeDispatcher struct {
	mu    sync.Mutex
	calls []Trigger
	out   Outcome
	block chan struct{}
	fn    func(context.Context, Trigger) Outcome
}

func (f *fakeDispatcher) TriggerJob(ctx context.Context, t Trigger) Outcome {
	f.mu.Lock()
	f.calls = append(f.calls, t)
	fn, out, block := f.fn, f.out, f.block
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, t)
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return Outcome{Err: ctx.Err()}
		}
	}
	return out
}

func (f *fakeDispatcher) Calls() []Trigger {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Trigger(nil), f.calls...)
}

func (f *fakeDispatcher) Set(fn func(context.Context, Trigger) Outcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fn = fn
}

type harness struct {
	t      *testing.T
	store  *MemStore
	disp   *fakeDispatcher
	clock  *fakeClock
	sched  *Scheduler
	done   chan struct{}
	cancel context.CancelFunc
	mu     sync.Mutex
	events []Event
	paused bool
}

func newHarness(t *testing.T, mutate func(*Config)) *harness {
	t.Helper()
	h := &harness{t: t, store: NewMemStore(), disp: &fakeDispatcher{}, clock: newClock(), done: make(chan struct{})}
	cfg := Config{
		Now: h.clock.Now,
		Eventf: func(e Event) {
			h.mu.Lock()
			h.events = append(h.events, e)
			h.mu.Unlock()
		},
		Paused: func() bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.paused
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := New(h.store, h.disp, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.sched = s
	t.Cleanup(func() { h.stop() })
	return h
}

func (h *harness) start() {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		_ = h.sched.Run(ctx)
		close(h.done)
	}()
	select {
	case <-h.sched.Ready():
	case <-time.After(5 * time.Second):
		h.t.Fatal("调度器未就绪")
	}
}

func (h *harness) stop() {
	if h.cancel == nil {
		return
	}
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		h.t.Fatal("调度器未在超时内停止")
	}
	h.cancel = nil
}

func (h *harness) setPaused(v bool) {
	h.mu.Lock()
	h.paused = v
	h.mu.Unlock()
	h.sched.Wake()
}

func (h *harness) job(id string) Job {
	h.t.Helper()
	j, ok, err := h.store.Get(id)
	if err != nil || !ok {
		h.t.Fatalf("读取作业 %s: ok=%v err=%v", id, ok, err)
	}
	return j
}

func (h *harness) waitRuns(id string, n int) Job {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if j, ok, _ := h.store.Get(id); ok && j.State.Runs >= n {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _, _ := h.store.Get(id)
	h.t.Fatalf("作业 %s 未跑到 %d 次: %+v", id, n, j.State)
	return Job{}
}

func (h *harness) eventKinds() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.events))
	for _, e := range h.events {
		out = append(out, e.Kind)
	}
	return out
}

func everyJob(id string, every time.Duration) Job {
	return Job{
		ID:      id,
		Name:    "周期作业",
		Enabled: true,
		Schedule: Schedule{
			Kind:    KindEvery,
			EveryMS: every.Milliseconds(),
		},
		Payload: Payload{Prompt: "汇报一下", Channel: "qq", ChatID: "u1", SessionKey: "qq:c2c:u1"},
	}
}

func atJob(id string, at time.Time) Job {
	return Job{
		ID:      id,
		Name:    "一次性作业",
		Enabled: true,
		Schedule: Schedule{
			Kind: KindAt,
			AtMS: at.UnixMilli(),
		},
		Payload: Payload{Prompt: "提醒喝水", Channel: "qq", ChatID: "u1", SessionKey: "qq:c2c:u1"},
	}
}

func TestAddEveryRunsAndAdvances(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.MissedPolicy = MissedRun })
	h.disp.out = Outcome{Tokens: 42}
	job, err := h.sched.Add(everyJob("", 60*time.Second))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	h.start()
	if want := h.clock.Now().Add(60 * time.Second).UnixMilli(); job.State.NextRunAtMS != want {
		t.Fatalf("首次计划时间 %d，期望 %d", job.State.NextRunAtMS, want)
	}
	h.clock.Advance(60 * time.Second)
	h.sched.Wake()
	j := h.waitRuns(job.ID, 1)
	if j.State.LastStatus != StatusOK {
		t.Fatalf("状态 %q，期望 ok", j.State.LastStatus)
	}
	if j.State.LastTokens != 42 {
		t.Fatalf("tokens=%d，期望 42", j.State.LastTokens)
	}
	if want := h.clock.Now().Add(60 * time.Second).UnixMilli(); j.State.NextRunAtMS != want {
		t.Fatalf("下一次 %d，期望 %d", j.State.NextRunAtMS, want)
	}
	calls := h.disp.Calls()
	if len(calls) != 1 || calls[0].JobID != job.ID || calls[0].SessionKey != "qq:c2c:u1" || calls[0].Channel != "qq" {
		t.Fatalf("执行调用异常: %+v", calls)
	}
}

func TestJobSerialAndNoPileUp(t *testing.T) {
	h := newHarness(t, nil)
	block := make(chan struct{})
	h.disp.block = block
	job, err := h.sched.Add(everyJob("", 30*time.Second))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	h.start()
	h.clock.Advance(30 * time.Second)
	h.sched.Wake()
	deadline := time.Now().Add(3 * time.Second)
	for len(h.disp.Calls()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(h.disp.Calls()) != 1 {
		t.Fatalf("首次未触发: %d", len(h.disp.Calls()))
	}
	h.clock.Advance(120 * time.Second)
	h.sched.Wake()
	time.Sleep(100 * time.Millisecond)
	if len(h.disp.Calls()) != 1 {
		t.Fatalf("执行中不应重入，实际触发 %d 次", len(h.disp.Calls()))
	}
	close(block)
	j := h.waitRuns(job.ID, 1)
	if next := j.State.NextRunAtMS; next <= h.clock.Now().UnixMilli() {
		t.Fatalf("下一次未推进到未来: %d", next)
	}
	kinds := h.eventKinds()
	found := false
	for _, k := range kinds {
		if k == EventMissed {
			found = true
		}
	}
	if !found {
		t.Fatalf("长作业应记录错过周期: %v", kinds)
	}
}

func TestAtJobRunsOnceAndDeletes(t *testing.T) {
	h := newHarness(t, nil)
	h.disp.out = Outcome{Tokens: 1}
	j := atJob("", h.clock.Now().Add(10*time.Second))
	j.DeleteAfterRun = true
	job, err := h.sched.Add(j)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	h.start()
	h.clock.Advance(10 * time.Second)
	h.sched.Wake()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok, _ := h.store.Get(job.ID); !ok {
			if calls := h.disp.Calls(); len(calls) != 1 {
				t.Fatalf("执行次数 %d，期望 1", len(calls))
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("一次性作业执行后未删除: %+v", h.job(job.ID))
}

func TestMissedPolicy(t *testing.T) {
	t.Run("skip", func(t *testing.T) {
		h := newHarness(t, func(c *Config) { c.MissedPolicy = MissedSkip })
		job := atJob("j1", h.clock.Now().Add(-time.Minute))
		job.State.NextRunAtMS = job.Schedule.AtMS
		if err := h.store.Put(job); err != nil {
			t.Fatalf("Put: %v", err)
		}
		h.start()
		time.Sleep(100 * time.Millisecond)
		if calls := h.disp.Calls(); len(calls) != 0 {
			t.Fatalf("skip 策略不应补跑: %d", len(calls))
		}
		got := h.job("j1")
		if got.State.LastStatus != StatusMissed || got.State.NextRunAtMS != 0 {
			t.Fatalf("错过状态异常: %+v", got.State)
		}
	})
	t.Run("run", func(t *testing.T) {
		h := newHarness(t, func(c *Config) { c.MissedPolicy = MissedRun })
		job := atJob("j2", h.clock.Now().Add(-time.Minute))
		job.State.NextRunAtMS = job.Schedule.AtMS
		if err := h.store.Put(job); err != nil {
			t.Fatalf("Put: %v", err)
		}
		h.start()
		h.waitRuns("j2", 1)
	})
}

func TestEveryMissedAtStartupAdvances(t *testing.T) {
	h := newHarness(t, nil)
	job := everyJob("j3", time.Minute)
	job.State.NextRunAtMS = h.clock.Now().Add(-5 * time.Minute).UnixMilli()
	if err := h.store.Put(job); err != nil {
		t.Fatalf("Put: %v", err)
	}
	h.start()
	time.Sleep(100 * time.Millisecond)
	if calls := h.disp.Calls(); len(calls) != 0 {
		t.Fatalf("周期作业不应补跑: %d", len(calls))
	}
	got := h.job("j3")
	if got.State.NextRunAtMS <= h.clock.Now().UnixMilli() {
		t.Fatalf("下一次未推进: %d", got.State.NextRunAtMS)
	}
	st := h.sched.Status()
	if st.Missed == 0 {
		t.Fatal("错过计数未累加")
	}
}

func TestPauseHoldsAtJob(t *testing.T) {
	h := newHarness(t, nil)
	h.setPaused(true)
	job, err := h.sched.Add(atJob("", h.clock.Now().Add(time.Second)))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	h.start()
	h.clock.Advance(2 * time.Second)
	h.sched.Wake()
	time.Sleep(150 * time.Millisecond)
	if calls := h.disp.Calls(); len(calls) != 0 {
		t.Fatalf("暂停期间不应触发: %d", len(calls))
	}
	h.setPaused(false)
	h.waitRuns(job.ID, 1)
}

func TestBudgetBlocksAndDefers(t *testing.T) {
	var mu sync.Mutex
	tokens := 0
	h := newHarness(t, func(c *Config) {
		c.Budget = func() (bool, string) {
			mu.Lock()
			defer mu.Unlock()
			if tokens >= 10 {
				return false, "当日 token 预算 10 已用尽"
			}
			return true, ""
		}
	})
	h.disp.Set(func(context.Context, Trigger) Outcome {
		mu.Lock()
		tokens += 10
		mu.Unlock()
		return Outcome{Tokens: 10}
	})
	first, err := h.sched.Add(everyJob("a", time.Minute))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	second, err := h.sched.Add(atJob("b", h.clock.Now().Add(2*time.Minute)))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	h.start()
	h.clock.Advance(time.Minute)
	h.sched.Wake()
	h.waitRuns(first.ID, 1)
	h.clock.Advance(time.Minute)
	h.sched.Wake()
	time.Sleep(150 * time.Millisecond)
	if calls := h.disp.Calls(); len(calls) != 1 {
		t.Fatalf("预算用尽后不应触发: %d", len(calls))
	}
	got := h.job(second.ID)
	if got.State.NextRunAtMS != second.State.NextRunAtMS {
		t.Fatalf("一次性作业应保留待跑: %+v", got.State)
	}
	if got := h.job(first.ID); got.State.NextRunAtMS <= h.clock.Now().UnixMilli() {
		t.Fatalf("周期作业应跳过而非堆积: %+v", got.State)
	}
	if st := h.sched.Status(); !strings.Contains(st.LastError, "预算") {
		t.Fatalf("状态未反映预算用尽: %+v", st)
	}
}

func TestFailureRecordedAndStatus(t *testing.T) {
	h := newHarness(t, nil)
	h.disp.Set(func(context.Context, Trigger) Outcome {
		return Outcome{Err: errors.New("模型超时"), NotifyErr: "通道未注册"}
	})
	job, err := h.sched.Add(atJob("", h.clock.Now().Add(time.Second)))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	h.start()
	h.clock.Advance(2 * time.Second)
	h.sched.Wake()
	got := h.waitRuns(job.ID, 1)
	if got.State.LastStatus != StatusFailed || got.State.LastError == "" {
		t.Fatalf("失败状态未记录: %+v", got.State)
	}
	if got.State.LastNotifyError != "通道未注册" {
		t.Fatalf("通知失败未记录: %+v", got.State)
	}
	if st := h.sched.Status(); st.Failures == 0 || st.LastError == "" {
		t.Fatalf("状态未反映失败: %+v", st)
	}
}

func TestRunNowDoesNotResetSchedule(t *testing.T) {
	h := newHarness(t, nil)
	h.disp.out = Outcome{Tokens: 3}
	job, err := h.sched.Add(everyJob("", time.Hour))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	h.start()
	if _, err := h.sched.RunNow(context.Background(), job.ID); err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	got := h.job(job.ID)
	if got.State.NextRunAtMS != job.State.NextRunAtMS {
		t.Fatalf("手动触发不应改计划: %d != %d", got.State.NextRunAtMS, job.State.NextRunAtMS)
	}
	if got.State.Runs != 1 {
		t.Fatalf("runs=%d", got.State.Runs)
	}
}

func TestUpdateAndRemove(t *testing.T) {
	h := newHarness(t, nil)
	h.start()
	job, err := h.sched.Add(everyJob("", time.Hour))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	updated, err := h.sched.Update(job.ID, func(j *Job) error {
		j.Name = "改名"
		j.Payload.Prompt = "新的提示"
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Name != "改名" || updated.Payload.Prompt != "新的提示" {
		t.Fatalf("更新未生效: %+v", updated)
	}
	if updated.State.NextRunAtMS != job.State.NextRunAtMS {
		t.Fatal("仅改文案不应重算计划时间")
	}
	if _, err := h.sched.Update(job.ID, func(j *Job) error {
		j.Schedule = Schedule{Kind: KindEvery, EveryMS: 2 * time.Hour.Milliseconds()}
		return nil
	}); err != nil {
		t.Fatalf("Update 周期: %v", err)
	}
	if err := h.sched.Remove(job.ID); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok, _ := h.store.Get(job.ID); ok {
		t.Fatal("删除后仍能读到作业")
	}
	if err := h.sched.Remove("missing"); err == nil {
		t.Fatal("删除不存在的作业应报错")
	}
}

func TestInterruptedNormalizedOnStart(t *testing.T) {
	h := newHarness(t, nil)
	job := everyJob("j4", time.Hour)
	job.State.NextRunAtMS = h.clock.Now().Add(time.Hour).UnixMilli()
	job.State.LastStatus = StatusRunning
	if err := h.store.Put(job); err != nil {
		t.Fatalf("Put: %v", err)
	}
	h.start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := h.job("j4"); got.State.LastStatus == StatusInterrupted {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("running 未归一为 interrupted: %+v", h.job("j4").State)
}
