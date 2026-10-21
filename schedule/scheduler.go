package schedule

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	EventTrigger     = "job_trigger"
	EventDone        = "job_done"
	EventFailed      = "job_failed"
	EventMissed      = "job_missed"
	EventSkipped     = "job_skipped"
	EventPaused      = "job_paused"
	EventBudget      = "job_budget"
	EventInterrupted = "job_interrupted"
	EventStore       = "job_store"
)

const (
	MissedRun  = "run"
	MissedSkip = "skip"
)

const (
	DefaultJobTimeout = 15 * time.Minute
	idleWait          = 60 * time.Second
	errorWait         = 5 * time.Second
	stopWait          = 5 * time.Second
)

type Trigger struct {
	JobID      string
	Name       string
	Prompt     string
	File       string
	Channel    string
	ChatID     string
	SessionKey string
}

type Outcome struct {
	Tokens    int
	NotifyErr string
	Err       error
}

type Dispatcher interface {
	TriggerJob(ctx context.Context, t Trigger) Outcome
}

type DispatcherFunc func(ctx context.Context, t Trigger) Outcome

func (f DispatcherFunc) TriggerJob(ctx context.Context, t Trigger) Outcome { return f(ctx, t) }

type Event struct {
	At     time.Time
	Kind   string
	JobID  string
	Name   string
	Text   string
	Tokens int
	Dur    time.Duration
}

type Config struct {
	MissedPolicy string
	JobTimeout   time.Duration
	Paused       func() bool
	Budget       func() (bool, string)
	Now          func() time.Time
	Eventf       func(Event)
}

type Status struct {
	Jobs        int       `json:"jobs"`
	Enabled     int       `json:"enabled"`
	Paused      bool      `json:"paused"`
	Running     int       `json:"running"`
	NextRunAt   time.Time `json:"next_run_at,omitempty"`
	Triggers    uint64    `json:"triggers"`
	Failures    uint64    `json:"failures"`
	Missed      uint64    `json:"missed"`
	LastError   string    `json:"last_error,omitempty"`
	LastErrorAt time.Time `json:"last_error_at,omitempty"`
}

type Scheduler struct {
	store Store
	disp  Dispatcher
	cfg   Config

	wake  chan struct{}
	ready chan struct{}
	wg    sync.WaitGroup

	mu        sync.Mutex
	running   map[string]int64
	deferred  map[string]string
	triggers  uint64
	failures  uint64
	missed    uint64
	lastErr   string
	lastErrAt time.Time
}

func New(store Store, disp Dispatcher, cfg Config) (*Scheduler, error) {
	if store == nil {
		return nil, errors.New("缺少作业存储")
	}
	if disp == nil {
		return nil, errors.New("缺少作业执行器")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = DefaultJobTimeout
	}
	switch strings.ToLower(strings.TrimSpace(cfg.MissedPolicy)) {
	case "", MissedRun:
		cfg.MissedPolicy = MissedRun
	case MissedSkip:
		cfg.MissedPolicy = MissedSkip
	default:
		return nil, fmt.Errorf("未知的错过策略 %q（run/skip）", cfg.MissedPolicy)
	}
	return &Scheduler{
		store:    store,
		disp:     disp,
		cfg:      cfg,
		wake:     make(chan struct{}, 1),
		ready:    make(chan struct{}),
		running:  map[string]int64{},
		deferred: map[string]string{},
	}, nil
}

func (s *Scheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Ready() <-chan struct{} { return s.ready }

func (s *Scheduler) Run(ctx context.Context) error {
	s.reconcile()
	close(s.ready)
	defer s.waitRuns()
	for {
		if ctx.Err() != nil {
			return nil
		}
		due, wait, err := s.plan()
		if err != nil {
			s.fail(err, EventStore, "读取作业失败")
			wait = errorWait
		}
		if len(due) > 0 {
			for _, j := range due {
				s.launch(ctx, j)
			}
			continue
		}
		if !s.sleep(ctx, wait) {
			return nil
		}
	}
}

func (s *Scheduler) waitRuns() {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(stopWait):
		s.emit(Event{Kind: EventStore, Text: fmt.Sprintf("调度器停机等待执行中超时 %s", stopWait)})
	}
}

func (s *Scheduler) sleep(ctx context.Context, d time.Duration) bool {
	if d < time.Second {
		d = time.Second
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (s *Scheduler) reconcile() {
	jobs, err := s.store.List()
	if err != nil {
		s.fail(err, EventStore, "读取作业失败")
		return
	}
	now := s.now()
	nowMS := MS(now)
	for _, j := range jobs {
		dirty := j.State.LastStatus == StatusRunning
		if dirty {
			j.State.LastStatus = StatusInterrupted
			s.emit(Event{Kind: EventInterrupted, JobID: j.ID, Name: j.Label(), Text: "上次执行未收尾（进程退出），已标记中断"})
		}
		if j.Enabled && !j.Done() {
			if verr := j.Validate(); verr != nil {
				s.emit(Event{Kind: EventStore, JobID: j.ID, Name: j.Label(), Text: "作业无效，已跳过: " + verr.Error()})
				if dirty {
					s.put(j, nowMS)
				}
				continue
			}
			before := j.State.NextRunAtMS
			j.InitNext(now)
			if j.State.NextRunAtMS != before {
				dirty = true
			}
			if next := j.State.NextRunAtMS; next > 0 && next <= nowMS {
				switch j.Schedule.Kind {
				case KindAt:
					if s.cfg.MissedPolicy == MissedSkip {
						j.State.LastStatus = StatusMissed
						j.State.NextRunAtMS = 0
						dirty = true
						s.countMissed(1)
						s.emit(Event{Kind: EventMissed, JobID: j.ID, Name: j.Label(), Text: "停机期间到期，按 skip 策略放弃"})
					}
				case KindEvery:
					next, missed := skipEvery(next, j.Schedule.EveryMS, nowMS)
					j.State.NextRunAtMS = next
					j.State.LastStatus = StatusMissed
					dirty = true
					s.countMissed(uint64(missed))
					s.emit(Event{Kind: EventMissed, JobID: j.ID, Name: j.Label(),
						Text: fmt.Sprintf("停机期间错过 %d 个周期，下次 %s", missed, FromMS(next).Format("01-02 15:04:05"))})
				case KindDaily:
					next, missed := SkipDaily(next, j.Schedule.AtTimeMS, j.Schedule.TZ, nowMS)
					j.State.NextRunAtMS = next
					j.State.LastStatus = StatusMissed
					dirty = true
					s.countMissed(uint64(missed))
					s.emit(Event{Kind: EventMissed, JobID: j.ID, Name: j.Label(),
						Text: fmt.Sprintf("停机期间错过 %d 天，下次 %s", missed, FromMS(next).Format("01-02 15:04:05"))})
				}
			}
		}
		if dirty {
			s.put(j, nowMS)
		}
	}
}

func (s *Scheduler) plan() ([]Job, time.Duration, error) {
	jobs, err := s.store.List()
	if err != nil {
		return nil, 0, err
	}
	nowMS := s.nowMS()
	blocked := s.blockedReason()
	var (
		due  []Job
		next int64
	)
	for _, j := range jobs {
		if !j.Enabled || j.Done() {
			continue
		}
		if s.inFlight(j.ID) {
			continue
		}
		n := j.State.NextRunAtMS
		if n <= 0 {
			continue
		}
		if n > nowMS {
			if next == 0 || n < next {
				next = n
			}
			continue
		}
		if blocked != "" {
			s.deferDue(j, blocked, nowMS)
			continue
		}
		due = append(due, j)
	}
	if len(due) > 0 {
		return due, 0, nil
	}
	if blocked != "" && next == 0 {
		return nil, idleWait, nil
	}
	if next == 0 {
		return nil, idleWait, nil
	}
	return nil, time.Duration(next-nowMS) * time.Millisecond, nil
}

func (s *Scheduler) deferDue(j Job, reason string, nowMS int64) {
	if kind := s.deferMark(j.ID, reason); kind {
		switch j.Schedule.Kind {
		case KindAt:
			s.emit(Event{Kind: EventPaused, JobID: j.ID, Name: j.Label(), Text: reason + "，一次性作业待恢复后补跑"})
		case KindEvery:
			next, missed := skipEvery(j.State.NextRunAtMS, j.Schedule.EveryMS, nowMS)
			j.State.NextRunAtMS = next
			j.UpdatedAtMS = nowMS
			s.put(j, nowMS)
			s.countMissed(uint64(missed))
			s.emit(Event{Kind: EventPaused, JobID: j.ID, Name: j.Label(),
				Text: fmt.Sprintf("%s，周期作业跳过 %d 次，下次 %s", reason, missed, FromMS(next).Format("01-02 15:04:05"))})
		case KindDaily:
			next, missed := SkipDaily(j.State.NextRunAtMS, j.Schedule.AtTimeMS, j.Schedule.TZ, nowMS)
			j.State.NextRunAtMS = next
			j.UpdatedAtMS = nowMS
			s.put(j, nowMS)
			s.countMissed(uint64(missed))
			s.emit(Event{Kind: EventPaused, JobID: j.ID, Name: j.Label(),
				Text: fmt.Sprintf("%s，周期作业跳过 %d 天，下次 %s", reason, missed, FromMS(next).Format("01-02 15:04:05"))})
		}
	}
}

func (s *Scheduler) deferMark(id, reason string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deferred[id] == reason {
		return false
	}
	s.deferred[id] = reason
	return true
}

func (s *Scheduler) launch(ctx context.Context, j Job) {
	planned := j.State.NextRunAtMS
	s.markRunning(j.ID, planned)
	s.clearDeferred(j.ID)
	j.State.LastStatus = StatusRunning
	j.UpdatedAtMS = s.nowMS()
	s.put(j, j.UpdatedAtMS)
	s.mu.Lock()
	s.triggers++
	s.mu.Unlock()
	s.emit(Event{Kind: EventTrigger, JobID: j.ID, Name: j.Label(), Text: oneLine(j.Payload.Prompt, 120)})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		start := s.now()
		rctx, cancel := context.WithTimeout(ctx, s.cfg.JobTimeout)
		defer cancel()
		out := s.disp.TriggerJob(rctx, Trigger{
			JobID:      j.ID,
			Name:       j.Label(),
			Prompt:     j.Payload.Prompt,
			File:       j.Payload.File,
			Channel:    j.Payload.Channel,
			ChatID:     j.Payload.ChatID,
			SessionKey: j.Payload.SessionKey,
		})
		s.finish(j, planned, out, s.now().Sub(start), false)
	}()
}

func (s *Scheduler) finish(base Job, planned int64, out Outcome, dur time.Duration, manual bool) {
	defer s.clearRunning(base.ID)
	now := s.now()
	nowMS := MS(now)
	cur, ok, err := s.store.Get(base.ID)
	if err != nil {
		s.fail(err, EventStore, "读取作业失败（执行结果未落盘）")
		s.Wake()
		return
	}
	if !ok {
		s.emit(Event{Kind: EventDone, JobID: base.ID, Name: base.Label(), Text: "作业已被删除，执行结果丢弃", Dur: dur})
		s.Wake()
		return
	}
	status := StatusOK
	errText := ""
	if out.Err != nil {
		status = StatusFailed
		errText = brief(out.Err.Error())
	}
	cur.State.Runs++
	cur.State.LastRunAtMS = nowMS
	cur.State.LastStatus = status
	cur.State.LastError = errText
	cur.State.LastNotifyError = brief(out.NotifyErr)
	cur.State.LastTokens = out.Tokens
	switch {
	case cur.Schedule.Kind == KindAt:
		if !manual || planned <= nowMS {
			cur.State.NextRunAtMS = 0
		}
	case manual && planned > nowMS:
		cur.State.NextRunAtMS = planned
	case cur.Schedule.Kind == KindDaily:
		next, missed := NextDaily(planned, cur.Schedule.AtTimeMS, cur.Schedule.TZ, nowMS)
		cur.State.NextRunAtMS = next
		if missed > 0 {
			s.countMissed(uint64(missed))
			s.emit(Event{Kind: EventMissed, JobID: cur.ID, Name: cur.Label(),
				Text: fmt.Sprintf("本轮耗时 %s，错过 %d 天，下次 %s", dur.Round(time.Second), missed, FromMS(next).Format("01-02 15:04:05"))})
		}
	default:
		next, missed := nextEvery(planned, cur.Schedule.EveryMS, nowMS)
		if next <= nowMS {
			next = nowMS + cur.Schedule.EveryMS
		}
		cur.State.NextRunAtMS = next
		if missed > 0 {
			s.countMissed(uint64(missed))
			s.emit(Event{Kind: EventMissed, JobID: cur.ID, Name: cur.Label(),
				Text: fmt.Sprintf("本轮耗时 %s，错过 %d 个周期，下次 %s", dur.Round(time.Second), missed, FromMS(next).Format("01-02 15:04:05"))})
		}
	}
	cur.UpdatedAtMS = nowMS
	if cur.DeleteAfterRun && cur.Schedule.Kind == KindAt && !manual {
		if derr := s.store.Delete(cur.ID); derr != nil {
			s.fail(derr, EventStore, "删除一次性作业失败")
		}
	} else {
		s.put(cur, nowMS)
	}
	if out.Err != nil {
		s.fail(out.Err, EventFailed, fmt.Sprintf("作业执行失败 job=%s", cur.Label()))
	} else {
		text := fmt.Sprintf("完成 runs=%d tokens=%d 耗时 %s", cur.State.Runs, out.Tokens, dur.Round(time.Second))
		if manual {
			text += "（手动触发）"
		}
		if out.NotifyErr != "" {
			text += " 通知失败: " + brief(out.NotifyErr)
		}
		s.emit(Event{Kind: EventDone, JobID: cur.ID, Name: cur.Label(), Text: text, Tokens: out.Tokens, Dur: dur})
	}
	s.Wake()
}

func (s *Scheduler) Add(j Job) (Job, error) {
	now := s.now()
	if j.ID == "" {
		j.ID = NewID(now)
	}
	if j.CreatedAtMS <= 0 {
		j.CreatedAtMS = MS(now)
	}
	j.UpdatedAtMS = MS(now)
	j.State = State{}
	j.InitNext(now)
	if err := j.Validate(); err != nil {
		return Job{}, err
	}
	if err := s.store.Put(j); err != nil {
		return Job{}, err
	}
	s.Wake()
	return j, nil
}

func (s *Scheduler) Update(id string, fn func(*Job) error) (Job, error) {
	cur, ok, err := s.store.Get(id)
	if err != nil {
		return Job{}, err
	}
	if !ok {
		return Job{}, fmt.Errorf("作业不存在: %s", id)
	}
	next := cur
	if err := fn(&next); err != nil {
		return Job{}, err
	}
	next.ID = cur.ID
	next.CreatedAtMS = cur.CreatedAtMS
	next.State = cur.State
	next.UpdatedAtMS = MS(s.now())
	if next.Schedule.AtMS != cur.Schedule.AtMS || next.Schedule.EveryMS != cur.Schedule.EveryMS ||
		next.Schedule.AtTimeMS != cur.Schedule.AtTimeMS || next.Schedule.TZ != cur.Schedule.TZ || next.Schedule.Kind != cur.Schedule.Kind {
		next.State.NextRunAtMS = 0
		next.State.LastRunAtMS = 0
		next.InitNext(s.now())
	}
	if err := next.Validate(); err != nil {
		return Job{}, err
	}
	if err := s.store.Put(next); err != nil {
		return Job{}, err
	}
	s.Wake()
	return next, nil
}

func (s *Scheduler) Remove(id string) error {
	if _, ok, err := s.store.Get(id); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("作业不存在: %s", id)
	}
	err := s.store.Delete(id)
	s.Wake()
	return err
}

func (s *Scheduler) Get(id string) (Job, bool, error) { return s.store.Get(id) }

func (s *Scheduler) List() ([]Job, error) { return s.store.List() }

func (s *Scheduler) RunNow(ctx context.Context, id string) (Job, error) {
	j, ok, err := s.store.Get(id)
	if err != nil {
		return Job{}, err
	}
	if !ok {
		return Job{}, fmt.Errorf("作业不存在: %s", id)
	}
	if s.inFlight(id) {
		return Job{}, fmt.Errorf("作业正在执行中: %s", id)
	}
	planned := j.State.NextRunAtMS
	if planned <= 0 {
		planned = s.nowMS()
	}
	s.markRunning(id, planned)
	rctx, cancel := context.WithTimeout(ctx, s.cfg.JobTimeout)
	defer cancel()
	start := s.now()
	out := s.disp.TriggerJob(rctx, Trigger{
		JobID:      j.ID,
		Name:       j.Label(),
		Prompt:     j.Payload.Prompt,
		File:       j.Payload.File,
		Channel:    j.Payload.Channel,
		ChatID:     j.Payload.ChatID,
		SessionKey: j.Payload.SessionKey,
	})
	s.finish(j, planned, out, s.now().Sub(start), true)
	if out.Err != nil {
		return j, out.Err
	}
	return j, nil
}

func (s *Scheduler) Status() Status {
	st := Status{Paused: s.paused()}
	s.mu.Lock()
	st.Triggers = s.triggers
	st.Failures = s.failures
	st.Missed = s.missed
	st.Running = len(s.running)
	st.LastError = s.lastErr
	st.LastErrorAt = s.lastErrAt
	s.mu.Unlock()
	jobs, err := s.store.List()
	if err != nil {
		st.LastError = err.Error()
		st.LastErrorAt = s.now()
		return st
	}
	st.Jobs = len(jobs)
	var next int64
	for _, j := range jobs {
		if j.Enabled {
			st.Enabled++
		}
		if !j.Enabled || j.Done() {
			continue
		}
		if n := j.State.NextRunAtMS; n > 0 && (next == 0 || n < next) {
			next = n
		}
	}
	if next > 0 {
		st.NextRunAt = FromMS(next)
	}
	if allow, reason := s.budget(); !allow {
		st.LastError = reason
		st.LastErrorAt = s.now()
	}
	return st
}

func (s *Scheduler) paused() bool { return s.cfg.Paused != nil && s.cfg.Paused() }

func (s *Scheduler) blockedReason() string {
	if s.paused() {
		return "暂停中"
	}
	if allow, reason := s.budget(); !allow {
		return reason
	}
	return ""
}

func (s *Scheduler) budget() (bool, string) {
	if s.cfg.Budget == nil {
		return true, ""
	}
	allow, reason := s.cfg.Budget()
	if !allow && strings.TrimSpace(reason) == "" {
		reason = "预算已用尽"
	}
	return allow, reason
}

func (s *Scheduler) inFlight(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.running[id]
	return ok
}

func (s *Scheduler) markRunning(id string, planned int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running[id] = planned
}

func (s *Scheduler) clearRunning(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.running, id)
}

func (s *Scheduler) countMissed(n uint64) {
	if n == 0 {
		return
	}
	s.mu.Lock()
	s.missed += n
	s.mu.Unlock()
}

func (s *Scheduler) put(j Job, nowMS int64) {
	j.UpdatedAtMS = nowMS
	if err := s.store.Put(j); err != nil {
		s.fail(err, EventStore, "写入作业失败 job="+j.Label())
	}
}

func (s *Scheduler) fail(err error, kind, text string) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.failures++
	s.lastErr = text + ": " + brief(err.Error())
	s.lastErrAt = s.now()
	s.mu.Unlock()
	s.emit(Event{Kind: kind, Text: text + ": " + brief(err.Error())})
}

func (s *Scheduler) emit(e Event) {
	if s.cfg.Eventf == nil {
		return
	}
	e.At = s.now()
	s.cfg.Eventf(e)
}

func (s *Scheduler) now() time.Time { return s.cfg.Now() }

func (s *Scheduler) nowMS() int64 { return MS(s.cfg.Now()) }

func (s *Scheduler) clearDeferred(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.deferred, id)
}

func brief(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) > 240 {
		return string(r[:240]) + "…"
	}
	return s
}
