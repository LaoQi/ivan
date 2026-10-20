package schedule

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type Kind string

const (
	KindAt    Kind = "at"
	KindEvery Kind = "every"
	KindDaily Kind = "daily"
)

const DayMS = int64(86400000)

const (
	StatusOK          = "ok"
	StatusFailed      = "failed"
	StatusMissed      = "missed"
	StatusRunning     = "running"
	StatusInterrupted = "interrupted"
	StatusBudget      = "budget"
)

const MinEveryMS = int64(1000)

type Schedule struct {
	Kind     Kind   `json:"kind"`
	AtMS     int64  `json:"at_ms,omitempty"`
	EveryMS  int64  `json:"every_ms,omitempty"`
	AtTimeMS int64  `json:"at_time_ms,omitempty"`
	Expr     string `json:"expr,omitempty"`
	TZ       string `json:"tz,omitempty"`
}

type Payload struct {
	Prompt     string `json:"prompt"`
	File       string `json:"file,omitempty"`
	Channel    string `json:"channel"`
	ChatID     string `json:"chat_id"`
	SessionKey string `json:"session_key"`
}

type State struct {
	NextRunAtMS     int64  `json:"next_run_at_ms,omitempty"`
	LastRunAtMS     int64  `json:"last_run_at_ms,omitempty"`
	LastStatus      string `json:"last_status,omitempty"`
	LastError       string `json:"last_error,omitempty"`
	LastNotifyError string `json:"last_notify_error,omitempty"`
	LastTokens      int    `json:"last_tokens,omitempty"`
	Runs            int    `json:"runs,omitempty"`
}

type Job struct {
	ID             string   `json:"id"`
	Name           string   `json:"name,omitempty"`
	Enabled        bool     `json:"enabled"`
	Schedule       Schedule `json:"schedule"`
	Payload        Payload  `json:"payload"`
	State          State    `json:"state"`
	DeleteAfterRun bool     `json:"delete_after_run,omitempty"`
	CreatedAtMS    int64    `json:"created_at_ms"`
	UpdatedAtMS    int64    `json:"updated_at_ms"`
}

func MS(t time.Time) int64 { return t.UnixMilli() }

func FromMS(ms int64) time.Time { return time.UnixMilli(ms) }

func (j Job) Label() string {
	if name := strings.TrimSpace(j.Name); name != "" {
		return name
	}
	return oneLine(j.Payload.Prompt, 24)
}

func (j Job) Validate() error {
	if j.Schedule.Expr != "" {
		return fmt.Errorf("暂不支持 cron 表达式（只支持 at/every）")
	}
	switch j.Schedule.Kind {
	case KindAt:
		if j.Schedule.AtMS <= 0 {
			return fmt.Errorf("一次性作业缺少触发时间")
		}
		if j.Schedule.EveryMS != 0 {
			return fmt.Errorf("一次性作业不能同时带周期")
		}
	case KindEvery:
		if j.Schedule.EveryMS < MinEveryMS {
			return fmt.Errorf("周期不能小于 %d 毫秒", MinEveryMS)
		}
		if j.Schedule.AtMS != 0 {
			return fmt.Errorf("周期作业不能同时带一次性时间")
		}
	case KindDaily:
		if j.Schedule.Expr != "" {
			return fmt.Errorf("daily 作业不能再带 cron 表达式")
		}
		if j.Schedule.AtTimeMS < 0 || j.Schedule.AtTimeMS >= DayMS {
			return fmt.Errorf("daily 作业的每日时刻超出范围（毫秒偏移需在 0 ~ %d 之间）", DayMS-1)
		}
		if _, err := Location(j.Schedule.TZ); err != nil {
			return err
		}
	default:
		return fmt.Errorf("未知的调度类型 %q（只支持 at/every/daily）", j.Schedule.Kind)
	}
	if strings.TrimSpace(j.Payload.Prompt) == "" && strings.TrimSpace(j.Payload.File) == "" {
		return fmt.Errorf("作业缺少 prompt")
	}
	if strings.TrimSpace(j.Payload.Channel) == "" {
		return fmt.Errorf("作业缺少回投通道")
	}
	return nil
}

func Location(tz string) (*time.Location, error) {
	name := strings.TrimSpace(tz)
	if name == "" || name == "local" {
		return time.Local, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("无法解析时区 %q: %w", tz, err)
	}
	return loc, nil
}

func dailyAt(t time.Time, atTimeMS int64) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).
		Add(time.Duration(atTimeMS) * time.Millisecond)
}

func NextDaily(fromMS, atTimeMS int64, tz string, nowMS int64) (int64, int) {
	loc, err := Location(tz)
	if err != nil {
		loc = time.Local
	}
	cur := dailyAt(FromMS(fromMS).In(loc), atTimeMS)
	if MS(cur) <= fromMS {
		cur = cur.AddDate(0, 0, 1)
	}
	missed := 0
	for MS(cur) <= nowMS {
		cur = cur.AddDate(0, 0, 1)
		missed++
	}
	return MS(cur), missed
}

func SkipDaily(fromMS, atTimeMS int64, tz string, nowMS int64) (int64, int) {
	loc, err := Location(tz)
	if err != nil {
		loc = time.Local
	}
	cur := dailyAt(FromMS(fromMS).In(loc), atTimeMS)
	missed := 0
	for MS(cur) <= nowMS {
		cur = cur.AddDate(0, 0, 1)
		missed++
	}
	return MS(cur), missed
}

func (j Job) Done() bool {
	return j.Schedule.Kind == KindAt && j.State.NextRunAtMS == 0 && j.State.LastRunAtMS > 0
}

func (j Job) NextRun() time.Time {
	if j.State.NextRunAtMS <= 0 {
		return time.Time{}
	}
	return FromMS(j.State.NextRunAtMS)
}

func (j *Job) InitNext(now time.Time) {
	if j.State.NextRunAtMS > 0 {
		return
	}
	switch j.Schedule.Kind {
	case KindAt:
		j.State.NextRunAtMS = j.Schedule.AtMS
	case KindEvery:
		base := j.CreatedAtMS
		if base <= 0 {
			base = MS(now)
		}
		j.State.NextRunAtMS = base + j.Schedule.EveryMS
	case KindDaily:
		next, _ := NextDaily(MS(now), j.Schedule.AtTimeMS, j.Schedule.TZ, MS(now))
		j.State.NextRunAtMS = next
	}
}

func NewID(now time.Time) string {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("job_%s_%s", now.UTC().Format("20060102_150405"), hex.EncodeToString(b[:]))
}

func nextEvery(planned, every, nowMS int64) (int64, int) {
	if every <= 0 {
		return 0, 0
	}
	next := planned + every
	missed := 0
	for next <= nowMS {
		next += every
		missed++
	}
	return next, missed
}

func skipEvery(planned, every, nowMS int64) (int64, int) {
	if every <= 0 {
		return 0, 0
	}
	next := planned
	missed := 0
	for next <= nowMS {
		next += every
		missed++
	}
	return next, missed
}

func oneLine(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if limit > 0 && len(s) > limit {
		return s[:limit] + "…"
	}
	return s
}
