package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
	"github.com/LaoQi/ivan/channel/cli"
	chlog "github.com/LaoQi/ivan/channel/log"
	qqchan "github.com/LaoQi/ivan/channel/qq"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/qqbot"
	"github.com/LaoQi/ivan/runner"
	"github.com/LaoQi/ivan/schedule"
	sqlitestore "github.com/LaoQi/ivan/schedule/sqlite"
)

type EventsStatus struct {
	Dir     string `json:"dir"`
	Written uint64 `json:"written"`
	Dropped uint64 `json:"dropped"`
}

type LogsStatus struct {
	Path   string `json:"path"`
	Lines  uint64 `json:"lines"`
	Errors uint64 `json:"errors"`
}

type Status struct {
	Running       bool             `json:"running"`
	Version       string           `json:"version"`
	PID           int              `json:"pid"`
	StartedAt     time.Time        `json:"started_at"`
	UptimeSeconds float64          `json:"uptime_seconds"`
	DataDir       string           `json:"data_dir"`
	Socket        string           `json:"socket"`
	Bus           bus.Counters     `json:"bus"`
	Events        EventsStatus     `json:"events"`
	Logs          LogsStatus       `json:"logs"`
	Runner        runner.Status    `json:"runner"`
	Channels      []channel.Status `json:"channels"`
	QQ            qqbot.Stats      `json:"qq,omitempty"`
	Schedule      *schedule.Status `json:"schedule,omitempty"`
	ScheduleError string           `json:"schedule_error,omitempty"`
}

type Daemon struct {
	cfg       *config.Config
	version   string
	out       io.Writer
	startedAt time.Time

	lock     *data.Lock
	logFile  *data.LogFile
	events   *data.EventLog
	human    *chlog.Sink
	state    *data.State
	bus      *bus.Bus
	mgr      *channel.Manager
	cli      *cli.Socket
	qq       *qqchan.Channel
	runner   *runner.Runner
	jobs     schedule.Store
	sched    *schedule.Scheduler
	schedErr string

	releaseOnce bool
}

func New(cfg *config.Config, version string, out io.Writer) (*Daemon, error) {
	if cfg == nil {
		return nil, errors.New("缺少配置")
	}
	d := &Daemon{cfg: cfg, version: version, out: out, startedAt: time.Now()}
	lock, err := data.AcquireLock(cfg.LockPath(), data.LockInfo{
		PID:       os.Getpid(),
		StartedAt: d.startedAt,
		Version:   version,
		Socket:    cfg.SocketPath(),
	})
	if err != nil {
		return nil, err
	}
	d.lock = lock
	logFile, err := data.NewLogFile(cfg.LogPath())
	if err != nil {
		d.Release()
		return nil, err
	}
	d.logFile = logFile
	d.human = chlog.New(logFile)
	events, err := data.NewEventLog(cfg.EventsDir(), cfg.Daemon.BusBuffer)
	if err != nil {
		d.Release()
		return nil, err
	}
	d.events = events
	state, err := data.LoadState(cfg.StatePath())
	if err != nil {
		d.Release()
		return nil, err
	}
	d.state = state
	d.bus = bus.New(cfg.Daemon.BusBuffer, cfg.Daemon.BusBuffer)
	d.cli = cli.NewSocket(cfg.SocketPath(), func(op, arg string) (any, error) { return d.Control(op, arg) })
	d.mgr = channel.New(d.bus, channel.WithSendTimeout(cfg.SendTimeout()))
	if err := d.mgr.Register(d.cli); err != nil {
		d.Release()
		return nil, err
	}
	if cfg.QQEnabled() {
		qq, err := qqchan.New(qqchan.Config{
			AppID:          cfg.Channels.QQ.AppID,
			AppSecret:      cfg.Channels.QQ.AppSecret,
			APIBase:        cfg.Channels.QQ.APIBase,
			TokenURL:       cfg.Channels.QQ.TokenURL,
			GatewayURL:     cfg.Channels.QQ.GatewayURL,
			AllowFrom:      cfg.Channels.QQ.AllowFrom,
			AllowGroups:    cfg.Channels.QQ.AllowGroups,
			MaxMessageLen:  cfg.Channels.QQ.MaxMessageLen,
			Markdown:       cfg.Channels.QQ.Markdown,
			StreamMaxLen:   cfg.Channels.QQ.StreamMaxLen,
			Stream:         cfg.Channels.QQ.Stream,
			Typing:         cfg.Channels.QQ.Typing,
			AllowProactive: cfg.Channels.QQ.AllowProactive,
			ReconnectMax:   time.Duration(cfg.Channels.QQ.ReconnectMaxSec) * time.Second,
			MediaDir:       cfg.MediaDir(),
			UserAgent:      config.UserAgent(d.version),
			Logf:           func(format string, args ...any) { _ = d.human.Notice(format, args...) },
		})
		if err != nil {
			d.Release()
			return nil, err
		}
		if err := d.mgr.Register(qq); err != nil {
			d.Release()
			return nil, err
		}
		d.qq = qq
	}
	if cfg.Schedule.Enabled {
		jobs, err := sqlitestore.Open(cfg.JobsDBPath())
		if err != nil {
			d.schedErr = err.Error()
			_ = d.human.Notice("计划任务不可用（作业库打开失败，原文件已保留）: %v", err)
			d.events.Note("job_store", "作业库打开失败: "+err.Error())
		} else {
			d.jobs = jobs
		}
	}
	r, err := runner.New(runner.Options{
		Cfg:    cfg,
		Log:    events,
		Human:  d.human,
		State:  state,
		Bus:    d.bus,
		Notify: func(ctx context.Context, m bus.OutboundMessage) error { return d.mgr.Route(ctx, m) },
		Paused: d.paused,
	})
	if err != nil {
		d.Release()
		return nil, err
	}
	d.runner = r
	if d.jobs != nil {
		sched, err := schedule.New(d.jobs, schedule.DispatcherFunc(r.TriggerJob), schedule.Config{
			MissedPolicy: cfg.Schedule.MissedPolicy,
			JobTimeout:   cfg.JobTimeout(),
			Paused:       d.paused,
			Budget:       r.Budget,
			Eventf:       d.scheduleEvent,
		})
		if err != nil {
			d.schedErr = err.Error()
			_ = d.human.Notice("计划任务不可用: %v", err)
			d.events.Note("job_store", "调度器装配失败: "+err.Error())
		} else {
			d.sched = sched
			r.SetScheduler(sched)
		}
	}
	return d, nil
}

func (d *Daemon) paused() bool {
	if d.state == nil {
		return false
	}
	return d.state.Paused()
}

func (d *Daemon) scheduleEvent(e schedule.Event) {
	_ = d.human.Emit(chlog.Event{
		Time:    e.At,
		Kind:    e.Kind,
		Session: e.JobID,
		Tool:    "schedule",
		Text:    e.Text,
		Dur:     e.Dur,
		Failed:  e.Kind == schedule.EventFailed,
	})
	text := e.Text
	if e.Name != "" {
		text = e.Name + " " + text
	}
	d.events.Note(e.Kind, text)
}

func (d *Daemon) Run(ctx context.Context) error {
	if err := d.mgr.Start(ctx); err != nil {
		d.Release()
		return err
	}
	go d.mgr.Consume(ctx)
	loopCtx, stopLoop := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- d.runner.Run(loopCtx) }()
	schedCtx, stopSched := context.WithCancel(context.Background())
	schedDone := make(chan struct{})
	if d.sched != nil {
		go func() {
			_ = d.sched.Run(schedCtx)
			close(schedDone)
		}()
	} else {
		close(schedDone)
	}

	_ = d.human.Notice("daemon 启动 version=%s pid=%d data_dir=%s socket=%s", d.version, os.Getpid(), d.cfg.DataDir, d.cfg.SocketPath())
	if d.sched != nil {
		_ = d.human.Notice("计划任务已启用 jobs_db=%s missed_policy=%s paused=%v", d.cfg.JobsDBPath(), d.cfg.Schedule.MissedPolicy, d.paused())
	}
	if d.out != nil {
		fmt.Fprintf(d.out, "ivan daemon 已启动 pid=%d socket=%s data_dir=%s\n", os.Getpid(), d.cfg.SocketPath(), d.cfg.DataDir)
	}

	select {
	case <-ctx.Done():
	case err := <-runDone:
		if err != nil {
			_ = d.human.Notice("入站消费循环退出: %v", err)
		}
	}
	stopSched()
	select {
	case <-schedDone:
	case <-time.After(10 * time.Second):
		_ = d.human.Notice("调度器未在超时内停止")
	}
	stopLoop()
	d.bus.CloseInbound()
	grace := d.cfg.ShutdownTimeout()
	if !d.runner.Wait(grace) {
		_ = d.human.Notice("停机等待超时 %s，取消在跑回合", grace)
		d.runner.CancelTurns()
		d.runner.Wait(5 * time.Second)
	}
	d.mgr.AbortStreams()
	d.bus.CloseOutbound()
	select {
	case <-d.mgr.Drained():
	case <-time.After(5 * time.Second):
		_ = d.human.Notice("出站队列刷出超时")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.mgr.Stop(stopCtx); err != nil {
		_ = d.human.Notice("停止通道出错: %v", err)
	}
	_ = d.human.Notice("daemon 停机完成")
	d.Release()
	return nil
}

func (d *Daemon) Status() Status {
	st := Status{
		Running:       true,
		Version:       d.version,
		PID:           os.Getpid(),
		StartedAt:     d.startedAt,
		UptimeSeconds: time.Since(d.startedAt).Seconds(),
		DataDir:       d.cfg.DataDir,
		Socket:        d.cfg.SocketPath(),
	}
	if d.bus != nil {
		st.Bus = d.bus.Stats()
	}
	if d.events != nil {
		st.Events = EventsStatus{Dir: d.cfg.EventsDir(), Written: d.events.Written(), Dropped: d.events.Dropped()}
	}
	if d.human != nil {
		st.Logs = LogsStatus{Path: d.cfg.LogPath(), Lines: d.human.Lines(), Errors: d.human.Errors()}
	}
	if d.runner != nil {
		st.Runner = d.runner.Status()
	}
	if d.mgr != nil {
		st.Channels = d.mgr.Status()
	}
	if d.qq != nil {
		st.QQ = d.qq.Stats()
	}
	if d.sched != nil {
		s := d.sched.Status()
		st.Schedule = &s
	}
	st.ScheduleError = d.schedErr
	return st
}

func (d *Daemon) Control(op, arg string) (any, error) {
	switch op {
	case cli.OpStatus:
		return d.Status(), nil
	case cli.OpJobsList:
		if d.jobs == nil {
			return nil, d.jobsUnavailable()
		}
		return d.jobs.List()
	case cli.OpJobsRemove:
		if d.sched == nil {
			return nil, d.jobsUnavailable()
		}
		id := strings.TrimSpace(arg)
		if id == "" {
			return nil, errors.New("缺少作业 id")
		}
		if err := d.sched.Remove(id); err != nil {
			return nil, err
		}
		_ = d.human.Notice("人工取消作业 %s", id)
		return map[string]string{"id": id, "status": "removed"}, nil
	case cli.OpJobsRun:
		if d.sched == nil {
			return nil, d.jobsUnavailable()
		}
		id := strings.TrimSpace(arg)
		if id == "" {
			return nil, errors.New("缺少作业 id")
		}
		if _, ok, err := d.sched.Get(id); err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("作业不存在: %s", id)
		}
		_ = d.human.Notice("人工触发作业 %s", id)
		go func() {
			if _, err := d.sched.RunNow(context.Background(), id); err != nil {
				_ = d.human.Notice("人工触发作业 %s 失败: %v", id, err)
			}
		}()
		return map[string]string{"id": id, "status": "started"}, nil
	case cli.OpCancel:
		if d.runner == nil {
			return nil, errors.New("runner 未装配")
		}
		key := strings.TrimSpace(arg)
		if key == "" {
			return nil, errors.New("缺少会话键")
		}
		if !d.runner.CancelTurn(key) {
			return nil, fmt.Errorf("会话没有在跑的回合: %s", key)
		}
		_ = d.human.Notice("人工取消会话回合 %s", key)
		return map[string]string{"session": key, "status": "cancelled"}, nil
	case cli.OpPause, cli.OpResume:
		if d.state == nil {
			return nil, errors.New("缺少状态文件，无法切换暂停开关")
		}
		paused := op == cli.OpPause
		if err := d.state.SetPaused(paused); err != nil {
			return nil, err
		}
		if d.sched != nil {
			d.sched.Wake()
		}
		_ = d.human.Notice("暂停开关切换为 paused=%v", paused)
		return map[string]bool{"paused": paused}, nil
	default:
		return nil, fmt.Errorf("不支持的控制指令: %s", op)
	}
}

func (d *Daemon) jobsUnavailable() error {
	if d.schedErr != "" {
		return errors.New("计划任务不可用: " + d.schedErr)
	}
	if !d.cfg.Schedule.Enabled {
		return errors.New("计划任务未启用（schedule.enabled=false）")
	}
	return errors.New("计划任务不可用")
}

func (d *Daemon) Release() {
	if d.releaseOnce {
		return
	}
	d.releaseOnce = true
	if d.jobs != nil {
		_ = d.jobs.Close()
	}
	if d.runner != nil {
		_ = d.runner.Close()
	}
	if d.mgr != nil {
		_ = d.mgr.Stop(context.Background())
	}
	if d.events != nil {
		_ = d.events.Close()
	}
	if d.logFile != nil {
		_ = d.logFile.Close()
	}
	if d.lock != nil {
		_ = d.lock.Release()
	}
}
