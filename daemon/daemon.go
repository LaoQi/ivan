package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
}

type Daemon struct {
	cfg       *config.Config
	version   string
	out       io.Writer
	startedAt time.Time

	lock    *data.Lock
	logFile *data.LogFile
	events  *data.EventLog
	human   *chlog.Sink
	state   *data.State
	bus     *bus.Bus
	mgr     *channel.Manager
	cli     *cli.Socket
	qq      *qqchan.Channel
	runner  *runner.Runner

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
	d.cli = cli.NewSocket(cfg.SocketPath(), func(op string) (any, error) { return d.Control(op) })
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
			AllowProactive: cfg.Channels.QQ.AllowProactive,
			ReconnectMax:   time.Duration(cfg.Channels.QQ.ReconnectMaxSec) * time.Second,
			MediaDir:       cfg.MediaDir(),
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
	r, err := runner.New(runner.Options{
		Cfg:   cfg,
		Log:   events,
		Human: d.human,
		State: state,
		Bus:   d.bus,
	})
	if err != nil {
		d.Release()
		return nil, err
	}
	d.runner = r
	return d, nil
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

	_ = d.human.Notice("daemon 启动 version=%s pid=%d data_dir=%s socket=%s", d.version, os.Getpid(), d.cfg.DataDir, d.cfg.SocketPath())
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
	stopLoop()
	d.bus.CloseInbound()
	grace := d.cfg.ShutdownTimeout()
	if !d.runner.Wait(grace) {
		_ = d.human.Notice("停机等待超时 %s，取消在跑回合", grace)
		d.runner.CancelTurns()
		d.runner.Wait(5 * time.Second)
	}
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
	return st
}

func (d *Daemon) Control(op string) (any, error) {
	switch op {
	case cli.OpStatus:
		return d.Status(), nil
	default:
		return nil, fmt.Errorf("不支持的控制指令: %s", op)
	}
}

func (d *Daemon) Release() {
	if d.releaseOnce {
		return
	}
	d.releaseOnce = true
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
