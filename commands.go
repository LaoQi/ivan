package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/LaoQi/tanya/ctty"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel/cli"
	chlog "github.com/LaoQi/ivan/channel/log"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/daemon"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/runner"
)

type flags struct {
	values map[string]string
	bools  map[string]bool
	rest   []string
}

func parseArgs(args []string, boolFlags ...string) (flags, error) {
	f := flags{values: map[string]string{}, bools: map[string]bool{}}
	isBool := func(name string) bool {
		for _, b := range boolFlags {
			if b == name {
				return true
			}
		}
		return false
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") || a == "-" {
			f.rest = append(f.rest, a)
			continue
		}
		name, value, has := strings.Cut(strings.TrimLeft(a, "-"), "=")
		switch {
		case has:
			f.values[name] = value
		case isBool(name):
			f.bools[name] = true
		case i+1 < len(args):
			f.values[name] = args[i+1]
			i++
		default:
			return f, fmt.Errorf("参数 --%s 缺少取值", name)
		}
	}
	return f, nil
}

func (f flags) intValue(name string) (int, error) {
	v, ok := f.values[name]
	if !ok {
		return 0, fmt.Errorf("参数 --%s 缺少取值", name)
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return 0, fmt.Errorf("参数 --%s 不是整数: %s", name, v)
	}
	return n, nil
}

func loadConfig() (*config.Config, int) {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return nil, 1
	}
	return cfg, 0
}

func cmdRun(args []string) int {
	f, err := parseArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	text := strings.TrimSpace(strings.Join(f.rest, " "))
	if text == "" {
		fmt.Fprintln(os.Stderr, "用法: ivan run [--session=键] \"指令\"")
		return 1
	}
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	log, err := data.NewEventLog(cfg.EventsDir(), cfg.Daemon.BusBuffer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	defer log.Close()
	logFile, err := data.NewLogFile(cfg.LogPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	defer logFile.Close()
	r, err := runner.New(runner.Options{
		Cfg:    cfg,
		Log:    log,
		Human:  chlog.New(logFile),
		State:  nil,
		Stream: os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	defer r.Close()
	ctx, cancel := signalContext()
	defer cancel()
	msg := bus.InboundMessage{
		Channel:    bus.ChannelCLI,
		ChatID:     "tty",
		ChatType:   bus.ChatDirect,
		SenderID:   "local",
		SenderName: "cli",
		MessageID:  "tty",
		Content:    text,
		SessionKey: cli.NormalizeSession(f.values["session"]),
		ReceivedAt: time.Now(),
	}
	if err := r.Dispatch(ctx, msg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	fmt.Fprintln(os.Stdout)
	return ctty.ExitStatus()
}

func cmdDaemon(args []string) int {
	if len(args) > 0 {
		fmt.Fprintln(os.Stderr, "用法: ivan daemon")
		return 1
	}
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	ctx, cancel := signalContext()
	defer cancel()
	d, err := daemon.New(cfg, version, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	if err := d.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	return ctty.ExitStatus()
}

func cmdSend(args []string) int {
	f, err := parseArgs(args, "no-wait", "n")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	text := strings.TrimSpace(strings.Join(f.rest, " "))
	if text == "" {
		fmt.Fprintln(os.Stderr, "用法: ivan send [--session=键] [--timeout=秒] [--no-wait] \"指令\"")
		return 1
	}
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	wait := cfg.SendTimeout()
	if f.bools["no-wait"] || f.bools["n"] {
		wait = 0
	}
	if v, ok := f.values["timeout"]; ok {
		d, err := parseDuration(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: --timeout %v\n", err)
			return 1
		}
		wait = d
	}
	reply, err := cli.Send(cfg.SocketPath(), text, f.values["session"], wait)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	if reply != "" {
		fmt.Println(reply)
	}
	return 0
}

func parseDuration(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if n, err := time.ParseDuration(v); err == nil {
		return n, nil
	}
	var secs float64
	if _, err := fmt.Sscanf(v, "%f", &secs); err != nil {
		return 0, fmt.Errorf("无法解析: %s", v)
	}
	return time.Duration(secs * float64(time.Second)), nil
}

type diskStatus struct {
	Running    bool                         `json:"running"`
	Version    string                       `json:"version"`
	DataDir    string                       `json:"data_dir"`
	Socket     string                       `json:"socket"`
	Lock       *data.LockInfo               `json:"lock,omitempty"`
	LockStale  bool                         `json:"lock_stale,omitempty"`
	StatePath  string                       `json:"state_path"`
	StateError string                       `json:"state_error,omitempty"`
	Sessions   map[string]data.SessionState `json:"sessions"`
}

func cmdStatus(args []string) int {
	f, err := parseArgs(args, "json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	if len(f.rest) > 0 {
		fmt.Fprintln(os.Stderr, "用法: ivan status [--json]")
		return 1
	}
	raw, err := cli.Status(cfg.SocketPath(), 5*time.Second)
	if err != nil {
		return statusFromDisk(cfg, f.bools["json"], err)
	}
	if f.bools["json"] {
		fmt.Println(string(raw))
		return 0
	}
	var st daemon.Status
	if err := json.Unmarshal(raw, &st); err != nil {
		fmt.Fprintf(os.Stderr, "error: 解析状态失败: %v\n", err)
		return 1
	}
	printStatus(st)
	return 0
}

func statusFromDisk(cfg *config.Config, jsonOut bool, cause error) int {
	st := diskStatus{
		Version:   version,
		DataDir:   cfg.DataDir,
		Socket:    cfg.SocketPath(),
		StatePath: cfg.StatePath(),
		Sessions:  map[string]data.SessionState{},
	}
	if info, err := data.ReadLockInfo(cfg.LockPath()); err == nil {
		st.Lock = &info
		st.LockStale = !data.ProcessAlive(info.PID)
	}
	state, err := data.LoadState(cfg.StatePath())
	if err != nil {
		st.StateError = err.Error()
	} else {
		st.Sessions = state.Snapshot()
	}
	if jsonOut {
		b, err := json.MarshalIndent(st, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 1
		}
		fmt.Println(string(b))
		return 0
	}
	fmt.Printf("%v\n", cause)
	fmt.Printf("socket    %s\n", st.Socket)
	fmt.Printf("data_dir  %s\n", st.DataDir)
	if st.Lock != nil {
		mark := ""
		if st.LockStale {
			mark = "（进程已不存在，锁文件残留）"
		}
		fmt.Printf("lock      pid=%d since=%s%s\n", st.Lock.PID, st.Lock.StartedAt.Format(time.RFC3339), mark)
	} else {
		fmt.Println("lock      无")
	}
	if st.StateError != "" {
		fmt.Printf("state     读取失败: %s\n", st.StateError)
		return 0
	}
	fmt.Printf("state     %s（%d 个会话映射）\n", st.StatePath, len(st.Sessions))
	for _, key := range sortedKeys(st.Sessions) {
		v := st.Sessions[key]
		fmt.Printf("session   %s → %s（%s）\n", key, v.SessionID, v.UpdatedAt.Format("01-02 15:04:05"))
	}
	return 0
}

func sortedKeys(m map[string]data.SessionState) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func printStatus(st daemon.Status) {
	fmt.Printf("ivan %s pid=%d running=%v started=%s uptime=%.0fs\n",
		st.Version, st.PID, st.Running, st.StartedAt.Format(time.RFC3339), st.UptimeSeconds)
	fmt.Printf("socket    %s\n", st.Socket)
	fmt.Printf("data_dir  %s\n", st.DataDir)
	fmt.Printf("bus       inbound=%d dropped=%d queued=%d | outbound=%d dropped=%d queued=%d%s\n",
		st.Bus.Inbound, st.Bus.InboundDropped, st.Bus.InboundQueued,
		st.Bus.Outbound, st.Bus.OutboundDropped, st.Bus.OutboundQueued, lastDrop(st.Bus))
	fmt.Printf("events    written=%d dropped=%d %s\n", st.Events.Written, st.Events.Dropped, st.Events.Dir)
	fmt.Printf("logs      lines=%d errors=%d %s\n", st.Logs.Lines, st.Logs.Errors, st.Logs.Path)
	fmt.Printf("runner    turns=%d failures=%d handoffs=%d continued=%d inflight=%d agents=%d/%d sessions=%d concurrent=%d\n",
		st.Runner.Turns, st.Runner.Failures, st.Runner.Handoffs, st.Runner.Continued,
		st.Runner.InFlight, st.Runner.Agents, st.Runner.MaxAgents, st.Runner.Sessions, st.Runner.MaxConcurrent)
	for _, c := range st.Channels {
		line := fmt.Sprintf("channel   %s running=%v max_len=%d sent=%d failed=%d unmatched=%d",
			c.Name, c.Running, c.MaxLen, c.Sent, c.Failed, c.Unmatched)
		if c.LastError != "" {
			line += fmt.Sprintf(" last_error=%q", c.LastError)
		}
		fmt.Println(line)
	}
	for _, a := range st.Runner.AgentList {
		fmt.Printf("agent     %s session=%s turns=%d idle=%.0fs running=%v\n", a.Key, a.Session, a.Turns, a.IdleSeconds, a.Running)
	}
}

func lastDrop(c bus.Counters) string {
	if c.LastDrop.IsZero() {
		return ""
	}
	return " last_drop=" + c.LastDrop.Format(time.RFC3339)
}
