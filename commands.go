package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LaoQi/battlelab/ctty"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel/cli"
	chlog "github.com/LaoQi/ivan/channel/log"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/daemon"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/runner"
	"github.com/LaoQi/ivan/schedule"
	sqlitestore "github.com/LaoQi/ivan/schedule/sqlite"
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
	var (
		progress bool
		onChunk  func(string)
	)
	if ctty.IsTerminal(int(os.Stdout.Fd())) {
		progress = true
		onChunk = func(text string) {
			fmt.Printf("\r\033[2K%s", lastLine(text))
			_ = os.Stdout.Sync()
		}
	}
	reply, err := cli.SendStream(cfg.SocketPath(), text, f.values["session"], wait, onChunk)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	if reply != "" {
		if progress {
			fmt.Print("\r\033[2K")
		}
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
	Paused     bool                         `json:"paused"`
	Sessions   map[string]data.SessionState `json:"sessions"`
	JobsDB     string                       `json:"jobs_db"`
	Jobs       int                          `json:"jobs"`
	JobsError  string                       `json:"jobs_error,omitempty"`
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
		st.Paused = state.Paused()
	}
	st.JobsDB = cfg.JobsDBPath()
	if jobs, err := loadJobsOffline(cfg); err != nil {
		st.JobsError = err.Error()
	} else {
		st.Jobs = len(jobs)
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
	fmt.Printf("state     %s（%d 个会话映射，paused=%v）\n", st.StatePath, len(st.Sessions), st.Paused)
	for _, key := range sortedKeys(st.Sessions) {
		v := st.Sessions[key]
		fmt.Printf("session   %s → %s（%s）\n", key, v.SessionID, v.UpdatedAt.Format("01-02 15:04:05"))
	}
	if st.JobsError != "" {
		fmt.Printf("schedule  作业库不可用: %s\n", st.JobsError)
	} else {
		fmt.Printf("schedule  %s（%d 个作业，用 ivan jobs 查看）\n", st.JobsDB, st.Jobs)
	}
	return 0
}

func loadJobsOffline(cfg *config.Config) ([]schedule.Job, error) {
	if !cfg.Schedule.Enabled {
		return nil, fmt.Errorf("计划任务未启用")
	}
	path := cfg.JobsDBPath()
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	store, err := sqlitestore.Open(path)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	return store.List()
}

func cmdNotify(args []string) int {
	f, err := parseArgs(args, "json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	text := strings.TrimSpace(strings.Join(f.rest, " "))
	if text == "" && !ctty.IsTerminal(int(os.Stdin.Fd())) {
		if b, rerr := io.ReadAll(os.Stdin); rerr == nil {
			text = strings.TrimSpace(string(b))
		}
	}
	if text == "" {
		fmt.Fprintln(os.Stderr, "用法: ivan notify [--title=标题] [--source=来源] \"正文\"")
		return 1
	}
	payload, err := json.Marshal(map[string]string{
		"title":  f.values["title"],
		"body":   text,
		"source": firstNonEmpty(f.values["source"], "cli"),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	raw, err := cli.Call(cfg.SocketPath(), cli.OpNotify, string(payload), cfg.SendTimeout())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	if f.bools["json"] {
		fmt.Println(string(raw))
		return 0
	}
	var res struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Targets []struct {
			Name   string `json:"name"`
			Result string `json:"result"`
			Error  string `json:"error"`
		} `json:"targets"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		fmt.Println(string(raw))
		return 0
	}
	status := firstNonEmpty(res.Status, "ok")
	if res.Message != "" {
		fmt.Printf("%s：%s\n", status, res.Message)
	} else {
		fmt.Println(status)
	}
	for _, t := range res.Targets {
		line := fmt.Sprintf("  %s: %s", t.Name, t.Result)
		if t.Error != "" {
			line += "（" + t.Error + "）"
		}
		fmt.Println(line)
	}
	if status == "failed" {
		return 1
	}
	return 0
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func cmdJobs(args []string) int {
	f, err := parseArgs(args, "json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	sub := "list"
	var arg string
	if len(f.rest) > 0 {
		sub = f.rest[0]
	}
	if len(f.rest) > 1 {
		arg = f.rest[1]
	}
	if len(f.rest) > 2 {
		usageJobs()
		return 1
	}
	switch sub {
	case "list", "ls":
		return jobsList(cfg, f.bools["json"])
	case "rm", "remove":
		return jobsControl(cfg, cli.OpJobsRemove, arg, "已取消作业 "+arg)
	case "run":
		return jobsControl(cfg, cli.OpJobsRun, arg, "已触发作业 "+arg+"（结果见日志）")
	default:
		usageJobs()
		return 1
	}
}

func usageJobs() {
	fmt.Fprintln(os.Stderr, "用法:\n  ivan jobs [list] [--json]\n  ivan jobs rm <id>\n  ivan jobs run <id>")
}

func jobsList(cfg *config.Config, jsonOut bool) int {
	raw, err := cli.Call(cfg.SocketPath(), cli.OpJobsList, "", 5*time.Second)
	jobs := []schedule.Job(nil)
	if err == nil {
		if err := json.Unmarshal(raw, &jobs); err != nil {
			fmt.Fprintf(os.Stderr, "error: 解析作业列表失败: %v\n", err)
			return 1
		}
	} else {
		jobs, err = loadJobsOffline(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 1
		}
	}
	if jsonOut {
		b, err := json.MarshalIndent(jobs, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 1
		}
		fmt.Println(string(b))
		return 0
	}
	if len(jobs) == 0 {
		fmt.Println("没有计划任务。")
		return 0
	}
	sortJobs(jobs)
	fmt.Printf("%-26s %-12s %-10s %-16s %-16s %s\n", "id", "名称", "类型", "下次触发", "上次执行", "状态")
	for _, j := range jobs {
		fmt.Printf("%-26s %-12s %-10s %-16s %-16s %s\n",
			j.ID, clip(labelOf(j), 12), kindOf(j), jobsTime(j.State.NextRunAtMS), jobsTime(j.State.LastRunAtMS), statusOf(j))
	}
	return 0
}

func jobsControl(cfg *config.Config, op, id, okText string) int {
	if strings.TrimSpace(id) == "" {
		usageJobs()
		return 1
	}
	if _, err := cli.Call(cfg.SocketPath(), op, id, 10*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v（作业的取消/触发需要 daemon 运行）\n", err)
		return 1
	}
	fmt.Println(okText)
	return 0
}

func sortJobs(jobs []schedule.Job) {
	sort.Slice(jobs, func(i, k int) bool {
		a, b := jobs[i].State.NextRunAtMS, jobs[k].State.NextRunAtMS
		if (a == 0) != (b == 0) {
			return b == 0
		}
		if a != b {
			return a < b
		}
		return jobs[i].ID < jobs[k].ID
	})
}

func labelOf(j schedule.Job) string {
	if name := strings.TrimSpace(j.Name); name != "" {
		return name
	}
	return strings.Join(strings.Fields(j.Payload.Prompt), " ")
}

func kindOf(j schedule.Job) string {
	switch j.Schedule.Kind {
	case schedule.KindAt:
		return "一次性"
	case schedule.KindDaily:
		d := time.Duration(j.Schedule.AtTimeMS) * time.Millisecond
		out := fmt.Sprintf("每天 %02d:%02d", int(d.Hours()), int(d.Minutes())%60)
		if j.Schedule.TZ != "" {
			out += " " + j.Schedule.TZ
		}
		return out
	default:
		return "每 " + (time.Duration(j.Schedule.EveryMS) * time.Millisecond).String()
	}
}

func jobsTime(ms int64) string {
	if ms <= 0 {
		return "—"
	}
	return schedule.FromMS(ms).Local().Format("01-02 15:04:05")
}

func statusOf(j schedule.Job) string {
	out := j.State.LastStatus
	if out == "" {
		out = "未跑过"
	}
	if !j.Enabled {
		out += "（停用）"
	}
	if j.State.LastError != "" {
		out += " 错误:" + clip(j.State.LastError, 40)
	}
	return out
}

func clip(s string, n int) string {
	return bus.Clip(s, n)
}

func cmdCancel(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: ivan cancel <会话键，如 qq:c2c:xxx 或 cli:default>")
		return 1
	}
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	key := strings.Join(args, " ")
	if _, err := cli.Call(cfg.SocketPath(), cli.OpCancel, key, 10*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v（取消回合需要 daemon 运行）\n", err)
		return 1
	}
	fmt.Println("已取消会话回合 " + key)
	return 0
}

func cmdPause(args []string) int {
	if len(args) > 0 {
		fmt.Fprintln(os.Stderr, "用法: ivan pause")
		return 1
	}
	return setPaused(true)
}

func cmdResume(args []string) int {
	if len(args) > 0 {
		fmt.Fprintln(os.Stderr, "用法: ivan resume")
		return 1
	}
	return setPaused(false)
}

func setPaused(paused bool) int {
	cfg, code := loadConfig()
	if cfg == nil {
		return code
	}
	op := cli.OpResume
	text := "已恢复（暂停开关 paused=false）"
	if paused {
		op = cli.OpPause
		text = "已暂停（暂停开关 paused=true）"
	}
	if _, err := cli.Call(cfg.SocketPath(), op, "", 5*time.Second); err == nil {
		fmt.Println(text)
		return 0
	}
	state, err := data.LoadState(cfg.StatePath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	if err := state.SetPaused(paused); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	fmt.Println(text + "（daemon 未运行，已写入状态文件）")
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
	fmt.Printf("runner    turns=%d failures=%d handoffs=%d continued=%d hints=%d inflight=%d agents=%d/%d sessions=%d concurrent=%d queue_dropped=%d pool_full=%d\n",
		st.Runner.Turns, st.Runner.Failures, st.Runner.Handoffs, st.Runner.Continued, st.Runner.Hinted,
		st.Runner.InFlight, st.Runner.Agents, st.Runner.MaxAgents, st.Runner.Sessions, st.Runner.MaxConcurrent,
		st.Runner.QueueDropped, st.Runner.PoolFull)
	budget := "不限"
	if st.Runner.MaxTokensPerDay > 0 {
		budget = strconv.Itoa(st.Runner.MaxTokensPerDay)
	}
	memory := "不限"
	if st.Runner.MemoryMaxBytes > 0 {
		memory = strconv.Itoa(st.Runner.MemoryMaxBytes)
	}
	fmt.Printf("budget    tokens_today=%d max_per_day=%s paused=%v hint=%d force=%d interrupted=%d memory_max=%s\n",
		st.Runner.TokensToday, budget, st.Runner.Paused, st.Runner.HandoffHint, st.Runner.HandoffForce,
		st.Runner.Interrupted, memory)
	for _, c := range st.Channels {
		line := fmt.Sprintf("channel   %s running=%v max_len=%d sent=%d failed=%d unmatched=%d",
			c.Name, c.Running, c.MaxLen, c.Sent, c.Failed, c.Unmatched)
		if c.StreamSent > 0 || c.StreamUpd > 0 || c.StreamFinal > 0 || c.StreamFail > 0 || c.Typing > 0 || c.StreamSkip > 0 {
			line += fmt.Sprintf(" stream=%d upd=%d final=%d skip=%d fail=%d typing=%d",
				c.StreamSent, c.StreamUpd, c.StreamFinal, c.StreamSkip, c.StreamFail, c.Typing)
		}
		if c.LastError != "" {
			line += fmt.Sprintf(" last_error=%q", c.LastError)
		}
		fmt.Println(line)
	}
	if st.QQ.Connected || st.QQ.Messages > 0 || st.QQ.SessionID != "" {
		line := fmt.Sprintf("qq        connected=%v messages=%d duplicates=%d reconnects=%d sent=%d md=%d send_errors=%d",
			st.QQ.Connected, st.QQ.Messages, st.QQ.Duplicates, st.QQ.Reconnects, st.QQ.Sent, st.QQ.MarkdownSent, st.QQ.SendErrors)
		if st.QQ.SessionID != "" {
			line += fmt.Sprintf(" session=%s", st.QQ.SessionID)
		}
		if st.QQ.LastError != "" {
			line += fmt.Sprintf(" last_error=%q", st.QQ.LastError)
		}
		fmt.Println(line)
	}
	if st.HTTP != nil {
		h := st.HTTP
		line := fmt.Sprintf("http      listen=%s running=%v notify=%d delivered=%d partial=%d failed=%d rejected=%d",
			h.Listen, h.Running, h.NotifyAccepted, h.NotifyDelivered, h.NotifyPartial, h.NotifyFailed, h.NotifyRejected)
		line += fmt.Sprintf(" | request=%d rejected=%d", h.RequestAccepted, h.RequestRejected)
		if h.LastError != "" {
			line += fmt.Sprintf(" last_error=%q", h.LastError)
		}
		fmt.Println(line)
	}
	if st.ScheduleError != "" {
		fmt.Printf("schedule  不可用: %s\n", st.ScheduleError)
	}
	if st.Schedule != nil {
		sch := st.Schedule
		line := fmt.Sprintf("schedule  jobs=%d enabled=%d running=%d paused=%v triggers=%d failures=%d missed=%d",
			sch.Jobs, sch.Enabled, sch.Running, sch.Paused, sch.Triggers, sch.Failures, sch.Missed)
		if !sch.NextRunAt.IsZero() {
			line += " next=" + sch.NextRunAt.Local().Format("01-02 15:04:05")
		}
		if sch.LastError != "" {
			line += fmt.Sprintf(" last_error=%q", sch.LastError)
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

func lastLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	return lines[len(lines)-1]
}
