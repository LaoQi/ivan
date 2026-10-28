package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/ivan/channel"
	"github.com/LaoQi/ivan/channel/cli"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func testConfig(t *testing.T, srv *fakellm.Server) *config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.Daemon.Socket = "run/ivan.sock"
	cfg.BaseURL = srv.BaseURL()
	cfg.APIKey = "test-key"
	cfg.Model = "mock-model"
	cfg.ApiProtocol = "chat"
	cfg.UserAgent = "ivan/test"
	cfg.ConfigPath = filepath.Join(dir, "config.yaml")
	cfg.SessionMode = "global"
	cfg.Daemon.ShutdownTimeoutSeconds = 3
	cfg.Runner.Stream = false
	cfg.HTTP.Listen = "127.0.0.1:0"
	return cfg
}

func waitSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("unix", path, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("socket 未就绪: %s", path)
}

type running struct {
	d      *Daemon
	cancel context.CancelFunc
	errCh  chan error
}

func start(t *testing.T, cfg *config.Config) *running {
	t.Helper()
	d, err := New(cfg, "test", io.Discard)
	if err != nil {
		t.Fatalf("daemon.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()
	waitSocket(t, cfg.SocketPath())
	return &running{d: d, cancel: cancel, errCh: errCh}
}

func (r *running) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.errCh:
		if err != nil {
			t.Fatalf("Run 返回错误: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("daemon 未在超时内停机")
	}
}

func fetchStatus(t *testing.T, cfg *config.Config) Status {
	t.Helper()
	raw, err := cli.Status(cfg.SocketPath(), 5*time.Second)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	var st Status
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("解析 status: %v", err)
	}
	return st
}

func TestSendStatusRestartResume(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "第一轮回复"}, fakellm.Step{Content: "第二轮回合"})
	defer srv.Close()
	cfg := testConfig(t, srv)

	run1 := start(t, cfg)
	if reply, err := cli.Send(cfg.SocketPath(), "你好", "", 10*time.Second); err != nil || reply != "第一轮回复" {
		t.Fatalf("send 往返失败: %q %v", reply, err)
	}
	st := fetchStatus(t, cfg)
	if !st.Running || st.PID != os.Getpid() {
		t.Fatalf("status 异常: %+v", st)
	}
	deadline := time.Now().Add(3 * time.Second)
	var cliStat *channel.Status
	for time.Now().Before(deadline) {
		for i := range st.Channels {
			if st.Channels[i].Name == "cli" {
				cliStat = &st.Channels[i]
			}
		}
		if cliStat != nil && cliStat.Sent > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
		st = fetchStatus(t, cfg)
	}
	if cliStat == nil || cliStat.Sent != 1 {
		t.Fatalf("通道状态异常: %+v", st.Channels)
	}
	if st.Runner.Turns != 1 || st.Runner.Agents != 1 || st.Bus.Inbound != 1 || st.Bus.Outbound != 1 {
		t.Fatalf("计数异常: %+v", st)
	}
	deadline = time.Now().Add(3 * time.Second)
	for st.Events.Written == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		st = fetchStatus(t, cfg)
	}
	if st.Events.Written == 0 {
		t.Fatal("事件未落盘")
	}
	if st.Runner.Sessions != 1 {
		t.Fatalf("会话映射数 %d，期望 1", st.Runner.Sessions)
	}
	state1, err := data.LoadState(cfg.StatePath())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	first, ok := state1.Get("cli:default")
	if !ok || first.SessionID == "" {
		t.Fatalf("映射未持久化: %+v", first)
	}
	if _, err := New(cfg, "test", io.Discard); !errors.Is(err, data.ErrLocked) {
		t.Fatalf("第二实例应被单实例锁拒绝: %v", err)
	}
	run1.stop(t)

	logRaw, err := os.ReadFile(cfg.LogPath())
	if err != nil {
		t.Fatalf("读日志: %v", err)
	}
	if !strings.Contains(string(logRaw), "daemon 启动") || !strings.Contains(string(logRaw), "停机完成") {
		t.Fatalf("日志内容异常: %s", logRaw)
	}
	if _, err := os.Stat(cfg.SocketPath()); !os.IsNotExist(err) {
		t.Fatalf("停机后 socket 应被清理: %v", err)
	}

	run2 := start(t, cfg)
	if reply, err := cli.Send(cfg.SocketPath(), "继续聊", "", 10*time.Second); err != nil || reply != "第二轮回合" {
		t.Fatalf("重启后 send 失败: %q %v", reply, err)
	}
	run2.stop(t)

	state2, err := data.LoadState(cfg.StatePath())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	second, _ := state2.Get("cli:default")
	if second.SessionID != first.SessionID {
		t.Fatalf("重启后会话未续上: %s → %s", first.SessionID, second.SessionID)
	}
	reqs := srv.Requests()
	if len(reqs) != 2 {
		t.Fatalf("请求数 %d，期望 2", len(reqs))
	}
	joined := ""
	for _, m := range reqs[1].Messages {
		joined += string(m)
	}
	if !strings.Contains(joined, "第一轮回复") {
		t.Fatalf("重启后未带上历史上下文: %s", joined)
	}
}

func TestInboundDroppedWhenBusClosed(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "x"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	r := start(t, cfg)
	defer r.stop(t)
	if _, err := cli.Send(cfg.SocketPath(), "hi", "", 5*time.Second); err != nil {
		t.Fatalf("send: %v", err)
	}
	st := fetchStatus(t, cfg)
	if st.Bus.LastDrop.IsZero() != true {
		t.Fatal("尚无丢弃时不应记录 last_drop")
	}
	if st.Bus.InboundDropped != 0 || st.Bus.OutboundDropped != 0 {
		t.Fatalf("不应有丢弃: %+v", st.Bus)
	}
}

func TestControlUnknownOp(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "x"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	d, err := New(cfg, "test", io.Discard)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Release()
	if _, err := d.Control("jobs", ""); err == nil {
		t.Fatal("未实现的 op 应报错")
	}
	if st, err := d.Control(cli.OpStatus, ""); err != nil {
		t.Fatalf("status: %v", err)
	} else if _, ok := st.(Status); !ok {
		t.Fatalf("status 返回类型异常: %T", st)
	}
}

func TestReleaseIsIdempotentAndUnlocks(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "x"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	d, err := New(cfg, "test", io.Discard)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	d.Release()
	d.Release()
	d1, err := New(cfg, "test", io.Discard)
	if err != nil {
		t.Fatalf("释放后应可再次启动: %v", err)
	}
	if _, err := New(cfg, "test", io.Discard); !errors.Is(err, data.ErrLocked) {
		t.Fatalf("重入实例应被拒绝: %v", err)
	}
	d1.Release()
	d2, err := New(cfg, "test", io.Discard)
	if err != nil {
		t.Fatalf("再次释放后应可启动: %v", err)
	}
	d2.Release()
}

func TestNewFailsOnCorruptState(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "x"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.StatePath(), []byte("{坏"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(cfg, "test", io.Discard); err == nil {
		t.Fatal("状态文件损坏应拒绝启动")
	}
	raw, _ := os.ReadFile(cfg.StatePath())
	if !strings.Contains(string(raw), "坏") {
		t.Fatalf("损坏文件应保留: %q", raw)
	}
}

func TestDaemonStreamsToCliClient(t *testing.T) {
	srv := fakellm.NewChunked(400*time.Millisecond, fakellm.Step{Content: "流式答案看看"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	cfg.Runner.Stream = true
	cfg.Runner.StreamFirstSeconds = 0
	cfg.Channels.CLI.Stream = true
	run := start(t, cfg)
	defer run.stop(t)

	var chunks []string
	reply, err := cli.SendStream(cfg.SocketPath(), "慢问题", "", 10*time.Second, func(s string) {
		chunks = append(chunks, s)
	})
	if err != nil {
		t.Fatalf("SendStream: %v", err)
	}
	if reply != "流式答案看看" {
		t.Fatalf("终局回复异常: %q", reply)
	}
	deadline := time.Now().Add(3 * time.Second)
	st := fetchStatus(t, cfg)
	for st.Channels[0].StreamFinal == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		st = fetchStatus(t, cfg)
	}
	c := st.Channels[0]
	if c.StreamFinal != 1 || c.StreamSent != 1 {
		t.Fatalf("流式计数异常: %+v", c)
	}
	if c.Sent != 0 {
		t.Fatalf("流式路径不应计普通发送: %+v", c)
	}
	if len(chunks) == 0 {
		t.Fatal("客户端未收到中间态增量")
	}
}
