package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/LaoQi/ivan/channel/cli"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stdout = old
	return <-done
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stderr = old
	return <-done
}

func writeConfig(t *testing.T, dir string) *config.Config {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	body := "base_url: http://127.0.0.1:1/v1\napi_key: k\nmodel: m\ndata_dir: " + dir + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IVAN_CONFIG", path)
	t.Setenv("IVAN_TANYA_CONFIG", filepath.Join(dir, "absent.yaml"))
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

func TestParseArgs(t *testing.T) {
	f, err := parseArgs([]string{"--session=work", "--timeout", "30", "你好 世界"}, "no-wait")
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if f.values["session"] != "work" || f.values["timeout"] != "30" {
		t.Fatalf("取值异常: %+v", f.values)
	}
	if strings.Join(f.rest, " ") != "你好 世界" {
		t.Fatalf("位置参数异常: %v", f.rest)
	}
	f2, err := parseArgs([]string{"--no-wait", "--json"}, "no-wait", "json")
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if !f2.bools["no-wait"] || !f2.bools["json"] {
		t.Fatalf("布尔开关异常: %+v", f2.bools)
	}
	if _, err := parseArgs([]string{"--session"}); err == nil {
		t.Fatal("缺取值应报错")
	}
	if _, err := parseArgs([]string{"--timeout=abc"}); err != nil {
		t.Fatalf("等号形式应可解析: %v", err)
	}
}

func TestParseDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"30":   30 * time.Second,
		"2m":   2 * time.Minute,
		"1.5s": 1500 * time.Millisecond,
	}
	for in, want := range cases {
		got, err := parseDuration(in)
		if err != nil || got != want {
			t.Errorf("parseDuration(%q) = %v, %v；期望 %v", in, got, err, want)
		}
	}
	if _, err := parseDuration("abc"); err == nil {
		t.Fatal("非法值应报错")
	}
}

func TestStatusFromDiskJSON(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir)
	state, err := data.LoadState(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Set("cli:default", data.SessionState{SessionID: "20261010-190000", Workspace: cfg.AgentDir("cli-default")}); err != nil {
		t.Fatal(err)
	}
	lock, err := data.AcquireLock(cfg.LockPath(), data.LockInfo{PID: os.Getpid(), Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()

	cause := fmt.Errorf("%w（socket %s：连接被拒绝）", cli.ErrNotRunning, cfg.SocketPath())
	out := captureStdout(t, func() {
		if code := statusFromDisk(cfg, true, cause); code != 0 {
			t.Errorf("退出码 %d", code)
		}
	})
	if !strings.Contains(out, `"running": false`) || !strings.Contains(out, "20261010-190000") {
		t.Fatalf("JSON 输出异常: %s", out)
	}
	human := captureStdout(t, func() {
		if code := statusFromDisk(cfg, false, cause); code != 0 {
			t.Errorf("退出码 %d", code)
		}
	})
	for _, want := range []string{"daemon 未运行", "cli:default → 20261010-190000", "lock      pid="} {
		if !strings.Contains(human, want) {
			t.Fatalf("人类可读输出缺 %q: %s", want, human)
		}
	}
}

func TestCmdStatusWithoutDaemon(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir)
	if _, err := data.LoadState(cfg.StatePath()); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() {
		if code := cmdStatus([]string{"--json"}); code != 0 {
			t.Errorf("退出码 %d", code)
		}
	})
	if !strings.Contains(out, `"running": false`) {
		t.Fatalf("未运行时状态输出异常: %s", out)
	}
}

func TestCmdRejectsBadInput(t *testing.T) {
	dir := t.TempDir()
	writeConfig(t, dir)
	captureStderr(t, func() {
		if code := cmdRun([]string{"   "}); code == 0 {
			t.Error("空指令应报错")
		}
		if code := cmdSend([]string{}); code == 0 {
			t.Error("空文本应报错")
		}
		if code := cmdDaemon([]string{"extra"}); code == 0 {
			t.Error("多余参数应报错")
		}
		if code := cmdStatus([]string{"多余"}); code == 0 {
			t.Error("多余参数应报错")
		}
		if code := cmdSend([]string{"--timeout=abc", "hi"}); code == 0 {
			t.Error("非法超时应报错")
		}
	})
}

func TestStatusFromDiskStaleLock(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir)
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	dead := exec.Command("true")
	if err := dead.Run(); err != nil {
		t.Fatalf("起子进程: %v", err)
	}
	body := fmt.Sprintf(`{"pid":%d,"started_at":"2026-10-10T00:00:00Z"}`+"\n", dead.Process.Pid)
	if err := os.WriteFile(cfg.LockPath(), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if data.ProcessAlive(dead.Process.Pid) {
		t.Skip("子进程 pid 仍被视为存活，跳过")
	}
	out := captureStdout(t, func() {
		statusFromDisk(cfg, false, cli.ErrNotRunning)
	})
	if !strings.Contains(out, "锁文件残留") {
		t.Fatalf("未识别残留锁: %s", out)
	}
	if !errors.Is(cli.ErrNotRunning, cli.ErrNotRunning) {
		t.Fatal("unreachable")
	}
}

func TestDispatchMeta(t *testing.T) {
	out := captureStdout(t, func() {
		if code, done := dispatchMeta([]string{"version"}); !done || code != 0 {
			t.Errorf("version: %d %v", code, done)
		}
	})
	if !strings.Contains(out, "ivan ") {
		t.Fatalf("版本输出异常: %s", out)
	}
	out = captureStdout(t, func() {
		if code, done := dispatchMeta([]string{"config"}); !done || code != 0 {
			t.Errorf("config: %d %v", code, done)
		}
	})
	if !strings.Contains(out, "data_dir") {
		t.Fatalf("配置示例输出异常: %s", out[:min(len(out), 80)])
	}
	if _, done := dispatchMeta([]string{"send"}); done {
		t.Fatal("send 不应由 meta 处理")
	}
}

func TestClipUsesRune(t *testing.T) {
	if got := clip("中文内容", 2); got != "中文…" {
		t.Fatalf("应按字符截断: %q", got)
	}
	if got := clip("中文", 10); got != "中文" {
		t.Fatalf("未超限应原样: %q", got)
	}
	if got := clip(strings.Repeat("字", 100), 40); strings.ContainsRune(got, utf8.RuneError) {
		t.Fatal("按字节切分产生了乱码")
	}
}
