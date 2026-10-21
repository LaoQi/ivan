package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultValues(t *testing.T) {
	cfg := Default()
	if cfg.BaseURL == "" || cfg.Model == "" || cfg.DataDir == "" || cfg.UserAgent == "" {
		t.Fatalf("默认配置缺必填项: %+v", cfg)
	}
	if cfg.SessionMode != DefaultSessionMode {
		t.Fatalf("SessionMode=%s", cfg.SessionMode)
	}
	if cfg.Runner.HandoffHintTokens != DefaultHandoffHintTokens || cfg.Runner.HandoffForceTokens != DefaultHandoffForceTokens {
		t.Fatalf("runner 默认值异常: %+v", cfg.Runner)
	}
	if cfg.Runner.AutoContinue {
		t.Fatal("auto_continue 必须默认关闭")
	}
	if cfg.ConfigPath == "" {
		t.Fatal("ConfigPath 未设置")
	}
}

func TestLoadFileAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "base_url: http://example.invalid/v1\napi_key: file-key\nmodel: file-model\ndata_dir: " + dir + "\nrunner:\n  handoff_hint_tokens: 32000\n  auto_continue: true\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IVAN_TANYA_CONFIG", filepath.Join(dir, "absent.yaml"))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APIKey != "file-key" || cfg.Model != "file-model" {
		t.Fatalf("文件值未生效: %+v", cfg)
	}
	if cfg.Runner.HandoffHintTokens != 32000 || !cfg.Runner.AutoContinue {
		t.Fatalf("runner 段未生效: %+v", cfg.Runner)
	}
	if cfg.Config.ConfigPath != path || cfg.Path != path {
		t.Fatalf("配置路径未回落: %+v", cfg)
	}

	t.Setenv("IVAN_MODEL", "env-model")
	t.Setenv("IVAN_API_KEY", "env-key")
	cfg2, err := Load(path)
	if err != nil {
		t.Fatalf("Load env: %v", err)
	}
	if cfg2.Model != "env-model" || cfg2.APIKey != "env-key" {
		t.Fatalf("env 未覆盖: %+v", cfg2)
	}
}

func TestLoadMissingFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("IVAN_TANYA_CONFIG", filepath.Join(dir, "absent.yaml"))
	cfg, err := Load(filepath.Join(dir, "absent.yaml"))
	if err != nil {
		t.Fatalf("缺文件应回落默认值: %v", err)
	}
	if cfg.BaseURL == "" {
		t.Fatal("默认 BaseURL 丢失")
	}
}

func TestBorrowTanyaConfig(t *testing.T) {
	dir := t.TempDir()
	borrowed := filepath.Join(dir, "tanya.yaml")
	if err := os.WriteFile(borrowed, []byte("base_url: http://tanya.invalid/v1\napi_key: tanya-key\nmodel: tanya-model\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IVAN_TANYA_CONFIG", borrowed)
	cfg, err := Load(filepath.Join(dir, "ivan.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.APIKey != "tanya-key" {
		t.Fatalf("未借用 tanya api_key: %q", cfg.APIKey)
	}
	if cfg.BaseURL != "http://tanya.invalid/v1" || cfg.Model != "tanya-model" {
		t.Fatalf("未借用 tanya 端点/模型: %s %s", cfg.BaseURL, cfg.Model)
	}

	t.Setenv("IVAN_API_KEY", "explicit")
	cfg2, err := Load(filepath.Join(dir, "ivan.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg2.APIKey != "explicit" {
		t.Fatalf("显式 env 应优先于借用: %q", cfg2.APIKey)
	}
}

func TestDaemonDefaultsAndPaths(t *testing.T) {
	cfg := Default()
	if cfg.Daemon.BusBuffer != DefaultBusBuffer || cfg.Daemon.MaxConcurrent != DefaultMaxConcurrent {
		t.Fatalf("daemon 默认值异常: %+v", cfg.Daemon)
	}
	if cfg.Runner.MaxAgents != DefaultMaxAgents || cfg.Runner.IdleTimeoutMinutes != DefaultIdleMinutes {
		t.Fatalf("池默认值异常: %+v", cfg.Runner)
	}
	if !cfg.CLIEnabled() {
		t.Fatal("cli 通道默认应启用")
	}
	if cfg.SendTimeout() != time.Duration(DefaultSendSeconds)*time.Second {
		t.Fatalf("SendTimeout=%v", cfg.SendTimeout())
	}
	if cfg.IdleTimeout() != time.Duration(DefaultIdleMinutes)*time.Minute {
		t.Fatalf("IdleTimeout=%v", cfg.IdleTimeout())
	}
	for _, p := range []string{cfg.SocketPath(), cfg.StatePath(), cfg.LockPath(), cfg.EventsDir(), cfg.LogPath()} {
		if !filepath.IsAbs(p) {
			t.Fatalf("路径应为绝对路径: %s", p)
		}
	}
	if cfg.AgentDir("cli-default") != filepath.Join(cfg.AgentsDir(), "cli-default") {
		t.Fatalf("AgentDir 异常: %s", cfg.AgentDir("cli-default"))
	}
}

func TestNormalizePathsAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "data_dir: " + dir + "\ndaemon:\n  socket: sock/ivan.sock\n  max_concurrent: -1\nrunner:\n  max_agents: 0\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IVAN_TANYA_CONFIG", filepath.Join(dir, "absent.yaml"))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := filepath.Join(dir, "sock", "ivan.sock"); cfg.SocketPath() != want {
		t.Fatalf("相对 socket 未落到 data_dir: %s", cfg.SocketPath())
	}
	if cfg.Daemon.MaxConcurrent != DefaultMaxConcurrent || cfg.Runner.MaxAgents != DefaultMaxAgents {
		t.Fatalf("非法值未回落到默认: %+v %+v", cfg.Daemon, cfg.Runner)
	}
	t.Setenv("IVAN_SOCKET", "/tmp/ivan-env.sock")
	cfg2, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg2.SocketPath() != "/tmp/ivan-env.sock" {
		t.Fatalf("IVAN_SOCKET 未生效: %s", cfg2.SocketPath())
	}
}

func TestExpandHomeDataDir(t *testing.T) {
	home, _ := os.UserHomeDir()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("data_dir: ~/.local/share/ivan-test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IVAN_TANYA_CONFIG", filepath.Join(dir, "absent.yaml"))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := filepath.Join(home, ".local/share/ivan-test"); cfg.DataDir != want {
		t.Fatalf("~ 未展开: %s", cfg.DataDir)
	}
	if want := filepath.Join(home, ".local/share/ivan-test", DefaultSocket); cfg.SocketPath() != want {
		t.Fatalf("socket 未跟随 data_dir: %s", cfg.SocketPath())
	}
}

func TestHandoffTierValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "data_dir: " + dir + "\nrunner:\n  handoff_hint_tokens: 300000\n  handoff_force_tokens: 200000\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "handoff_force_tokens") {
		t.Fatalf("强制档小于建议档应报错，得到 %v", err)
	}
}

func TestGlobalAgentsPath(t *testing.T) {
	cfg := Default()
	home, _ := os.UserHomeDir()
	if want := filepath.Join(home, filepath.FromSlash(GlobalAgentsSuffix)); cfg.GlobalAgentsPath() != want {
		t.Fatalf("默认全局说明路径 = %q，期望 %q", cfg.GlobalAgentsPath(), want)
	}
	if !strings.Contains(cfg.GlobalAgentsPath(), "/.config/ivan/AGENTS.md") {
		t.Fatalf("默认路径应指向 ivan 自己的配置目录: %q", cfg.GlobalAgentsPath())
	}

	for _, off := range []string{"off", "OFF", "none", "-", "  off  "} {
		cfg.GlobalAgents = off
		if got := cfg.GlobalAgentsPath(); got != "" {
			t.Fatalf("%q 应禁用全局说明，实际 %q", off, got)
		}
	}

	cfg.GlobalAgents = "~/my/agents.md"
	cfg.normalize()
	if cfg.GlobalAgentsPath() != filepath.Join(home, "my", "agents.md") {
		t.Fatalf("~ 未展开: %q", cfg.GlobalAgentsPath())
	}
}

func TestQQMarkdownDefaultsOn(t *testing.T) {
	cfg := Default()
	if !cfg.Channels.QQ.Markdown {
		t.Fatal("QQ markdown 应默认开启")
	}
}

func TestQQMarkdownFromYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "channels:\n  qq:\n    enabled: true\n    markdown: false\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Channels.QQ.Markdown {
		t.Fatal("配置中的 markdown: false 应生效")
	}
}
