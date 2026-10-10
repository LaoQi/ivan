package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultValues(t *testing.T) {
	cfg := Default()
	if cfg.BaseURL == "" || cfg.Model == "" || cfg.DataDir == "" || cfg.UserAgent == "" {
		t.Fatalf("默认配置缺必填项: %+v", cfg)
	}
	if cfg.SessionMode != DefaultSessionMode {
		t.Fatalf("SessionMode=%s", cfg.SessionMode)
	}
	if cfg.Runner.ContextWindowTokens <= 0 || cfg.Runner.HandoffThreshold <= 0 {
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
	body := "base_url: http://example.invalid/v1\napi_key: file-key\nmodel: file-model\ndata_dir: " + dir + "\nrunner:\n  context_window_tokens: 32000\n  auto_continue: true\n"
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
	if cfg.Runner.ContextWindowTokens != 32000 || !cfg.Runner.AutoContinue {
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
