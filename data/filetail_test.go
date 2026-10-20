package data

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStateRunningTurnLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if got := s.Running(); len(got) != 0 {
		t.Fatalf("初始不应有回合标记: %v", got)
	}
	if err := s.BeginTurn("cli:default", RunningTurn{Channel: "cli", ChatID: "c1", Session: "20261022-010101"}); err != nil {
		t.Fatalf("BeginTurn: %v", err)
	}
	got := s.Running()
	rt, ok := got["cli:default"]
	if !ok || rt.Channel != "cli" || rt.ChatID != "c1" || rt.Session != "20261022-010101" || rt.StartedAt.IsZero() {
		t.Fatalf("回合标记内容异常: %+v", rt)
	}

	again, err := LoadState(path)
	if err != nil {
		t.Fatalf("重载: %v", err)
	}
	if _, ok := again.Running()["cli:default"]; !ok {
		t.Fatalf("回合标记应落盘: %v", again.Running())
	}

	if err := again.EndTurn("cli:default"); err != nil {
		t.Fatalf("EndTurn: %v", err)
	}
	if got := again.Running(); len(got) != 0 {
		t.Fatalf("EndTurn 后应为空: %v", got)
	}
	if err := again.EndTurn("不存在"); err != nil {
		t.Fatalf("EndTurn 不存在的键应静默: %v", err)
	}

	if err := again.BeginTurn("cli:a", RunningTurn{Channel: "cli"}); err != nil {
		t.Fatalf("BeginTurn: %v", err)
	}
	if err := again.BeginTurn("cli:b", RunningTurn{Channel: "schedule"}); err != nil {
		t.Fatalf("BeginTurn: %v", err)
	}
	left, err := again.DrainRunning()
	if err != nil {
		t.Fatalf("DrainRunning: %v", err)
	}
	if len(left) != 2 || left["cli:b"].Channel != "schedule" {
		t.Fatalf("DrainRunning 内容异常: %+v", left)
	}
	if got := again.Running(); len(got) != 0 {
		t.Fatalf("DrainRunning 应清空: %v", got)
	}
	reload, err := LoadState(path)
	if err != nil {
		t.Fatalf("重载: %v", err)
	}
	if got := reload.Running(); len(got) != 0 {
		t.Fatalf("DrainRunning 应落盘: %v", got)
	}
	if left, err := reload.DrainRunning(); err != nil || len(left) != 0 {
		t.Fatalf("无标记时 DrainRunning 应为空: %v %v", left, err)
	}
}

func TestTrimFileTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "memory", "cli-default.md")
	if removed, err := TrimFileTail(path, 100); err != nil || removed != 0 {
		t.Fatalf("文件不存在应静默: %d %v", removed, err)
	}

	line := "0123456789\n"
	content := strings.Repeat(line, 30)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if removed, err := TrimFileTail(path, 0); err != nil || removed != 0 {
		t.Fatalf("maxBytes<=0 不应裁剪: %d %v", removed, err)
	}
	if removed, err := TrimFileTail(path, 4096); err != nil || removed != 0 {
		t.Fatalf("未超阈值不应裁剪: %d %v", removed, err)
	}

	removed, err := TrimFileTail(path, 100)
	if err != nil {
		t.Fatalf("TrimFileTail: %v", err)
	}
	if removed <= 0 {
		t.Fatalf("应裁掉内容: %d", removed)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(raw)
	if !strings.HasPrefix(text, "<!-- 记忆已裁剪") {
		t.Fatalf("缺少裁剪标记行: %q", text[:40])
	}
	if !strings.HasSuffix(text, line) {
		t.Fatalf("应按整行保留尾部: %q", text[len(text)-20:])
	}
	tail := text[strings.Index(text, "-->")+4:]
	tail = strings.TrimPrefix(tail, "\n")
	if len(tail) > 100 || len(tail)%len(line) != 0 {
		t.Fatalf("尾部不是整行且超出上限: %d 字节", len(tail))
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("裁剪后权限应为 0600: %v", st.Mode().Perm())
	}
	if size, err := FileSize(path); err != nil || size != len(raw) {
		t.Fatalf("FileSize: %d %v", size, err)
	}
	if size, err := FileSize(filepath.Join(dir, "nope")); err != nil || size != 0 {
		t.Fatalf("FileSize 不存在应返回 0: %d %v", size, err)
	}
}
