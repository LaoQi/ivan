package data

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStateSetGetReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if s.Len() != 0 || s.Path() != path {
		t.Fatalf("初始状态异常: %d %s", s.Len(), s.Path())
	}
	if _, ok := s.Get("cli:default"); ok {
		t.Fatal("不存在的键不应命中")
	}
	if err := s.Set("cli:default", SessionState{SessionID: "20261010-190000", Workspace: "/tmp/ws"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Set("qq:c2c:u1", SessionState{SessionID: "20261010-190001", Workspace: "/tmp/ws2"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok := s.Get("cli:default")
	if !ok || got.SessionID != "20261010-190000" || got.UpdatedAt.IsZero() {
		t.Fatalf("Get 异常: %+v", got)
	}
	again, err := LoadState(path)
	if err != nil {
		t.Fatalf("重新读取: %v", err)
	}
	if again.Len() != 2 {
		t.Fatalf("重载后条数 %d，期望 2", again.Len())
	}
	if st, _ := again.Get("qq:c2c:u1"); st.Workspace != "/tmp/ws2" {
		t.Fatalf("重载内容异常: %+v", st)
	}
	if keys := again.Keys(); len(keys) != 2 || keys[0] != "cli:default" {
		t.Fatalf("Keys 未排序: %v", keys)
	}
	if err := again.Delete("cli:default"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if again.Len() != 1 {
		t.Fatalf("删除后条数 %d", again.Len())
	}
	if err := again.Delete("不存在"); err != nil {
		t.Fatalf("删除不存在的键应静默: %v", err)
	}
}

func TestStateAtomicWriteAndCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	s, _ := LoadState(path)
	if err := s.Set("k", SessionState{SessionID: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Fatalf("残留临时文件: %s", e.Name())
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("文件权限 %v，期望 0600", info.Mode().Perm())
	}
	if err := os.WriteFile(path, []byte("{坏 JSON"), 0o600); err != nil {
		t.Fatalf("写坏文件: %v", err)
	}
	if _, err := LoadState(path); err == nil {
		t.Fatal("损坏的状态文件应报错")
	}
	raw, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(raw), "坏 JSON") {
		t.Fatalf("损坏文件应被保留: %q %v", raw, err)
	}
}

func TestStateMappingTimestamps(t *testing.T) {
	s, _ := LoadState(filepath.Join(t.TempDir(), "state.json"))
	before := time.Now()
	if err := s.Set("a", SessionState{SessionID: "s1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	st, _ := s.Get("a")
	if st.UpdatedAt.Before(before.Add(-time.Second)) {
		t.Fatalf("UpdatedAt 未写入: %v", st.UpdatedAt)
	}
	snap := s.Snapshot()
	snap["a"] = SessionState{SessionID: "篡改"}
	if got, _ := s.Get("a"); got.SessionID != "s1" {
		t.Fatal("Snapshot 应返回副本")
	}
}
