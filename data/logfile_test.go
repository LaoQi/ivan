package data

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogFileAppendAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "ivan.log")
	l, err := NewLogFile(path)
	if err != nil {
		t.Fatalf("NewLogFile: %v", err)
	}
	if _, err := l.Write([]byte("raw line\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := l.Printf("daemon 启动 pid=%d", 7); err != nil {
		t.Fatalf("Printf: %v", err)
	}
	if l.Path() != path {
		t.Fatalf("路径异常: %s", l.Path())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读日志: %v", err)
	}
	if !strings.Contains(string(raw), "raw line") || !strings.Contains(string(raw), "daemon 启动 pid=7") {
		t.Fatalf("日志内容异常: %q", raw)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := l.Write([]byte("x")); err == nil {
		t.Fatal("关闭后写入应报错")
	}
	if err := l.Close(); err != nil {
		t.Fatalf("重复 Close 应静默: %v", err)
	}
	l2, err := NewLogFile(path)
	if err != nil {
		t.Fatalf("重新打开: %v", err)
	}
	defer l2.Close()
	if err := l2.Printf("第二行"); err != nil {
		t.Fatalf("Printf: %v", err)
	}
	raw, _ = os.ReadFile(path)
	if !strings.Contains(string(raw), "raw line") || !strings.Contains(string(raw), "第二行") {
		t.Fatalf("追加模式失效: %q", raw)
	}
}
