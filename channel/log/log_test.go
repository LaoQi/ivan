package log

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("磁盘满") }

func TestFormatLine(t *testing.T) {
	var buf bytes.Buffer
	s := New(&buf)
	ts := time.Date(2026, 10, 10, 18, 30, 5, 0, time.UTC)
	if err := s.Emit(Event{Time: ts, Kind: "turn_end", Session: "cli-default", Dur: 4200 * time.Millisecond, Context: 1702, Total: 3394}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	line := buf.String()
	if !strings.HasPrefix(line, "2026-10-10T18:30:05Z turn_end") {
		t.Fatalf("行格式异常: %q", line)
	}
	for _, want := range []string{"session=cli-default", "dur=4.2s", "ctx=1702", "total=3394"} {
		if !strings.Contains(line, want) {
			t.Fatalf("缺字段 %s: %q", want, line)
		}
	}
	if strings.Count(line, "\n") != 1 {
		t.Fatalf("应恰好一行: %q", line)
	}
}

func TestFormatMultilineAndTruncate(t *testing.T) {
	var buf bytes.Buffer
	s := New(&buf)
	_ = s.Emit(Event{Kind: "error", Err: "第一行\n第二行\t第三", Failed: true})
	line := buf.String()
	if strings.Contains(line, "\n第二行") {
		t.Fatalf("多行未压平: %q", line)
	}
	if !strings.Contains(line, "failed=true") || !strings.Contains(line, "第一行 第二行 第三") {
		t.Fatalf("字段异常: %q", line)
	}
	buf.Reset()
	_ = s.Emit(Event{Kind: "tool_end", Tool: "run_shell", Text: strings.Repeat("字", 500)})
	if len(buf.String()) > 400 {
		t.Fatalf("未截断: %d", len(buf.String()))
	}
	if s.Lines() != 2 {
		t.Fatalf("行计数异常: %d", s.Lines())
	}
}

func TestNoticeAndWriteErrors(t *testing.T) {
	s := New(failWriter{})
	if err := s.Notice("启动 pid=%d", 7); err == nil {
		t.Fatal("写入失败应返回错误")
	}
	if s.Errors() != 1 {
		t.Fatalf("错误计数异常: %d", s.Errors())
	}
	var nilSink *Sink
	if err := nilSink.Emit(Event{Kind: "daemon"}); err != nil {
		t.Fatalf("空 sink 应静默: %v", err)
	}
	if nilSink.Lines() != 0 {
		t.Fatal("空 sink 行计数应为 0")
	}
}
