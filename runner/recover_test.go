package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func requestContains(t *testing.T, srv *fakellm.Server, want string) bool {
	t.Helper()
	reqs := srv.Requests()
	if len(reqs) == 0 {
		t.Fatal("没有请求")
	}
	for _, msg := range reqs[len(reqs)-1].Messages {
		if strings.Contains(string(msg), want) {
			return true
		}
	}
	return false
}

func TestRunningTurnMarkedAndCleared(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-release
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, `data: {"choices":[{"delta":{"content":"ok"}}]}`+"\n\n")
		fmt.Fprint(w, `data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":1,"total_tokens":11}}`+"\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer ts.Close()

	cfg := bareConfig(t, ts.URL+"/v1")
	r := newP4Rig(t, cfg, nil)
	done := make(chan error, 1)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:default", "c1", "你好")) }()

	var running map[string]data.RunningTurn
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if running = r.opts.State.Running(); len(running) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(running) != 1 {
		t.Fatalf("回合进行中应有一条标记，实际 %v", running)
	}
	rt, ok := running["cli:default"]
	if !ok || rt.Channel != bus.ChannelCLI || rt.ChatID != "c1" || rt.StartedAt.IsZero() {
		t.Fatalf("回合标记内容异常: %+v", rt)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if left := r.opts.State.Running(); len(left) != 0 {
		t.Fatalf("回合结束后应清空标记，实际 %v", left)
	}
	disk, err := data.LoadState(cfg.StatePath())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if len(disk.Running()) != 0 {
		t.Fatalf("落盘后不应残留标记: %v", disk.Running())
	}
}

func TestInterruptedTurnRecoveredAndNoticed(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "好的", Usage: fakellm.Usage{PromptTokens: 20, CompletionTokens: 2, TotalTokens: 22}})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())

	pre, err := data.LoadState(cfg.StatePath())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if err := pre.BeginTurn("cli:default", data.RunningTurn{Channel: bus.ChannelCLI, ChatID: "c1"}); err != nil {
		t.Fatalf("BeginTurn: %v", err)
	}

	r := newP4Rig(t, cfg, nil)
	if got := r.Status().Interrupted; got != 1 {
		t.Fatalf("interrupted 计数 %d，期望 1", got)
	}
	disk, err := data.LoadState(cfg.StatePath())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if left := disk.Running(); len(left) != 0 {
		t.Fatalf("残留标记应清理落盘: %v", left)
	}

	if err := r.Dispatch(context.Background(), inbound("cli:default", "c1", "接着做")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !requestContains(t, srv, "上一次回合在执行中因进程退出被中断") {
		t.Fatalf("首个请求没有带上中断提示: %v", srv.Requests()[0].Messages)
	}

	if err := r.Dispatch(context.Background(), inbound("cli:default", "c1", "再来一次")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if again := lastMessage(t, srv); strings.Contains(again, "被中断") {
		t.Fatalf("中断提示只应注入一次（后续回合不应重复注入）: %s", again)
	}
}

func TestMemoryTrimmedAfterTurn(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "记下了", Usage: fakellm.Usage{PromptTokens: 20, CompletionTokens: 2, TotalTokens: 22}})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.MemoryMaxBytes = 200
	r := newP4Rig(t, cfg, nil)

	path := r.memoryPath("cli:default")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	line := "旧记忆一条比较长的内容\n"
	big := strings.Repeat(line, 40)
	if err := os.WriteFile(path, []byte(big), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := r.Dispatch(context.Background(), inbound("cli:default", "c1", "整理记忆")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(raw)
	if !strings.HasPrefix(text, "<!-- 记忆已裁剪") {
		t.Fatalf("裁剪后应带标记行: %s", text[:60])
	}
	if strings.HasSuffix(text, "\n\n") || !strings.HasSuffix(text, line) {
		t.Fatalf("裁剪应按整行保留尾部: %q", text[len(text)-40:])
	}
	if len(raw) > 200+120 {
		t.Fatalf("裁剪后体积 %d 仍过大", len(raw))
	}
	if strings.Count(text, line) >= 40 {
		t.Fatalf("旧内容应被裁掉一部分")
	}
}

func TestMemoryBelowLimitUntouched(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok", Usage: fakellm.Usage{PromptTokens: 20, CompletionTokens: 2, TotalTokens: 22}})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.MemoryMaxBytes = 4096
	r := newP4Rig(t, cfg, nil)

	path := r.memoryPath("cli:default")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := "一条短记忆\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := r.Dispatch(context.Background(), inbound("cli:default", "c1", "你好")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(raw) != content {
		t.Fatalf("未超阈值的记忆不应被改动: %q", raw)
	}
}

func TestGlobalAgentsIsolatedToIvan(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok", Usage: fakellm.Usage{PromptTokens: 20, CompletionTokens: 2, TotalTokens: 22}})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())

	dir := t.TempDir()
	global := filepath.Join(dir, "AGENTS.md")
	if err := os.WriteFile(global, []byte("ivan 专属全局指令 X"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	cfg.GlobalAgents = global
	r := newP4Rig(t, cfg, nil)
	if err := r.Dispatch(context.Background(), inbound("cli:default", "c1", "你好")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	reqs := srv.Requests()
	if len(reqs) == 0 || len(reqs[0].Messages) == 0 {
		t.Fatal("没有请求")
	}
	system := string(reqs[0].Messages[0])
	if !strings.Contains(system, "ivan 专属全局指令 X") {
		t.Fatalf("系统提示未带上 ivan 的全局说明: %s", system)
	}
	if !strings.Contains(system, "# 全局说明（") || strings.Contains(system, ".config/tanya/AGENTS.md") {
		t.Fatalf("全局说明标题应指向 ivan 自己的路径: %s", system)
	}
}

func TestGlobalAgentsOff(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "ok", Usage: fakellm.Usage{PromptTokens: 20, CompletionTokens: 2, TotalTokens: 22}})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.GlobalAgents = "off"
	r := newP4Rig(t, cfg, nil)
	if err := r.Dispatch(context.Background(), inbound("cli:default", "c1", "你好")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	reqs := srv.Requests()
	system := string(reqs[0].Messages[0])
	if strings.Contains(system, "# 全局说明") || strings.Contains(system, "全局说明（") {
		t.Fatalf("off 时不应读全局说明: %s", system)
	}
}
