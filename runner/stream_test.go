package runner

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	chlog "github.com/LaoQi/ivan/channel/log"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func streamConfig(t *testing.T, baseURL string) *config.Config {
	t.Helper()
	cfg := bareConfig(t, baseURL)
	cfg.Runner.Stream = true
	cfg.Runner.StreamFirstSeconds = 0
	cfg.Runner.StreamEverySeconds = 1
	return cfg
}

// slowServer 在写出首段正文前阻塞 hold，用来观察静默期的中间态投递。
func slowServer(t *testing.T, step fakellm.Step, hold time.Duration) *fakellm.Server {
	t.Helper()
	return fakellm.NewDelay(hold, step)
}

type sink struct {
	mu   sync.Mutex
	got  []bus.OutboundMessage
	wake chan struct{}
}

func newSink() *sink { return &sink{wake: make(chan struct{}, 64)} }

func (s *sink) notify(_ context.Context, m bus.OutboundMessage) error {
	s.mu.Lock()
	s.got = append(s.got, m)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

func (s *sink) wait(t *testing.T, d time.Duration, fn func([]bus.OutboundMessage) bool) []bus.OutboundMessage {
	t.Helper()
	deadline := time.After(d)
	for {
		s.mu.Lock()
		snapshot := append([]bus.OutboundMessage(nil), s.got...)
		s.mu.Unlock()
		if fn(snapshot) {
			return snapshot
		}
		select {
		case <-s.wake:
		case <-deadline:
			return snapshot
		}
	}
}

func TestStreamPublishesFinalOnlyOnSimpleTurn(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答案"})
	defer srv.Close()
	cfg := streamConfig(t, srv.BaseURL())
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	sk := newSink()
	r.opts.Notify = sk.notify
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "问题")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got := sk.wait(t, time.Second, func(m []bus.OutboundMessage) bool { return len(m) > 0 })
	if len(got) == 0 {
		t.Fatal("没有出站消息")
	}
	last := got[len(got)-1]
	if !last.Final || last.Stream == "" || last.Content != "答案" {
		t.Fatalf("终局消息异常: %+v", last)
	}
	for i, m := range got[:len(got)-1] {
		if m.Final {
			t.Fatalf("第 %d 条不应是终局: %+v", i, m)
		}
	}
}

func TestStreamEmitsTypingWhileSilent(t *testing.T) {
	srv := slowServer(t, fakellm.Step{Content: "慢答案"}, 1500*time.Millisecond)
	defer srv.Close()
	cfg := streamConfig(t, srv.BaseURL())
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	sk := newSink()
	r.opts.Notify = sk.notify
	done := make(chan error, 1)
	go func() {
		err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "慢问题"))
		done <- err
	}()
	got := sk.wait(t, 3*time.Second, func(m []bus.OutboundMessage) bool {
		for _, x := range m {
			if x.Kind == bus.KindTyping {
				return true
			}
		}
		return false
	})
	typing := 0
	for _, m := range got {
		if m.Kind == bus.KindTyping {
			typing++
		}
	}
	if typing == 0 {
		t.Fatalf("静默期未投递输入状态: %+v", got)
	}
	if err := <-done; err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	all := sk.wait(t, time.Second, func(m []bus.OutboundMessage) bool {
		return len(m) > 0 && m[len(m)-1].Final
	})
	last := all[len(all)-1]
	if !last.Final || last.Content != "慢答案" {
		t.Fatalf("终局异常: %+v", last)
	}
}

func TestStreamContentUpdatesShareSingleStreamID(t *testing.T) {
	srv := fakellm.New(fakellm.Step{
		Content: "先写一段比较长的正文用来触发流式更新",
		Tools:   []fakellm.ToolCall{{Name: "run_shell", Args: `{"command":"echo hi"}`}},
		Usage:   fakellm.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}, fakellm.Step{Content: "最终答案"})
	defer srv.Close()
	cfg := streamConfig(t, srv.BaseURL())
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	sk := newSink()
	r.opts.Notify = sk.notify
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "干活")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got := sk.wait(t, 2*time.Second, func(m []bus.OutboundMessage) bool {
		return len(m) > 0 && m[len(m)-1].Final
	})
	ids := map[string]bool{}
	var final bus.OutboundMessage
	for _, m := range got {
		if m.Stream == "" {
			continue
		}
		ids[m.Stream] = true
		if m.Final {
			final = m
		}
	}
	if len(ids) != 1 {
		t.Fatalf("流式消息应共用同一 id，实际 %d 个: %v", len(ids), ids)
	}
	if !strings.Contains(final.Content, "最终答案") {
		t.Fatalf("终局内容异常: %q", final.Content)
	}
}

func TestStreamDisabledByConfig(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答案"})
	defer srv.Close()
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.Stream = false
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	sk := newSink()
	r.opts.Notify = sk.notify
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "问题")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got := sk.wait(t, 500*time.Millisecond, func(m []bus.OutboundMessage) bool { return len(m) > 0 })
	for _, m := range got {
		if m.Stream != "" {
			t.Fatalf("关闭流式后不应出现流式消息: %+v", m)
		}
	}
}

func TestStreamFinalKeepsErrorKind(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Status: 500})
	defer srv.Close()
	cfg := streamConfig(t, srv.BaseURL())
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	sk := newSink()
	r.opts.Notify = sk.notify
	if err := r.Dispatch(context.Background(), inbound("cli:default", "r1", "问题")); err == nil {
		t.Fatal("模型报错时 Dispatch 应返回错误")
	}
	got := sk.wait(t, time.Second, func(m []bus.OutboundMessage) bool {
		return len(m) > 0 && m[len(m)-1].Final
	})
	last := got[len(got)-1]
	if last.Kind != bus.KindError {
		t.Fatalf("失败回合应以 error 类型收尾: %+v", last)
	}
}
