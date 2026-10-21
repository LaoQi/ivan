package runner

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
	chlog "github.com/LaoQi/ivan/channel/log"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/internal/fakellm"
)

type fakeChannel struct {
	name string

	mu      sync.Mutex
	opens   []bus.OutboundMessage
	updates []string
	finals  []string
	typing  int
	plain   []string
}

func (f *fakeChannel) Name() string { return f.name }

func (f *fakeChannel) Start(context.Context, *bus.Bus) error { return nil }

func (f *fakeChannel) Stop(context.Context) error { return nil }

func (f *fakeChannel) Send(_ context.Context, msg bus.OutboundMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if msg.Kind == bus.KindTyping {
		f.typing++
		return nil
	}
	f.plain = append(f.plain, msg.Content)
	return nil
}

func (f *fakeChannel) StreamMaxLen() int { return 1200 }

func (f *fakeChannel) StreamOpen(_ context.Context, msg bus.OutboundMessage) (channel.StreamHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens = append(f.opens, msg)
	return f, nil
}

func (f *fakeChannel) Typing(context.Context, bus.OutboundMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.typing++
	return nil
}

func (f *fakeChannel) Update(_ context.Context, text string, done bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if done {
		f.finals = append(f.finals, text)
		return nil
	}
	f.updates = append(f.updates, text)
	return nil
}

func (f *fakeChannel) snapshot() (opens []bus.OutboundMessage, updates, finals, plain []string, typing int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bus.OutboundMessage(nil), f.opens...), append([]string(nil), f.updates...),
		append([]string(nil), f.finals...), append([]string(nil), f.plain...), f.typing
}

// TestStreamThroughManager 走完整链路：runner → bus → Manager → 通道流式句柄。
func TestStreamThroughManager(t *testing.T) {
	srv := fakellm.NewDelay(1500*time.Millisecond, fakellm.Step{Content: "最终长答案"})
	defer srv.Close()
	cfg := streamConfig(t, srv.BaseURL())
	b := bus.New(16, 16)
	mgr := channel.New(b, channel.WithRetries(1), channel.WithRetryDelay(0))
	fc := &fakeChannel{name: "cli"}
	if err := mgr.Register(fc); err != nil {
		t.Fatalf("Register: %v", err)
	}
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard), Bus: b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Consume(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Dispatch(ctx, inbound("cli:default", "r1", "慢问题")) }()
	if err := <-done; err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, _, finals, _, _ := fc.snapshot()
		if len(finals) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	opens, _, finals, plain, typing := fc.snapshot()
	if len(opens) != 1 {
		t.Fatalf("开流次数 %d，期望 1（应只有终局正文）", len(opens))
	}
	if len(finals) != 1 || !strings.Contains(finals[0], "最终长答案") {
		t.Fatalf("定稿异常: %v", finals)
	}
	if typing == 0 {
		t.Fatal("静默期未发输入状态")
	}
	if len(plain) != 0 {
		t.Fatalf("流式路径不应产生普通消息: %v", plain)
	}
}

// TestStreamFallsBackWhenChannelLacksStreaming 验证不支持流式的通道只收到终局普通消息。
func TestStreamFallsBackWhenChannelLacksStreaming(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "普通答案"})
	defer srv.Close()
	cfg := streamConfig(t, srv.BaseURL())
	b := bus.New(16, 16)
	mgr := channel.New(b, channel.WithRetries(1), channel.WithRetryDelay(0))
	if err := mgr.Register(&plainChannel{name: "cli"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	r, err := New(Options{Cfg: cfg, Human: chlog.New(io.Discard), Bus: b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Consume(ctx)
	if err := r.Dispatch(ctx, inbound("cli:default", "r2", "问题")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	var st channel.Status
	for {
		st = mgr.Status()[0]
		if st.Sent > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st.Sent != 1 || st.StreamSent != 0 {
		t.Fatalf("退化路径计数异常: %+v", st)
	}
}

type plainChannel struct {
	name string
	mu   sync.Mutex
	got  []string
}

func (p *plainChannel) Name() string { return p.name }

func (p *plainChannel) Start(context.Context, *bus.Bus) error { return nil }

func (p *plainChannel) Stop(context.Context) error { return nil }

func (p *plainChannel) Send(_ context.Context, msg bus.OutboundMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if msg.Kind != bus.KindTyping {
		p.got = append(p.got, msg.Content)
	}
	return nil
}

func (p *plainChannel) messages() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.got...)
}

var _ = config.Default
