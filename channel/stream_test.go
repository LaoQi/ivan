package channel

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
)

type fakeStreamer struct {
	fakeChannel
	streamLen int
	mu        sync.Mutex
	opens     []bus.OutboundMessage
	updates   []string
	finals    []string
	typing    int
	failOn    map[int]bool
	n         int
}

func (f *fakeStreamer) StreamMaxLen() int {
	if f.streamLen > 0 {
		return f.streamLen
	}
	return f.maxLen
}

func (f *fakeStreamer) StreamOpen(_ context.Context, msg bus.OutboundMessage) (StreamHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	if f.failOn[f.n] {
		return nil, errors.New("模拟开流失败")
	}
	f.opens = append(f.opens, msg)
	return &fakeHandle{owner: f}, nil
}

func (f *fakeStreamer) Typing(context.Context, bus.OutboundMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.typing++
	return nil
}

type fakeHandle struct{ owner *fakeStreamer }

func (h *fakeHandle) Update(_ context.Context, text string, done bool) error {
	h.owner.mu.Lock()
	defer h.owner.mu.Unlock()
	if done {
		h.owner.finals = append(h.owner.finals, text)
	} else {
		h.owner.updates = append(h.owner.updates, text)
	}
	return nil
}

func (f *fakeStreamer) snapshot() (opens []bus.OutboundMessage, updates, finals []string, typing int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bus.OutboundMessage(nil), f.opens...), append([]string(nil), f.updates...), append([]string(nil), f.finals...), f.typing
}

func TestRouteStreamOpensUpdatesAndFinalizes(t *testing.T) {
	m := New(bus.New(4, 4), WithRetries(1), WithRetryDelay(0))
	f := &fakeStreamer{fakeChannel: fakeChannel{name: "qq", maxLen: 0}}
	if err := m.Register(f); err != nil {
		t.Fatal(err)
	}
	base := bus.OutboundMessage{Channel: "qq", ChatID: "u1", ReplyTo: "m1", Stream: "s1"}
	if err := m.Route(context.Background(), base); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Stream: "s1", Content: "你"}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Stream: "s1", Content: "你好"}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Stream: "s1", Content: "你好！", Final: true}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	opens, updates, finals, _ := f.snapshot()
	if len(opens) != 1 {
		t.Fatalf("开流次数 %d，期望 1（首条空内容不建流）", len(opens))
	}
	if len(updates) != 1 || updates[0] != "你好" {
		t.Fatalf("中间更新异常: %v", updates)
	}
	if opens[0].Content != "你" {
		t.Fatalf("首片内容异常: %+v", opens[0])
	}
	if len(finals) != 1 || finals[0] != "你好！" {
		t.Fatalf("定稿异常: %v", finals)
	}
	st := m.Status()[0]
	if st.StreamSent != 1 || st.StreamUpd != 1 || st.StreamFinal != 1 || st.Sent != 0 {
		t.Fatalf("流式计数异常: %+v", st)
	}
}

func TestRouteStreamTypingAndFallback(t *testing.T) {
	m := New(bus.New(4, 4), WithRetries(1), WithRetryDelay(0))
	plain := &fakeChannel{name: "sink", maxLen: 0}
	if err := m.Register(plain); err != nil {
		t.Fatal(err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "sink", ChatID: "u1", Stream: "s1", Content: "中间态"}); err != nil {
		t.Fatalf("不支持流式的通道不应报错: %v", err)
	}
	if msgs := plain.messages(); len(msgs) != 0 {
		t.Fatalf("中间态不应作为普通消息投递: %v", msgs)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "sink", ChatID: "u1", Stream: "s1", Content: "最终答案", Final: true}); err != nil {
		t.Fatalf("终局投递失败: %v", err)
	}
	msgs := plain.messages()
	if len(msgs) != 1 || msgs[0].Content != "最终答案" || msgs[0].Stream != "" {
		t.Fatalf("退化投递异常: %+v", msgs)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "sink", Kind: bus.KindTyping, ChatID: "u1"}); err != nil {
		t.Fatalf("输入状态对非流式通道应静默: %v", err)
	}
}

func TestRouteTyping(t *testing.T) {
	m := New(bus.New(4, 4), WithRetries(1), WithRetryDelay(0))
	f := &fakeStreamer{fakeChannel: fakeChannel{name: "qq", maxLen: 0}}
	if err := m.Register(f); err != nil {
		t.Fatal(err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Kind: bus.KindTyping}); err != nil {
		t.Fatalf("Typing: %v", err)
	}
	_, _, _, typing := f.snapshot()
	if typing != 1 {
		t.Fatalf("输入状态次数 %d，期望 1", typing)
	}
	if st := m.Status()[0]; st.Typing != 1 {
		t.Fatalf("计数异常: %+v", st)
	}
}

func TestRouteStreamOverflowSplitsTail(t *testing.T) {
	m := New(bus.New(4, 4), WithRetries(1), WithRetryDelay(0))
	f := &fakeStreamer{fakeChannel: fakeChannel{name: "cli", maxLen: 0}, streamLen: 10}
	if err := m.Register(f); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("甲", 30)
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "cli", ChatID: "r1", Stream: "s1", Content: long[:10]}); err != nil {
		t.Fatal(err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "cli", ChatID: "r1", Stream: "s1", Content: long, Final: true}); err != nil {
		t.Fatal(err)
	}
	_, _, finals, _ := f.snapshot()
	if len(finals) != 1 {
		t.Fatalf("定稿次数 %d", len(finals))
	}
	if !strings.HasSuffix(finals[0], streamEllipsis) {
		t.Fatalf("溢出定稿应带截断标记: %q", finals[0])
	}
	msgs := f.messages()
	if len(msgs) != 1 {
		t.Fatalf("溢出余下内容应补发一条: %d", len(msgs))
	}
	if !strings.HasSuffix(msgs[0].Content, "甲") || msgs[0].Stream != "" {
		t.Fatalf("补发内容异常: %+v", msgs[0])
	}
}

func TestRouteStreamOpenFailureFallsBack(t *testing.T) {
	m := New(bus.New(4, 4), WithRetries(1), WithRetryDelay(0))
	f := &fakeStreamer{fakeChannel: fakeChannel{name: "qq", maxLen: 0}, failOn: map[int]bool{1: true}}
	if err := m.Register(f); err != nil {
		t.Fatal(err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Stream: "s1", Content: "部分"}); err != nil {
		t.Fatalf("开流失败不应阻塞回合: %v", err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Stream: "s1", Content: "最终", Final: true}); err != nil {
		t.Fatalf("终局投递失败: %v", err)
	}
	msgs := f.messages()
	if len(msgs) != 1 || msgs[0].Content != "最终" {
		t.Fatalf("开流失败应退化为普通消息: %+v", msgs)
	}
	st := m.Status()[0]
	if st.StreamFail != 1 || st.Sent != 1 {
		t.Fatalf("计数异常: %+v", st)
	}
}

func TestRouteStreamConcurrentUpdatesSerialize(t *testing.T) {
	m := New(bus.New(8, 8), WithRetries(1), WithRetryDelay(0))
	f := &fakeStreamer{fakeChannel: fakeChannel{name: "qq", maxLen: 0}}
	if err := m.Register(f); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = m.Route(context.Background(), bus.OutboundMessage{
				Channel: "qq", ChatID: "u1", Stream: "s1", Content: strings.Repeat("a", i+1),
			})
		}(i)
	}
	wg.Wait()
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Stream: "s1", Content: "done", Final: true}); err != nil {
		t.Fatal(err)
	}
	opens, _, finals, _ := f.snapshot()
	if len(opens) != 1 {
		t.Fatalf("并发同 id 只应开一次流: %d", len(opens))
	}
	if len(finals) != 1 {
		t.Fatalf("定稿应只发生一次: %d", len(finals))
	}
}

func TestAbortStreams(t *testing.T) {
	m := New(bus.New(4, 4), WithRetries(1), WithRetryDelay(0))
	f := &fakeStreamer{fakeChannel: fakeChannel{name: "qq", maxLen: 0}}
	if err := m.Register(f); err != nil {
		t.Fatal(err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "qq", ChatID: "u1", Stream: "s1", Content: "半截"}); err != nil {
		t.Fatal(err)
	}
	m.AbortStreams()
	_, _, finals, _ := f.snapshot()
	if len(finals) != 1 || finals[0] != "半截" {
		t.Fatalf("停机应原位收尾: %v", finals)
	}
	deadline := time.Now().Add(time.Second)
	for m.streams.len() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := m.streams.len(); n != 0 {
		t.Fatalf("停机后残留 %d 条流", n)
	}
}
