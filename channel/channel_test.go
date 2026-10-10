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

type fakeChannel struct {
	name    string
	maxLen  int
	failN   int
	unroute bool
	mu      sync.Mutex
	sent    []bus.OutboundMessage
	started bool
	stopped bool
	events  *[]string
}

func (f *fakeChannel) Name() string { return f.name }

func (f *fakeChannel) MaxLen() int { return f.maxLen }

func (f *fakeChannel) Start(context.Context, *bus.Bus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = true
	if f.events != nil {
		*f.events = append(*f.events, "start:"+f.name)
	}
	return nil
}

func (f *fakeChannel) Stop(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = true
	if f.events != nil {
		*f.events = append(*f.events, "stop:"+f.name)
	}
	return nil
}

func (f *fakeChannel) Send(_ context.Context, msg bus.OutboundMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return errors.New("模拟发送失败")
	}
	if f.unroute {
		return ErrUnroutable
	}
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeChannel) messages() []bus.OutboundMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bus.OutboundMessage(nil), f.sent...)
}

func TestSplit(t *testing.T) {
	if got := Split("短", 0); len(got) != 1 || got[0] != "短" {
		t.Fatalf("limit<=0 不应切分: %q", got)
	}
	if got := Split("", 10); len(got) != 1 {
		t.Fatalf("空串应返回单段: %q", got)
	}
	text := "第一段内容。\n\n第二段内容。\n\n第三段内容。"
	parts := Split(text, 12)
	if len(parts) < 2 {
		t.Fatalf("长文本应切分: %q", parts)
	}
	for _, p := range parts {
		if RuneLen(p) > 12 {
			t.Fatalf("切分超出上限: %q", p)
		}
	}
	if joined := strings.Join(parts, "\n\n"); !strings.Contains(strings.ReplaceAll(joined, "\n", ""), "第二段内容") {
		t.Fatalf("切分丢内容: %q", parts)
	}
	hard := strings.Repeat("字", 25)
	hp := Split(hard, 10)
	if len(hp) != 3 {
		t.Fatalf("无边界文本应硬切: %q", hp)
	}
	if strings.Join(hp, "") != hard {
		t.Fatalf("硬切丢字符: %q", hp)
	}
	lines := "aaaa\nbbbb\ncccc\ndddd"
	lp := Split(lines, 10)
	for _, p := range lp {
		if RuneLen(p) > 10 {
			t.Fatalf("按行切分超限: %q", p)
		}
	}
}

func TestSanitize(t *testing.T) {
	if got := Sanitize("a\r\nb\x00c\x1bd\te"); got != "a\nbcd\te" {
		t.Fatalf("控制字符未清理: %q", got)
	}
	if got := Sanitize("正常文本"); got != "正常文本" {
		t.Fatalf("明文被改动: %q", got)
	}
}

func TestManagerRegisterAndStartStopOrder(t *testing.T) {
	b := bus.New(4, 4)
	var events []string
	m := New(b)
	a := &fakeChannel{name: "a", maxLen: 10, events: &events}
	c := &fakeChannel{name: "c", maxLen: 10, events: &events}
	if err := m.Register(a); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	if err := m.Register(c); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	if err := m.Register(&fakeChannel{name: "a"}); err == nil {
		t.Fatal("重复注册应报错")
	}
	if err := m.Register(&fakeChannel{name: " "}); err == nil {
		t.Fatal("空名应报错")
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	want := []string{"start:a", "start:c", "stop:c", "stop:a"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("启停顺序 %v，期望 %v", events, want)
	}
	if !a.started || !a.stopped {
		t.Fatal("通道未被启停")
	}
}

func TestManagerRouteSplitsAndCounts(t *testing.T) {
	b := bus.New(4, 4)
	m := New(b, WithRetries(1), WithRetryDelay(0))
	f := &fakeChannel{name: "cli", maxLen: 5}
	if err := m.Register(f); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "cli", ChatID: "r1", Content: "一二三四五六七八九十十一"}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	msgs := f.messages()
	if len(msgs) < 3 {
		t.Fatalf("未按上限切分: %d 段", len(msgs))
	}
	for _, msg := range msgs {
		if RuneLen(msg.Content) > 5 {
			t.Fatalf("切分超限: %q", msg.Content)
		}
		if msg.ChatID != "r1" {
			t.Fatalf("路由丢失 ChatID: %+v", msg)
		}
	}
	st := m.Status()
	if len(st) != 1 || st[0].Sent != len(msgs) || st[0].Failed != 0 {
		t.Fatalf("状态异常: %+v", st)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "nope", Content: "x"}); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("未注册通道应报 ErrUnroutable: %v", err)
	}
}

func TestManagerRetryAndUnmatched(t *testing.T) {
	b := bus.New(4, 4)
	m := New(b, WithRetries(2), WithRetryDelay(0))
	flaky := &fakeChannel{name: "flaky", maxLen: 0, failN: 1}
	unmatched := &fakeChannel{name: "sink", maxLen: 0, unroute: true}
	if err := m.Register(flaky); err != nil {
		t.Fatal(err)
	}
	if err := m.Register(unmatched); err != nil {
		t.Fatal(err)
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "flaky", Content: "hi"}); err != nil {
		t.Fatalf("重试后应成功: %v", err)
	}
	if len(flaky.messages()) != 1 {
		t.Fatalf("重试未生效: %d", len(flaky.messages()))
	}
	if err := m.Route(context.Background(), bus.OutboundMessage{Channel: "sink", Content: "hi"}); err != nil {
		t.Fatalf("无目标不应视为失败: %v", err)
	}
	st := m.Status()
	for _, s := range st {
		switch s.Name {
		case "flaky":
			if s.Sent != 1 || s.Failed != 0 {
				t.Fatalf("flaky 状态异常: %+v", s)
			}
		case "sink":
			if s.Unmatched != 1 || s.Failed != 0 {
				t.Fatalf("sink 状态异常: %+v", s)
			}
		}
	}
}

func TestManagerConsumeDrainsThenStops(t *testing.T) {
	b := bus.New(4, 4)
	m := New(b, WithRetries(1), WithRetryDelay(0))
	f := &fakeChannel{name: "cli", maxLen: 0}
	if err := m.Register(f); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		m.Consume(context.Background())
		close(done)
	}()
	if !b.Post(bus.OutboundMessage{Channel: "cli", Content: "a"}) {
		t.Fatal("投递失败")
	}
	b.CloseOutbound()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Consume 未在出站流关闭后返回")
	}
	select {
	case <-m.Drained():
	default:
		t.Fatal("Drained 未关闭")
	}
	if len(f.messages()) != 1 {
		t.Fatalf("出站消息未被消费: %d", len(f.messages()))
	}
}
