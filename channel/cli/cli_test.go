package cli

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
)

type harness struct {
	t    *testing.T
	bus  *bus.Bus
	sock *Socket
	path string
	mu   sync.Mutex
	seen []bus.InboundMessage
	stop chan struct{}
	once sync.Once
}

func newHarness(t *testing.T, control Control) *harness {
	t.Helper()
	h := &harness{t: t, bus: bus.New(8, 8), stop: make(chan struct{})}
	h.path = filepath.Join(t.TempDir(), "ivan.sock")
	h.sock = NewSocket(h.path, control)
	if err := h.sock.Start(context.Background(), h.bus); err != nil {
		t.Fatalf("Start: %v", err)
	}
	go func() {
		for {
			select {
			case msg, ok := <-h.bus.Inbound():
				if !ok {
					return
				}
				h.mu.Lock()
				h.seen = append(h.seen, msg)
				h.mu.Unlock()
				h.bus.Post(bus.OutboundMessage{Channel: msg.Channel, ChatID: msg.ChatID, ReplyTo: msg.MessageID, Content: "echo: " + msg.Content})
			case <-h.stop:
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case msg, ok := <-h.bus.Outbound():
				if !ok {
					return
				}
				_ = h.sock.Send(context.Background(), msg)
			case <-h.stop:
				return
			}
		}
	}()
	t.Cleanup(func() {
		h.once.Do(func() { close(h.stop) })
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.sock.Stop(ctx)
		h.bus.Close()
	})
	return h
}

func (h *harness) messages() []bus.InboundMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]bus.InboundMessage(nil), h.seen...)
}

func TestSocketRoundTrip(t *testing.T) {
	h := newHarness(t, nil)
	text, err := Send(h.path, "你好", "", 3*time.Second)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if text != "echo: 你好" {
		t.Fatalf("回复异常: %q", text)
	}
	msgs := h.messages()
	if len(msgs) != 1 {
		t.Fatalf("入站消息数 %d，期望 1", len(msgs))
	}
	got := msgs[0]
	if got.Channel != Name || got.ChatID == "" || got.MessageID != got.ChatID {
		t.Fatalf("入站消息字段异常: %+v", got)
	}
	if got.SessionKey != DefaultSession {
		t.Fatalf("默认会话键异常: %q", got.SessionKey)
	}
	if got.Content != "你好" {
		t.Fatalf("内容异常: %q", got.Content)
	}
}

func TestSessionOverride(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := Send(h.path, "hi", "qq:1", 3*time.Second); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := Send(h.path, "hi", "work", 3*time.Second); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msgs := h.messages()
	if msgs[0].SessionKey != "qq:1" {
		t.Fatalf("显式键未保留: %q", msgs[0].SessionKey)
	}
	if msgs[1].SessionKey != "cli:work" {
		t.Fatalf("短名未加前缀: %q", msgs[1].SessionKey)
	}
}

func TestNoWaitReturnsImmediately(t *testing.T) {
	h := newHarness(t, nil)
	start := time.Now()
	if _, err := Send(h.path, "不等回复", "", 0); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("不等待模式耗时过长: %v", elapsed)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(h.messages()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(h.messages()) != 1 {
		t.Fatal("消息未入总线")
	}
}

func TestStatusAndPing(t *testing.T) {
	h := newHarness(t, func(op, arg string) (any, error) {
		if op == OpJobsRemove {
			return map[string]any{"removed": arg}, nil
		}
		if op != OpStatus {
			return nil, errors.New("不支持: " + op)
		}
		return map[string]any{"pid": 42, "name": "ivan"}, nil
	})
	raw, err := Status(h.path, 3*time.Second)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("解析 status: %v", err)
	}
	if payload["name"] != "ivan" || payload["pid"].(float64) != 42 {
		t.Fatalf("status 内容异常: %v", payload)
	}
	if err := Ping(h.path, 3*time.Second); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	raw, err = Call(h.path, OpJobsRemove, "job_1", 3*time.Second)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	var removed map[string]any
	if err := json.Unmarshal(raw, &removed); err != nil {
		t.Fatalf("解析 jobs/remove: %v", err)
	}
	if removed["removed"] != "job_1" {
		t.Fatalf("控制指令参数未透传: %v", removed)
	}
}

func TestBadFrames(t *testing.T) {
	h := newHarness(t, nil)
	cl, err := Dial(h.path, time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cl.Close()
	if err := cl.Do(Frame{ID: "1", Op: OpSend, Text: "   "}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	f, err := cl.Read(time.Second)
	if err != nil || f.Op != OpError || !strings.Contains(f.Error, "text 为空") {
		t.Fatalf("空文本应报错: %+v %v", f, err)
	}
	if err := cl.Do(Frame{ID: "2", Op: "nope"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	f, err = cl.Read(time.Second)
	if err != nil || f.Op != OpError || !strings.Contains(f.Error, "不支持的控制指令") {
		t.Fatalf("未知 op 应报错: %+v %v", f, err)
	}
	if _, err := cl.c.Write([]byte("{坏帧}\n")); err != nil {
		t.Fatalf("写坏帧: %v", err)
	}
	f, err = cl.Read(time.Second)
	if err != nil || f.Op != OpError || !strings.Contains(f.Error, "帧解析失败") {
		t.Fatalf("坏帧应报错: %+v %v", f, err)
	}
}

func TestSendDroppedWhenBusClosed(t *testing.T) {
	h := newHarness(t, nil)
	h.bus.CloseInbound()
	cl, err := Dial(h.path, time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cl.Close()
	if err := cl.Do(Frame{ID: "1", Op: OpSend, Text: "hi"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	f, err := cl.Read(time.Second)
	if err != nil || f.Op != OpError || !strings.Contains(f.Error, "丢弃") {
		t.Fatalf("停机时应回错误帧: %+v %v", f, err)
	}
}

func TestSendUnroutableAfterReply(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := Send(h.path, "hi", "", 3*time.Second); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := h.sock.Send(context.Background(), bus.OutboundMessage{Channel: Name, ChatID: "不存在", Content: "x"}); !errors.Is(err, channel.ErrUnroutable) {
		t.Fatalf("无目标应报 ErrUnroutable: %v", err)
	}
}

func TestSecondListenerRejectedAndStopReleases(t *testing.T) {
	h := newHarness(t, nil)
	other := NewSocket(h.path, nil)
	if err := other.Start(context.Background(), bus.New(1, 1)); err == nil {
		other.Stop(context.Background())
		t.Fatal("同路径二次监听应失败")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.sock.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := Dial(h.path, 200*time.Millisecond); err == nil {
		t.Fatal("停止后不应还能连接")
	}
	if _, err := Send(h.path, "hi", "", time.Second); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("期望 ErrNotRunning: %v", err)
	}
}

func TestMaxLenIsUnlimited(t *testing.T) {
	if got := NewSocket("/tmp/x.sock", nil).MaxLen(); got != 0 {
		t.Fatalf("cli 不应切分: %d", got)
	}
}

func TestNormalizeSession(t *testing.T) {
	cases := map[string]string{"": DefaultSession, "  ": DefaultSession, "work": "cli:work", "qq:1": "qq:1"}
	for in, want := range cases {
		if got := NormalizeSession(in); got != want {
			t.Errorf("NormalizeSession(%q) = %q want %q", in, got, want)
		}
	}
}
