package bus

import (
	"testing"
	"time"
)

func TestPublishNonBlockingDrop(t *testing.T) {
	b := New(1, 1)
	if !b.Publish(InboundMessage{Channel: "cli"}) {
		t.Fatal("首条应投递成功")
	}
	if b.Publish(InboundMessage{Channel: "cli"}) {
		t.Fatal("缓冲满时应丢弃")
	}
	st := b.Stats()
	if st.Inbound != 1 || st.InboundDropped != 1 {
		t.Fatalf("计数异常: %+v", st)
	}
	if st.LastDrop.IsZero() {
		t.Fatal("未记录丢弃时间")
	}
	if st.InboundQueued != 1 {
		t.Fatalf("队列深度 %d，期望 1", st.InboundQueued)
	}
}

func TestPostAndConsume(t *testing.T) {
	b := New(2, 2)
	if !b.Post(OutboundMessage{Channel: "cli", ChatID: "r1", Content: "hi"}) {
		t.Fatal("出站投递失败")
	}
	select {
	case m := <-b.Outbound():
		if m.ChatID != "r1" || m.Content != "hi" {
			t.Fatalf("出站消息异常: %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("未读到出站消息")
	}
	if b.Stats().Outbound != 1 {
		t.Fatalf("出站计数异常: %+v", b.Stats())
	}
}

func TestClosedBusRejectsAndClosesStreams(t *testing.T) {
	b := New(2, 2)
	b.CloseInbound()
	if b.Publish(InboundMessage{Channel: "cli"}) {
		t.Fatal("关闭后不应接受入站")
	}
	if _, ok := <-b.Inbound(); ok {
		t.Fatal("入站流应已关闭")
	}
	if b.Stats().InboundDropped != 1 {
		t.Fatalf("关闭后的投递应计入丢弃: %+v", b.Stats())
	}
	b.CloseInbound()
	b.CloseOutbound()
	if b.Post(OutboundMessage{Channel: "cli"}) {
		t.Fatal("关闭后不应接受出站")
	}
	if _, ok := <-b.Outbound(); ok {
		t.Fatal("出站流应已关闭")
	}
	b.Close()
	if !b.InboundClosed() || !b.OutboundClosed() {
		t.Fatal("关闭状态未置位")
	}
}

func TestSessionKeyRules(t *testing.T) {
	cases := []struct {
		name string
		msg  InboundMessage
		want string
	}{
		{"显式键优先", InboundMessage{Channel: "qq", SessionKey: "custom:1"}, "custom:1"},
		{"qq 单聊", InboundMessage{Channel: "qq", ChatType: ChatDirect, ChatID: "u1"}, "qq:c2c:u1"},
		{"qq 群聊", InboundMessage{Channel: "qq", ChatType: ChatGroup, ChatID: "g1"}, "qq:group:g1"},
		{"cli 默认", InboundMessage{Channel: "cli", ChatID: "req-9"}, "cli:default"},
		{"未知通道", InboundMessage{Channel: "webhook", ChatID: "n1"}, "webhook:n1"},
		{"空通道", InboundMessage{}, "unknown:default"},
	}
	for _, c := range cases {
		if got := SessionKey(c.msg); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestKeySlug(t *testing.T) {
	cases := map[string]string{
		"cli:default":      "cli-default",
		"qq:c2c:OPEN/ID==": "qq-c2c-OPEN-ID",
		"":                 "session",
		":::":              "session",
	}
	for in, want := range cases {
		if got := KeySlug(in); got != want {
			t.Errorf("KeySlug(%q) = %q want %q", in, got, want)
		}
	}
	long := "qq:group:" + string(make([]byte, 0)) + "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdef"
	if got := KeySlug(long); len(got) > slugLimit+16 {
		t.Errorf("超长键未截断: %d", len(got))
	}
	if KeySlug(long) == KeySlug(long+"x") {
		t.Error("超长键截断后应带摘要避免碰撞")
	}
}
