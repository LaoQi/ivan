package runner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/LaoQi/ivan/internal/fakellm"
)

// TestQueuedMessageDoesNotHoldSem 固化并发槽语义：同一会话排队的消息不占跨会话并发额度。
//
// 场景：max_concurrent=2，A 会话第一个回合卡在上游。
// 新行为：A 的后续消息只在该会话队列里待着，B 会话立刻拿到第二个槽 → 假服务端收到 2 个请求。
// 旧行为：A 的第二条消息先抢到槽 2 再等会话锁，B 被堵死 → 假服务端只收到 1 个请求。
func TestQueuedMessageDoesNotHoldSem(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	srv.Hold()
	t.Cleanup(srv.Release)
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Daemon.MaxConcurrent = 2
	r := newP4Rig(t, cfg, nil)

	done := make(chan error, 3)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:a", "a1", "A 第一问")) }()
	waitCalls(t, srv, 1)

	go func() { done <- r.Dispatch(context.Background(), inbound("cli:a", "a2", "A 第二问")) }()
	waitInFlight(t, r, 1)
	time.Sleep(150 * time.Millisecond) // 给排队消息足够时间（错误实现下它会在这期间抢槽并堵住 B）
	if n := srv.Calls(); n != 1 {
		t.Fatalf("同会话排队的第二条消息不应立刻执行，假服务端却收到 %d 个请求", n)
	}

	go func() { done <- r.Dispatch(context.Background(), inbound("cli:b", "b1", "B 的请求")) }()
	// 关键断言：B 会话没被 A 的排队消息堵住（给足时间，卡住即说明并发槽被白占）
	deadline := time.Now().Add(3 * time.Second)
	for srv.Calls() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("B 会话被堵住：同会话排队的消息占住了并发槽（假服务端仅收到 %d 个请求）", srv.Calls())
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv.Release()
	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, ErrQueueFull) {
				t.Fatalf("回合失败: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("回合未在超时内完成")
		}
	}
}

// TestSameSessionFIFOOrder 固化同会话顺序：后到的消息一定在后执行。
//
// 三条：第一条在跑时后两条同时排队，此时队列长度 > 1，LIFO 与 FIFO 才会产生不同结果。
func TestSameSessionFIFOOrder(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"}, fakellm.Step{Content: "答"}, fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	srv.Hold()
	t.Cleanup(srv.Release)
	cfg := bareConfig(t, srv.BaseURL())
	r := newP4Rig(t, cfg, nil)

	done := make(chan error, 3)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:default", "a", "第一问")) }()
	waitCalls(t, srv, 1) // 第一条已进入执行，后两条的到达顺序因而是确定的
	// 顺序投递：必须等前一条确认入队后再发下一条，否则“到达顺序”取决于 goroutine 调度
	for _, q := range []struct{ id, text string }{{"b", "第二问"}, {"c", "第三问"}} {
		want := 2
		if q.id == "c" {
			want = 3
		}
		go func(id, text string) {
			done <- r.Dispatch(context.Background(), inbound("cli:default", id, text))
		}(q.id, q.text)
		waitQueuedTotal(t, r, "cli:default", want)
	}
	srv.Release()
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("回合未完成")
		}
	}

	reqs := srv.Requests()
	if len(reqs) != 3 {
		t.Fatalf("请求数 %d，期望 3", len(reqs))
	}
	want := []string{"第一问", "第二问", "第三问"}
	for i, w := range want {
		if got := lastUserText(t, reqs[i].Messages); got != w {
			t.Fatalf("同会话顺序错乱：第 %d 个请求是 %q，期望 %q", i+1, got, w)
		}
	}
}

// TestQueueFullDropsAndCounts 固化队列上限：超限丢弃、报错并计数，不静默吞掉。
func TestQueueFullDropsAndCounts(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	srv.Hold()
	t.Cleanup(srv.Release)
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.SessionQueue = 1
	r := newP4Rig(t, cfg, nil)

	done := make(chan error, 2)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:default", "a", "第一问")) }()
	waitCalls(t, srv, 1)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:default", "b", "第二问")) }()
	waitQueuedTotal(t, r, "cli:default", 2) // 第二条已占住唯一队列位

	// 第三个必须立刻被拒；没有上限时会一直挂着，所以自己判超时（t.Fatal 而非等测试超时）
	rejected := make(chan error, 1)
	go func() { rejected <- r.Dispatch(context.Background(), inbound("cli:default", "c", "第三问")) }()
	var err error
	select {
	case err = <-rejected:
	case <-time.After(3 * time.Second):
		t.Fatal("超出队列上限的消息没有被拒绝（一直挂起）")
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("队列已满应返回 ErrQueueFull，实际 %v", err)
	}
	if st := r.Status(); st.QueueDropped != 1 {
		t.Fatalf("queue_dropped=%d，期望 1", st.QueueDropped)
	}
	srv.Release()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("回合未完成")
		}
	}
}

func waitCalls(t *testing.T, srv *fakellm.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Calls() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待假服务端收到 %d 个请求超时（实际 %d）", want, srv.Calls())
}

// waitQueuedTotal 等待该会话「在跑 + 排队」的总数达到 want。
func waitQueuedTotal(t *testing.T, r *Runner, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.poolMu.Lock()
		inst := r.pool[key]
		r.poolMu.Unlock()
		if inst != nil {
			total := inst.pending()
			if inst.mu.TryLock() {
				inst.mu.Unlock()
			} else {
				total++
			}
			if total >= want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待会话 %s 在跑+排队达到 %d 超时", key, want)
}

func waitInFlight(t *testing.T, r *Runner, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.Status().InFlight >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待在跑回合超时（want=%d）", want)
}

func lastUserText(t *testing.T, msgs []json.RawMessage) string {
	t.Helper()
	for i := len(msgs) - 1; i >= 0; i-- {
		var m struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(msgs[i], &m); err != nil {
			continue
		}
		if m.Role == "user" {
			return m.Content
		}
	}
	return ""
}
