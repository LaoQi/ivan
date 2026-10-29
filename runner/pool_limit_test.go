package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LaoQi/ivan/internal/fakellm"
)

// TestPoolFullRejectsNewSession 固化池上限：满池且无可淘汰实例时明确拒绝，不再静默超限。
func TestPoolFullRejectsNewSession(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	srv.Hold()
	t.Cleanup(srv.Release)
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.MaxAgents = 1
	r := newP4Rig(t, cfg, nil)

	// 第一个会话占住唯一实例并卡在回合里
	busy := make(chan error, 1)
	go func() { busy <- r.Dispatch(context.Background(), inbound("cli:a", "a", "长任务")) }()
	waitCalls(t, srv, 1)

	_, err := r.acquire("cli:b")
	if !errors.Is(err, ErrPoolFull) {
		t.Fatalf("满池应返回 ErrPoolFull，实际 %v", err)
	}
	if n := len(r.pool); n != 1 {
		t.Fatalf("池大小 %d，期望仍为 1（不得静默超限）", n)
	}
	if st := r.Status(); st.PoolFull != 1 {
		t.Fatalf("pool_full=%d，期望 1", st.PoolFull)
	}
	srv.Release()
	select {
	case <-busy:
	case <-time.After(5 * time.Second):
		t.Fatal("回合未完成")
	}
}

// TestPoolDoesNotEvictInstanceWithQueuedWork 固化淘汰条件：带排队消息的实例不能被淘汰。
//
// 否则会话键会再建一个同工作区的新实例，两个实例并发写同一批会话文件（探针 R11）。
func TestPoolDoesNotEvictInstanceWithQueuedWork(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	srv.Hold()
	t.Cleanup(srv.Release)
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.MaxAgents = 2
	r := newP4Rig(t, cfg, nil)

	// A 会话：首回合卡住，第二条入队（锁被占 + 队列非空，无论如何都不可淘汰）
	done := make(chan error, 3)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:a", "a1", "A 第一问")) }()
	waitCalls(t, srv, 1)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:a", "a2", "A 第二问")) }()
	waitQueuedTotal(t, r, "cli:a", 2)

	// B 会话填满池（此时池 = A + B，A 不可淘汰）
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:b", "b1", "B 问")) }()
	waitCalls(t, srv, 2)

	// 第三个会话进不来：A 有排队工作、B 在跑，都不可淘汰
	if _, err := r.acquire("cli:c"); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("无可淘汰实例时应返回 ErrPoolFull，实际 %v", err)
	}
	if _, ok := r.pool["cli:a"]; !ok {
		t.Fatal("带排队消息的实例被淘汰了")
	}

	srv.Release()
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("回合未完成")
		}
	}
}

// TestPoolIdleCheckConsidersQueue 单独固化「锁空闲但队列还有活」这一分支。
//
// 直接构造该状态：worker 标志在、队列非空，但实例锁可以被抢到。
// 只判 TryLock 的实现会把它当成空闲实例淘汰掉。
func TestPoolIdleCheckConsidersQueue(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	cfg := bareConfig(t, srv.BaseURL())
	r := newP4Rig(t, cfg, nil)

	inst := mustAcquire(t, r, "cli:a")
	inst.queue = []queuedTurn{{ctx: context.Background(), msg: inbound("cli:a", "a1", "排队中")}}
	inst.worker = true

	if inst.idleLocked() {
		t.Fatal("带排队消息（worker 在跑）的实例不应被判为空闲")
	}

	// 队列清空但 worker 标志仍在（worker 正在取任务）→ 同样不可淘汰
	inst.queue = nil
	if inst.idleLocked() {
		t.Fatal("worker 尚未退出时不应判为空闲")
	}

	inst.worker = false
	if !inst.idleLocked() {
		t.Fatal("既无回合在跑也无排队消息时应判为空闲")
	}
}

// TestReapSkipsInstanceWithQueuedWork 固化 reap（闲置回收）与 acquire 共用同一空闲判定。
//
// 实例锁空闲但队列有活时，定时回收也不能动它。
func TestReapSkipsInstanceWithQueuedWork(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	cfg := bareConfig(t, srv.BaseURL())
	r := newP4Rig(t, cfg, nil)

	inst := mustAcquire(t, r, "cli:a")
	inst.lastUsed = time.Now().Add(-2 * time.Hour)
	inst.queue = []queuedTurn{{ctx: context.Background(), msg: inbound("cli:a", "a1", "排队中")}}
	inst.worker = true

	if n := r.reap(time.Minute); n != 0 {
		t.Fatalf("带排队消息的实例不应被闲置回收，实际回收 %d 个", n)
	}
	if _, ok := r.pool["cli:a"]; !ok {
		t.Fatal("带排队消息的实例被回收了")
	}

	inst.queue = nil
	inst.worker = false
	if n := r.reap(time.Minute); n != 1 {
		t.Fatalf("真正空闲的实例应被回收，实际回收 %d 个", n)
	}
}

// TestPoolEvictsIdleWithWorkerFlagCleared 固化反向条件：worker 已退出、队列为空的实例可淘汰。
func TestPoolEvictsIdleWithWorkerFlagCleared(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	cfg := bareConfig(t, srv.BaseURL())
	cfg.Runner.MaxAgents = 1
	r := newP4Rig(t, cfg, nil)

	if err := r.Dispatch(context.Background(), inbound("cli:a", "a", "跑完")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	inst := mustAcquire(t, r, "cli:a")
	if inst.working() {
		t.Fatal("回合结束后 worker 应已退出")
	}
	if inst.pending() != 0 {
		t.Fatalf("回合结束后队列应为空，实际 %d", inst.pending())
	}
	// 池满但实例空闲 → 可淘汰，新会话能进来
	if _, err := r.acquire("cli:b"); err != nil {
		t.Fatalf("空闲实例应被淘汰以腾出位置: %v", err)
	}
}
