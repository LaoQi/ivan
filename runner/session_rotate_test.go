package runner

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func TestSessionsListsStateAndPool(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	r := newP4Rig(t, bareConfig(t, srv.BaseURL()), nil)

	for _, key := range []string{"cli:a", "cli:b"} {
		if err := r.Dispatch(context.Background(), inbound(key, key, "你好")); err != nil {
			t.Fatalf("Dispatch %s: %v", key, err)
		}
	}
	list := r.Sessions()
	if len(list) != 2 {
		t.Fatalf("期望 2 个会话键，得到 %d：%+v", len(list), list)
	}
	if list[0].Key != "cli:a" || list[1].Key != "cli:b" {
		t.Fatalf("会话键应升序：%+v", list)
	}
	for _, s := range list {
		if !s.Pooled || s.Running || s.Queued != 0 {
			t.Fatalf("会话 %s 应为池内空闲：%+v", s.Key, s)
		}
		if s.Session == "" || s.Workspace == "" {
			t.Fatalf("会话 %s 缺映射或工作区：%+v", s.Key, s)
		}
	}
}

func TestRotateSessionNoInstanceIsNoop(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	r := newP4Rig(t, bareConfig(t, srv.BaseURL()), nil)
	const key = "cli:ghost"
	want := "20260101-000000"
	if err := r.opts.State.Set(key, data.SessionState{SessionID: want, Workspace: "/tmp/ghost"}); err != nil {
		t.Fatalf("预置映射: %v", err)
	}

	rot, err := r.RotateSession(key)
	if err != nil {
		t.Fatalf("RotateSession: %v", err)
	}
	if rot.Rotated || rot.Pooled {
		t.Fatalf("无实例时不该轮换：%+v", rot)
	}
	if strings.TrimSpace(rot.Reason) == "" {
		t.Fatal("应给出原因")
	}
	st, ok := r.opts.State.Get(key)
	if !ok || st.SessionID != want {
		t.Fatalf("无实例时映射应保持不变：%+v ok=%v", st, ok)
	}
}

func TestRotateSessionSwitchesToFreshSession(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"}, fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	r := newP4Rig(t, bareConfig(t, srv.BaseURL()), nil)

	if err := r.Dispatch(context.Background(), inbound("cli:a", "a", "你好")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	ag, ok := r.Agent("cli:a")
	if !ok {
		t.Fatal("会话 cli:a 应在池中")
	}
	oldID := ag.SessionID()
	oldFile := ag.SessionFile()
	if oldID == "" || oldFile == "" {
		t.Fatalf("应有旧会话：id=%q file=%q", oldID, oldFile)
	}
	before, err := os.ReadFile(oldFile)
	if err != nil {
		t.Fatalf("读旧会话文件: %v", err)
	}

	rot, err := r.RotateSession("cli:a")
	if err != nil {
		t.Fatalf("RotateSession: %v", err)
	}
	if !rot.Rotated || !rot.Pooled {
		t.Fatalf("应轮换：%+v", rot)
	}
	if rot.OldID != oldID {
		t.Fatalf("旧 id 期望 %s，得到 %s", oldID, rot.OldID)
	}
	if rot.NewID == "" || rot.NewID == oldID {
		t.Fatalf("新 id 应不同于旧 id：%+v", rot)
	}
	if got := r.mustSessionID(t, "cli:a"); got != rot.NewID {
		t.Fatalf("实例应切到新会话 %s，得到 %s", rot.NewID, got)
	}
	st, ok := r.opts.State.Get("cli:a")
	if !ok || st.SessionID != rot.NewID {
		t.Fatalf("映射应写回新 id %s，得到 %+v", rot.NewID, st)
	}

	after, err := os.ReadFile(oldFile)
	if err != nil {
		t.Fatalf("旧会话文件应保留: %v", err)
	}
	if string(after) != string(before) {
		t.Fatalf("旧会话文件不应被改动（旧 %d 字节，现 %d 字节）", len(before), len(after))
	}
}

func TestRotateSessionFreshContext(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Content: "记住了"},
		fakellm.Step{Content: "不知道"},
	)
	t.Cleanup(srv.Close)
	r := newP4Rig(t, bareConfig(t, srv.BaseURL()), nil)

	if err := r.Dispatch(context.Background(), inbound("cli:a", "a", "暗号是 ZEBRA-42")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if _, err := r.RotateSession("cli:a"); err != nil {
		t.Fatalf("RotateSession: %v", err)
	}
	if err := r.Dispatch(context.Background(), inbound("cli:a", "a", "暗号是多少")); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	reqs := srv.Requests()
	if len(reqs) < 2 {
		t.Fatalf("期望至少 2 次请求，得到 %d", len(reqs))
	}
	for _, raw := range reqs[len(reqs)-1].Messages {
		var m struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		if strings.Contains(m.Content, "ZEBRA-42") {
			t.Fatal("轮换后新会话不应带上旧回合内容")
		}
	}
}

func TestRotateSessionRejectsBusy(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	srv.Hold()
	t.Cleanup(srv.Release)
	r := newP4Rig(t, bareConfig(t, srv.BaseURL()), nil)

	done := make(chan error, 1)
	go func() { done <- r.Dispatch(context.Background(), inbound("cli:a", "a", "长回合")) }()
	waitCalls(t, srv, 1)

	if _, err := r.RotateSession("cli:a"); err == nil {
		t.Fatal("回合在跑时应拒绝轮换")
	}
	srv.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("回合失败: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("回合未结束")
	}
	if _, err := r.RotateSession("cli:a"); err != nil {
		t.Fatalf("回合结束后应可轮换: %v", err)
	}
}

func TestRotateSessionMissingKeyRejected(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	t.Cleanup(srv.Close)
	r := newP4Rig(t, bareConfig(t, srv.BaseURL()), nil)
	if _, err := r.RotateSession("   "); err == nil {
		t.Fatal("空会话键应报错")
	}
}

func (r *Runner) mustSessionID(t *testing.T, key string) string {
	t.Helper()
	ag, ok := r.Agent(key)
	if !ok {
		t.Fatalf("会话 %s 不在池中", key)
	}
	return ag.SessionID()
}
