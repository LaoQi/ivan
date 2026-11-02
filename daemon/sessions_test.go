package daemon

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LaoQi/ivan/channel/cli"
	"github.com/LaoQi/ivan/internal/fakellm"
	"github.com/LaoQi/ivan/runner"
)

func TestSessionsControlListAndRotate(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"}, fakellm.Step{Content: "答"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	if _, err := cli.Send(cfg.SocketPath(), "你好", "cli:a", 10*time.Second); err != nil {
		t.Fatalf("send: %v", err)
	}

	raw, err := cli.Call(cfg.SocketPath(), cli.OpSessionsList, "", 5*time.Second)
	if err != nil {
		t.Fatalf("sessions/list: %v", err)
	}
	var list []runner.SessionStatus
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("解析会话列表: %v", err)
	}
	if len(list) != 1 || list[0].Key != "cli:a" || !list[0].Pooled {
		t.Fatalf("会话列表不符合预期：%+v", list)
	}
	oldID := list[0].Session

	raw, err = cli.Call(cfg.SocketPath(), cli.OpSessionsNew, "cli:a", 30*time.Second)
	if err != nil {
		t.Fatalf("sessions/new: %v", err)
	}
	var rot runner.SessionRotation
	if err := json.Unmarshal(raw, &rot); err != nil {
		t.Fatalf("解析轮换结果: %v", err)
	}
	if !rot.Rotated || rot.OldID != oldID || rot.NewID == "" || rot.NewID == oldID {
		t.Fatalf("轮换结果不符合预期：%+v", rot)
	}

	raw, err = cli.Call(cfg.SocketPath(), cli.OpSessionsList, "", 5*time.Second)
	if err != nil {
		t.Fatalf("sessions/list 二次: %v", err)
	}
	list = nil
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("解析会话列表: %v", err)
	}
	if len(list) != 1 || list[0].Session != rot.NewID {
		t.Fatalf("列表应显示新会话 %s：%+v", rot.NewID, list)
	}
}

func TestSessionsRotateUnknownKeyNoop(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	raw, err := cli.Call(cfg.SocketPath(), cli.OpSessionsNew, "cli:ghost", 10*time.Second)
	if err != nil {
		t.Fatalf("sessions/new: %v", err)
	}
	var rot runner.SessionRotation
	if err := json.Unmarshal(raw, &rot); err != nil {
		t.Fatalf("解析轮换结果: %v", err)
	}
	if rot.Rotated || rot.Pooled || rot.Reason == "" {
		t.Fatalf("无实例时应如实说明：%+v", rot)
	}
}

func TestSessionsRotateMissingKeyRejected(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "答"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	if _, err := cli.Call(cfg.SocketPath(), cli.OpSessionsNew, "  ", 5*time.Second); err == nil {
		t.Fatal("空会话键应报错")
	}
}
