package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
	"github.com/LaoQi/ivan/internal/fakellm"
)

type fakeFileChan struct {
	name    string
	mu      sync.Mutex
	texts   []string
	files   [][]bus.MediaRef
	results []channel.NoticeResult
}

func (f *fakeFileChan) Name() string                          { return f.name }
func (f *fakeFileChan) MaxLen() int                           { return 0 }
func (f *fakeFileChan) Start(context.Context, *bus.Bus) error { return nil }
func (f *fakeFileChan) Stop(context.Context) error            { return nil }
func (f *fakeFileChan) Send(context.Context, bus.OutboundMessage) error {
	return fmt.Errorf("%w: 测试通道不做定向投递", channel.ErrUnroutable)
}
func (f *fakeFileChan) NoticeFiles(ctx context.Context, text string, files []bus.MediaRef) []channel.NoticeResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.texts = append(f.texts, text)
	f.files = append(f.files, files)
	return f.results
}

func TestFileBroadcastReportsPerTarget(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "unused"})
	defer srv.Close()
	cfg := httpConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	fc := &fakeFileChan{name: "filechan", results: []channel.NoticeResult{
		{Target: "u1"},
		{Target: "u2", Err: errors.New("附件超过上限")},
	}}
	if err := run.d.mgr.Register(fc); err != nil {
		t.Fatalf("register: %v", err)
	}

	files := []bus.MediaRef{{Path: "/tmp/a.txt", MIME: "text/plain", Name: "a.txt"}}
	results := run.d.broadcastFiles(context.Background(), "报告", files)
	var got *string
	for _, r := range results {
		if r.Name == "filechan:u1" {
			if r.Result != "sent" {
				t.Fatalf("u1 应为 sent: %+v", r)
			}
			out := r.Result
			got = &out
		}
		if r.Name == "filechan:u2" {
			if r.Result != "failed" || r.Error == "" {
				t.Fatalf("u2 应如实回报失败: %+v", r)
			}
		}
	}
	if got == nil {
		t.Fatalf("缺少 u1 回执: %+v", results)
	}
	if len(fc.texts) != 1 || fc.texts[0] != "报告" {
		t.Fatalf("说明文字未透传: %v", fc.texts)
	}
	if len(fc.files) != 1 || len(fc.files[0]) != 1 || fc.files[0][0].Path != "/tmp/a.txt" {
		t.Fatalf("附件未透传: %+v", fc.files)
	}
}

func TestFileBroadcastNoSupportingChannel(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "unused"})
	defer srv.Close()
	cfg := httpConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	results := run.d.broadcastFiles(context.Background(), "x", nil)
	if len(results) == 0 {
		t.Fatal("应至少返回一条回执")
	}
	for _, r := range results {
		if r.Result != "no_targets" || r.Error == "" {
			t.Fatalf("无附件通道时应如实回报 no_targets: %+v", results)
		}
	}
}

func TestFilesSummaryCounts(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "unused"})
	defer srv.Close()
	cfg := httpConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	fc := &fakeFileChan{name: "filechan", results: []channel.NoticeResult{{Target: "u1"}}}
	if err := run.d.mgr.Register(fc); err != nil {
		t.Fatalf("register: %v", err)
	}
	sent, total, names := run.d.filesSummary(context.Background(), "报告", []bus.MediaRef{{Path: "/tmp/a.txt"}})
	if sent == 0 {
		t.Fatalf("应至少成功 1 个目标，得到 sent=%d total=%d", sent, total)
	}
	if total < sent {
		t.Fatalf("total 应不小于 sent: sent=%d total=%d", sent, total)
	}
	if names == "" {
		t.Fatal("成功目标名不能为空")
	}
}

type recFileChan struct {
	name string
	mu   sync.Mutex
	got  [][]bus.MediaRef
	text []string
}

func (r *recFileChan) Name() string                                    { return r.name }
func (r *recFileChan) MaxLen() int                                     { return 0 }
func (r *recFileChan) Start(context.Context, *bus.Bus) error           { return nil }
func (r *recFileChan) Stop(context.Context) error                      { return nil }
func (r *recFileChan) Send(context.Context, bus.OutboundMessage) error { return nil }
func (r *recFileChan) NoticeFiles(ctx context.Context, text string, files []bus.MediaRef) []channel.NoticeResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, files)
	r.text = append(r.text, text)
	return []channel.NoticeResult{{Target: "u1"}}
}
func (r *recFileChan) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func TestSendFileToolEndToEnd(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{ID: "c1", Name: "send_file", Args: `{"paths":["report.txt"],"text":"报告给你"}`}}},
		fakellm.Step{Content: "文件已发送"},
	)
	defer srv.Close()
	cfg := testConfig(t, srv)
	cfg.SessionMode = "global"
	run := start(t, cfg)
	defer run.stop(t)

	ws := cfg.AgentDir("cli-default")
	if err := os.MkdirAll(ws, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "report.txt"), []byte("hello file"), 0o600); err != nil {
		t.Fatal(err)
	}

	rc := &recFileChan{name: "recfile"}
	if err := run.d.mgr.Register(rc); err != nil {
		t.Fatalf("register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := run.d.runner.DispatchTurn(ctx, bus.InboundMessage{
		Channel: "cli", ChatID: "default", Content: "把报告发我", SessionKey: "cli:default",
	}); err != nil {
		t.Fatalf("回合失败: %v", err)
	}
	if rc.count() == 0 {
		t.Fatal("send_file 未走到通道（工具未注册或回执未接线）")
	}
	got := rc.got[0]
	if len(got) != 1 || !strings.HasSuffix(got[0].Path, "report.txt") {
		t.Fatalf("附件路径错误: %+v", got)
	}
}
