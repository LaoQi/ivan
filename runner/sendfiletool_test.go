package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LaoQi/ivan/bus"
)

func fileToolRunner(t *testing.T, ws string, send func(context.Context, string, []bus.MediaRef) (int, int, string)) *Runner {
	t.Helper()
	return &Runner{opts: Options{SendFiles: send}}
}

func callFileTool(t *testing.T, r *Runner, args map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	inst := &instance{ws: t.TempDir()}
	if ws, ok := args["__ws"].(string); ok {
		inst.ws = ws
		delete(args, "__ws")
	}
	tool := r.sendFileTool(inst)
	return tool.Invoke(context.Background(), string(raw)).Text
}

func TestSendFileToolPassesRefs(t *testing.T) {
	var got []bus.MediaRef
	var text string
	r := fileToolRunner(t, "", func(ctx context.Context, txt string, files []bus.MediaRef) (int, int, string) {
		text = txt
		got = files
		return 1, 1, "qq:u-1"
	})
	path := filepath.Join(t.TempDir(), "report.csv")
	if err := os.WriteFile(path, []byte("a,b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := callFileTool(t, r, map[string]any{"paths": []string{path}, "text": "报告"})
	if !strings.HasPrefix(out, "ok:") {
		t.Fatalf("期望 ok，得到 %s", out)
	}
	if text != "报告" {
		t.Fatalf("说明文字未透传: %q", text)
	}
	if len(got) != 1 || got[0].Path != path {
		t.Fatalf("文件引用错误: %+v", got)
	}
	if got[0].Name != "report.csv" || got[0].Bytes != 4 {
		t.Fatalf("附件元数据错误: %+v", got[0])
	}
	if !strings.HasPrefix(got[0].MIME, "text/csv") {
		t.Fatalf("MIME 应按扩展名补齐: %q", got[0].MIME)
	}
}

func TestSendFileToolRelativePathUsesWorkspace(t *testing.T) {
	ws := t.TempDir()
	if err := os.WriteFile(filepath.Join(ws, "out.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	var got []bus.MediaRef
	r := fileToolRunner(t, "", func(ctx context.Context, txt string, files []bus.MediaRef) (int, int, string) {
		got = files
		return 1, 1, "qq"
	})
	if out := callFileTool(t, r, map[string]any{"paths": []string{"out.txt"}, "__ws": ws}); !strings.HasPrefix(out, "ok:") {
		t.Fatalf("期望 ok，得到 %s", out)
	}
	if len(got) != 1 || got[0].Path != filepath.Join(ws, "out.txt") {
		t.Fatalf("相对路径应解析到工作区: %+v", got)
	}
}

func TestSendFileToolRejectsBadPaths(t *testing.T) {
	r := fileToolRunner(t, "", func(ctx context.Context, txt string, files []bus.MediaRef) (int, int, string) {
		return 1, 1, "qq"
	})
	dir := t.TempDir()
	cases := []struct {
		name string
		arg  []string
		want string
	}{
		{"不存在", []string{filepath.Join(dir, "nope.txt")}, "文件不可用"},
		{"目录", []string{dir}, "是目录不是文件"},
	}
	for _, tc := range cases {
		out := callFileTool(t, r, map[string]any{"paths": tc.arg})
		if !strings.HasPrefix(out, "error:") || !strings.Contains(out, tc.want) {
			t.Fatalf("%s: 期望错误含 %q，得到 %s", tc.name, tc.want, out)
		}
	}
}

func TestSendFileToolLimits(t *testing.T) {
	r := fileToolRunner(t, "", func(ctx context.Context, txt string, files []bus.MediaRef) (int, int, string) {
		return 1, 1, "qq"
	})
	path := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := callFileTool(t, r, map[string]any{"paths": []string{path, path, path, path}})
	if !strings.Contains(out, "一次最多发送") {
		t.Fatalf("超量应拒绝: %s", out)
	}
	if out := callFileTool(t, r, map[string]any{"paths": []string{}}); !strings.Contains(out, "paths 不能为空") {
		t.Fatalf("空列表应拒绝: %s", out)
	}
}

func TestSendFileToolDedupesAndReports(t *testing.T) {
	var got []bus.MediaRef
	r := fileToolRunner(t, "", func(ctx context.Context, txt string, files []bus.MediaRef) (int, int, string) {
		got = files
		return 0, 2, ""
	})
	path := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := callFileTool(t, r, map[string]any{"paths": []string{path, path}})
	if len(got) != 1 {
		t.Fatalf("重复路径应去重，得到 %d", len(got))
	}
	if !strings.HasPrefix(out, "error:") {
		t.Fatalf("全部失败应如实回报，得到 %s", out)
	}
}

func TestSendFileToolWithoutOutlet(t *testing.T) {
	r := &Runner{}
	path := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := callFileTool(t, r, map[string]any{"paths": []string{path}})
	if !strings.Contains(out, "没有附件投递出口") {
		t.Fatalf("无出口应如实报错，得到 %s", out)
	}
}
