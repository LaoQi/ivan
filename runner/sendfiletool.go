package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/LaoQi/battlelab/agent"

	"github.com/LaoQi/ivan/bus"
)

const maxSendFiles = 3

const sendFileDesc = `把本地文件发给用户（附件投递，尽力而为广播）。
- 用在需要把生成的文件、图片、报告交给用户时：截图、导出的 CSV/PDF、日志摘录。
- 一次最多 3 个文件；路径必须存在且是普通文件。
- 不选择出口也不选收件人：交给各通道的默认通知出口，回执如实汇总。
- 返回 ok（全部成功）/ partial（部分成功）/ error（全部失败或没有可投递的通道）。`

const sendFileParams = `{
  "type": "object",
  "properties": {
    "paths": {
      "type": "array",
      "items": {"type": "string"},
      "description": "要发送的本地文件路径（1-3 个，绝对路径或相对当前工作区）"
    },
    "text": {"type": "string", "description": "随文件一起发的说明文字（可选，简洁）"}
  },
  "required": ["paths"]
}`

type sendFileArgs struct {
	Paths []string `json:"paths"`
	Text  string   `json:"text"`
}

func (r *Runner) sendFileTool(inst *instance) agent.Tool {
	return agent.NewTool("send_file", sendFileDesc, sendFileParams,
		func(ctx context.Context, argsJSON string) agent.ToolResult {
			return agent.ToolResult{Text: r.sendFileRun(ctx, inst, argsJSON)}
		})
}

func (r *Runner) sendFileRun(ctx context.Context, inst *instance, argsJSON string) string {
	var a sendFileArgs
	if err := json.Unmarshal([]byte(argsJSON), &a); err != nil {
		return fmt.Sprintf(agent.MsgParseArgs, err)
	}
	if r.opts.SendFiles == nil {
		return "error: 当前进程没有附件投递出口，无法发送文件"
	}
	if len(a.Paths) == 0 {
		return "error: paths 不能为空"
	}
	if len(a.Paths) > maxSendFiles {
		return fmt.Sprintf("error: 一次最多发送 %d 个文件，收到 %d 个", maxSendFiles, len(a.Paths))
	}
	files := make([]bus.MediaRef, 0, len(a.Paths))
	seen := map[string]struct{}{}
	for _, p := range a.Paths {
		ref, err := attachmentRef(inst.ws, p)
		if err != nil {
			return "error: " + err.Error()
		}
		if _, dup := seen[ref.Path]; dup {
			continue
		}
		seen[ref.Path] = struct{}{}
		files = append(files, ref)
	}
	if len(files) == 0 {
		return "error: 没有可发送的文件"
	}
	sent, total, names := r.opts.SendFiles(ctx, strings.TrimSpace(a.Text), files)
	switch {
	case total == 0:
		return "error: 没有实现通知出口的通道，无法发送文件"
	case sent == 0:
		return fmt.Sprintf("error: 全部附件出口投递失败（%d 个目标）", total)
	case sent < total:
		return fmt.Sprintf("partial: 已发 %d/%d 个目标（成功：%s）", sent, total, names)
	default:
		return fmt.Sprintf("ok: 已把 %d 个文件发到 %d 个目标（%s）", len(files), sent, names)
	}
}

// attachmentRef 校验路径并生成附件引用：必须是可读的普通文件，不复核大小
// （通道侧按平台限制判定，这里只拦掉不存在、目录、设备文件之类的明显错误）。
func attachmentRef(workspace, p string) (bus.MediaRef, error) {
	path := strings.TrimSpace(p)
	if path == "" {
		return bus.MediaRef{}, fmt.Errorf("文件路径为空")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(workspace, path)
	}
	path = filepath.Clean(path)
	info, err := os.Stat(path)
	if err != nil {
		return bus.MediaRef{}, fmt.Errorf("文件不可用 %s: %w", p, err)
	}
	if info.IsDir() {
		return bus.MediaRef{}, fmt.Errorf("是目录不是文件: %s", p)
	}
	if !info.Mode().IsRegular() {
		return bus.MediaRef{}, fmt.Errorf("不是普通文件: %s", p)
	}
	f, err := os.Open(path)
	if err != nil {
		return bus.MediaRef{}, fmt.Errorf("文件不可读 %s: %w", p, err)
	}
	_ = f.Close()
	return bus.MediaRef{
		Path:  path,
		Name:  filepath.Base(path),
		MIME:  mimeByName(path),
		Bytes: info.Size(),
	}, nil
}

func mimeByName(path string) string {
	if mt := mime.TypeByExtension(strings.ToLower(filepath.Ext(path))); mt != "" {
		return mt
	}
	return "application/octet-stream"
}
