package prompt

import (
	_ "embed"
	"fmt"
	"strings"
	"time"
)

//go:embed system_prompt.md
var base string

type Info struct {
	Workspace string
	DataDir   string
	Memory    string
	Goals     string
	Session   string
}

func System(info Info) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(base))
	b.WriteString("\n\n## 环境\n")
	fmt.Fprintf(&b, "- 工作区: %s\n", info.Workspace)
	fmt.Fprintf(&b, "- 数据目录: %s\n", info.DataDir)
	if info.Session != "" {
		fmt.Fprintf(&b, "- 会话键: %s\n", info.Session)
	}
	if info.Memory != "" {
		fmt.Fprintf(&b, "- 记忆文件: %s（需要背景时先读；有值得长期记住的结论就追加一条带日期的要点）\n", info.Memory)
	}
	if info.Goals != "" {
		fmt.Fprintf(&b, "- 目标与待办: %s（文件存在时先读，按它推进）\n", info.Goals)
	}
	fmt.Fprintf(&b, "- 当前时间: %s\n", time.Now().Format("2006-01-02 15:04:05 -0700"))
	return b.String()
}
