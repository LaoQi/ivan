package prompt

import (
	_ "embed"
	"fmt"
	"strings"
	"time"
)

//go:embed system_prompt.md
var base string

func System(workspace, dataDir string) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(base))
	b.WriteString("\n\n## 环境\n")
	fmt.Fprintf(&b, "- 工作区: %s\n", workspace)
	fmt.Fprintf(&b, "- 数据目录: %s\n", dataDir)
	fmt.Fprintf(&b, "- 当前时间: %s\n", time.Now().Format("2006-01-02 15:04:05 -0700"))
	return b.String()
}
