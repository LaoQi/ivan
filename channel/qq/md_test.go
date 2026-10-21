package qq

import (
	"strings"
	"testing"
)

func TestHasMarkdown(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"纯文本回复", false},
		{"2*3 是 6", false},
		{"## 结论", true},
		{"- 列表项", true},
		{"1. 有序项", true},
		{"> 引用", true},
		{"**加粗**", true},
		{"用 `pwd` 查看", true},
		{"见 [文档](https://example.com)", true},
		{"```go\nfmt.Println()\n```", true},
		{"***", true},
	}
	for _, c := range cases {
		if got := hasMarkdown(c.text); got != c.want {
			t.Fatalf("hasMarkdown(%q)=%v，期望 %v", c.text, got, c.want)
		}
	}
}

func TestToQQMarkdownConvertsTable(t *testing.T) {
	src := "| 文件 | 大小 |\n|---|---|\n| a.go | 12B |\n| b.go | 34B |\n"
	got := toQQMarkdown(src)
	if strings.Contains(got, "|") {
		t.Fatalf("表格应被降级:\n%s", got)
	}
	if !strings.Contains(got, "- a.go · 大小=12B") {
		t.Fatalf("降级形态不符:\n%s", got)
	}
	if !strings.Contains(got, "- b.go · 大小=34B") {
		t.Fatalf("第二行缺失:\n%s", got)
	}
}

func TestToQQMarkdownConvertsFence(t *testing.T) {
	src := "说明：\n```go\nfmt.Println(1)\n```\n完。"
	got := toQQMarkdown(src)
	if strings.Contains(got, "```") {
		t.Fatalf("围栏应被剥离:\n%s", got)
	}
	if !strings.Contains(got, "    fmt.Println(1)") {
		t.Fatalf("代码应转为缩进:\n%s", got)
	}
	if !strings.Contains(got, "> go") {
		t.Fatalf("应保留语言标记:\n%s", got)
	}
}

func TestToQQMarkdownStripsLocalImages(t *testing.T) {
	got := toQQMarkdown("看图 ![截图](/tmp/a.png) 结束")
	if strings.Contains(got, "/tmp/a.png") {
		t.Fatalf("本地图片路径应被剥离:\n%s", got)
	}
	if !strings.Contains(got, "[图片: 截图]") {
		t.Fatalf("应降级为说明文字:\n%s", got)
	}
	if got := toQQMarkdown("![x](https://example.com/a.png)"); !strings.Contains(got, "https://example.com/a.png") {
		t.Fatalf("公网图片应保留:\n%s", got)
	}
	if got := toQQMarkdown("![x](qqbot-media://image/v1/tok)"); !strings.Contains(got, "qqbot-media://") {
		t.Fatalf("平台素材引用应保留:\n%s", got)
	}
}

func TestSplitMarkdownPlainAndLimits(t *testing.T) {
	if parts := splitMarkdown("短", 100); len(parts) != 1 {
		t.Fatalf("短文本不应切分: %+v", parts)
	}
	if parts := splitMarkdown("任意", 0); len(parts) != 1 || parts[0] != "任意" {
		t.Fatalf("limit<=0 不切分: %+v", parts)
	}
	long := strings.Repeat("这是一句较长的中文。", 20)
	parts := splitMarkdown(long, 20)
	if len(parts) < 2 {
		t.Fatalf("长文本应切分: %d", len(parts))
	}
	for i, p := range parts {
		if len([]rune(p)) > 20 {
			t.Fatalf("第 %d 片超限 %d", i, len([]rune(p)))
		}
	}
	if joined := strings.Join(parts, ""); !strings.Contains(joined, "这是一句") {
		t.Fatalf("内容丢失: %q", joined)
	}
}

func TestSplitMarkdownSplitsInsideFence(t *testing.T) {
	body := "```go\n" + strings.Repeat("line\n", 20) + "```"
	parts := splitMarkdown(body, 30)
	if len(parts) < 2 {
		t.Fatalf("围栏内应切分: %d", len(parts))
	}
	for i, p := range parts {
		if !hasCompleteFence(p) {
			t.Fatalf("第 %d 片围栏未闭合:\n%s", i, p)
		}
	}
}

func TestSplitMarkdownHardCutOverlongLine(t *testing.T) {
	parts := splitMarkdown(strings.Repeat("x", 100), 10)
	if len(parts) != 10 {
		t.Fatalf("超长单行应硬切 10 片，得到 %d", len(parts))
	}
	for i, p := range parts {
		if len([]rune(p)) > 10 {
			t.Fatalf("第 %d 片超限", i)
		}
	}
}

func TestCloseMarkdown(t *testing.T) {
	if got := closeMarkdown("已闭合\n```\ncode\n```"); strings.Count(got, "```") != 2 {
		t.Fatalf("已闭合不应补围栏: %q", got)
	}
	if got := closeMarkdown("```go\ncode"); strings.Count(got, "```") != 2 {
		t.Fatalf("未闭合应补围栏: %q", got)
	}
}
