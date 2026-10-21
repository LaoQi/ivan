package bus

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRuneLen(t *testing.T) {
	if got := RuneLen("中文abc"); got != 5 {
		t.Fatalf("RuneLen=%d，期望 5", got)
	}
	if got := RuneLen(""); got != 0 {
		t.Fatalf("空串 RuneLen=%d", got)
	}
}

func TestClipByRune(t *testing.T) {
	if got := Clip("中文内容", 0); got != "中文内容" {
		t.Fatalf("limit<=0 不应截断: %q", got)
	}
	if got := Clip("中文内容", -1); got != "中文内容" {
		t.Fatalf("limit<0 不应截断: %q", got)
	}
	if got := Clip("中文内容", 2); got != "中文…" {
		t.Fatalf("应按字符截断并加省略号: %q", got)
	}
	if got := Clip("中文", 5); got != "中文" {
		t.Fatalf("未超限应原样: %q", got)
	}
	// 关键回归：中文 3 字节/字，按字节判定会切出乱码或越界
	long := strings.Repeat("字", 5000)
	got := Clip(long, 2048)
	if RuneLen(got) != 2049 { // 2048 字 + 省略号
		t.Fatalf("截断后字符数=%d，期望 2049", RuneLen(got))
	}
	if strings.ContainsRune(got, utf8.RuneError) {
		t.Fatal("截断产生乱码")
	}
}

func TestTruncateByRune(t *testing.T) {
	if got := Truncate("中文内容", 0); got != "中文内容" {
		t.Fatalf("limit<=0 不应截断: %q", got)
	}
	if got := Truncate("中文内容", 2); got != "中文" {
		t.Fatalf("应按字符截断: %q", got)
	}
	if got := Truncate(strings.Repeat("中", 10), 3); got != "中中中" {
		t.Fatalf("中文应切 3 字: %q", got)
	}
}

func TestOneLine(t *testing.T) {
	if got := OneLine("a\nb\tc  d", 0); got != "a b c d" {
		t.Fatalf("应折叠空白: %q", got)
	}
	if got := OneLine("第一行\n第二行", 3); got != "第一行…" {
		t.Fatalf("应按字符截断: %q", got)
	}
	if got := OneLine(strings.Repeat("字", 500), 200); RuneLen(got) != 201 {
		t.Fatalf("字符数=%d，期望 201", RuneLen(got))
	}
}

func TestKeySlugTruncatesByRune(t *testing.T) {
	key := strings.Repeat("字", 100)
	slug := KeySlug(key)
	if RuneLen(slug) > slugLimit+1+8 {
		t.Fatalf("slug 过长: %d 字符", RuneLen(slug))
	}
	if strings.ContainsRune(slug, utf8.RuneError) {
		t.Fatal("slug 出现乱码")
	}
}
