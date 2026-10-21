package bus

import (
	"strings"
	"unicode/utf8"
)

// 文本长度一律按 rune（字符）计，与终端显示、平台字符数口径一致；
// 字节口径只用于文件大小（如 data.TrimFileTail）。

func RuneLen(s string) int { return utf8.RuneCountInString(s) }

// Clip 按字符数截断到 limit，超出时追加省略号；limit <= 0 表示不截断。
func Clip(s string, limit int) string {
	if limit <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "…"
}

// Truncate 按字符数硬截断（不追加标记）；limit <= 0 表示不截断。
func Truncate(s string, limit int) string {
	if limit <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit])
}

// OneLine 折叠空白为单行并按字符数截断，用于日志与状态行。
func OneLine(s string, limit int) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	return Clip(s, limit)
}
