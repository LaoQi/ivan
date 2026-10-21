package qq

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/LaoQi/ivan/qqbot"
)

// qq 官方 Markdown 支持：标题、加粗/斜体/删除线、链接、图片（公网 URL 或 qqbot-media://）、
// 有序/无序列表、嵌套列表、块引用、水平线、空行换行。
// 表格与代码块官方未在「Markdown 消息」页列出，此处降级为平台能稳定渲染的形态，
// 避免整条消息被判 40034011「无效的 markdown 内容」。

const (
	mdFence     = "```"
	mdEllipsis  = "…"
	mdBullet    = "- "
	mdIndent    = "    "
	mdQuotePref = "> "
)

// hasMarkdown 判断文本是否含 markdown 语法特征。纯文本回合继续走 msg_type=0，
// 只对真正用上语法的正文承担 markdown 渲染与降级的风险。
func hasMarkdown(text string) bool {
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, mdFence) || strings.HasPrefix(trimmed, "~~~") {
			return true
		}
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "> ") || trimmed == "> " {
			return true
		}
		if trimmed == "***" || trimmed == "---" || trimmed == "___" {
			return true
		}
		if strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "* ") || strings.HasPrefix(trimmed, "+ ") {
			if trimmed != "* " {
				return true
			}
		}
		if orderedListItem(trimmed) {
			return true
		}
		if inlineMarkdown(trimmed) {
			return true
		}
	}
	return false
}

// inlineMarkdown 识别行内语法：强调、行内代码、链接、图片。
func inlineMarkdown(line string) bool {
	if strings.Contains(line, "**") || strings.Contains(line, "~~") || strings.Contains(line, "`") {
		return true
	}
	if strings.Contains(line, "](") || strings.Contains(line, "](<") {
		return true
	}
	if strings.HasPrefix(line, "![") {
		return true
	}
	if strings.Contains(line, "://") && (strings.Contains(line, "<http") || strings.Contains(line, "](")) {
		return true
	}
	// 单个 * 或 _ 成对出现才当语法，避免误伤 2*3 之类的普通文本
	if strings.Count(line, "*") >= 2 || strings.Count(line, "_") >= 2 {
		return true
	}
	return false
}

func orderedListItem(line string) bool {
	i := 0
	for i < len(line) && line[i] >= '0' && line[i] <= '9' {
		i++
	}
	if i == 0 || i >= len(line) {
		return false
	}
	if line[i] != '.' && line[i] != ')' {
		return false
	}
	return i+1 < len(line) && line[i+1] == ' '
}

// toQQMarkdown 把通用 markdown 收敛到 QQ 支持的子集：表格转要点行、代码围栏剥成缩进、
// 非公网图片剥成说明文字。输入必须是已去控制字符的文本。
func toQQMarkdown(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		if isFence(trimmed) {
			i = convertFence(lines, i, &out)
			continue
		}
		if isTableRow(trimmed) && i+1 < len(lines) && isTableDelimiter(strings.TrimSpace(lines[i+1])) {
			i = convertTable(lines, i, &out)
			continue
		}
		out = append(out, stripLocalImages(line))
		i++
	}
	return strings.Join(out, "\n")
}

func isFence(trimmed string) bool {
	return strings.HasPrefix(trimmed, mdFence) || strings.HasPrefix(trimmed, "~~~")
}

func convertFence(lines []string, start int, out *[]string) int {
	fence := mdFence
	if strings.HasPrefix(strings.TrimSpace(lines[start]), "~~~") {
		fence = "~~~"
	}
	lang := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(lines[start]), fence))
	body := make([]string, 0, 8)
	i := start + 1
	for i < len(lines) {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), fence) {
			i++
			break
		}
		body = append(body, lines[i])
		i++
	}
	if lang != "" {
		*out = append(*out, mdQuotePref+lang)
	}
	for _, b := range body {
		*out = append(*out, mdIndent+b)
	}
	return i
}

func isTableDelimiter(trimmed string) bool {
	if trimmed == "" {
		return false
	}
	body := strings.Trim(trimmed, "|")
	if strings.TrimSpace(body) == "" {
		return false
	}
	for _, cell := range strings.Split(body, "|") {
		cell = strings.TrimSpace(cell)
		if cell == "" {
			continue
		}
		cell = strings.TrimPrefix(cell, ":")
		cell = strings.TrimSuffix(cell, ":")
		if strings.Trim(cell, "-") != "" {
			return false
		}
	}
	return true
}

func isTableRow(trimmed string) bool {
	return strings.HasPrefix(trimmed, "|") && strings.HasSuffix(trimmed, "|") && strings.Count(trimmed, "|") >= 2
}

func convertTable(lines []string, start int, out *[]string) int {
	header := splitCells(lines[start])
	i := start + 2
	rows := make([][]string, 0, 4)
	for i < len(lines) && isTableRow(strings.TrimSpace(lines[i])) {
		rows = append(rows, splitCells(lines[i]))
		i++
	}
	for _, row := range rows {
		parts := make([]string, 0, len(row))
		for j, cell := range row {
			cell = strings.TrimSpace(cell)
			if cell == "" {
				continue
			}
			if j == 0 {
				parts = append(parts, cell)
				continue
			}
			key := ""
			if j < len(header) {
				key = strings.TrimSpace(header[j])
			}
			if key != "" {
				parts = append(parts, key+"="+cell)
			} else {
				parts = append(parts, cell)
			}
		}
		if len(parts) == 0 {
			continue
		}
		*out = append(*out, mdBullet+strings.Join(parts, " · "))
	}
	return i
}

func splitCells(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	return strings.Split(line, "|")
}

// stripLocalImages 把无法公网访问的图片（本地路径、附件引用）降级为说明文字。
func stripLocalImages(line string) string {
	var b strings.Builder
	for i := 0; i < len(line); {
		if !strings.HasPrefix(line[i:], "![") {
			b.WriteByte(line[i])
			i++
			continue
		}
		close := strings.Index(line[i:], "](")
		if close < 0 {
			b.WriteByte(line[i])
			i++
			continue
		}
		altEnd := i + close
		end := strings.Index(line[altEnd:], ")")
		if end < 0 {
			b.WriteByte(line[i])
			i++
			continue
		}
		alt := strings.TrimSpace(strings.TrimPrefix(line[i:altEnd], "!["))
		target := strings.TrimSpace(line[altEnd+2 : altEnd+end])
		target = strings.Trim(target, "<>")
		if !publicImageURL(target) {
			alt = strings.TrimSpace(strings.Split(alt, "#")[0])
			if alt == "" {
				alt = "图片"
			}
			b.WriteString("[图片: " + alt + "]")
			i = altEnd + end + 1
			continue
		}
		b.WriteString(line[i : altEnd+end+1])
		i = altEnd + end + 1
	}
	return b.String()
}

func publicImageURL(target string) bool {
	if strings.HasPrefix(target, "qqbot-media://") {
		return true
	}
	lower := strings.ToLower(target)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

// splitMarkdown 按通道上限切分 markdown，尽量不在围栏内断开；被切断的围栏会补上闭合标记，
// 保证每一片自身都是合法 markdown（整条消息按片顺序投递）。
func splitMarkdown(text string, limit int) []string {
	if limit <= 0 {
		return []string{text}
	}
	lines := strings.Split(text, "\n")
	parts := make([]string, 0, 4)
	var cur []string
	curLen := 0
	inFence := false
	flush := func(open bool) {
		if len(cur) == 0 {
			return
		}
		chunk := strings.Join(cur, "\n")
		if open {
			chunk += "\n" + mdFence
		}
		parts = append(parts, chunk)
		cur = nil
		curLen = 0
	}
	for _, line := range lines {
		lineLen := utf8.RuneCountInString(line) + 1
		if inFence && curLen+lineLen > limit {
			// 围栏内超限：在行边界切，补闭合、下一片重开围栏
			flush(true)
			cur = append(cur, mdFence)
			curLen = len(mdFence) + 1
		}
		if !inFence && curLen+lineLen > limit && len(cur) > 0 {
			flush(false)
		}
		if lineLen-1 > limit {
			// 单行超限：硬切
			rest := []rune(line)
			for len(rest) > 0 {
				n := limit - curLen
				if n <= 0 {
					flush(inFence)
					if inFence {
						cur = append(cur, mdFence)
						curLen = len(mdFence) + 1
					}
					n = limit - curLen
				}
				if n > len(rest) {
					n = len(rest)
				}
				cur = append(cur, string(rest[:n]))
				curLen += n
				rest = rest[n:]
				if len(rest) > 0 {
					flush(inFence)
					if inFence {
						cur = append(cur, mdFence)
						curLen = len(mdFence) + 1
					}
				}
			}
			continue
		}
		if isFence(strings.TrimSpace(line)) {
			inFence = !inFence
		}
		cur = append(cur, line)
		curLen += lineLen
	}
	flush(inFence)
	if len(parts) == 0 {
		return []string{text}
	}
	return parts
}

// hasCompleteFence 判断文本里的代码围栏是否成对（用于流式截断时决定是否补闭合）。
func hasCompleteFence(text string) bool {
	count := 0
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, mdFence) || strings.HasPrefix(trimmed, "~~~") {
			count++
		}
	}
	return count%2 == 0
}

// closemarkdown 在流式截断处补上未闭合的围栏，避免半截代码块破坏渲染。
func closeMarkdown(text string) string {
	if hasCompleteFence(text) {
		return text
	}
	return text + "\n" + mdFence
}

// streamHandle 包装 qqbot.Stream：markdown 流式的中间态可能停在围栏中途，
// 补上闭合标记再下发，避免半截代码块破坏整条消息的渲染。
// 追加发生在尾部，不影响平台要求的「已下发内容前缀」约束。
type streamHandle struct {
	stream *qqbot.Stream
}

func (h *streamHandle) Update(ctx context.Context, text string, done bool) error {
	return h.stream.Update(ctx, closeMarkdown(text), done)
}
