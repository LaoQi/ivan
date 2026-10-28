package http

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/LaoQi/ivan/bus"
)

type NoticeBody struct {
	Title     string `json:"title"`
	Body      string `json:"body"`
	Text      string `json:"text"`
	Severity  string `json:"severity"`
	Source    string `json:"source"`
	DedupeKey string `json:"dedupe_key"`
}

type TargetResult struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Result  string `json:"result"`
	Error   string `json:"error,omitempty"`
}

func (c *Channel) handleNotify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "只支持 POST")
		return
	}
	var in NoticeBody
	if err := decodeBody(w, r, c.cfg.MaxBodyBytes, &in); err != nil {
		c.notifyRejected.Add(1)
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	out, err := c.notice(r.Context(), in)
	code := http.StatusOK
	switch {
	case err == nil:
	default:
		code = http.StatusServiceUnavailable
		if errors.Is(err, ErrEmpty) || errors.Is(err, ErrEmptySource) {
			code = http.StatusBadRequest
		} else if errors.Is(err, ErrRateLimited) {
			code = http.StatusTooManyRequests
		}
	}
	writeJSON(w, code, out)
}

func (c *Channel) SendNotice(ctx context.Context, in NoticeBody) (map[string]any, error) {
	return c.notice(ctx, in)
}

func (c *Channel) notice(ctx context.Context, in NoticeBody) (map[string]any, error) {
	source := strings.TrimSpace(in.Source)
	if source == "" {
		c.notifyRejected.Add(1)
		return map[string]any{"status": "rejected", "message": "缺少 source（通知来源服务名）"}, ErrEmptySource
	}
	title := strings.TrimSpace(in.Title)
	body := strings.TrimSpace(firstNonEmpty(in.Body, in.Text))
	if title == "" && body == "" {
		c.notifyRejected.Add(1)
		return map[string]any{"status": "rejected", "message": ErrEmpty.Error()}, ErrEmpty
	}
	if !c.notifyWin.allow(c.cfg.NotifyPerMinute, time.Now()) {
		c.notifyRejected.Add(1)
		c.audit("ext_notify_rejected", "rate_limited source="+bus.OneLine(source, sourceLimit))
		return map[string]any{"status": "rejected", "message": ErrRateLimited.Error()}, ErrRateLimited
	}
	dedupeKey := strings.TrimSpace(in.DedupeKey)
	if dedupeKey != "" {
		dedupeKey = source + "|" + dedupeKey
	}
	if c.dedup.seen(dedupeKey, time.Now()) {
		c.notifyDeduped.Add(1)
		c.audit("ext_notify_deduped", "source="+bus.OneLine(source, sourceLimit)+" key="+bus.OneLine(dedupeKey, 128))
		return map[string]any{"status": "deduped", "message": "同一 dedupe_key 已在去重窗口内投递过", "targets": []TargetResult{}}, nil
	}
	content, truncated := formatNotice(title, strings.TrimSpace(in.Severity), source, body)
	if truncated {
		c.notifyTruncated.Add(1)
	}
	results, ok, partial := c.broadcast(ctx, content)
	c.notifyAccepted.Add(1)
	switch {
	case ok:
		c.notifyDelivered.Add(1)
	case partial:
		c.notifyPartial.Add(1)
	default:
		c.notifyFailed.Add(1)
		c.recordErr(errors.New("通知投递全部失败: " + summarizeResults(results)))
	}
	c.audit("ext_notify", "source="+bus.OneLine(source, sourceLimit)+" targets="+bus.OneLine(summarizeResults(results), 128))
	status := "failed"
	if ok {
		status = "delivered"
	} else if partial {
		status = "partial"
	}
	out := map[string]any{"status": status, "targets": results, "deduped": false, "truncated": truncated}
	if status == "failed" {
		out["message"] = "通知投递全部失败（或没有可投递的通知出口）"
		return out, errors.New(out["message"].(string))
	}
	return out, nil
}

func (c *Channel) broadcast(ctx context.Context, content string) ([]TargetResult, bool, bool) {
	if c.cfg.Broadcast == nil {
		return []TargetResult{{Name: "-", Result: "failed", Error: "通知出口未接通"}}, false, false
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	results := c.cfg.Broadcast(dctx, content)
	sent, failed := 0, 0
	for _, r := range results {
		switch r.Result {
		case "sent":
			sent++
		default:
			failed++
		}
	}
	return results, failed == 0 && sent > 0, sent > 0 && failed > 0
}

func summarizeResults(results []TargetResult) string {
	var b strings.Builder
	for i, r := range results {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(r.Name)
		b.WriteString("=")
		b.WriteString(r.Result)
		if r.Error != "" {
			b.WriteString("(")
			b.WriteString(bus.OneLine(r.Error, 64))
			b.WriteString(")")
		}
	}
	return b.String()
}

// formatNotice 组装投递正文：`[severity] title` + 正文（+ 来源脚注）。
func formatNotice(title, severity, source, body string) (string, bool) {
	truncated := false
	if title != "" {
		if clipped := bus.Truncate(title, titleLimit); clipped != title {
			truncated = true
			title = clipped
		}
	}
	if clipped := bus.Truncate(body, bodyLimit); clipped != body {
		truncated = true
		body = clipped
	}
	if strings.TrimSpace(title) == "" && strings.TrimSpace(body) == "" {
		return "", truncated
	}
	var b strings.Builder
	if sev := strings.TrimSpace(severity); sev != "" && !strings.EqualFold(sev, "info") {
		b.WriteString("[")
		b.WriteString(bus.OneLine(sev, 32))
		b.WriteString("] ")
	}
	if title != "" {
		b.WriteString(title)
		if body != "" {
			b.WriteString("\n\n")
		}
	}
	b.WriteString(body)
	if source != "" {
		b.WriteString("\n\n— ")
		b.WriteString(bus.OneLine(source, sourceLimit))
	}
	if truncated {
		b.WriteString("\n（内容过长已截断）")
	}
	return b.String(), truncated
}
