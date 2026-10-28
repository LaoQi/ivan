package http

import (
	"context"
	"errors"
	"fmt"
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

type targetResult struct {
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
	title := strings.TrimSpace(in.Title)
	body := strings.TrimSpace(firstNonEmpty(in.Body, in.Text))
	if title == "" && body == "" {
		c.notifyRejected.Add(1)
		writeErr(w, http.StatusBadRequest, ErrEmpty.Error())
		return
	}
	if len(c.cfg.Targets) == 0 {
		c.notifyFailed.Add(1)
		c.recordErr(errors.New("未配置通知目标（http.targets 为空）"))
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "failed", "message": "未配置通知目标（http.targets 为空）", "targets": []targetResult{},
		})
		return
	}
	if !c.notifyWin.allow(c.cfg.NotifyPerMinute, time.Now()) {
		c.notifyRejected.Add(1)
		c.audit("ext_notify_rejected", "rate_limited source="+bus.OneLine(in.Source, sourceLimit))
		writeErr(w, http.StatusTooManyRequests, ErrRateLimited.Error())
		return
	}
	source := strings.TrimSpace(in.Source)
	now := time.Now()
	dedupeKey := strings.TrimSpace(in.DedupeKey)
	if dedupeKey != "" {
		dedupeKey = source + "|" + dedupeKey
	}
	if c.dedup.seen(dedupeKey, now) {
		c.notifyDeduped.Add(1)
		c.audit("ext_notify_deduped", "source="+bus.OneLine(source, sourceLimit)+" key="+bus.OneLine(dedupeKey, 128))
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "deduped",
			"message": "同一 dedupe_key 已在去重窗口内投递过",
			"targets": []targetResult{},
		})
		return
	}

	content, truncated := formatNotice(title, strings.TrimSpace(in.Severity), source, body)
	if truncated {
		c.notifyTruncated.Add(1)
	}
	results, ok, partial := c.fanout(r.Context(), content)
	c.notifyAccepted.Add(1)
	switch {
	case ok:
		c.notifyDelivered.Add(1)
	case partial:
		c.notifyPartial.Add(1)
	default:
		c.notifyFailed.Add(1)
		c.recordErr(fmt.Errorf("通知投递全部失败: %d 个目标", len(results)))
	}
	c.audit("ext_notify", fmt.Sprintf("source=%s severity=%s targets=%d ok=%v partial=%v",
		bus.OneLine(source, sourceLimit), bus.OneLine(in.Severity, 32), len(results), ok, partial))

	code := http.StatusOK
	status := "delivered"
	switch {
	case ok:
	case partial:
		status = "partial"
		code = http.StatusOK
	default:
		status = "failed"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status":    status,
		"targets":   results,
		"deduped":   false,
		"truncated": truncated,
	})
}

// fanout 把同一条通知逐个投给配置的目标；任一目标失败不影响其余。
func (c *Channel) fanout(ctx context.Context, content string) ([]targetResult, bool, bool) {
	if c.cfg.Deliver == nil {
		return []targetResult{{Name: "-", Result: "failed", Error: "未接出站投递钩子"}}, false, false
	}
	results := make([]targetResult, 0, len(c.cfg.Targets))
	sent, failed := 0, 0
	for _, t := range c.cfg.Targets {
		msg := bus.OutboundMessage{
			Channel: t.Channel,
			ChatID:  t.ChatID,
			Content: content,
			Kind:    bus.KindNotice,
		}
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		err := c.cfg.Deliver(dctx, msg)
		cancel()
		res := targetResult{Name: t.Name, Channel: t.Channel}
		switch {
		case err == nil:
			res.Result = "sent"
			sent++
		default:
			res.Result = "failed"
			res.Error = err.Error()
			failed++
		}
		results = append(results, res)
	}
	return results, failed == 0 && sent > 0, sent > 0 && failed > 0
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
	if src := strings.TrimSpace(source); src != "" {
		b.WriteString("\n\n— ")
		b.WriteString(bus.OneLine(src, sourceLimit))
	}
	if truncated {
		b.WriteString("\n（内容过长已截断）")
	}
	return b.String(), truncated
}

// SendNotice 供本地入口（ivan notify 走 socket）复用同一 fan-out 管线。
func (c *Channel) SendNotice(ctx context.Context, in NoticeBody) (map[string]any, error) {
	title := strings.TrimSpace(in.Title)
	body := strings.TrimSpace(firstNonEmpty(in.Body, in.Text))
	if title == "" && body == "" {
		return nil, ErrEmpty
	}
	if len(c.cfg.Targets) == 0 {
		c.notifyFailed.Add(1)
		return nil, errors.New("未配置通知目标（http.targets 为空）")
	}
	if !c.notifyWin.allow(c.cfg.NotifyPerMinute, time.Now()) {
		c.notifyRejected.Add(1)
		return nil, ErrRateLimited
	}
	source := strings.TrimSpace(in.Source)
	dedupeKey := strings.TrimSpace(in.DedupeKey)
	if dedupeKey != "" {
		dedupeKey = source + "|" + dedupeKey
	}
	if c.dedup.seen(dedupeKey, time.Now()) {
		c.notifyDeduped.Add(1)
		return map[string]any{"status": "deduped", "targets": []targetResult{}}, nil
	}
	content, truncated := formatNotice(title, strings.TrimSpace(in.Severity), source, body)
	if truncated {
		c.notifyTruncated.Add(1)
	}
	results, ok, partial := c.fanout(ctx, content)
	c.notifyAccepted.Add(1)
	switch {
	case ok:
		c.notifyDelivered.Add(1)
	case partial:
		c.notifyPartial.Add(1)
	default:
		c.notifyFailed.Add(1)
		c.recordErr(fmt.Errorf("通知投递全部失败: %d 个目标", len(results)))
	}
	c.audit("ext_notify", fmt.Sprintf("source=%s targets=%d ok=%v partial=%v local=true",
		bus.OneLine(source, sourceLimit), len(results), ok, partial))
	status := "failed"
	if ok {
		status = "delivered"
	} else if partial {
		status = "partial"
	}
	out := map[string]any{"status": status, "targets": results, "truncated": truncated}
	if status == "failed" {
		return out, errors.New("通知投递全部失败")
	}
	return out, nil
}
