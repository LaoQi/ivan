package http

import (
	_ "embed"
	"net/http"
)

//go:embed guide.md
var guideFS []byte

func (c *Channel) handleGuide(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeErr(w, http.StatusMethodNotAllowed, "只支持 GET")
		return
	}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(guideFS)
}

func (c *Channel) handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeErr(w, http.StatusMethodNotAllowed, "只支持 GET")
		return
	}
	m := map[string]any{
		"schema":       "service-manifest/v1",
		"name":         "ivan",
		"summary":      "常驻后台 agent：定时触发与计划任务、QQ 单聊通道；对外提供通知入口（/notify）与异步 agent 请求（/request）",
		"capabilities": []string{"agent", "ai", "assistant", "schedule", "cron", "qq", "notify"},
		"usage":        "通知（source 必填）: curl -X POST {{base}}/notify -H 'Content-Type: application/json' -d '{\"source\":\"ci\",\"title\":\"构建失败\"}'；agent 请求: POST {{base}}/request 受理，GET {{base}}/request?session=<键> 轮询；接口指南: curl -s {{base}}/guide.md",
		"endpoints": []map[string]string{
			{"name": "request", "proto": "http", "url": "{{base}}/request", "note": "POST 提交 agent 任务（source/purpose/text 必填，异步受理返回会话键）；GET ?session= 轮询结果"},
			{"name": "notify", "proto": "http", "url": "{{base}}/notify", "note": "纯转发通知（source 必填，title/body/severity/dedupe_key），广播到各通道默认通知出口，请求方不可也不需指定目标"},
			{"name": "guide", "proto": "http", "url": "{{base}}/guide.md", "note": "接口指南（Markdown，先读这个）"},
			{"name": "health", "proto": "http", "url": "{{base}}/healthz", "note": "健康检查（服务目录探活即打这里）"},
		},
		"detail_url": "{{base}}/guide.md",
	}
	if v := c.cfg.Version; v != "" {
		m["version"] = v
	}
	writeJSON(w, http.StatusOK, m)
}
