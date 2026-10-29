package http

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestManifestSelfDoc(t *testing.T) {
	c := newTestChannel(t, nil, nil)
	resp, out := doJSON(t, http.MethodGet, c.url(t, PathManifest), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%v", resp.StatusCode, out)
	}
	if out["schema"] != "service-manifest/v1" {
		t.Fatalf("schema = %v", out["schema"])
	}
	if out["name"] != "ivan" {
		t.Fatalf("name = %v", out["name"])
	}
	if v, ok := out["version"]; ok {
		t.Fatalf("未配置版本时不应输出 version，got %v", v)
	}
	eps, ok := out["endpoints"].([]any)
	if !ok || len(eps) != 4 {
		t.Fatalf("endpoints = %#v", out["endpoints"])
	}
	first, _ := eps[0].(map[string]any)
	if first["url"].(string) != "{{base}}/request" {
		t.Fatalf("endpoint url 应带 {{base}} 占位符: %#v", first)
	}
	if out["detail_url"] != "{{base}}/guide.md" {
		t.Fatalf("detail_url = %v", out["detail_url"])
	}
	if !strings.Contains(out["usage"].(string), "{{base}}/notify") {
		t.Fatalf("usage 缺占位符: %v", out["usage"])
	}
	notifyNote := ""
	for _, e := range eps {
		m, _ := e.(map[string]any)
		if m["name"] == "notify" {
			notifyNote, _ = m["note"].(string)
		}
	}
	if !strings.Contains(notifyNote, "no_targets") {
		t.Fatalf("notify 端点说明应给出回执取值，得到 %q", notifyNote)
	}
	// 接口文档是给调用方看的：不暴露 ivan 内部的目标认领机制
	for _, text := range []string{out["usage"].(string), notifyNote} {
		if strings.Contains(text, "认领") || strings.Contains(text, "openid") {
			t.Fatalf("接口说明不应暴露内部机制: %q", text)
		}
	}
}

func TestManifestVersionAndMethod(t *testing.T) {
	c := New(Config{Listen: "127.0.0.1:0", Version: "abc1234"})
	if err := c.Start(t.Context(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(t.Context()) })
	resp, out := doJSON(t, http.MethodGet, c.url(t, PathManifest), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d", resp.StatusCode)
	}
	if out["version"] != "abc1234" {
		t.Fatalf("version = %v", out["version"])
	}
	resp, _ = doJSON(t, http.MethodPost, c.url(t, PathManifest), map[string]string{"x": "1"})
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST 状态码 = %d, want 405", resp.StatusCode)
	}
}

func TestGuideEndpoint(t *testing.T) {
	c := newTestChannel(t, nil, nil)
	resp, err := http.Get(c.url(t, PathGuide))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/markdown") {
		t.Fatalf("Content-Type = %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"POST /notify", "POST /request", "GET /request?session=",
		"/healthz", "/.well-known/service.json", "/guide.md",
		"targets[].result", "no_targets", "不要重试",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("指南缺少 %q", want)
		}
	}
	// 接口指南是给调用方看的：不暴露 ivan 内部的目标认领机制
	for _, leak := range []string{"认领", "openid", "open_id", "channel.Noticer", "no_targets 不要重试（重试不会"} {
		if strings.Contains(text, leak) {
			t.Fatalf("指南不应暴露内部实现 %q", leak)
		}
	}
	resp2, err := http.Post(c.url(t, PathGuide), "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST 状态码 = %d, want 405", resp2.StatusCode)
	}
}
