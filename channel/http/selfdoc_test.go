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
	for _, want := range []string{"POST /notify", "POST /request", "GET /request?session=", "/healthz", "/.well-known/service.json", "/guide.md"} {
		if !strings.Contains(text, want) {
			t.Fatalf("指南缺少 %q", want)
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
