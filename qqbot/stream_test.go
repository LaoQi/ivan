package qqbot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type streamRecorder struct {
	mu     sync.Mutex
	chunks []streamChunk
	typing []map[string]any
	paths  []string
	nextID string
}

func newStreamServer(t *testing.T, rec *streamRecorder) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/app/getAppAccessToken", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"tok-s","expires_in":"3600"}`))
	})
	mux.HandleFunc("/v2/users/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		rec.mu.Lock()
		rec.paths = append(rec.paths, r.URL.Path)
		if r.URL.Path == "/v2/users/u1/stream_messages" {
			raw, _ := json.Marshal(body)
			var ch streamChunk
			_ = json.Unmarshal(raw, &ch)
			rec.chunks = append(rec.chunks, ch)
		} else {
			rec.typing = append(rec.typing, body)
		}
		rec.mu.Unlock()
		if r.URL.Path == "/v2/users/u1/stream_messages" {
			_, _ = w.Write([]byte(`{"id":"stream-1","timestamp":"2026-10-21T00:00:00Z","remain_msg_len":100}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"msg-1","timestamp":"2026-10-21T00:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newStreamClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := New(Config{
		AppID: "app", AppSecret: "sec",
		APIBase: srv.URL, TokenURL: srv.URL + "/app/getAppAccessToken",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestOpenStreamAndUpdateCarriesStreamID(t *testing.T) {
	rec := &streamRecorder{}
	srv := newStreamServer(t, rec)
	c := newStreamClient(t, srv)
	st, err := c.OpenStream(context.Background(), Target{UserOpenID: "u1"}, "第一段", "text", "m-1", 1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if err := st.Update(context.Background(), "第一段第二段", false); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := st.Update(context.Background(), "第一段第二段完", true); err != nil {
		t.Fatalf("Update(done): %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.chunks) != 3 {
		t.Fatalf("分片数 %d，期望 3", len(rec.chunks))
	}
	first := rec.chunks[0]
	if first.StreamMsgID != "" || first.Index != 0 || first.InputState != InputStateGenerating {
		t.Fatalf("首片异常: %+v", first)
	}
	if first.MsgID != "m-1" || first.MsgSeq != 1 || first.InputMode != InputModeReplace {
		t.Fatalf("首片被动回复字段异常: %+v", first)
	}
	for i, ch := range rec.chunks[1:] {
		if ch.StreamMsgID != "stream-1" {
			t.Fatalf("第 %d 片未携带 stream_msg_id: %+v", i+1, ch)
		}
		if ch.Index != i+1 {
			t.Fatalf("第 %d 片 index=%d", i+1, ch.Index)
		}
	}
	if last := rec.chunks[2]; last.InputState != InputStateFinished {
		t.Fatalf("结束片未标记 input_state=10: %+v", last)
	}
}

func TestOpenStreamRejectsGroup(t *testing.T) {
	rec := &streamRecorder{}
	srv := newStreamServer(t, rec)
	c := newStreamClient(t, srv)
	if _, err := c.OpenStream(context.Background(), Target{GroupOpenID: "g1"}, "x", "text", "", 0); err == nil || !strings.Contains(err.Error(), "群聊") {
		t.Fatalf("群聊应拒绝流式: %v", err)
	}
}

func TestTypingSendsInputNotify(t *testing.T) {
	rec := &streamRecorder{}
	srv := newStreamServer(t, rec)
	c := newStreamClient(t, srv)
	if err := c.Typing(context.Background(), Target{UserOpenID: "u1"}, "m-1", 1, 0); err != nil {
		t.Fatalf("Typing: %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.typing) != 1 {
		t.Fatalf("输入状态请求数 %d", len(rec.typing))
	}
	body := rec.typing[0]
	if body["msg_type"] != float64(MsgTypeInput) {
		t.Fatalf("msg_type 异常: %+v", body)
	}
	notify, _ := body["input_notify"].(map[string]any)
	if notify == nil || notify["input_type"] != float64(1) {
		t.Fatalf("input_notify 异常: %+v", body)
	}
	if notify["input_second"] != float64(60) {
		t.Fatalf("input_second 未收敛到上限: %+v", notify)
	}
	if body["msg_id"] != "m-1" {
		t.Fatalf("缺 msg_id: %+v", body)
	}
}

func TestOpenStreamMarkdownContentType(t *testing.T) {
	rec := &streamRecorder{}
	srv := newStreamServer(t, rec)
	c := newStreamClient(t, srv)
	st, err := c.OpenStream(context.Background(), Target{UserOpenID: "u1"}, "## 标题", ContentTypeMarkdown, "m-1", 1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if err := st.Update(context.Background(), "## 标题\n正文", false); err != nil {
		t.Fatalf("Update: %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.chunks) != 2 {
		t.Fatalf("分片数 %d", len(rec.chunks))
	}
	for i, ch := range rec.chunks {
		if ch.ContentType != ContentTypeMarkdown {
			t.Fatalf("第 %d 片 content_type=%q", i, ch.ContentType)
		}
	}
}

func TestOpenStreamContentTypeDefaultsToText(t *testing.T) {
	rec := &streamRecorder{}
	srv := newStreamServer(t, rec)
	c := newStreamClient(t, srv)
	st, err := c.OpenStream(context.Background(), Target{UserOpenID: "u1"}, "纯文本", "", "m-1", 1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = st
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.chunks[0].ContentType != ContentTypeText {
		t.Fatalf("空 content_type 应收敛为 text，得到 %q", rec.chunks[0].ContentType)
	}
}
