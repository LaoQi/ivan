package qqbot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type fakeGateway struct {
	t        *testing.T
	srv      *httptest.Server
	upgrader websocket.Upgrader

	mu           sync.Mutex
	identified   chan identifyData
	resumed      chan resumeData
	heartbeats   chan int
	helloMs      int
	readySession string
	sendIdentify bool
	dropAfterN   int
	conns        int
	conn         *websocket.Conn
	wmu          sync.Mutex
	sends        []sendCall
	userAgents   []string
	pending      []map[string]any
	sendStatus   int
	sendBody     string
	ackHeartbeat bool
	noReady      bool
}

type sendCall struct {
	Path string
	Body map[string]any
}

func newFakeGateway(t *testing.T) *fakeGateway {
	f := &fakeGateway{
		t:            t,
		identified:   make(chan identifyData, 8),
		resumed:      make(chan resumeData, 8),
		heartbeats:   make(chan int, 32),
		helloMs:      4000,
		readySession: "sess-1",
		ackHeartbeat: true,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/gateway/bot", func(w http.ResponseWriter, r *http.Request) {
		url := "ws" + strings.TrimPrefix(f.srv.URL, "http") + "/ws"
		_ = json.NewEncoder(w).Encode(GatewayInfo{URL: url, Shards: 1})
	})
	mux.HandleFunc("/app/getAppAccessToken", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"tok-abc","expires_in":"3600"}`))
	})
	mux.HandleFunc("/ws", f.serveWS)
	mux.HandleFunc("/v2/users/", f.serveSend)
	mux.HandleFunc("/v2/groups/", f.serveSend)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeGateway) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := f.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	f.mu.Lock()
	f.conns++
	f.conn = conn
	f.mu.Unlock()

	f.connWrite(map[string]any{"op": opHello, "d": map[string]any{"heartbeat_interval": f.helloMs}})

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		switch env.Op {
		case opIdentify:
			var d identifyData
			_ = json.Unmarshal(env.D, &d)
			f.identified <- d
			if f.noReady {
				continue
			}
			f.connWrite(map[string]any{
				"op": opDispatch, "s": 1, "t": EventReady,
				"d": map[string]any{
					"session_id": f.readySession, "shard": []int{0, 1},
					"user": map[string]any{"id": "bot-1", "username": "测试机器人"},
				},
			})
		case opResume:
			var d resumeData
			_ = json.Unmarshal(env.D, &d)
			f.resumed <- d
			f.connWrite(map[string]any{"op": opDispatch, "s": 5, "t": EventResumed, "d": map[string]any{}})
		case opHeartbeat:
			var seq int
			_ = json.Unmarshal(env.D, &seq)
			f.heartbeats <- seq
			if f.ackHeartbeat {
				f.connWrite(map[string]any{"op": opHeartbeatAck, "d": nil})
			}
		}
	}
}

func (f *fakeGateway) serveSend(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "QQBot tok-abc" {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"err_code":11244,"message":"token 校验失败"}`))
		return
	}
	f.mu.Lock()
	st, errBody := f.sendStatus, f.sendBody
	f.mu.Unlock()
	if st >= 400 {
		w.WriteHeader(st)
		_, _ = w.Write([]byte(errBody))
		return
	}
	body := map[string]any{}
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.sends = append(f.sends, sendCall{Path: r.URL.Path, Body: body})
	f.userAgents = append(f.userAgents, r.Header.Get("User-Agent"))
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"id":"msg-out-1","timestamp":"2026-10-20T00:00:00Z"}`))
}

func (f *fakeGateway) lastSend() (sendCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sends) == 0 {
		return sendCall{}, false
	}
	return f.sends[len(f.sends)-1], true
}

func (f *fakeGateway) pushMessage(kind string, payload map[string]any) {
	f.connWrite(map[string]any{"op": opDispatch, "s": 10, "t": kind, "d": payload})
}

func (f *fakeGateway) connWrite(v any) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()
	if conn != nil {
		_ = conn.WriteJSON(v)
	}
}

func (f *fakeGateway) killConn() {
	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (f *fakeGateway) lastUserAgent() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.userAgents) == 0 {
		return ""
	}
	return f.userAgents[len(f.userAgents)-1]
}

func (f *fakeGateway) setHelloMs(ms int) {
	f.mu.Lock()
	f.helloMs = ms
	f.mu.Unlock()
}

func (f *fakeGateway) flush(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	conn := f.conn
	items := f.pending
	f.pending = nil
	f.mu.Unlock()
	if conn == nil {
		t.Fatal("尚无活动连接")
	}
	waitFor(t, 2*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.conn != nil
	}, "连接就绪")
	for _, it := range items {
		kind, _ := it["kind"].(string)
		d, _ := it["d"].(map[string]any)
		f.pushMessage(kind, d)
	}
}

func testConfig(f *fakeGateway) Config {
	return Config{
		AppID:      "app-1",
		AppSecret:  "secret-1",
		APIBase:    f.srv.URL,
		TokenURL:   f.srv.URL + "/app/getAppAccessToken",
		GatewayURL: "",
		MinBackoff: 50 * time.Millisecond,
		MaxBackoff: 200 * time.Millisecond,
	}
}

type recordingHandler struct {
	ready chan ReadyInfo
	msgs  chan MessageEvent
	errs  chan error
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		ready: make(chan ReadyInfo, 8),
		msgs:  make(chan MessageEvent, 16),
		errs:  make(chan error, 16),
	}
}

func (h *recordingHandler) OnReady(r ReadyInfo)      { h.ready <- r }
func (h *recordingHandler) OnMessage(m MessageEvent) { h.msgs <- m }
func (h *recordingHandler) OnError(err error)        { h.errs <- err }

func waitFor(t *testing.T, d time.Duration, fn func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("超时等待: %s", msg)
}

func startClient(t *testing.T, f *fakeGateway) (*Client, *recordingHandler) {
	t.Helper()
	c, err := New(testConfig(f))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := newRecordingHandler()
	if err := c.Start(context.Background(), h); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	return c, h
}
