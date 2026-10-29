package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
)

const (
	Name         = "http"
	PathNotify   = "/notify"
	PathRequest  = "/request"
	PathHealth   = "/healthz"
	PathManifest = "/.well-known/service.json"
	PathGuide    = "/guide.md"

	titleLimit  = 200
	bodyLimit   = 8192
	sourceLimit = 64
	identLimit  = 128
	purposeLim  = 200
)

var (
	ErrEmpty       = errors.New("通知内容为空")
	ErrEmptySource = errors.New("缺少 source")
	ErrRateLimited = errors.New("请求频率超限")
)

type Config struct {
	Listen           string
	Version          string
	MaxBodyBytes     int
	NotifyPerMinute  int
	RequestPerMinute int
	RequestSession   string
	Broadcast        func(ctx context.Context, content string) []TargetResult
	Notice           func(ctx context.Context, content string)
	Submit           func(msg bus.InboundMessage) bool
	Query            func(session string) (RequestState, bool)
	Audit            func(kind, text string)
	Logf             func(format string, args ...any)
}

type RequestState struct {
	Running    bool
	Failed     bool
	Reply      string
	FinishedAt time.Time
}

type Stats struct {
	Listen          string    `json:"listen"`
	Running         bool      `json:"running"`
	NotifyAccepted  uint64    `json:"notify_accepted"`
	NotifyDelivered uint64    `json:"notify_delivered"`
	NotifyPartial   uint64    `json:"notify_partial"`
	NotifyFailed    uint64    `json:"notify_failed"`
	NotifyTruncated uint64    `json:"notify_truncated"`
	NotifyRejected  uint64    `json:"notify_rejected"`
	RequestAccepted uint64    `json:"request_accepted"`
	RequestRejected uint64    `json:"request_rejected"`
	LastError       string    `json:"last_error,omitempty"`
	LastErrorAt     time.Time `json:"last_error_at,omitempty"`
}

type window struct {
	mu    sync.Mutex
	start time.Time
	count int
}

func (w *window) allow(limit int, now time.Time) bool {
	if limit <= 0 {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if now.Sub(w.start) >= time.Minute {
		w.start = now
		w.count = 0
	}
	if w.count >= limit {
		return false
	}
	w.count++
	return true
}

type Channel struct {
	cfg Config

	mu         sync.Mutex
	bus        *bus.Bus
	srv        *http.Server
	ln         net.Listener
	closed     bool
	notifyWin  window
	requestWin window

	notifyAccepted  atomic.Uint64
	notifyDelivered atomic.Uint64
	notifyPartial   atomic.Uint64
	notifyFailed    atomic.Uint64
	notifyTruncated atomic.Uint64
	notifyRejected  atomic.Uint64

	requestAccepted atomic.Uint64
	requestRejected atomic.Uint64

	errMu     sync.Mutex
	lastErr   string
	lastErrAt time.Time
}

func New(cfg Config) *Channel {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 65536
	}
	return &Channel{
		cfg: cfg,
	}
}

func (c *Channel) Name() string { return Name }

func (c *Channel) MaxLen() int { return 0 }

func (c *Channel) Start(ctx context.Context, b *bus.Bus) error {
	addr := strings.TrimSpace(c.cfg.Listen)
	if addr == "" {
		return errors.New("http.listen 为空")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(PathNotify, c.handleNotify)
	mux.HandleFunc(PathRequest, c.handleRequest)
	mux.HandleFunc(PathHealth, c.handleHealth)
	mux.HandleFunc(PathManifest, c.handleManifest)
	mux.HandleFunc(PathGuide, c.handleGuide)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		ln.Close()
		return errors.New("通道已停止")
	}
	c.bus = b
	c.ln = ln
	c.srv = srv
	c.mu.Unlock()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.recordErr(err)
			c.logf("http 监听退出: %v", err)
		}
	}()
	return nil
}

func (c *Channel) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	srv := c.srv
	c.mu.Unlock()
	if srv == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		return err
	}
	return nil
}

func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	return fmt.Errorf("%w: http 无待回复请求（/request 为异步，结果经查询端点获取）", channel.ErrUnroutable)
}

func (c *Channel) Stats() Stats {
	c.mu.Lock()
	running := c.srv != nil && !c.closed
	listen := c.cfg.Listen
	if c.ln != nil {
		listen = c.ln.Addr().String()
	}
	c.mu.Unlock()
	c.errMu.Lock()
	lastErr, lastErrAt := c.lastErr, c.lastErrAt
	c.errMu.Unlock()
	return Stats{
		Listen:          listen,
		Running:         running,
		NotifyAccepted:  c.notifyAccepted.Load(),
		NotifyDelivered: c.notifyDelivered.Load(),
		NotifyPartial:   c.notifyPartial.Load(),
		NotifyFailed:    c.notifyFailed.Load(),
		NotifyTruncated: c.notifyTruncated.Load(),
		NotifyRejected:  c.notifyRejected.Load(),
		RequestAccepted: c.requestAccepted.Load(),
		RequestRejected: c.requestRejected.Load(),
		LastError:       lastErr,
		LastErrorAt:     lastErrAt,
	}
}

func (c *Channel) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeErr(w, http.StatusMethodNotAllowed, "只支持 GET")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "channel": Name})
}

type requestBody struct {
	Source   string `json:"source"`
	Identity string `json:"identity"`
	Purpose  string `json:"purpose"`
	Text     string `json:"text"`
	Session  string `json:"session"`
}

func (c *Channel) handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		c.submitRequest(w, r)
	case http.MethodGet:
		c.queryRequest(w, r)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "只支持 POST（提交）与 GET（查询）")
	}
}

func (c *Channel) submitRequest(w http.ResponseWriter, r *http.Request) {
	var in requestBody
	if err := decodeBody(w, r, c.cfg.MaxBodyBytes, &in); err != nil {
		c.rejectRequest(w, err)
		return
	}
	source := strings.TrimSpace(in.Source)
	purpose := strings.TrimSpace(in.Purpose)
	text := strings.TrimSpace(in.Text)
	switch {
	case source == "":
		c.rejectRequest(w, errors.New("缺少 source（请求来源服务名）"))
		return
	case purpose == "":
		c.rejectRequest(w, errors.New("缺少 purpose（这次请求的目的）"))
		return
	case text == "":
		c.rejectRequest(w, errors.New("缺少 text（要 agent 做的事）"))
		return
	}
	if !c.requestWin.allow(c.cfg.RequestPerMinute, time.Now()) {
		c.requestRejected.Add(1)
		c.audit("ext_request_rejected", "rate_limited source="+bus.OneLine(source, sourceLimit))
		writeErr(w, http.StatusTooManyRequests, ErrRateLimited.Error())
		return
	}
	session := strings.TrimSpace(in.Session)
	if session == "" {
		session = strings.TrimSpace(c.cfg.RequestSession)
	}
	if session == "" {
		session = Name + ":" + bus.KeySlug(source)
	}
	id := newID()
	c.mu.Lock()
	stopped := c.closed || c.bus == nil
	c.mu.Unlock()
	if stopped {
		c.requestRejected.Add(1)
		writeErr(w, http.StatusServiceUnavailable, "HTTP 入口正在停机")
		return
	}
	msg := bus.InboundMessage{
		Channel:    Name,
		ChatID:     id,
		ChatType:   bus.ChatDirect,
		SenderID:   firstNonEmpty(strings.TrimSpace(in.Identity), source),
		SenderName: source,
		MessageID:  id,
		Content:    formatRequest(source, strings.TrimSpace(in.Identity), purpose, text),
		SessionKey: session,
		ReceivedAt: time.Now(),
		Raw: map[string]string{
			"source":   source,
			"identity": strings.TrimSpace(in.Identity),
			"purpose":  purpose,
		},
	}
	if c.cfg.Submit == nil || !c.cfg.Submit(msg) {
		c.requestRejected.Add(1)
		c.audit("ext_request_rejected", "busy source="+bus.OneLine(source, sourceLimit))
		writeErr(w, http.StatusServiceUnavailable, "总线繁忙或正在停机，请求被丢弃")
		return
	}
	c.requestAccepted.Add(1)
	c.announceStart(id, source, strings.TrimSpace(in.Identity), purpose, text)
	c.audit("ext_request", fmt.Sprintf("source=%s identity=%s purpose=%s session=%s id=%s",
		bus.OneLine(source, sourceLimit), bus.OneLine(strings.TrimSpace(in.Identity), identLimit),
		bus.OneLine(purpose, purposeLim), session, id))
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":  "accepted",
		"session": session,
		"id":      id,
	})
}

// announceStart 在受理后立刻告诉用户「有外部请求、准备做什么」。
// 不消耗模型回合：字段本来就是结构化的，直接渲染即可。
func (c *Channel) announceStart(id, source, identity, purpose, text string) {
	if c.cfg.Notice == nil {
		return
	}
	var b strings.Builder
	b.WriteString("收到外部请求 ")
	b.WriteString(id)
	b.WriteString("\n来源: ")
	b.WriteString(bus.OneLine(source, 64))
	if identity != "" {
		b.WriteString("\n身份: ")
		b.WriteString(bus.OneLine(identity, 64))
	}
	b.WriteString("\n目的: ")
	b.WriteString(bus.OneLine(purpose, 200))
	b.WriteString("\n内容: ")
	b.WriteString(bus.OneLine(text, 300))
	b.WriteString("\n\n开始处理，完成后回报结果。")
	c.cfg.Notice(context.WithoutCancel(context.Background()), b.String())
}

func (c *Channel) queryRequest(w http.ResponseWriter, r *http.Request) {
	session := strings.TrimSpace(r.URL.Query().Get("session"))
	if session == "" {
		c.rejectRequest(w, errors.New("缺少 session（提交时返回的会话 ID）"))
		return
	}
	if c.cfg.Query == nil {
		writeErr(w, http.StatusServiceUnavailable, "查询未接通")
		return
	}
	st, ok := c.cfg.Query(session)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"status": "unknown", "session": session,
			"message": "会话不存在或尚无回合结果",
		})
		return
	}
	if st.Running {
		writeJSON(w, http.StatusOK, map[string]any{"status": "running", "session": session})
		return
	}
	status := "done"
	if st.Failed {
		status = "error"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      status,
		"session":     session,
		"reply":       st.Reply,
		"finished_at": st.FinishedAt,
	})
}

func (c *Channel) rejectRequest(w http.ResponseWriter, err error) {
	c.requestRejected.Add(1)
	writeErr(w, http.StatusBadRequest, err.Error())
}

func formatRequest(source, identity, purpose, text string) string {
	var b strings.Builder
	b.WriteString("[外部服务请求] 来源=")
	b.WriteString(source)
	if identity != "" {
		b.WriteString(" 身份=")
		b.WriteString(identity)
	}
	b.WriteString(" 目的=")
	b.WriteString(purpose)
	b.WriteString("\n")
	b.WriteString(text)
	return b.String()
}

func decodeBody(w http.ResponseWriter, r *http.Request, limit int, dst any) error {
	body := http.MaxBytesReader(w, r.Body, int64(limit))
	raw, err := io.ReadAll(body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return fmt.Errorf("请求体超过上限 %d 字节", limit)
		}
		return fmt.Errorf("读取请求体失败: %w", err)
	}
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.Contains(ct, "json") || (ct == "" && strings.HasPrefix(strings.TrimSpace(string(raw)), "{")) {
		if err := json.Unmarshal(raw, dst); err != nil {
			return fmt.Errorf("JSON 解析失败: %w", err)
		}
		return nil
	}
	switch v := dst.(type) {
	case *requestBody:
		v.Text = string(raw)
	case *NoticeBody:
		v.Body = string(raw)
	default:
		return errors.New("不支持的请求体类型")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"status": "rejected", "message": msg})
}

func (c *Channel) audit(kind, text string) {
	if c.cfg.Audit == nil {
		return
	}
	c.cfg.Audit(kind, text)
}

func (c *Channel) logf(format string, args ...any) {
	if c.cfg.Logf == nil {
		return
	}
	c.cfg.Logf(format, args...)
}

func (c *Channel) recordErr(err error) {
	if err == nil {
		return
	}
	c.errMu.Lock()
	c.lastErr = err.Error()
	c.lastErrAt = time.Now()
	c.errMu.Unlock()
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

var idSeq atomic.Uint64

func newID() string {
	return fmt.Sprintf("req-%d-%s-%d", os.Getpid(), time.Now().Format("150405.000"), idSeq.Add(1))
}
