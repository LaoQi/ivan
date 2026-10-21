package qqbot

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	Connected     bool      `json:"connected"`
	SessionID     string    `json:"session_id,omitempty"`
	Messages      uint64    `json:"messages"`
	Duplicates    uint64    `json:"duplicates"`
	Reconnects    uint64    `json:"reconnects"`
	Sent          uint64    `json:"sent"`
	MarkdownSent  uint64    `json:"markdown_sent,omitempty"`
	SendErrors    uint64    `json:"send_errors"`
	LastError     string    `json:"last_error,omitempty"`
	LastErrorAt   time.Time `json:"last_error_at,omitempty"`
	LastHeartbeat time.Time `json:"last_heartbeat,omitempty"`
}

type Client struct {
	cfg     Config
	tokens  *tokenSource
	dedup   *msgCache
	handler Handler

	presign *http.Client

	mu        sync.Mutex
	ws        *wsConn
	sessionID string
	lastAck   time.Time
	lastErr   error
	lastErrAt time.Time

	reconnects atomic.Uint64
	messages   atomic.Uint64
	duplicates atomic.Uint64
	sent       atomic.Uint64
	markdown   atomic.Uint64
	sendErrors atomic.Uint64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config) (*Client, error) {
	if cfg.AppID == "" || cfg.AppSecret == "" {
		return nil, errors.New("qqbot 需要 app_id 与 app_secret")
	}
	cfg = cfg.withDefaults()
	c := &Client{cfg: cfg, dedup: newMsgCache(5*time.Minute, 4096)}
	c.tokens = newTokenSource(cfg)
	return c, nil
}

func (c *Client) Start(ctx context.Context, h Handler) error {
	if h == nil {
		return errors.New("qqbot 缺少事件处理器")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.handler = h
	if _, err := c.tokens.Token(ctx); err != nil {
		c.cfg.Logf("qqbot 初始取 token 失败（后台持续重试）: %v", err)
		c.recordError(err)
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.tokens.keepFresh(runCtx)
	}()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.gatewayLoop(runCtx)
	}()
	return nil
}

func (c *Client) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Lock()
	w := c.ws
	c.mu.Unlock()
	if w != nil {
		_ = w.c.Close()
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

func (c *Client) recordError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	c.lastErr = err
	c.lastErrAt = time.Now()
	c.mu.Unlock()
}

func (c *Client) Stats() Stats {
	c.mu.Lock()
	connected := c.ws != nil
	sid := c.sessionID
	lastErr := c.lastErr
	lastErrAt := c.lastErrAt
	lastAck := c.lastAck
	c.mu.Unlock()
	st := Stats{
		Connected:     connected,
		SessionID:     sid,
		Messages:      c.messages.Load(),
		Duplicates:    c.duplicates.Load(),
		Reconnects:    c.reconnects.Load(),
		Sent:          c.sent.Load(),
		MarkdownSent:  c.markdown.Load(),
		SendErrors:    c.sendErrors.Load(),
		LastHeartbeat: lastAck,
	}
	if lastErr != nil {
		st.LastError = lastErr.Error()
		st.LastErrorAt = lastErrAt
	}
	return st
}

func (c *Client) countSent()      { c.sent.Add(1) }
func (c *Client) countMarkdown()  { c.markdown.Add(1) }
func (c *Client) countSendErr()   { c.sendErrors.Add(1) }
func (c *Client) countMessage()   { c.messages.Add(1) }
func (c *Client) countDuplicate() { c.duplicates.Add(1) }
