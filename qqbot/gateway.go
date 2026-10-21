package qqbot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	opDispatch       = 0
	opHeartbeat      = 1
	opIdentify       = 2
	opResume         = 6
	opReconnect      = 7
	opInvalidSession = 9
	opHello          = 10
	opHeartbeatAck   = 11
)

type wsConn struct {
	c   *websocket.Conn
	mu  sync.Mutex
	seq int
	muS sync.Mutex
}

func (w *wsConn) writeJSON(v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.c.WriteJSON(v)
}

func (w *wsConn) setSeq(s int) {
	w.muS.Lock()
	w.seq = s
	w.muS.Unlock()
}

func (w *wsConn) currentSeq() int {
	w.muS.Lock()
	defer w.muS.Unlock()
	return w.seq
}

type session struct {
	id  string
	seq int
}

func (c *Client) gatewayLoop(ctx context.Context) {
	backoff := c.cfg.MinBackoff
	var sess *session
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.connectAndServe(ctx, &sess)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.cfg.Logf("qqbot 网关连接中断: %v", err)
			c.recordError(err)
			c.reportError(err)
			c.reconnects.Add(1)
		}
		wait := jitter(backoff)
		if !sleepCtx(ctx, wait) {
			return
		}
		backoff = nextBackoff(backoff, c.cfg.MaxBackoff)
		if sess == nil || !sessResumable(err) {
			backoff = c.cfg.MinBackoff
		}
	}
}

func sessResumable(err error) bool {
	var re *reconnectError
	return errors.As(err, &re) && !re.identify
}

type reconnectError struct {
	identify bool
	cause    error
}

func (e *reconnectError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return "需要重新识别"
}

func (c *Client) connectAndServe(ctx context.Context, sess **session) error {
	url, err := c.gatewayURL(ctx)
	if err != nil {
		return err
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("qqbot 连接网关失败: %w", err)
	}
	w := &wsConn{c: conn}
	done := make(chan struct{})
	var closeOnce sync.Once
	closeConn := func() {
		closeOnce.Do(func() {
			_ = conn.Close()
			close(done)
		})
	}
	defer closeConn()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	gatewayErr := make(chan error, 1)
	fail := func(err error) {
		select {
		case gatewayErr <- err:
		default:
		}
		closeConn()
	}

	if err := c.handshake(ctx, w, sess); err != nil {
		return err
	}
	go c.readLoop(connCtx, w, sess, fail)
	c.setConnected(w)
	defer c.clearConnected(w)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-gatewayErr:
		return err
	}
}

func (c *Client) setConnected(w *wsConn) {
	c.mu.Lock()
	c.ws = w
	c.lastAck = time.Now()
	c.mu.Unlock()
}

func (c *Client) clearConnected(w *wsConn) {
	c.mu.Lock()
	if c.ws == w {
		c.ws = nil
	}
	c.mu.Unlock()
}

func (c *Client) handshake(ctx context.Context, w *wsConn, sess **session) error {
	_ = w.c.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, raw, err := w.c.ReadMessage()
	if err != nil {
		return fmt.Errorf("qqbot 读取 hello 失败: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("qqbot hello 解析失败: %w", err)
	}
	if env.Op != opHello {
		return fmt.Errorf("qqbot 期望 op10 hello，收到 op%d", env.Op)
	}
	var hello helloPayload
	_ = json.Unmarshal(env.D, &hello)
	interval := hello.HeartbeatInterval
	if interval <= 0 {
		interval = 30000
	}

	token, err := c.tokens.Token(ctx)
	if err != nil {
		return err
	}
	token = wsToken(token)
	if *sess != nil {
		frame := envelope{Op: opResume, D: mustJSON(resumeData{Token: token, SessionID: (*sess).id, Seq: (*sess).seq})}
		if err := w.writeJSON(frame); err != nil {
			return fmt.Errorf("qqbot 发送 resume 失败: %w", err)
		}
		c.cfg.Logf("qqbot 已发送 resume session=%s seq=%d", shortID((*sess).id), (*sess).seq)
	} else {
		frame := envelope{Op: opIdentify, D: mustJSON(identifyData{
			Token:      token,
			Intents:    c.cfg.Intents,
			Shard:      c.cfg.Shard,
			Properties: c.identifyProperties(),
		})}
		if err := w.writeJSON(frame); err != nil {
			return fmt.Errorf("qqbot 发送 identify 失败: %w", err)
		}
		props := c.identifyProperties()
		c.cfg.Logf("qqbot 已发送 identify intents=%d shard=%v browser=%s os=%s",
			c.cfg.Intents, c.cfg.Shard, props["$browser"], props["$os"])
	}
	_ = w.c.SetReadDeadline(time.Time{})

	go c.heartbeatLoop(ctx, w, time.Duration(interval)*time.Millisecond)
	return nil
}

func (c *Client) readLoop(ctx context.Context, w *wsConn, sess **session, fail func(error)) {
	for {
		if ctx.Err() != nil {
			return
		}
		_ = w.c.SetReadDeadline(time.Now().Add(120 * time.Second))
		_, raw, err := w.c.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				fail(fmt.Errorf("qqbot 读取网关消息失败: %w", err))
			}
			return
		}
		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			c.cfg.Logf("qqbot 无法解析网关帧: %v", err)
			continue
		}
		if env.S != nil {
			w.setSeq(*env.S)
			if *sess != nil {
				(*sess).seq = *env.S
			}
		}
		switch env.Op {
		case opDispatch:
			c.handleDispatch(w, sess, env)
		case opHeartbeatAck:
			c.markAck()
		case opReconnect:
			c.cfg.Logf("qqbot 收到 op7 重连指令")
			fail(&reconnectError{identify: true, cause: errors.New("服务端要求重连")})
			return
		case opInvalidSession:
			c.cfg.Logf("qqbot 收到 op9 会话失效")
			*sess = nil
			fail(&reconnectError{identify: true, cause: errors.New("会话失效")})
			return
		case opHello:
			c.cfg.Logf("qqbot 重复收到 op10 hello")
		default:
			c.cfg.Logf("qqbot 未知 opcode %d", env.Op)
		}
	}
}

func (c *Client) handleDispatch(w *wsConn, sess **session, env envelope) {
	switch env.T {
	case EventReady:
		var rp readyPayload
		if err := json.Unmarshal(env.D, &rp); err != nil {
			c.cfg.Logf("qqbot READY 解析失败: %v", err)
			return
		}
		*sess = &session{id: rp.SessionID, seq: w.currentSeq()}
		c.mu.Lock()
		c.sessionID = rp.SessionID
		c.mu.Unlock()
		c.cfg.Logf("qqbot 已就绪 session=%s user=%s shard=%v", shortID(rp.SessionID), rp.User.Username, rp.Shard)
		c.handler.OnReady(ReadyInfo{SessionID: rp.SessionID, Shard: rp.Shard, UserID: rp.User.ID, Username: rp.User.Username})
	case EventResumed:
		if *sess != nil {
			(*sess).seq = w.currentSeq()
		}
		c.cfg.Logf("qqbot 会话已恢复（事件补投由去重兜底）")
	case EventC2CMessage, EventGroupAt:
		var rp rawPayload
		if err := json.Unmarshal(env.D, &rp); err != nil {
			c.cfg.Logf("qqbot 消息事件解析失败: %v", err)
			return
		}
		if c.dedup.Seen(rp.ID) {
			c.countDuplicate()
			return
		}
		c.countMessage()
		ev := MessageEvent{
			Kind:        env.T,
			ID:          rp.ID,
			Content:     rp.Content,
			Timestamp:   rp.Timestamp,
			Author:      rp.Author,
			GroupOpenID: rp.GroupOpenID,
			Attachments: rp.Attachments,
			Raw:         env.D,
			ReceivedAt:  time.Now(),
		}
		if ev.Kind == EventGroupAt {
			ev.UserOpenID = rp.Author.MemberOpenID
		} else {
			ev.UserOpenID = rp.Author.UserOpenID
		}
		c.handler.OnMessage(ev)
	default:
		c.cfg.Logf("qqbot 忽略未处理事件 %s", env.T)
	}
}

func (c *Client) heartbeatLoop(ctx context.Context, w *wsConn, interval time.Duration) {
	send := interval * 4 / 5
	if send < time.Second {
		send = time.Second
	}
	timer := time.NewTimer(send)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		c.mu.Lock()
		lastAck := c.lastAck
		c.mu.Unlock()
		if !lastAck.IsZero() && time.Since(lastAck) > 2*send {
			c.cfg.Logf("qqbot 心跳未确认，判定连接僵死，主动重连")
			_ = w.c.Close()
			return
		}
		frame := envelope{Op: opHeartbeat, D: mustJSON(w.currentSeq())}
		if err := w.writeJSON(frame); err != nil {
			c.cfg.Logf("qqbot 发送心跳失败: %v", err)
			_ = w.c.Close()
			return
		}
		timer.Reset(send)
	}
}

func (c *Client) markAck() {
	c.mu.Lock()
	c.lastAck = time.Now()
	c.mu.Unlock()
}

func (c *Client) reportError(err error) {
	if c.handler != nil {
		c.handler.OnError(err)
	}
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return raw
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	delta := time.Duration(rand.Int63n(int64(d/2) + 1))
	return d/2 + delta
}

func (c *Client) identifyProperties() map[string]string {
	browser := "ivan"
	if fields := strings.Fields(c.cfg.UserAgent); len(fields) > 0 {
		browser = fields[0]
	}
	return map[string]string{
		"$os":      runtime.GOOS,
		"$browser": browser,
		"$device":  "ivan",
	}
}

const wsTokenPrefix = "QQBot "

func wsToken(token string) string {
	if token == "" || strings.HasPrefix(token, wsTokenPrefix) {
		return token
	}
	return wsTokenPrefix + token
}

// shortID 按字符（rune）截断标识用于日志，避免多字节字符被切出乱码。
func shortID(id string) string {
	r := []rune(id)
	if len(r) <= 8 {
		return id
	}
	return string(r[:8])
}
