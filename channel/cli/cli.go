package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
)

const (
	Name           = "cli"
	DefaultSession = "cli:default"

	OpSend   = "send"
	OpStatus = "status"
	OpPing   = "ping"
	OpReply  = "reply"
	OpError  = "error"
	OpAck    = "ack"
	OpPong   = "pong"

	OpJobsList   = "jobs/list"
	OpJobsRemove = "jobs/remove"
	OpJobsRun    = "jobs/run"
	OpPause      = "pause"
	OpCancel     = "cancel"
	OpResume     = "resume"

	maxFrame = 1 << 20
)

var ErrNotRunning = errors.New("daemon 未运行")

type Frame struct {
	ID      string          `json:"id,omitempty"`
	Op      string          `json:"op"`
	Text    string          `json:"text,omitempty"`
	Session string          `json:"session,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type Control func(op, arg string) (any, error)

type Socket struct {
	path    string
	control Control

	mu      sync.Mutex
	bus     *bus.Bus
	ln      net.Listener
	conns   map[*conn]struct{}
	pending map[string]*conn
	closed  bool
	wg      sync.WaitGroup
}

func NewSocket(path string, control Control) *Socket {
	return &Socket{
		path:    path,
		control: control,
		conns:   map[*conn]struct{}{},
		pending: map[string]*conn{},
	}
}

func (s *Socket) Name() string { return Name }

func (s *Socket) MaxLen() int { return 0 }

func (s *Socket) Path() string { return s.path }

func (s *Socket) Start(ctx context.Context, b *bus.Bus) error {
	if err := prepareSocket(s.path); err != nil {
		return err
	}
	ln, err := net.Listen("unix", s.path)
	if err != nil {
		return err
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		ln.Close()
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return errors.New("通道已停止")
	}
	s.bus = b
	s.ln = ln
	s.mu.Unlock()
	go s.accept()
	return nil
}

func (s *Socket) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.ln
	conns := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	if ln != nil {
		ln.Close()
	}
	for _, c := range conns {
		c.c.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	os.Remove(s.path)
	return nil
}

func (s *Socket) Send(ctx context.Context, msg bus.OutboundMessage) error {
	s.mu.Lock()
	cn := s.pending[msg.ChatID]
	if cn != nil {
		delete(s.pending, msg.ChatID)
	}
	s.mu.Unlock()
	if cn == nil {
		return fmt.Errorf("%w: cli 无对应请求 %q", channel.ErrUnroutable, msg.ChatID)
	}
	op := OpReply
	if msg.Kind == bus.KindError {
		op = OpError
	}
	return cn.write(Frame{ID: msg.ChatID, Op: op, Text: msg.Content})
}

func (s *Socket) accept() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.serve(c)
	}
}

type conn struct {
	s   *Socket
	c   net.Conn
	wmu sync.Mutex
}

func (cn *conn) write(f Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	cn.wmu.Lock()
	defer cn.wmu.Unlock()
	_, err = cn.c.Write(b)
	return err
}

func (s *Socket) serve(c net.Conn) {
	defer s.wg.Done()
	cn := &conn{s: s, c: c}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		c.Close()
		return
	}
	s.conns[cn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, cn)
		for id, pc := range s.pending {
			if pc == cn {
				delete(s.pending, id)
			}
		}
		s.mu.Unlock()
		c.Close()
	}()
	sc := bufio.NewScanner(c)
	sc.Buffer(make([]byte, 0, 64*1024), maxFrame)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var f Frame
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			_ = cn.write(Frame{Op: OpError, Error: "帧解析失败: " + err.Error()})
			continue
		}
		s.handle(cn, f)
	}
}

func (s *Socket) handle(cn *conn, f Frame) {
	switch f.Op {
	case OpSend:
		s.handleSend(cn, f)
		return
	case OpPing:
		_ = cn.write(Frame{ID: f.ID, Op: OpPong})
		return
	}
	raw, err := s.callControl(f.Op, f.Text)
	if err != nil {
		_ = cn.write(Frame{ID: f.ID, Op: OpError, Error: err.Error()})
		return
	}
	_ = cn.write(Frame{ID: f.ID, Op: f.Op, Data: raw})
}

func (s *Socket) handleSend(cn *conn, f Frame) {
	text := strings.TrimSpace(f.Text)
	if text == "" {
		_ = cn.write(Frame{ID: f.ID, Op: OpError, Error: "text 为空"})
		return
	}
	id := strings.TrimSpace(f.ID)
	if id == "" {
		id = NewID()
	}
	s.mu.Lock()
	if s.closed || s.bus == nil {
		s.mu.Unlock()
		_ = cn.write(Frame{ID: id, Op: OpError, Error: "通道正在停止，消息未受理"})
		return
	}
	s.pending[id] = cn
	b := s.bus
	s.mu.Unlock()
	msg := bus.InboundMessage{
		Channel:    Name,
		ChatID:     id,
		ChatType:   bus.ChatDirect,
		SenderID:   "local",
		SenderName: "cli",
		MessageID:  id,
		Content:    f.Text,
		SessionKey: NormalizeSession(f.Session),
		ReceivedAt: time.Now(),
	}
	if !b.Publish(msg) {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		_ = cn.write(Frame{ID: id, Op: OpError, Error: "总线繁忙或正在停机，消息被丢弃"})
		return
	}
	_ = cn.write(Frame{ID: id, Op: OpAck})
}

func (s *Socket) callControl(op, arg string) (json.RawMessage, error) {
	if s.control == nil {
		return nil, errors.New("不支持的控制指令: " + op)
	}
	v, err := s.control(op, arg)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func prepareSocket(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	c, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err == nil {
		c.Close()
		return fmt.Errorf("socket 已被占用: %s（daemon 可能已在运行）", path)
	}
	return os.Remove(path)
}

func NormalizeSession(session string) string {
	s := strings.TrimSpace(session)
	switch {
	case s == "":
		return DefaultSession
	case strings.Contains(s, ":"):
		return s
	default:
		return Name + ":" + s
	}
}

var idSeq atomic.Uint64

func NewID() string {
	return fmt.Sprintf("%d-%s-%d", os.Getpid(), time.Now().Format("150405.000"), idSeq.Add(1))
}

type Client struct {
	c net.Conn
	r *bufio.Reader
}

func Dial(path string, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	c, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return nil, fmt.Errorf("%w（socket %s: %v）", ErrNotRunning, path, err)
	}
	return &Client{c: c, r: bufio.NewReader(c)}, nil
}

func (c *Client) Do(f Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = c.c.Write(b)
	return err
}

func (c *Client) Read(timeout time.Duration) (Frame, error) {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if err := c.c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return Frame{}, err
	}
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return Frame{}, err
	}
	var f Frame
	if err := json.Unmarshal(line, &f); err != nil {
		return Frame{}, fmt.Errorf("响应解析失败: %w", err)
	}
	return f, nil
}

func (c *Client) Close() error { return c.c.Close() }

func Send(path, text, session string, wait time.Duration) (string, error) {
	cl, err := Dial(path, wait)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	id := NewID()
	if err := cl.Do(Frame{ID: id, Op: OpSend, Text: text, Session: session}); err != nil {
		return "", err
	}
	if wait <= 0 {
		return "", nil
	}
	deadline := time.Now().Add(wait)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return "", errors.New("等待回复超时")
		}
		f, err := cl.Read(left)
		if err != nil {
			return "", err
		}
		switch f.Op {
		case OpAck:
			continue
		case OpReply:
			return f.Text, nil
		case OpError:
			return "", errors.New(f.Error)
		default:
			continue
		}
	}
}

func Call(path, op, arg string, timeout time.Duration) (json.RawMessage, error) {
	cl, err := Dial(path, timeout)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	if err := cl.Do(Frame{ID: NewID(), Op: op, Text: arg}); err != nil {
		return nil, err
	}
	for i := 0; i < 4; i++ {
		f, err := cl.Read(timeout)
		if err != nil {
			return nil, err
		}
		if f.Op == op {
			return f.Data, nil
		}
		if f.Op == OpError {
			return nil, errors.New(f.Error)
		}
	}
	return nil, fmt.Errorf("未收到 %s 响应", op)
}

func Status(path string, timeout time.Duration) (json.RawMessage, error) {
	return Call(path, OpStatus, "", timeout)
}

func Ping(path string, timeout time.Duration) error {
	cl, err := Dial(path, timeout)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := cl.Do(Frame{ID: NewID(), Op: OpPing}); err != nil {
		return err
	}
	f, err := cl.Read(timeout)
	if err != nil {
		return err
	}
	if f.Op != OpPong {
		return fmt.Errorf("期望 pong，收到 %s", f.Op)
	}
	return nil
}
