package channel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/LaoQi/ivan/bus"
)

const DefaultMaxLen = 1200

var ErrUnroutable = errors.New("出站消息没有投递目标")

type Channel interface {
	Name() string
	Start(ctx context.Context, b *bus.Bus) error
	Stop(ctx context.Context) error
	Send(ctx context.Context, msg bus.OutboundMessage) error
}

type Limiter interface {
	MaxLen() int
}

type Status struct {
	Name        string    `json:"name"`
	Running     bool      `json:"running"`
	MaxLen      int       `json:"max_len"`
	Sent        int       `json:"sent"`
	Failed      int       `json:"failed"`
	Unmatched   int       `json:"unmatched"`
	LastError   string    `json:"last_error,omitempty"`
	LastErrorAt time.Time `json:"last_error_at,omitempty"`
}

type chanStat struct {
	running   bool
	maxLen    int
	sent      int
	failed    int
	unmatched int
	lastErr   string
	lastErrAt time.Time
}

type Manager struct {
	bus         *bus.Bus
	retries     int
	retryDelay  time.Duration
	sendTimeout time.Duration

	mu      sync.Mutex
	order   []string
	chans   map[string]Channel
	status  map[string]*chanStat
	drained chan struct{}
	once    sync.Once
}

type Option func(*Manager)

func WithRetries(n int) Option {
	return func(m *Manager) {
		if n > 0 {
			m.retries = n
		}
	}
}

func WithRetryDelay(d time.Duration) Option {
	return func(m *Manager) { m.retryDelay = d }
}

func WithSendTimeout(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.sendTimeout = d
		}
	}
}

func New(b *bus.Bus, opts ...Option) *Manager {
	m := &Manager{
		bus:         b,
		retries:     2,
		retryDelay:  200 * time.Millisecond,
		sendTimeout: 10 * time.Second,
		chans:       map[string]Channel{},
		status:      map[string]*chanStat{},
		drained:     make(chan struct{}),
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

func (m *Manager) Register(c Channel) error {
	name := strings.TrimSpace(c.Name())
	if name == "" {
		return errors.New("通道名不能为空")
	}
	maxLen := DefaultMaxLen
	if l, ok := c.(Limiter); ok {
		maxLen = l.MaxLen()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.chans[name]; dup {
		return fmt.Errorf("通道重复注册: %s", name)
	}
	m.chans[name] = c
	m.status[name] = &chanStat{maxLen: maxLen}
	m.order = append(m.order, name)
	return nil
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	order := append([]string(nil), m.order...)
	m.mu.Unlock()
	started := make([]string, 0, len(order))
	for _, name := range order {
		c := m.chans[name]
		if err := c.Start(ctx, m.bus); err != nil {
			for i := len(started) - 1; i >= 0; i-- {
				_ = m.chans[started[i]].Stop(context.Background())
			}
			return fmt.Errorf("启动通道 %s 失败: %w", name, err)
		}
		m.mu.Lock()
		m.status[name].running = true
		m.mu.Unlock()
		started = append(started, name)
	}
	return nil
}

func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	order := append([]string(nil), m.order...)
	m.mu.Unlock()
	var firstErr error
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		if err := m.chans[name].Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("停止通道 %s 失败: %w", name, err)
		}
		m.mu.Lock()
		m.status[name].running = false
		m.mu.Unlock()
	}
	return firstErr
}

func (m *Manager) Consume(ctx context.Context) {
	defer m.once.Do(func() { close(m.drained) })
	for {
		select {
		case msg, ok := <-m.bus.Outbound():
			if !ok {
				return
			}
			m.Route(ctx, msg)
		case <-ctx.Done():
			m.drain()
			return
		}
	}
}

func (m *Manager) drain() {
	for {
		select {
		case msg, ok := <-m.bus.Outbound():
			if !ok {
				return
			}
			m.Route(context.Background(), msg)
		default:
			return
		}
	}
}

func (m *Manager) Drained() <-chan struct{} { return m.drained }

func (m *Manager) Route(ctx context.Context, msg bus.OutboundMessage) error {
	name := strings.TrimSpace(msg.Channel)
	m.mu.Lock()
	c := m.chans[name]
	st := m.status[name]
	m.mu.Unlock()
	if c == nil || st == nil {
		return fmt.Errorf("%w: 通道未注册 %q", ErrUnroutable, msg.Channel)
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.sendTimeout)
	defer cancel()
	parts := Split(Sanitize(msg.Content), st.maxLen)
	var (
		firstErr error
		failed   int
	)
	for _, part := range parts {
		out := msg
		out.Content = part
		err := m.deliver(dctx, c, out)
		switch {
		case err == nil:
			m.count(name, func(s *chanStat) { s.sent++ })
		case errors.Is(err, ErrUnroutable):
			m.count(name, func(s *chanStat) { s.unmatched++ })
		default:
			failed++
			m.count(name, func(s *chanStat) {
				s.failed++
				s.lastErr = err.Error()
				s.lastErrAt = time.Now()
			})
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if failed > 0 {
		return firstErr
	}
	return nil
}

func (m *Manager) deliver(ctx context.Context, c Channel, msg bus.OutboundMessage) error {
	attempts := m.retries
	var err error
	for i := 0; i < attempts; i++ {
		err = c.Send(ctx, msg)
		if err == nil || errors.Is(err, ErrUnroutable) {
			return err
		}
		if i+1 < attempts && m.retryDelay > 0 {
			select {
			case <-time.After(m.retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return err
}

func (m *Manager) count(name string, fn func(*chanStat)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st := m.status[name]; st != nil {
		fn(st)
	}
}

func (m *Manager) Status() []Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Status, 0, len(m.order))
	for _, name := range m.order {
		st := m.status[name]
		out = append(out, Status{
			Name:        name,
			Running:     st.running,
			MaxLen:      st.maxLen,
			Sent:        st.sent,
			Failed:      st.failed,
			Unmatched:   st.unmatched,
			LastError:   st.lastErr,
			LastErrorAt: st.lastErrAt,
		})
	}
	return out
}

func Sanitize(s string) string {
	if !strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 && r != '\n' && r != '\t' }) && !strings.ContainsRune(s, '\r') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '\r':
			continue
		case r < 0x20 && r != '\n' && r != '\t':
			continue
		case r == 0x7f:
			continue
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func Split(text string, limit int) []string {
	if limit <= 0 {
		return []string{text}
	}
	runes := []rune(text)
	if len(runes) <= limit {
		return []string{text}
	}
	parts := make([]string, 0, len(runes)/limit+1)
	for len(runes) > 0 {
		if len(runes) <= limit {
			parts = append(parts, string(runes))
			break
		}
		cut := cutPoint(runes[:limit])
		if cut <= 0 || cut > limit {
			cut = limit
		}
		parts = append(parts, string(runes[:cut]))
		rest := runes[cut:]
		for len(rest) > 0 && (rest[0] == '\n' || rest[0] == ' ') {
			rest = rest[1:]
		}
		runes = rest
	}
	return parts
}

func cutPoint(window []rune) int {
	if i := lastIndexFold(window, []rune("\n\n")); i > 0 {
		return i + 2
	}
	for i := len(window) - 1; i > 0; i-- {
		if window[i] == '\n' {
			return i + 1
		}
	}
	half := len(window) / 2
	for _, sep := range []rune{'。', '！', '？', '．', '；', '.', '!', '?', ';'} {
		for i := len(window) - 1; i >= half; i-- {
			if window[i] == sep {
				return i + 1
			}
		}
	}
	return len(window)
}

func lastIndexFold(window, sub []rune) int {
	if len(sub) == 0 || len(window) < len(sub) {
		return -1
	}
	for i := len(window) - len(sub); i > 0; i-- {
		match := true
		for j, r := range sub {
			if window[i+j] != r {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func RuneLen(s string) int { return utf8.RuneCountInString(s) }
