package channel

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/LaoQi/ivan/bus"
)

// StreamHandle 由通道实现：同一条消息可被多次原位更新（QQ 流式消息）。
type StreamHandle interface {
	Update(ctx context.Context, text string, done bool) error
}

// StreamOpener 是通道的可选能力：把一条出站消息开成可更新的流式消息。
// 未实现的通道自动退化——流式更新丢弃，终局文本按普通消息投递。
type StreamOpener interface {
	StreamOpen(ctx context.Context, msg bus.OutboundMessage) (StreamHandle, error)
}

// Typist 是通道的可选能力：临时状态（如 QQ 的“输入中”），不产生消息条数。
type Typist interface {
	Typing(ctx context.Context, msg bus.OutboundMessage) error
}

// streamEllipsis 是流式正文超上限时的收尾标记；余下内容作为普通消息补发。
const streamEllipsis = "…"

type streamRef struct {
	// lock 保证同一条逻辑消息的更新串行执行（QQ 流式接口要求分片有序、前缀一致）。
	lock sync.Mutex

	src     bus.OutboundMessage
	limit   int
	handle  StreamHandle
	opening bool
	visible string
	done    bool
	skip    bool
	refs    int
}

type streamTable struct {
	mu sync.Mutex
	m  map[string]*streamRef
}

func newStreamTable() *streamTable { return &streamTable{m: map[string]*streamRef{}} }

func streamKey(channel, id string) string { return channel + "\x00" + id }

func (t *streamTable) hold(channel string, msg bus.OutboundMessage, limit int) (*streamRef, bool) {
	k := streamKey(channel, msg.Stream)
	t.mu.Lock()
	defer t.mu.Unlock()
	ref := t.m[k]
	if ref == nil {
		ref = &streamRef{src: msg, limit: limit}
		t.m[k] = ref
		ref.refs = 1
		return ref, true
	}
	ref.refs++
	ref.src = msg
	return ref, false
}

// beginOpen 预约一次开流：已开成或在开的过程中返回 false（开流是幂等的，失败者跳过即可）。
func (t *streamTable) beginOpen(channel, id string) (*streamRef, bool) {
	k := streamKey(channel, id)
	t.mu.Lock()
	defer t.mu.Unlock()
	ref := t.m[k]
	if ref == nil || ref.handle != nil || ref.opening || ref.done || ref.skip {
		return ref, false
	}
	ref.opening = true
	return ref, true
}

func (t *streamTable) opened(channel, id string, h StreamHandle, text string, err error) *streamRef {
	k := streamKey(channel, id)
	t.mu.Lock()
	defer t.mu.Unlock()
	ref := t.m[k]
	if ref == nil {
		return nil
	}
	ref.opening = false
	if err != nil {
		ref.skip = true
		return ref
	}
	ref.handle = h
	ref.visible = text
	return ref
}

// drop 把条目从表里摘除（连同引用计数归零），供终局定稿独占使用。
func (t *streamTable) drop(channel, id string) *streamRef {
	k := streamKey(channel, id)
	t.mu.Lock()
	defer t.mu.Unlock()
	ref := t.m[k]
	if ref == nil {
		return nil
	}
	delete(t.m, k)
	ref.refs = 0
	return ref
}

func (t *streamTable) release(channel, id string) {
	k := streamKey(channel, id)
	t.mu.Lock()
	defer t.mu.Unlock()
	ref := t.m[k]
	if ref == nil {
		return
	}
	ref.refs--
	if ref.refs <= 0 && (ref.done || ref.skip) {
		delete(t.m, k)
	}
}

func (t *streamTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}

// AbortStreams 停机时收尾仍在更新的流式消息（尽力而为）。
func (m *Manager) AbortStreams() {
	m.streams.mu.Lock()
	refs := make([]*streamRef, 0, len(m.streams.m))
	for _, ref := range m.streams.m {
		refs = append(refs, ref)
	}
	m.streams.m = map[string]*streamRef{}
	m.streams.mu.Unlock()
	for _, ref := range refs {
		if ref.handle == nil {
			continue
		}
		_ = ref.handle.Update(context.Background(), ref.visible, true)
	}
}

func (m *Manager) routeTyping(ctx context.Context, name string, c Channel, msg bus.OutboundMessage) error {
	t, ok := c.(Typist)
	if !ok {
		m.bump(name, func(s *chanStat) { s.streamSkip++ })
		return nil
	}
	dctx, cancel := m.streamCtx(context.WithoutCancel(ctx))
	defer cancel()
	if err := t.Typing(dctx, msg); err != nil {
		m.bump(name, func(s *chanStat) { s.typingFail++ })
		m.recordStreamError(name, err)
		return nil
	}
	m.bump(name, func(s *chanStat) { s.typing++ })
	return nil
}

// routeStream 处理流式更新（Stream 非空、Final 为假）与终局（Final 为真）。
//
// QQ 的流式接口只允许“在上次已下发内容之后追加”，所以流式消息里承载的是正文本身，
// 不能塞“处理中…”这类会被改写掉的状态文字（那属于 Typist 的职责）。
func (m *Manager) routeStream(ctx context.Context, name string, c Channel, msg bus.OutboundMessage) error {
	base := context.WithoutCancel(ctx)
	opener, _ := c.(StreamOpener)
	text := Sanitize(msg.Content)
	if msg.Final {
		return m.finishStream(base, name, c, opener, msg, text)
	}
	if opener == nil {
		m.bump(name, func(s *chanStat) { s.streamSkip++ })
		return nil
	}
	if strings.TrimSpace(text) == "" {
		return nil
	}
	limit := m.streamLimit(c)
	ref, _ := m.streams.hold(name, msg, limit)
	defer m.streams.release(name, msg.Stream)
	ref.lock.Lock()
	defer ref.lock.Unlock()
	if ref.done || ref.skip {
		return nil
	}
	if ref.handle != nil {
		return m.updateStream(base, name, ref, text)
	}
	opening, ok := m.streams.beginOpen(name, msg.Stream)
	if !ok {
		if opening == nil || opening.done || opening.skip {
			return nil
		}
		m.bump(name, func(s *chanStat) { s.streamSkip++ })
		return nil
	}
	dctx, cancel := m.streamCtx(base)
	defer cancel()
	src := ref.src
	src.Content = fit(text, limit)
	handle, err := opener.StreamOpen(dctx, src)
	m.streams.opened(name, msg.Stream, handle, fit(text, limit), err)
	if err != nil {
		m.recordStreamError(name, err)
		return nil
	}
	m.bump(name, func(s *chanStat) { s.streamSent++ })
	return nil
}

func (m *Manager) updateStream(base context.Context, name string, ref *streamRef, text string) error {
	visible := fit(text, ref.limit)
	if visible == ref.visible {
		m.bump(name, func(s *chanStat) { s.streamSkip++ })
		return nil
	}
	dctx, cancel := m.streamCtx(base)
	defer cancel()
	if err := ref.handle.Update(dctx, visible, false); err != nil {
		ref.skip = true
		m.recordStreamError(name, err)
		return nil
	}
	ref.visible = visible
	m.bump(name, func(s *chanStat) { s.streamUpd++ })
	return nil
}

// finishStream 定稿：有流式消息就原位更新到终局（必要时截断 + 补发余下内容），
// 否则退化为普通消息投递。
func (m *Manager) finishStream(base context.Context, name string, c Channel, opener StreamOpener, msg bus.OutboundMessage, text string) error {
	ref := m.streams.drop(name, msg.Stream)
	if ref != nil {
		ref.lock.Lock()
		defer ref.lock.Unlock()
	}
	if ref == nil || ref.handle == nil || opener == nil {
		if ref != nil {
			ref.done = true
		}
		if strings.TrimSpace(text) == "" {
			m.bump(name, func(s *chanStat) { s.streamSkip++ })
			return nil
		}
		src := msg
		if ref != nil {
			src = ref.src
		}
		return m.sendPlain(base, name, c, src, text)
	}
	limit := ref.limit
	head := fit(text, limit)
	rest := ""
	done := head
	if RuneLen(text) > RuneLen(head) {
		done = head + streamEllipsis
		rest = string([]rune(text)[RuneLen(head):])
	}
	dctx, cancel := m.streamCtx(base)
	err := ref.handle.Update(dctx, done, true)
	cancel()
	if err != nil {
		ref.done = true
		m.recordStreamError(name, err)
		return m.sendPlain(base, name, c, ref.src, text)
	}
	ref.visible = done
	ref.done = true
	m.bump(name, func(s *chanStat) { s.streamFinal++ })
	if strings.TrimSpace(rest) == "" {
		return nil
	}
	return m.sendPlain(base, name, c, ref.src, rest)
}

// sendPlain 把文本按通道上限切分后作为普通消息投递。
func (m *Manager) sendPlain(base context.Context, name string, c Channel, msg bus.OutboundMessage, text string) error {
	text = strings.TrimSpace(text)
	if text == "" || c == nil {
		return nil
	}
	st := m.stat(name)
	out := msg
	out.Stream = ""
	out.Seq = 0
	out.Final = true
	dctx, cancel := context.WithTimeout(base, m.sendTimeout)
	defer cancel()
	var firstErr error
	for _, part := range Split(Sanitize(text), st.maxLen) {
		out.Content = part
		err := m.deliver(dctx, c, out)
		switch {
		case err == nil:
			m.bump(name, func(s *chanStat) { s.sent++ })
		case errors.Is(err, ErrUnroutable):
			m.bump(name, func(s *chanStat) { s.unmatched++ })
		default:
			m.bump(name, func(s *chanStat) { s.failed++; s.lastErr = err.Error(); s.lastErrAt = time.Now() })
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (m *Manager) bump(name string, fn func(*chanStat)) { m.count(name, fn) }

func (m *Manager) stat(name string) *chanStat {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.status[name]
	if st == nil {
		return &chanStat{maxLen: DefaultMaxLen}
	}
	return st
}

func (m *Manager) recordStreamError(name string, err error) {
	if err == nil {
		return
	}
	m.bump(name, func(s *chanStat) {
		s.streamFail++
		s.lastErr = err.Error()
		s.lastErrAt = time.Now()
	})
}

func (m *Manager) streamLimit(c Channel) int {
	if l, ok := c.(StreamLimit); ok {
		return l.StreamMaxLen()
	}
	return DefaultStreamLimit
}

func (m *Manager) streamCtx(base context.Context) (context.Context, context.CancelFunc) {
	if m.streamTimeout <= 0 {
		return base, func() {}
	}
	return context.WithTimeout(base, m.streamTimeout)
}

func fit(text string, limit int) string {
	if limit <= 0 || RuneLen(text) <= limit {
		return text
	}
	return string([]rune(text)[:limit])
}
