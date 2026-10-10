package bus

import (
	"sync"
	"sync/atomic"
	"time"
)

const DefaultBuffer = 64

const (
	KindReply  = "reply"
	KindNotice = "notice"
	KindError  = "error"
)

const (
	ChatDirect = "direct"
	ChatGroup  = "group"
)

type MediaRef struct {
	Path  string `json:"path,omitempty"`
	MIME  string `json:"mime,omitempty"`
	Name  string `json:"name,omitempty"`
	Bytes int64  `json:"bytes,omitempty"`
}

type InboundMessage struct {
	Channel    string            `json:"channel"`
	Account    string            `json:"account,omitempty"`
	ChatID     string            `json:"chat_id,omitempty"`
	ChatType   string            `json:"chat_type,omitempty"`
	SenderID   string            `json:"sender_id,omitempty"`
	SenderName string            `json:"sender_name,omitempty"`
	MessageID  string            `json:"message_id,omitempty"`
	Content    string            `json:"content,omitempty"`
	Media      []MediaRef        `json:"media,omitempty"`
	Mentioned  bool              `json:"mentioned,omitempty"`
	SessionKey string            `json:"session_key,omitempty"`
	ReceivedAt time.Time         `json:"received_at,omitempty"`
	Raw        map[string]string `json:"raw,omitempty"`
}

type OutboundMessage struct {
	Channel string     `json:"channel"`
	ChatID  string     `json:"chat_id,omitempty"`
	ReplyTo string     `json:"reply_to,omitempty"`
	Content string     `json:"content,omitempty"`
	Media   []MediaRef `json:"media,omitempty"`
	Kind    string     `json:"kind,omitempty"`
}

type Counters struct {
	Inbound         uint64    `json:"inbound"`
	InboundDropped  uint64    `json:"inbound_dropped"`
	InboundQueued   int       `json:"inbound_queued"`
	Outbound        uint64    `json:"outbound"`
	OutboundDropped uint64    `json:"outbound_dropped"`
	OutboundQueued  int       `json:"outbound_queued"`
	LastDrop        time.Time `json:"last_drop,omitempty"`
}

type Bus struct {
	mu        sync.Mutex
	in        chan InboundMessage
	out       chan OutboundMessage
	inClosed  bool
	outClosed bool

	inCount  atomic.Uint64
	inDrop   atomic.Uint64
	outCount atomic.Uint64
	outDrop  atomic.Uint64

	dropMu   sync.Mutex
	lastDrop time.Time
}

func New(inCap, outCap int) *Bus {
	if inCap <= 0 {
		inCap = DefaultBuffer
	}
	if outCap <= 0 {
		outCap = DefaultBuffer
	}
	return &Bus{
		in:  make(chan InboundMessage, inCap),
		out: make(chan OutboundMessage, outCap),
	}
}

func (b *Bus) Inbound() <-chan InboundMessage { return b.in }

func (b *Bus) Outbound() <-chan OutboundMessage { return b.out }

func (b *Bus) Publish(m InboundMessage) bool {
	b.mu.Lock()
	if b.inClosed {
		b.mu.Unlock()
		b.markDrop()
		b.inDrop.Add(1)
		return false
	}
	select {
	case b.in <- m:
		b.mu.Unlock()
		b.inCount.Add(1)
		return true
	default:
		b.mu.Unlock()
		b.markDrop()
		b.inDrop.Add(1)
		return false
	}
}

func (b *Bus) Post(m OutboundMessage) bool {
	b.mu.Lock()
	if b.outClosed {
		b.mu.Unlock()
		b.markDrop()
		b.outDrop.Add(1)
		return false
	}
	select {
	case b.out <- m:
		b.mu.Unlock()
		b.outCount.Add(1)
		return true
	default:
		b.mu.Unlock()
		b.markDrop()
		b.outDrop.Add(1)
		return false
	}
}

func (b *Bus) CloseInbound() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inClosed {
		return
	}
	b.inClosed = true
	close(b.in)
}

func (b *Bus) CloseOutbound() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outClosed {
		return
	}
	b.outClosed = true
	close(b.out)
}

func (b *Bus) Close() {
	b.CloseInbound()
	b.CloseOutbound()
}

func (b *Bus) InboundClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inClosed
}

func (b *Bus) OutboundClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.outClosed
}

func (b *Bus) Stats() Counters {
	b.dropMu.Lock()
	last := b.lastDrop
	b.dropMu.Unlock()
	b.mu.Lock()
	inClosed, outClosed := b.inClosed, b.outClosed
	b.mu.Unlock()
	c := Counters{
		Inbound:         b.inCount.Load(),
		InboundDropped:  b.inDrop.Load(),
		Outbound:        b.outCount.Load(),
		OutboundDropped: b.outDrop.Load(),
		LastDrop:        last,
	}
	if !inClosed {
		c.InboundQueued = len(b.in)
	}
	if !outClosed {
		c.OutboundQueued = len(b.out)
	}
	return c
}

func (b *Bus) markDrop() {
	b.dropMu.Lock()
	b.lastDrop = time.Now()
	b.dropMu.Unlock()
}
