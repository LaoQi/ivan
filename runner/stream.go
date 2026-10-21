package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LaoQi/ivan/bus"
)

const streamTypingEvery = 45 * time.Second

var streamSeq atomic.Uint64

func streamID(key string) string {
	return fmt.Sprintf("%s#%d", key, streamSeq.Add(1))
}

// streamState 把一次回合的回复装进“一条消息”（通道侧原位更新），
// 目的是让用户看得到还在跑：正文就绪前用“输入中”状态覆盖静默期，正文就绪后流式跟进。
// 整条回复只占一条消息，不产生逐里程碑的进度消息。
type streamState struct {
	r      *Runner
	target bus.OutboundMessage
	id     string
	first  time.Duration
	every  time.Duration
	maxLen int

	start    time.Time
	stop     chan struct{}
	done     chan struct{}
	wake     chan struct{}
	stopOnce sync.Once

	mu       sync.Mutex
	text     string
	sent     string
	sentAt   time.Time
	typingAt time.Time
}

func (t *task) streamStart() {
	if t.msg.Channel == bus.ChannelSchedule {
		return
	}
	target := bus.OutboundMessage{
		Channel: replyChannel(t.msg),
		ChatID:  t.msg.ChatID,
		ReplyTo: t.msg.MessageID,
	}
	if !t.r.streamEnabled(target.Channel) {
		return
	}
	t.stream = newStreamState(t.r, target, streamID(t.key))
}

func newStreamState(r *Runner, target bus.OutboundMessage, id string) *streamState {
	s := &streamState{
		r:      r,
		target: target,
		id:     id,
		first:  r.cfg.StreamFirst(),
		every:  r.cfg.StreamEvery(),
		maxLen: r.cfg.Runner.StreamMaxChars,
		start:  time.Now(),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		wake:   make(chan struct{}, 1),
	}
	go s.loop()
	return s
}

func (s *streamState) loop() {
	defer close(s.done)
	interval := s.every / 2
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			s.push(now)
		case <-s.wake:
			s.push(time.Now())
		}
	}
}

func (t *task) syncStream() {
	if t.stream == nil {
		return
	}
	t.stream.set(t.text())
}

func (s *streamState) set(text string) {
	s.mu.Lock()
	s.text = text
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// push 按节流投递中间态：有正文就原位更新；正文未就绪时每 streamTypingEvery 发一次“输入中”。
func (s *streamState) push(now time.Time) {
	s.mu.Lock()
	text := strings.TrimSpace(s.text)
	if len(text) > s.maxLen {
		text = string([]rune(text)[:s.maxLen])
	}
	last, sentAt, typingAt := s.sent, s.sentAt, s.typingAt
	s.mu.Unlock()

	if text != "" {
		first := last == ""
		if text == last {
			return
		}
		if !first && now.Sub(sentAt) < s.every {
			return
		}
		if s.send(text, false) != nil {
			return
		}
		s.mu.Lock()
		s.sent, s.sentAt = text, now
		s.mu.Unlock()
		return
	}
	if now.Sub(s.start) < s.first || now.Sub(typingAt) < streamTypingEvery {
		return
	}
	out := s.target
	out.Kind = bus.KindTyping
	if s.r.publish(out) == nil {
		s.mu.Lock()
		s.typingAt = now
		s.mu.Unlock()
	}
}

func (s *streamState) send(text string, final bool) error {
	out := s.target
	out.Stream = s.id
	out.Kind = bus.KindReply
	out.Final = final
	out.Content = text
	return s.r.publish(out)
}

// finish 停止节流并投递终局：正文原样交给通道，由通道决定原位定稿还是普通消息（含溢出补发）。
// 终局沿用入站聚合结果的消息类型（reply/error），不改写语义。
func (s *streamState) finish(out bus.OutboundMessage) error {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.done
	text := strings.TrimSpace(out.Content)
	if text == "" {
		s.mu.Lock()
		text = strings.TrimSpace(s.text)
		s.mu.Unlock()
	}
	if text == "" {
		return nil
	}
	out.Stream = s.id
	out.Final = true
	out.Content = text
	return s.r.publish(out)
}

func clipText(text string, limit int) string {
	if limit <= 0 {
		return text
	}
	r := []rune(text)
	if len(r) <= limit {
		return text
	}
	return string(r[:limit])
}

// publish 投递中间态/终局：优先走总线（与默认出站同一条路径），无总线时直投钩子。
func (r *Runner) publish(msg bus.OutboundMessage) error {
	if r.opts.Bus != nil {
		if r.opts.Bus.Post(msg) {
			return nil
		}
		r.dropped.Add(1)
		return errors.New("出站队列已满或已关闭")
	}
	if r.opts.Notify != nil {
		if err := r.opts.Notify(context.Background(), msg); err != nil {
			return err
		}
		return nil
	}
	return errors.New("无出站投递路径")
}

func (r *Runner) streamEnabled(channel string) bool {
	return r.cfg.StreamEnabled(channel)
}
