package log

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const textLimit = 200

type Event struct {
	Time    time.Time
	Kind    string
	Session string
	Channel string
	Tool    string
	Text    string
	Err     string
	Dur     time.Duration
	Context int
	Total   int
	Failed  bool
	Handoff bool
}

type Sink struct {
	mu    sync.Mutex
	out   io.Writer
	lines atomic.Uint64
	errs  atomic.Uint64
}

func New(out io.Writer) *Sink { return &Sink{out: out} }

func (s *Sink) Emit(e Event) error {
	if s == nil || s.out == nil {
		return nil
	}
	line := format(e)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := io.WriteString(s.out, line); err != nil {
		s.errs.Add(1)
		return err
	}
	s.lines.Add(1)
	return nil
}

func (s *Sink) Notice(format string, args ...any) error {
	return s.Emit(Event{Kind: "daemon", Text: fmt.Sprintf(format, args...)})
}

func (s *Sink) Lines() uint64 {
	if s == nil {
		return 0
	}
	return s.lines.Load()
}

func (s *Sink) Errors() uint64 {
	if s == nil {
		return 0
	}
	return s.errs.Load()
}

func format(e Event) string {
	ts := e.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	kind := strings.TrimSpace(e.Kind)
	if kind == "" {
		kind = "event"
	}
	var b strings.Builder
	b.WriteString(ts.Format(time.RFC3339))
	b.WriteByte(' ')
	b.WriteString(kind)
	field := func(name, value string) {
		if value == "" {
			return
		}
		b.WriteByte(' ')
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(value)
	}
	field("session", e.Session)
	field("channel", e.Channel)
	field("tool", e.Tool)
	if e.Dur > 0 {
		field("dur", e.Dur.Round(time.Millisecond).String())
	}
	if e.Context > 0 {
		field("ctx", fmt.Sprint(e.Context))
	}
	if e.Total > 0 {
		field("total", fmt.Sprint(e.Total))
	}
	if e.Failed {
		field("failed", "true")
	}
	if e.Handoff {
		field("handoff", "true")
	}
	field("text", quote(e.Text))
	field("err", quote(e.Err))
	b.WriteByte('\n')
	return b.String()
}

func quote(s string) string {
	s = strings.TrimSpace(oneLine(s))
	if s == "" {
		return ""
	}
	if len(s) > textLimit {
		s = s[:textLimit] + "…"
	}
	if strings.ContainsAny(s, " \t") {
		return fmt.Sprintf("%q", s)
	}
	return s
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.Join(strings.Fields(s), " ")
}
