package data

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LaoQi/tanya/agent"
)

type record struct {
	TS           string `json:"ts"`
	Kind         string `json:"kind"`
	Text         string `json:"text,omitempty"`
	Tool         string `json:"tool,omitempty"`
	Args         string `json:"args,omitempty"`
	Result       string `json:"result,omitempty"`
	Failed       bool   `json:"failed,omitempty"`
	Interrupted  bool   `json:"interrupted,omitempty"`
	DurMS        int64  `json:"dur_ms,omitempty"`
	PromptTokens int    `json:"prompt_tokens,omitempty"`
	TotalTokens  int    `json:"total_tokens,omitempty"`
	Context      int    `json:"context_tokens,omitempty"`
}

type EventLog struct {
	dir     string
	ch      chan record
	stop    chan struct{}
	done    chan struct{}
	closeMu sync.Mutex
	closed  bool
	dropped atomic.Uint64
	written atomic.Uint64
}

func NewEventLog(dir string, buf int) (*EventLog, error) {
	if buf <= 0 {
		buf = 256
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	l := &EventLog{
		dir:  dir,
		ch:   make(chan record, buf),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go l.loop()
	return l, nil
}

func (l *EventLog) Emit(e agent.Event) {
	l.push(toRecord(e))
}

func (l *EventLog) Note(kind, text string) {
	l.push(record{TS: time.Now().Format(time.RFC3339Nano), Kind: kind, Text: text})
}

func (l *EventLog) push(r record) {
	l.closeMu.Lock()
	if l.closed {
		l.closeMu.Unlock()
		return
	}
	l.closeMu.Unlock()
	select {
	case l.ch <- r:
	default:
		l.dropped.Add(1)
	}
}

func (l *EventLog) Dropped() uint64 { return l.dropped.Load() }

func (l *EventLog) Written() uint64 { return l.written.Load() }

func (l *EventLog) Close() error {
	l.closeMu.Lock()
	if l.closed {
		l.closeMu.Unlock()
		return nil
	}
	l.closed = true
	close(l.stop)
	l.closeMu.Unlock()
	<-l.done
	return nil
}

func (l *EventLog) loop() {
	defer close(l.done)
	var (
		file *os.File
		day  string
	)
	write := func(r record) {
		name := time.Now().Format("2006-01-02")
		if file == nil || day != name {
			if file != nil {
				file.Close()
			}
			path := filepath.Join(l.dir, name+".jsonl")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return
			}
			f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return
			}
			file, day = f, name
		}
		b, err := json.Marshal(r)
		if err != nil {
			return
		}
		if _, err := file.Write(append(b, '\n')); err != nil {
			return
		}
		l.written.Add(1)
	}
	for {
		select {
		case r := <-l.ch:
			write(r)
		case <-l.stop:
			for {
				select {
				case r := <-l.ch:
					write(r)
				default:
					if file != nil {
						file.Close()
					}
					return
				}
			}
		}
	}
}

func toRecord(e agent.Event) record {
	r := record{TS: time.Now().Format(time.RFC3339Nano), Kind: kindName(e.Kind)}
	switch e.Kind {
	case agent.EventContent, agent.EventReasoning:
		r.Text = e.Text
	case agent.EventToolCall:
		r.Tool, r.Args = e.ToolName, e.ToolArgs
	case agent.EventToolStart:
		r.Tool, r.Args = e.ToolName, e.ToolArgs
	case agent.EventToolEnd:
		r.Tool = e.ToolName
		r.Result = e.Result.Text
	case agent.EventResponse:
		r.DurMS = e.Response.Duration.Milliseconds()
		r.Context = e.Response.ContextTokens
		if e.Response.Usage != nil {
			r.PromptTokens = e.Response.Usage.PromptTokens
			r.TotalTokens = e.Response.Usage.TotalTokens
		}
	case agent.EventTurnEnd:
		r.DurMS = e.Turn.Duration.Milliseconds()
		r.Failed = e.Turn.Failed
		r.Interrupted = e.Turn.Interrupted
	}
	return r
}

func kindName(k agent.EventKind) string {
	switch k {
	case agent.EventRequestStart:
		return "request_start"
	case agent.EventReasoning:
		return "reasoning"
	case agent.EventContent:
		return "content"
	case agent.EventToolCall:
		return "tool_call"
	case agent.EventToolStart:
		return "tool_start"
	case agent.EventToolEnd:
		return "tool_end"
	case agent.EventResponse:
		return "response"
	case agent.EventReasoningEnd:
		return "reasoning_end"
	case agent.EventTurnEnd:
		return "turn_end"
	}
	return "unknown"
}
