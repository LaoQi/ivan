package fakellm

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
)

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type ToolCall struct {
	ID   string
	Name string
	Args string
}

type Step struct {
	Content   string
	Reasoning string
	Tools     []ToolCall
	Usage     Usage
	Status    int
}

type Request struct {
	Model    string            `json:"model"`
	Messages []json.RawMessage `json:"messages"`
	Tools    []json.RawMessage `json:"tools"`
	Stream   bool              `json:"stream"`
}

type Server struct {
	ts    *httptest.Server
	mu    sync.Mutex
	steps []Step
	idx   int
	reqs  []Request
}

func New(steps ...Step) *Server {
	s := &Server{steps: steps}
	s.ts = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *Server) BaseURL() string { return s.ts.URL + "/v1" }

func (s *Server) Close() { s.ts.Close() }

func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Request, len(s.reqs))
	copy(out, s.reqs)
	return out
}

func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.reqs)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/chat/completions" {
		http.NotFound(w, r)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var req Request
	_ = json.Unmarshal(body, &req)
	s.mu.Lock()
	s.reqs = append(s.reqs, req)
	step := Step{Content: "（假模型步骤用尽）"}
	if s.idx < len(s.steps) {
		step = s.steps[s.idx]
	}
	s.idx++
	s.mu.Unlock()

	if step.Status != 0 {
		w.WriteHeader(step.Status)
		fmt.Fprint(w, `{"error":{"message":"mock failure"}}`)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)
	emit := func(v any) {
		b, _ := json.Marshal(v)
		fmt.Fprintf(w, "data: %s\n\n", b)
		if fl != nil {
			fl.Flush()
		}
	}
	if step.Reasoning != "" {
		emit(deltaChunk(map[string]any{"reasoning_content": step.Reasoning}))
	}
	if step.Content != "" {
		emit(deltaChunk(map[string]any{"content": step.Content}))
	}
	for i, tc := range step.Tools {
		id := tc.ID
		if id == "" {
			id = fmt.Sprintf("call_%d", i+1)
		}
		emit(deltaChunk(map[string]any{"tool_calls": []any{map[string]any{
			"index":    i,
			"id":       id,
			"type":     "function",
			"function": map[string]any{"name": tc.Name, "arguments": tc.Args},
		}}}))
	}
	if step.Usage.TotalTokens > 0 {
		emit(map[string]any{"choices": []any{}, "usage": step.Usage})
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	if fl != nil {
		fl.Flush()
	}
}

func deltaChunk(delta map[string]any) map[string]any {
	return map[string]any{"choices": []any{map[string]any{"delta": delta}}}
}
