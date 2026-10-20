package data

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const stateVersion = 1

type SessionState struct {
	SessionID string    `json:"session_id"`
	Workspace string    `json:"workspace,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type Usage struct {
	Date   string `json:"date,omitempty"`
	Tokens int    `json:"tokens,omitempty"`
}

type stateDoc struct {
	Version   int                     `json:"version"`
	UpdatedAt time.Time               `json:"updated_at,omitempty"`
	Paused    bool                    `json:"paused,omitempty"`
	Usage     Usage                   `json:"usage,omitempty"`
	Sessions  map[string]SessionState `json:"sessions"`
}

type State struct {
	path string
	mu   sync.Mutex
	doc  stateDoc
}

func LoadState(path string) (*State, error) {
	s := &State{
		path: path,
		doc:  stateDoc{Version: stateVersion, Sessions: map[string]SessionState{}},
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		var doc stateDoc
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, fmt.Errorf("状态文件解析失败 %s: %w（原文件已保留，请人工处理）", path, err)
		}
		if doc.Version == 0 {
			doc.Version = stateVersion
		}
		if doc.Sessions == nil {
			doc.Sessions = map[string]SessionState{}
		}
		s.doc = doc
	case os.IsNotExist(err):
	default:
		return nil, fmt.Errorf("读取状态文件失败 %s: %w", path, err)
	}
	return s, nil
}

func (s *State) Path() string { return s.path }

func (s *State) Get(key string) (SessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.doc.Sessions[key]
	return st, ok
}

func (s *State) Set(key string, st SessionState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.doc.Sessions == nil {
		s.doc.Sessions = map[string]SessionState{}
	}
	st.UpdatedAt = time.Now()
	s.doc.Sessions[key] = st
	return s.flushLocked()
}

func (s *State) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.doc.Sessions[key]; !ok {
		return nil
	}
	delete(s.doc.Sessions, key)
	return s.flushLocked()
}

func (s *State) Paused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doc.Paused
}

func (s *State) SetPaused(v bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.doc.Paused == v {
		return nil
	}
	s.doc.Paused = v
	return s.flushLocked()
}

func (s *State) TokensToday() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.doc.Usage.Date != usageDay() {
		return 0
	}
	return s.doc.Usage.Tokens
}

func (s *State) AddTokens(n int) (int, error) {
	if n <= 0 {
		return s.TokensToday(), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	day := usageDay()
	if s.doc.Usage.Date != day {
		s.doc.Usage = Usage{Date: day}
	}
	s.doc.Usage.Tokens += n
	return s.doc.Usage.Tokens, s.flushLocked()
}

func usageDay() string { return time.Now().Format("2006-01-02") }

func (s *State) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.doc.Sessions)
}

func (s *State) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.doc.Sessions))
	for k := range s.doc.Sessions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *State) Snapshot() map[string]SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]SessionState, len(s.doc.Sessions))
	for k, v := range s.doc.Sessions {
		out[k] = v
	}
	return out
}

func (s *State) flushLocked() error {
	s.doc.Version = stateVersion
	s.doc.UpdatedAt = time.Now()
	b, err := json.MarshalIndent(s.doc, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(s.path, append(b, '\n'))
}

func writeAtomic(path string, b []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	cleanup := func() {
		f.Close()
		os.Remove(tmp)
	}
	if _, err := f.Write(b); err != nil {
		cleanup()
		return err
	}
	if err := f.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}
