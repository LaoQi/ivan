package data

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type LogFile struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

func NewLogFile(path string) (*LogFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &LogFile{path: path, f: f}, nil
}

func (l *LogFile) Path() string { return l.path }

func (l *LogFile) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return 0, os.ErrClosed
	}
	return l.f.Write(p)
}

func (l *LogFile) Printf(format string, args ...any) error {
	line := fmt.Sprintf("%s %s\n", time.Now().Format(time.RFC3339), fmt.Sprintf(format, args...))
	_, err := l.Write([]byte(line))
	return err
}

func (l *LogFile) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}
