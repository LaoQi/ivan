//go:build unix

package data

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

var ErrLocked = errors.New("已有实例在运行")

type LockInfo struct {
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	Version   string    `json:"version,omitempty"`
	Socket    string    `json:"socket,omitempty"`
}

type Lock struct {
	f *os.File
}

func AcquireLock(path string, info LockInfo) (*Lock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		prev, _ := decodeLockInfo(f)
		f.Close()
		if prev.PID > 0 {
			return nil, fmt.Errorf("%w: pid=%d since=%s（%s）", ErrLocked, prev.PID, prev.StartedAt.Format(time.RFC3339), path)
		}
		return nil, fmt.Errorf("%w: %s", ErrLocked, path)
	}
	if info.StartedAt.IsZero() {
		info.StartedAt = time.Now()
	}
	if err := writeLockInfo(f, info); err != nil {
		f.Close()
		return nil, err
	}
	return &Lock{f: f}, nil
}

func ReadLockInfo(path string) (LockInfo, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return LockInfo{}, err
	}
	var info LockInfo
	if err := json.Unmarshal(bytes.TrimSpace(b), &info); err != nil {
		return LockInfo{}, err
	}
	return info, nil
}

func (l *Lock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	cerr := l.f.Close()
	l.f = nil
	if err != nil {
		return err
	}
	return cerr
}

func writeLockInfo(f *os.File, info LockInfo) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func decodeLockInfo(f *os.File) (LockInfo, error) {
	var info LockInfo
	if _, err := f.Seek(0, 0); err != nil {
		return info, err
	}
	b, err := io.ReadAll(io.LimitReader(f, 4096))
	if err != nil {
		return info, err
	}
	err = json.Unmarshal(bytes.TrimSpace(b), &info)
	return info, err
}
