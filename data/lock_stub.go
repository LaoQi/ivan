//go:build !unix

package data

import (
	"errors"
	"time"
)

var ErrLocked = errors.New("已有实例在运行")

type LockInfo struct {
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	Version   string    `json:"version,omitempty"`
	Socket    string    `json:"socket,omitempty"`
}

type Lock struct{}

func AcquireLock(path string, info LockInfo) (*Lock, error) {
	return nil, errors.New("当前平台不支持文件锁")
}

func ReadLockInfo(path string) (LockInfo, error) {
	return LockInfo{}, errors.New("当前平台不支持文件锁")
}

func (l *Lock) Release() error { return nil }
