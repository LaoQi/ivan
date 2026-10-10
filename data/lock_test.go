//go:build unix

package data

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLockExcludesSecondInstance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ivan.lock")
	first, err := AcquireLock(path, LockInfo{PID: os.Getpid(), Version: "test", Socket: "/tmp/x.sock"})
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	_, err = AcquireLock(path, LockInfo{PID: os.Getpid()})
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("第二个实例应被拒绝: %v", err)
	}
	if !strings.Contains(err.Error(), "pid=") {
		t.Fatalf("错误信息应含持有者 pid: %v", err)
	}
	info, err := ReadLockInfo(path)
	if err != nil {
		t.Fatalf("ReadLockInfo: %v", err)
	}
	if info.Version != "test" || info.Socket != "/tmp/x.sock" || info.StartedAt.IsZero() {
		t.Fatalf("锁信息异常: %+v", info)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	second, err := AcquireLock(path, LockInfo{PID: os.Getpid()})
	if err != nil {
		t.Fatalf("释放后应可再次获取: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	var nilLock *Lock
	if err := nilLock.Release(); err != nil {
		t.Fatalf("空锁释放应静默: %v", err)
	}
}

func TestLockInfoDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "ivan.lock")
	l, err := AcquireLock(path, LockInfo{PID: 42})
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	defer l.Release()
	info, err := ReadLockInfo(path)
	if err != nil {
		t.Fatalf("ReadLockInfo: %v", err)
	}
	if info.PID != 42 || time.Since(info.StartedAt) > time.Minute {
		t.Fatalf("锁信息异常: %+v", info)
	}
}

func TestReadLockInfoMissing(t *testing.T) {
	if _, err := ReadLockInfo(filepath.Join(t.TempDir(), "nope.lock")); err == nil {
		t.Fatal("缺失文件应报错")
	}
}
