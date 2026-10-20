package runner

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LaoQi/ivan/schedule"
)

func (r *Runner) taskFilePath(name string) string {
	v := strings.TrimSpace(name)
	if v == "" {
		return ""
	}
	if filepath.IsAbs(v) {
		return v
	}
	return filepath.Join(r.cfg.TasksDir(), filepath.Base(v))
}

func (r *Runner) ReadTaskFile(name string) (string, string, error) {
	path := r.taskFilePath(name)
	if path == "" {
		return "", "", nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", path, err
	}
	return strings.TrimSpace(string(raw)), path, nil
}

func (r *Runner) WriteTaskFile(id, content string) (string, error) {
	path := r.cfg.TaskPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".task-*.tmp")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	cleanup := func() {
		f.Close()
		os.Remove(tmp)
	}
	if _, err := f.WriteString(content); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Chmod(0o600); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return path, nil
}

func (r *Runner) deleteTaskFile(id string) {
	path := r.cfg.TaskPath(id)
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		r.notice("删除任务文件失败 %s: %v", path, err)
	}
}

func (r *Runner) NewJobID() string { return schedule.NewID(time.Now()) }
