package sqlite

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LaoQi/ivan/schedule"
)

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jobs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func sampleJob(id string) schedule.Job {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	return schedule.Job{
		ID:      id,
		Name:    "每日汇报",
		Enabled: true,
		Schedule: schedule.Schedule{
			Kind:     schedule.KindDaily,
			AtTimeMS: int64(20 * time.Hour / time.Millisecond),
			TZ:       "Asia/Shanghai",
		},
		Payload: schedule.Payload{
			Prompt:     "汇总昨天的 git 提交",
			File:       "/data/tasks/job_1.md",
			Channel:    "qq",
			ChatID:     "u1",
			SessionKey: "qq:c2c:u1",
		},
		State: schedule.State{
			NextRunAtMS:     now.Add(24 * time.Hour).UnixMilli(),
			LastRunAtMS:     now.UnixMilli(),
			LastStatus:      schedule.StatusOK,
			LastError:       "上次的小错",
			LastNotifyError: "通知没送到",
			LastTokens:      1234,
			Runs:            7,
		},
		DeleteAfterRun: false,
		CreatedAtMS:    now.UnixMilli(),
		UpdatedAtMS:    now.UnixMilli(),
	}
}

func TestPutGetListDelete(t *testing.T) {
	s, _ := newStore(t)
	j := sampleJob("job_1")
	if err := s.Put(j); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := s.Get("job_1")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if got != j {
		t.Fatalf("字段往返不一致:\n got=%+v\nwant=%+v", got, j)
	}
	j.Name = "改名"
	j.State.Runs = 8
	if err := s.Put(j); err != nil {
		t.Fatalf("Put 覆盖: %v", err)
	}
	got, _, _ = s.Get("job_1")
	if got.Name != "改名" || got.State.Runs != 8 {
		t.Fatalf("覆盖未生效: %+v", got)
	}
	if err := s.Put(sampleJob("job_2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	list, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 || list[0].ID != "job_1" || list[1].ID != "job_2" {
		t.Fatalf("List 顺序或数量异常: %+v", list)
	}
	if err := s.Delete("job_1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := s.Get("job_1"); ok {
		t.Fatal("删除后仍可读到")
	}
	if err := s.Put(schedule.Job{}); err == nil {
		t.Fatal("空 id 应报错")
	}
}

func TestReopenPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "jobs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Put(sampleJob("job_1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again, err := Open(path)
	if err != nil {
		t.Fatalf("重新打开: %v", err)
	}
	defer again.Close()
	got, ok, err := again.Get("job_1")
	if err != nil || !ok {
		t.Fatalf("重启后读取: ok=%v err=%v", ok, err)
	}
	if got.Name != "每日汇报" || got.State.NextRunAtMS != sampleJob("job_1").State.NextRunAtMS {
		t.Fatalf("重启后数据不一致: %+v", got)
	}
}

func TestCorruptFileRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	if err := os.WriteFile(path, []byte("这不是 sqlite 数据库"), 0o600); err != nil {
		t.Fatalf("写坏文件: %v", err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("损坏文件应显式报错")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("原文件丢失: %v", err)
	}
	if string(raw) != "这不是 sqlite 数据库" {
		t.Fatalf("原文件被改写: %q", raw)
	}
}

func TestSchemaTooNewRejected(t *testing.T) {
	s, path := newStore(t)
	if _, err := s.db.Exec("pragma user_version = 99"); err != nil {
		t.Fatalf("改写版本: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := Open(path); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("期望 ErrSchemaTooNew，得到 %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s, _ := newStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := "job_" + string(rune('a'+n))
			for k := 0; k < 20; k++ {
				j := sampleJob(id)
				j.State.Runs = k
				if err := s.Put(j); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, _, err := s.Get(id); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if _, err := s.List(); err != nil {
					t.Errorf("List: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	list, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 8 {
		t.Fatalf("作业数 %d，期望 8", len(list))
	}
}

func TestMigrateV1ToCurrent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("打开旧库: %v", err)
	}
	if _, err := db.Exec(`create table jobs (
		id text primary key,
		name text not null default '',
		enabled integer not null default 1,
		kind text not null,
		at_ms integer not null default 0,
		every_ms integer not null default 0,
		expr text not null default '',
		tz text not null default '',
		prompt text not null default '',
		channel text not null default '',
		chat_id text not null default '',
		session_key text not null default '',
		next_run_at_ms integer not null default 0,
		last_run_at_ms integer not null default 0,
		last_status text not null default '',
		last_error text not null default '',
		last_notify_error text not null default '',
		last_tokens integer not null default 0,
		runs integer not null default 0,
		delete_after_run integer not null default 0,
		created_at_ms integer not null default 0,
		updated_at_ms integer not null default 0
	)`); err != nil {
		t.Fatalf("建旧表: %v", err)
	}
	if _, err := db.Exec(`insert into jobs (id, name, kind, every_ms, prompt, channel, chat_id, session_key, next_run_at_ms)
		values ('job_old', '旧作业', 'every', 60000, '旧提示词', 'qq', 'u1', 'qq:c2c:u1', 0)`); err != nil {
		t.Fatalf("写旧数据: %v", err)
	}
	if _, err := db.Exec("pragma user_version = 1"); err != nil {
		t.Fatalf("写版本: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("关闭旧库: %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("迁移打开失败: %v", err)
	}
	defer s.Close()
	got, ok, err := s.Get("job_old")
	if err != nil || !ok {
		t.Fatalf("迁移后读取失败: ok=%v err=%v", ok, err)
	}
	if got.Schedule.Kind != schedule.KindEvery || got.Schedule.EveryMS != 60000 || got.Payload.Prompt != "旧提示词" {
		t.Fatalf("旧字段丢失: %+v", got)
	}
	if got.Schedule.AtTimeMS != 0 || got.Payload.File != "" {
		t.Fatalf("新列应为零值: %+v", got.Schedule)
	}
	if err := s.Put(sampleJob("job_new")); err != nil {
		t.Fatalf("迁移后写入失败: %v", err)
	}
	if again, _, _ := s.Get("job_new"); again.Schedule.AtTimeMS == 0 || again.Payload.File == "" {
		t.Fatalf("新列未持久化: %+v", again)
	}
}
