package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/LaoQi/ivan/schedule"
)

const SchemaVersion = 1

const opTimeout = 5 * time.Second

var ErrSchemaTooNew = errors.New("作业数据库版本高于当前程序支持的版本")

type Store struct {
	path string
	db   *sql.DB
	mu   sync.Mutex
}

func Open(path string) (*Store, error) {
	p := strings.TrimSpace(path)
	if p == "" {
		return nil, errors.New("作业数据库路径为空")
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return nil, fmt.Errorf("解析作业数据库路径失败 %s: %w", p, err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return nil, fmt.Errorf("创建作业数据库目录失败 %s: %w", filepath.Dir(abs), err)
	}
	db, err := sql.Open("sqlite", dsn(abs))
	if err != nil {
		return nil, fmt.Errorf("打开作业数据库失败 %s: %w", abs, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	s := &Store{path: abs, db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func dsn(path string) string {
	params := []string{
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(FULL)",
		"_pragma=busy_timeout(5000)",
	}
	return "file:" + path + "?" + strings.Join(params, "&")
}

func (s *Store) Path() string { return s.path }

func (s *Store) init() error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	var version int
	if err := s.db.QueryRowContext(ctx, "pragma user_version").Scan(&version); err != nil {
		return fmt.Errorf("读取作业数据库失败 %s（原文件已保留，请人工处理）: %w", s.path, err)
	}
	switch {
	case version == SchemaVersion:
		return nil
	case version > SchemaVersion:
		return fmt.Errorf("%w: %s 版本 %d，程序支持 %d", ErrSchemaTooNew, s.path, version, SchemaVersion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("初始化作业数据库失败 %s: %w", s.path, err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `create table if not exists jobs (
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
		return fmt.Errorf("创建作业表失败 %s: %w", s.path, err)
	}
	if _, err := tx.ExecContext(ctx, `create index if not exists jobs_due on jobs(enabled, next_run_at_ms)`); err != nil {
		return fmt.Errorf("创建作业索引失败 %s: %w", s.path, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("pragma user_version = %d", SchemaVersion)); err != nil {
		return fmt.Errorf("写作业数据库版本失败 %s: %w", s.path, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交作业数据库初始化失败 %s: %w", s.path, err)
	}
	return nil
}

const jobColumns = `id, name, enabled, kind, at_ms, every_ms, expr, tz, prompt, channel, chat_id, session_key,
	next_run_at_ms, last_run_at_ms, last_status, last_error, last_notify_error, last_tokens, runs,
	delete_after_run, created_at_ms, updated_at_ms`

func (s *Store) List() ([]schedule.Job, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `select `+jobColumns+` from jobs order by next_run_at_ms, id`)
	if err != nil {
		return nil, wrap("查询作业失败", err)
	}
	defer rows.Close()
	var out []schedule.Job
	for rows.Next() {
		j, err := scan(rows)
		if err != nil {
			return nil, wrap("读取作业记录失败", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("遍历作业失败", err)
	}
	return out, nil
}

func (s *Store) Get(id string) (schedule.Job, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	row := s.db.QueryRowContext(ctx, `select `+jobColumns+` from jobs where id = ?`, id)
	j, err := scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return schedule.Job{}, false, nil
	}
	if err != nil {
		return schedule.Job{}, false, wrap("读取作业失败", err)
	}
	return j, true, nil
}

func (s *Store) Put(j schedule.Job) error {
	if strings.TrimSpace(j.ID) == "" {
		return errors.New("作业缺少 id")
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	_, err := s.db.ExecContext(ctx, `insert into jobs (`+jobColumns+`)
		values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		on conflict(id) do update set
			name=excluded.name, enabled=excluded.enabled, kind=excluded.kind, at_ms=excluded.at_ms,
			every_ms=excluded.every_ms, expr=excluded.expr, tz=excluded.tz, prompt=excluded.prompt,
			channel=excluded.channel, chat_id=excluded.chat_id, session_key=excluded.session_key,
			next_run_at_ms=excluded.next_run_at_ms, last_run_at_ms=excluded.last_run_at_ms,
			last_status=excluded.last_status, last_error=excluded.last_error,
			last_notify_error=excluded.last_notify_error, last_tokens=excluded.last_tokens,
			runs=excluded.runs, delete_after_run=excluded.delete_after_run,
			created_at_ms=excluded.created_at_ms, updated_at_ms=excluded.updated_at_ms`,
		j.ID, j.Name, boolInt(j.Enabled), string(j.Schedule.Kind), j.Schedule.AtMS, j.Schedule.EveryMS,
		j.Schedule.Expr, j.Schedule.TZ, j.Payload.Prompt, j.Payload.Channel, j.Payload.ChatID,
		j.Payload.SessionKey, j.State.NextRunAtMS, j.State.LastRunAtMS, j.State.LastStatus,
		j.State.LastError, j.State.LastNotifyError, j.State.LastTokens, j.State.Runs,
		boolInt(j.DeleteAfterRun), j.CreatedAtMS, j.UpdatedAtMS)
	if err != nil {
		return wrap("写入作业失败", err)
	}
	return nil
}

func (s *Store) Delete(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, `delete from jobs where id = ?`, id); err != nil {
		return wrap("删除作业失败", err)
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

type scanner interface {
	Scan(dest ...any) error
}

func scan(row scanner) (schedule.Job, error) {
	var (
		j       schedule.Job
		enabled int
		del     int
		kind    string
	)
	err := row.Scan(&j.ID, &j.Name, &enabled, &kind, &j.Schedule.AtMS, &j.Schedule.EveryMS,
		&j.Schedule.Expr, &j.Schedule.TZ, &j.Payload.Prompt, &j.Payload.Channel, &j.Payload.ChatID,
		&j.Payload.SessionKey, &j.State.NextRunAtMS, &j.State.LastRunAtMS, &j.State.LastStatus,
		&j.State.LastError, &j.State.LastNotifyError, &j.State.LastTokens, &j.State.Runs,
		&del, &j.CreatedAtMS, &j.UpdatedAtMS)
	if err != nil {
		return schedule.Job{}, err
	}
	j.Schedule.Kind = schedule.Kind(kind)
	j.Enabled = enabled != 0
	j.DeleteAfterRun = del != 0
	return j, nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func wrap(text string, err error) error {
	return fmt.Errorf("%s: %w", text, err)
}
