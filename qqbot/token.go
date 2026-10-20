package qqbot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type tokenSource struct {
	cfg Config

	mu        sync.Mutex
	token     string
	refreshAt time.Time
	expireAt  time.Time
	lastErr   error
}

func newTokenSource(cfg Config) *tokenSource {
	return &tokenSource{cfg: cfg}
}

func (t *tokenSource) Token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if t.token != "" && now.Before(t.refreshAt) {
		return t.token, nil
	}
	if err := t.refreshLocked(ctx); err != nil {
		if t.token != "" && now.Before(t.expireAt) {
			t.lastErr = err
			return t.token, nil
		}
		return "", err
	}
	return t.token, nil
}

func (t *tokenSource) LastError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastErr
}

func (t *tokenSource) refreshLocked(ctx context.Context) error {
	body, err := json.Marshal(map[string]string{
		"appId":        t.cfg.AppID,
		"clientSecret": t.cfg.AppSecret,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.cfg.TokenURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", t.cfg.UserAgent)
	}
	resp, err := t.cfg.HTTPClient.Do(req)
	if err != nil {
		t.lastErr = err
		return fmt.Errorf("获取 access_token 失败: %w", err)
	}
	defer resp.Body.Close()
	raw := readLimited(resp.Body)
	if resp.StatusCode != http.StatusOK {
		err := errorFromResponse("getAppAccessToken", resp.StatusCode, raw)
		t.lastErr = err
		return err
	}
	var out tokenResult
	if err := json.Unmarshal(raw, &out); err != nil {
		t.lastErr = err
		return fmt.Errorf("解析 access_token 响应失败: %w", err)
	}
	if out.AccessToken == "" {
		err := errorFromResponse("getAppAccessToken", resp.StatusCode, raw)
		if err.Message == "" {
			err.Message = "响应缺少 access_token"
		}
		t.lastErr = err
		return err
	}
	seconds := out.ExpiresIn.Int()
	if seconds <= 0 {
		seconds = 3600
	}
	now := time.Now()
	ttl := time.Duration(seconds) * time.Second
	t.token = out.AccessToken
	t.expireAt = now.Add(ttl)
	t.refreshAt = now.Add(ttl * 4 / 5)
	t.lastErr = nil
	return nil
}

func (t *tokenSource) keepFresh(ctx context.Context) {
	interval := 30 * time.Second
	backoff := t.cfg.MinBackoff
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		if _, err := t.Token(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			t.cfg.Logf("qqbot token 刷新失败（%s 后重试）: %v", backoff, err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, t.cfg.MaxBackoff)
			continue
		}
		backoff = t.cfg.MinBackoff
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		next = max
	}
	if next <= 0 {
		next = time.Second
	}
	return next
}

var errNoTarget = errors.New("qqbot 缺少发送目标")
