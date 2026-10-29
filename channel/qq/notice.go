package qq

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/LaoQi/ivan/channel"
	"github.com/LaoQi/ivan/qqbot"
)

const (
	stateVersion  = 1
	tokenAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	tokenHalf     = 4
)

type channelState struct {
	Version    int       `json:"version"`
	ClaimToken string    `json:"claim_token"`
	NoticeTo   []string  `json:"notice_to,omitempty"`
	ClaimedAt  time.Time `json:"claimed_at,omitempty"`
}

type stateBox struct {
	mu   sync.Mutex
	path string
	doc  channelState
}

func loadState(path string) (*stateBox, error) {
	box := &stateBox{path: path, doc: channelState{Version: stateVersion}}
	if path == "" {
		return box, nil
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		var doc channelState
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, fmt.Errorf("qq 通道状态解析失败 %s: %w（原文件已保留，请人工处理）", path, err)
		}
		if doc.Version == 0 {
			doc.Version = stateVersion
		}
		doc.ClaimToken = strings.TrimSpace(doc.ClaimToken)
		doc.NoticeTo = normalizeTargets(doc.NoticeTo)
		box.doc = doc
	case os.IsNotExist(err):
	default:
		return nil, fmt.Errorf("读取 qq 通道状态失败 %s: %w", path, err)
	}
	return box, nil
}

func (b *stateBox) targets() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.doc.NoticeTo...)
}

func (b *stateBox) token() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.doc.ClaimToken
}

func (b *stateBox) ensureToken() (string, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.doc.ClaimToken != "" {
		return b.doc.ClaimToken, false, nil
	}
	tok, err := newToken()
	if err != nil {
		return "", false, err
	}
	b.doc.ClaimToken = tok
	if err := b.flushLocked(); err != nil {
		return "", false, err
	}
	return tok, true, nil
}

func (b *stateBox) claim(openid string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tok, err := newToken()
	if err != nil {
		return "", err
	}
	b.doc.ClaimToken = tok
	b.doc.NoticeTo = []string{openid}
	b.doc.ClaimedAt = time.Now()
	if err := b.flushLocked(); err != nil {
		return "", err
	}
	return tok, nil
}

func (b *stateBox) matchesToken(text string) bool {
	want := b.token()
	if want == "" {
		return false
	}
	got := strings.ToUpper(strings.TrimSpace(text))
	want = strings.ToUpper(want)
	if len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (b *stateBox) flushLocked() error {
	if b.path == "" {
		return errors.New("qq 通道状态未配置路径")
	}
	b.doc.Version = stateVersion
	b.doc.NoticeTo = normalizeTargets(b.doc.NoticeTo)
	raw, err := json.MarshalIndent(b.doc, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(b.path, append(raw, '\n'))
}

func normalizeTargets(list []string) []string {
	out := make([]string, 0, len(list))
	seen := map[string]struct{}{}
	for _, item := range list {
		s := strings.TrimSpace(item)
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func newToken() (string, error) {
	buf := make([]byte, tokenHalf*2)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	chars := make([]byte, 0, len(buf)+1)
	for i, v := range buf {
		if i == tokenHalf {
			chars = append(chars, '-')
		}
		chars = append(chars, tokenAlphabet[int(v)%len(tokenAlphabet)])
	}
	return string(chars), nil
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
	if err := f.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := f.Chmod(0o600); err != nil {
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
	return nil
}

func (c *Channel) Notice(ctx context.Context, content string) []channel.NoticeResult {
	text := strings.TrimSpace(content)
	if text == "" {
		return nil
	}
	targets := c.state.targets()
	if len(targets) == 0 {
		return nil
	}
	dctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	parts := channel.Split(text, c.cfg.MaxMessageLen)
	out := make([]channel.NoticeResult, 0, len(targets))
	for _, openid := range targets {
		out = append(out, channel.NoticeResult{
			Target: openid,
			Err:    c.noticeTo(dctx, openid, parts),
		})
	}
	return out
}

func (c *Channel) noticeTo(ctx context.Context, openid string, parts []string) error {
	target := qqbot.Target{UserOpenID: openid}
	var firstErr error
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		if err := c.sendBody(ctx, target, part, "", 0); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			c.cfg.Logf("qq 通知投递失败 target=%s: %v", shortID(openid), err)
			continue
		}
	}
	return firstErr
}

// claimToken 在私聊消息命中认领令牌时把该账号设为通知目标，并把新令牌写回通道状态文件。
func (c *Channel) claimToken(ev qqbot.MessageEvent) bool {
	if ev.Kind != qqbot.EventC2CMessage {
		return false
	}
	openid := strings.TrimSpace(ev.UserOpenID)
	if openid == "" || !c.state.matchesToken(ev.Content) {
		return false
	}
	if !c.allowed(false, openid, openid) {
		c.cfg.Logf("qq 认领令牌拒绝（不在 allow_from 白名单）user=%s", shortID(openid))
		return true
	}
	tok, err := c.state.claim(openid)
	if err != nil {
		c.cfg.Logf("qq 认领令牌写入失败 user=%s: %v", shortID(openid), err)
		return true
	}
	c.cfg.Logf("qq 通知目标已认领 user=%s，新令牌已写回 %s", shortID(openid), c.state.path)
	reply := fmt.Sprintf("已认领：该账号设为通知目标。新令牌 %s（私聊发送可改发给其他账号）", tok)
	c.replyDirect(context.Background(), openid, ev.ID, reply)
	return true
}

func (c *Channel) replyDirect(ctx context.Context, openid, msgID, text string) {
	seq, replyTo, err := c.nextSeq(msgID, false)
	if err != nil {
		c.cfg.Logf("qq 认领确认回复失败: %v", err)
		return
	}
	if _, err := c.client.SendText(ctx, qqbot.Target{UserOpenID: openid}, text, replyTo, seq); err != nil {
		c.cfg.Logf("qq 认领确认回复失败: %v", err)
	}
}

func (c *Channel) initState() {
	tok, created, err := c.state.ensureToken()
	if err != nil {
		c.cfg.Logf("qq 通知认领令牌初始化失败: %v", err)
		return
	}
	if created {
		c.cfg.Logf("qq 通知认领令牌已生成并写入 %s: %s（私聊发送该令牌即把该账号设为通知目标）", c.state.path, tok)
		return
	}
	if len(c.state.targets()) == 0 {
		c.cfg.Logf("qq 通知认领令牌: %s（私聊发送该令牌即把该账号设为通知目标）", tok)
	}
}

var _ channel.Noticer = (*Channel)(nil)
