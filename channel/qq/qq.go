package qq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
	"github.com/LaoQi/ivan/qqbot"
)

const (
	Name           = "qq"
	DefaultMaxLen  = 1200
	DefaultMediaMB = 5
)

const (
	passiveWindowC2C   = 60 * time.Minute
	passiveWindowGroup = 5 * time.Minute
	passiveMaxC2C      = 4
	passiveMaxGroup    = 5
)

type Config struct {
	AppID          string
	AppSecret      string
	AllowFrom      []string
	AllowGroups    []string
	MaxMessageLen  int
	AllowProactive bool
	ReconnectMax   time.Duration
	MediaDir       string
	MediaMaxBytes  int64
	Timeout        time.Duration
	APIBase        string
	TokenURL       string
	GatewayURL     string
	Logf           func(format string, args ...any)
	HTTPClient     *http.Client
}

type Channel struct {
	cfg    Config
	client *qqbot.Client

	mu      sync.Mutex
	groups  map[string]struct{}
	seqs    map[string]*replyState
	targets map[string]qqbot.Target
	started bool
}

type replyState struct {
	start time.Time
	count int
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.AppID) == "" || strings.TrimSpace(cfg.AppSecret) == "" {
		return nil, errors.New("qq 通道需要 app_id 与 app_secret")
	}
	if cfg.MaxMessageLen <= 0 {
		cfg.MaxMessageLen = DefaultMaxLen
	}
	if cfg.MediaMaxBytes <= 0 {
		cfg.MediaMaxBytes = int64(DefaultMediaMB) << 20
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.Timeout}
	}
	client, err := qqbot.New(qqbot.Config{
		AppID:      cfg.AppID,
		AppSecret:  cfg.AppSecret,
		APIBase:    cfg.APIBase,
		TokenURL:   cfg.TokenURL,
		GatewayURL: cfg.GatewayURL,
		MaxBackoff: cfg.ReconnectMax,
		Logf:       cfg.Logf,
	})
	if err != nil {
		return nil, err
	}
	return &Channel{
		cfg:     cfg,
		client:  client,
		groups:  map[string]struct{}{},
		seqs:    map[string]*replyState{},
		targets: map[string]qqbot.Target{},
	}, nil
}

func (c *Channel) Name() string { return Name }

func (c *Channel) MaxLen() int { return c.cfg.MaxMessageLen }

func (c *Channel) Start(ctx context.Context, b *bus.Bus) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("qq 通道已启动")
	}
	c.started = true
	c.mu.Unlock()
	return c.client.Start(ctx, handler{ch: c, bus: b})
}

func (c *Channel) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.started = false
	c.mu.Unlock()
	return c.client.Stop(ctx)
}

func (c *Channel) Stats() qqbot.Stats { return c.client.Stats() }

type handler struct {
	ch  *Channel
	bus *bus.Bus
}

func (h handler) OnReady(r qqbot.ReadyInfo) {
	h.ch.cfg.Logf("qq 通道就绪 session=%s bot=%s", shortID(r.SessionID), r.Username)
}

func (h handler) OnError(err error) {
	if err != nil {
		h.ch.cfg.Logf("qq 通道错误: %v", err)
	}
}

func (h handler) OnMessage(ev qqbot.MessageEvent) {
	msg, ok := h.ch.translate(ev)
	if !ok {
		return
	}
	if h.bus == nil || !h.bus.Publish(msg) {
		h.ch.cfg.Logf("qq 入站消息被丢弃（总线繁忙或停机）: %s", shortID(ev.ID))
	}
}

func (c *Channel) translate(ev qqbot.MessageEvent) (bus.InboundMessage, bool) {
	group := ev.Kind == qqbot.EventGroupAt
	chatType := bus.ChatDirect
	chatID := ev.UserOpenID
	if group {
		chatType = bus.ChatGroup
		chatID = ev.GroupOpenID
	}
	if chatID == "" {
		c.cfg.Logf("qq 事件缺少会话标识，忽略: %s", ev.Kind)
		return bus.InboundMessage{}, false
	}
	if !c.allowed(group, chatID, ev.UserOpenID) {
		c.cfg.Logf("qq 消息被白名单拦截 chat=%s user=%s", shortID(chatID), shortID(ev.UserOpenID))
		return bus.InboundMessage{}, false
	}
	target := qqbot.Target{UserOpenID: chatID}
	if group {
		target = qqbot.Target{GroupOpenID: chatID}
	}
	c.mu.Lock()
	if group {
		c.groups[chatID] = struct{}{}
	}
	c.targets[chatID] = target
	c.mu.Unlock()

	received := ev.ReceivedAt
	if received.IsZero() {
		received = time.Now()
	}
	return bus.InboundMessage{
		Channel:    Name,
		ChatID:     chatID,
		ChatType:   chatType,
		SenderID:   ev.UserOpenID,
		MessageID:  ev.ID,
		Content:    strings.TrimSpace(ev.Content),
		Media:      c.fetchMedia(ev),
		Mentioned:  group,
		ReceivedAt: received,
		Raw: map[string]string{
			"msg_id":       ev.ID,
			"group_openid": ev.GroupOpenID,
			"user_openid":  ev.UserOpenID,
			"kind":         ev.Kind,
		},
	}, true
}

func (c *Channel) allowed(group bool, chatID, sender string) bool {
	if group {
		if len(c.cfg.AllowGroups) > 0 && !contains(c.cfg.AllowGroups, chatID) {
			return false
		}
		return true
	}
	if len(c.cfg.AllowFrom) > 0 && !contains(c.cfg.AllowFrom, sender) {
		return false
	}
	return true
}

func contains(list []string, v string) bool {
	for _, item := range list {
		if strings.TrimSpace(item) == v {
			return true
		}
	}
	return false
}

func (c *Channel) fetchMedia(ev qqbot.MessageEvent) []bus.MediaRef {
	if len(ev.Attachments) == 0 || c.cfg.MediaDir == "" {
		return nil
	}
	refs := make([]bus.MediaRef, 0, len(ev.Attachments))
	for i, att := range ev.Attachments {
		if strings.TrimSpace(att.URL) == "" {
			continue
		}
		ref, err := c.downloadAttachment(ev, att, i)
		if err != nil {
			c.cfg.Logf("qq 附件下载失败: %v", err)
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

func (c *Channel) downloadAttachment(ev qqbot.MessageEvent, att qqbot.Attachment, idx int) (bus.MediaRef, error) {
	if err := os.MkdirAll(c.cfg.MediaDir, 0o700); err != nil {
		return bus.MediaRef{}, err
	}
	req, err := http.NewRequest(http.MethodGet, att.URL, nil)
	if err != nil {
		return bus.MediaRef{}, err
	}
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return bus.MediaRef{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return bus.MediaRef{}, fmt.Errorf("下载附件 HTTP %d", resp.StatusCode)
	}
	name := mediaName(ev.ID, att, idx)
	path := filepath.Join(c.cfg.MediaDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return bus.MediaRef{}, err
	}
	n, err := io.Copy(f, io.LimitReader(resp.Body, c.cfg.MediaMaxBytes))
	cerr := f.Close()
	if err != nil {
		_ = os.Remove(path)
		return bus.MediaRef{}, err
	}
	if cerr != nil {
		return bus.MediaRef{}, cerr
	}
	mime := att.ContentType
	if mime == "" {
		mime = mimeFromExt(filepath.Ext(name))
	}
	return bus.MediaRef{Path: path, MIME: mime, Name: name, Bytes: n}, nil
}

func mediaName(id string, att qqbot.Attachment, idx int) string {
	ext := ""
	if att.Filename != "" {
		ext = filepath.Ext(att.Filename)
	}
	if ext == "" {
		ext = extFromContentType(att.ContentType)
	}
	if ext == "" {
		ext = ".bin"
	}
	return fmt.Sprintf("%s-%d%s", shortID(id), idx, ext)
}

func extFromContentType(ct string) string {
	switch strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0])) {
	case "image/png":
		return ".png"
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	default:
		return ""
	}
}

func mimeFromExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	default:
		return "application/octet-stream"
	}
}

func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	target, ok := c.resolveTarget(msg)
	if !ok {
		return fmt.Errorf("%w: qq 未知会话 %q", channel.ErrUnroutable, msg.ChatID)
	}
	group := target.IsGroup()
	content := strings.TrimSpace(msg.Content)
	if content == "" && len(msg.Media) == 0 {
		return nil
	}
	if content != "" {
		seq, replyTo, err := c.nextSeq(msg.ReplyTo, group)
		if err != nil {
			return err
		}
		if _, err := c.client.SendText(ctx, target, content, replyTo, seq); err != nil {
			return err
		}
	}
	for _, ref := range msg.Media {
		seq, replyTo, err := c.nextSeq(msg.ReplyTo, group)
		if err != nil {
			return err
		}
		fileInfo, err := c.upload(ctx, target, ref)
		if err != nil {
			return err
		}
		if _, err := c.client.SendImage(ctx, target, fileInfo, replyTo, seq); err != nil {
			return err
		}
	}
	return nil
}

func (c *Channel) upload(ctx context.Context, target qqbot.Target, ref bus.MediaRef) (string, error) {
	if strings.TrimSpace(ref.Path) == "" {
		return "", errors.New("qq 图片缺少本地路径")
	}
	data, err := os.ReadFile(ref.Path)
	if err != nil {
		return "", err
	}
	if int64(len(data)) > c.cfg.MediaMaxBytes {
		return "", fmt.Errorf("图片超过 %d 字节上限，需改走分片上传", c.cfg.MediaMaxBytes)
	}
	return c.client.UploadImageBytes(ctx, target, data)
}

func (c *Channel) resolveTarget(msg bus.OutboundMessage) (qqbot.Target, bool) {
	chatID := strings.TrimSpace(msg.ChatID)
	if chatID == "" {
		return qqbot.Target{}, false
	}
	c.mu.Lock()
	target, ok := c.targets[chatID]
	_, isGroup := c.groups[chatID]
	c.mu.Unlock()
	if ok {
		return target, true
	}
	if isGroup || contains(c.cfg.AllowGroups, chatID) {
		return qqbot.Target{GroupOpenID: chatID}, true
	}
	return qqbot.Target{UserOpenID: chatID}, true
}

func (c *Channel) nextSeq(replyTo string, group bool) (int, string, error) {
	replyTo = strings.TrimSpace(replyTo)
	if replyTo == "" {
		if !c.cfg.AllowProactive {
			return 0, "", nil
		}
		return 0, "", nil
	}
	window := passiveWindowC2C
	maxReplies := passiveMaxC2C
	if group {
		window = passiveWindowGroup
		maxReplies = passiveMaxGroup
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, st := range c.seqs {
		if now.Sub(st.start) > window {
			delete(c.seqs, id)
		}
	}
	st := c.seqs[replyTo]
	if st == nil {
		st = &replyState{start: now}
		c.seqs[replyTo] = st
	}
	if st.count >= maxReplies {
		if !c.cfg.AllowProactive {
			return 0, "", fmt.Errorf("qq 被动回复已达上限 %d 次（msg_id=%s）", maxReplies, shortID(replyTo))
		}
		return 0, "", nil
	}
	st.count++
	return st.count, replyTo, nil
}

func shortID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
