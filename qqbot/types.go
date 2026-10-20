package qqbot

import (
	"encoding/json"
	"net/http"
	"time"
)

const (
	DefaultAPIBase  = "https://api.sgroup.qq.com"
	DefaultTokenURL = "https://api.bot.qq.com/app/getAppAccessToken"
	DefaultIntents  = 1 << 25
)

const (
	IntentsGroupAndC2C = 1 << 25
)

const (
	ChatC2C   = "c2c"
	ChatGroup = "group"
)

const (
	EventReady      = "READY"
	EventResumed    = "RESUMED"
	EventC2CMessage = "C2C_MESSAGE_CREATE"
	EventGroupAt    = "GROUP_AT_MESSAGE_CREATE"
)

const (
	MsgTypeText     = 0
	MsgTypeMarkdown = 2
	MsgTypeInput    = 6
	MsgTypeMedia    = 7
)

type Config struct {
	AppID      string
	AppSecret  string
	Intents    int
	Shard      [2]int
	APIBase    string
	TokenURL   string
	GatewayURL string
	UserAgent  string
	HTTPClient *http.Client
	Logf       func(format string, args ...any)
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

func (c Config) withDefaults() Config {
	if c.Intents == 0 {
		c.Intents = DefaultIntents
	}
	if c.Shard[1] == 0 {
		c.Shard = [2]int{0, 1}
	}
	if c.APIBase == "" {
		c.APIBase = DefaultAPIBase
	}
	if c.TokenURL == "" {
		c.TokenURL = DefaultTokenURL
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if c.MinBackoff <= 0 {
		c.MinBackoff = time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 60 * time.Second
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	return c
}

type Target struct {
	UserOpenID  string
	GroupOpenID string
}

func (t Target) IsGroup() bool { return t.GroupOpenID != "" }

func (t Target) valid() bool { return t.UserOpenID != "" || t.GroupOpenID != "" }

func (t Target) messagesPath() string {
	if t.IsGroup() {
		return "/v2/groups/" + t.GroupOpenID + "/messages"
	}
	return "/v2/users/" + t.UserOpenID + "/messages"
}

func (t Target) filesPath() string {
	if t.IsGroup() {
		return "/v2/groups/" + t.GroupOpenID + "/files"
	}
	return "/v2/users/" + t.UserOpenID + "/files"
}

type MessageAuthor struct {
	ID           string `json:"id,omitempty"`
	UserOpenID   string `json:"user_openid,omitempty"`
	MemberOpenID string `json:"member_openid,omitempty"`
	UnionOpenID  string `json:"union_openid,omitempty"`
}

type Attachment struct {
	URL         string `json:"url"`
	ContentType string `json:"content_type"`
	Filename    string `json:"filename,omitempty"`
	Size        int64  `json:"size,omitempty"`
	Width       int    `json:"width,omitempty"`
	Height      int    `json:"height,omitempty"`
}

type MessageEvent struct {
	Kind        string
	ID          string
	Content     string
	Timestamp   string
	Author      MessageAuthor
	UserOpenID  string
	GroupOpenID string
	Attachments []Attachment
	Raw         json.RawMessage
	ReceivedAt  time.Time
}

type ReadyInfo struct {
	SessionID string
	Shard     [2]int
	UserID    string
	Username  string
}

type GatewayInfo struct {
	URL               string `json:"url"`
	Shards            int    `json:"shards"`
	SessionStartLimit struct {
		Total          int `json:"total"`
		Remaining      int `json:"remaining"`
		ResetAfter     int `json:"reset_after"`
		MaxConcurrency int `json:"max_concurrency"`
	} `json:"session_start_limit"`
}

type Handler interface {
	OnReady(ReadyInfo)
	OnMessage(MessageEvent)
	OnError(error)
}

type HandlerFunc struct {
	Ready   func(ReadyInfo)
	Message func(MessageEvent)
	Err     func(error)
}

func (h HandlerFunc) OnReady(r ReadyInfo) {
	if h.Ready != nil {
		h.Ready(r)
	}
}

func (h HandlerFunc) OnMessage(m MessageEvent) {
	if h.Message != nil {
		h.Message(m)
	}
}

func (h HandlerFunc) OnError(err error) {
	if h.Err != nil {
		h.Err(err)
	}
}

type rawPayload struct {
	ID          string        `json:"id"`
	Content     string        `json:"content"`
	Timestamp   string        `json:"timestamp"`
	Author      MessageAuthor `json:"author"`
	GroupOpenID string        `json:"group_openid"`
	Attachments []Attachment  `json:"attachments"`
}

type readyPayload struct {
	Version   int    `json:"version"`
	SessionID string `json:"session_id"`
	Shard     [2]int `json:"shard"`
	User      struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	} `json:"user"`
}

type envelope struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  *int            `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type helloPayload struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type identifyData struct {
	Token      string            `json:"token"`
	Intents    int               `json:"intents"`
	Shard      [2]int            `json:"shard"`
	Properties map[string]string `json:"properties"`
}

type resumeData struct {
	Token     string `json:"token"`
	SessionID string `json:"session_id"`
	Seq       int    `json:"seq"`
}

type sendTextData struct {
	Content string `json:"content"`
	MsgType int    `json:"msg_type"`
	MsgID   string `json:"msg_id,omitempty"`
	MsgSeq  int    `json:"msg_seq,omitempty"`
	EventID string `json:"event_id,omitempty"`
}

type sendMediaData struct {
	Content string     `json:"content,omitempty"`
	MsgType int        `json:"msg_type"`
	MsgID   string     `json:"msg_id,omitempty"`
	MsgSeq  int        `json:"msg_seq,omitempty"`
	Media   mediaField `json:"media"`
}

type mediaField struct {
	FileInfo string `json:"file_info"`
}

type sendResult struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
}

type uploadResult struct {
	FileUUID string `json:"file_uuid"`
	FileInfo string `json:"file_info"`
	TTL      int    `json:"ttl"`
}

type tokenResult struct {
	AccessToken string  `json:"access_token"`
	ExpiresIn   flexInt `json:"expires_in"`
	ErrCode     flexInt `json:"code"`
	Message     string  `json:"message"`
}
