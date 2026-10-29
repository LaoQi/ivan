package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/LaoQi/battlelab/agent"
)

var Version = "dev"

const (
	DefaultSessionMode        = "global"
	DefaultMaxRetries         = 2
	maxRetriesLimit           = 20
	DataDirSuffix             = ".local/share/ivan"
	ConfigPathSuffix          = ".config/ivan/config.yaml"
	TanyaConfigPathSuffix     = ".config/tanya/config.yaml"
	GlobalAgentsSuffix        = ".config/ivan/AGENTS.md"
	UserAgentURL              = "https://github.com/LaoQi/ivan"
	EnvPrefix                 = "IVAN_"
	DefaultSocket             = "ivan.sock"
	DefaultBusBuffer          = 64
	DefaultMaxConcurrent      = 2
	DefaultShutdownSeconds    = 30
	DefaultSendSeconds        = 60
	DefaultMaxAgents          = 8
	DefaultIdleMinutes        = 30
	DefaultJobsDB             = "jobs.db"
	DefaultJobTimeoutSeconds  = 900
	DefaultHandoffHintTokens  = 180000
	DefaultHandoffForceTokens = 256000
	DefaultStreamFirstSeconds = 3
	DefaultStreamEverySeconds = 1
	DefaultStreamMaxChars     = 2048
	DefaultHTTPListen         = "127.0.0.1:8787"
	DefaultHTTPMaxBody        = 65536
	DefaultHTTPNotifyPerMin   = 30
	DefaultHTTPRequestPerMin  = 5
)

type RunnerConfig struct {
	HandoffHintTokens  int  `yaml:"handoff_hint_tokens"`
	HandoffForceTokens int  `yaml:"handoff_force_tokens"`
	AutoContinue       bool `yaml:"auto_continue"`
	MaxTurnsPerTask    int  `yaml:"max_turns_per_task"`
	MaxTokensPerTask   int  `yaml:"max_tokens_per_task"`
	MaxTokensPerDay    int  `yaml:"max_tokens_per_day"`
	MemoryMaxBytes     int  `yaml:"memory_max_bytes"`
	MaxAgents          int  `yaml:"max_agents"`
	IdleTimeoutMinutes int  `yaml:"idle_timeout_minutes"`
	Stream             bool `yaml:"stream"`
	StreamFirstSeconds int  `yaml:"stream_first_seconds"`
	StreamEverySeconds int  `yaml:"stream_every_seconds"`
	StreamMaxChars     int  `yaml:"stream_max_chars"`
}

type DaemonConfig struct {
	Socket                 string `yaml:"socket"`
	BusBuffer              int    `yaml:"bus_buffer"`
	MaxConcurrent          int    `yaml:"max_concurrent"`
	ShutdownTimeoutSeconds int    `yaml:"shutdown_timeout_seconds"`
	SendTimeoutSeconds     int    `yaml:"send_timeout_seconds"`
}

type CLIConfig struct {
	Enabled bool `yaml:"enabled"`
	Stream  bool `yaml:"stream"`
}

type QQConfig struct {
	Enabled         bool     `yaml:"enabled"`
	AppID           string   `yaml:"app_id"`
	AppSecret       string   `yaml:"app_secret"`
	APIBase         string   `yaml:"api_base"`
	TokenURL        string   `yaml:"token_url"`
	GatewayURL      string   `yaml:"gateway_url"`
	AllowFrom       []string `yaml:"allow_from"`
	AllowGroups     []string `yaml:"allow_groups"`
	MaxMessageLen   int      `yaml:"max_message_len"`
	Markdown        bool     `yaml:"markdown"`
	Stream          bool     `yaml:"stream"`
	StreamMaxLen    int      `yaml:"stream_max_len"`
	Typing          bool     `yaml:"typing"`
	AllowProactive  bool     `yaml:"allow_proactive"`
	ReconnectMaxSec int      `yaml:"reconnect_max_sec"`
}

type HTTPRequestConfig struct {
	Session string `yaml:"session"`
}

type HTTPConfig struct {
	Enabled          bool              `yaml:"enabled"`
	Listen           string            `yaml:"listen"`
	MaxBodyBytes     int               `yaml:"max_body_bytes"`
	NotifyPerMinute  int               `yaml:"notify_per_minute"`
	RequestPerMinute int               `yaml:"request_per_minute"`
	Request          HTTPRequestConfig `yaml:"request"`
}

type ChannelsConfig struct {
	CLI CLIConfig `yaml:"cli"`
	QQ  QQConfig  `yaml:"qq"`
}

type ScheduleConfig struct {
	Enabled           bool   `yaml:"enabled"`
	JobsDB            string `yaml:"jobs_db"`
	MissedPolicy      string `yaml:"missed_policy"`
	JobTimeoutSeconds int    `yaml:"job_timeout_seconds"`
}

type Config struct {
	agent.Config  `yaml:",inline"`
	Runner        RunnerConfig   `yaml:"runner"`
	Daemon        DaemonConfig   `yaml:"daemon"`
	Channels      ChannelsConfig `yaml:"channels"`
	Schedule      ScheduleConfig `yaml:"schedule"`
	HTTP          HTTPConfig     `yaml:"http"`
	GlobalAgents  string         `yaml:"global_agents"`
	ImageMaxBytes int            `yaml:"image_max_bytes"`
	ImageResize   bool           `yaml:"image_resize"`
	Path          string         `yaml:"-"`
}

func UserAgent(version string) string {
	return fmt.Sprintf("ivan/%s (+%s)", version, UserAgentURL)
}

func Default() *Config {
	home, _ := os.UserHomeDir()
	path := filepath.Join(home, filepath.FromSlash(ConfigPathSuffix))
	cfg := &Config{
		Config: agent.Config{
			ConfigPath:       path,
			BaseURL:          "https://api.openai.com/v1",
			Model:            "deepseek-v4-flash",
			Temperature:      0.7,
			ApiProtocol:      "responses",
			MaxRetries:       DefaultMaxRetries,
			SessionMode:      DefaultSessionMode,
			UserAgent:        UserAgent(Version),
			DataDir:          filepath.Join(home, filepath.FromSlash(DataDirSuffix)),
			ArchiveThreshold: agent.DefaultArchiveThreshold,
			ArchiveKeep:      agent.DefaultArchiveKeep,
		},
		Runner: RunnerConfig{
			HandoffHintTokens:  DefaultHandoffHintTokens,
			HandoffForceTokens: DefaultHandoffForceTokens,
			AutoContinue:       false,
			MaxTurnsPerTask:    8,
			MaxAgents:          DefaultMaxAgents,
			IdleTimeoutMinutes: DefaultIdleMinutes,
			Stream:             true,
			StreamFirstSeconds: DefaultStreamFirstSeconds,
			StreamEverySeconds: DefaultStreamEverySeconds,
			StreamMaxChars:     DefaultStreamMaxChars,
		},
		Daemon: DaemonConfig{
			Socket:                 DefaultSocket,
			BusBuffer:              DefaultBusBuffer,
			MaxConcurrent:          DefaultMaxConcurrent,
			ShutdownTimeoutSeconds: DefaultShutdownSeconds,
			SendTimeoutSeconds:     DefaultSendSeconds,
		},
		Channels: ChannelsConfig{
			CLI: CLIConfig{Enabled: true, Stream: true},
			QQ: QQConfig{
				MaxMessageLen: 2048, Markdown: true, Stream: true, StreamMaxLen: 2048, Typing: true,
				ReconnectMaxSec: 60,
			},
		},
		Schedule: ScheduleConfig{
			Enabled:           true,
			JobsDB:            DefaultJobsDB,
			MissedPolicy:      "run",
			JobTimeoutSeconds: DefaultJobTimeoutSeconds,
		},
		HTTP: HTTPConfig{
			Enabled:          true,
			Listen:           DefaultHTTPListen,
			MaxBodyBytes:     DefaultHTTPMaxBody,
			NotifyPerMinute:  DefaultHTTPNotifyPerMin,
			RequestPerMinute: DefaultHTTPRequestPerMin,
		},
		GlobalAgents:  filepath.Join(home, filepath.FromSlash(GlobalAgentsSuffix)),
		ImageMaxBytes: agent.DefaultImageMaxBytes,
		ImageResize:   true,
		Path:          path,
	}
	cfg.normalize()
	return cfg
}

func Load(path string) (*Config, error) {
	cfg := Default()
	if v := os.Getenv(EnvPrefix + "CONFIG"); v != "" && path == "" {
		path = v
	}
	if path == "" {
		path = cfg.Path
	}
	cfg.Path = path
	cfg.Config.ConfigPath = path

	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := yaml.Unmarshal(raw, cfg); err != nil {
			return nil, fmt.Errorf("配置文件解析失败 %s: %w", path, err)
		}
	case os.IsNotExist(err):
	default:
		return nil, fmt.Errorf("读取配置文件失败 %s: %w", path, err)
	}
	cfg.Config.ConfigPath = path
	applyEnv(cfg)
	if cfg.APIKey == "" {
		borrowTanya(cfg)
	}
	cfg.normalize()
	if cfg.Runner.HandoffForceTokens <= cfg.Runner.HandoffHintTokens {
		return nil, fmt.Errorf("handoff_force_tokens(%d) 必须大于 handoff_hint_tokens(%d)",
			cfg.Runner.HandoffForceTokens, cfg.Runner.HandoffHintTokens)
	}
	return cfg, nil
}

func applyEnv(cfg *Config) {
	set := func(key string, dst *string) {
		if v := os.Getenv(EnvPrefix + key); v != "" {
			*dst = v
		}
	}
	set("BASE_URL", &cfg.BaseURL)
	set("API_KEY", &cfg.APIKey)
	set("MODEL", &cfg.Model)
	set("DATA_DIR", &cfg.DataDir)
	set("API_PROTOCOL", &cfg.ApiProtocol)
	set("REASONING_EFFORT", &cfg.ReasoningEffort)
	set("SESSION_MODE", &cfg.SessionMode)
	set("IMAGE_DETAIL", &cfg.ImageDetail)
	if v := os.Getenv(EnvPrefix + "MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxRetries = n
		}
	}
	set("SOCKET", &cfg.Daemon.Socket)
	set("HTTP_LISTEN", &cfg.HTTP.Listen)
	set("QQ_APP_ID", &cfg.Channels.QQ.AppID)
	set("QQ_APP_SECRET", &cfg.Channels.QQ.AppSecret)
}

func (c *Config) normalize() {
	c.DataDir = expandHome(c.DataDir)
	if strings.TrimSpace(c.GlobalAgents) == "" {
		home, _ := os.UserHomeDir()
		c.GlobalAgents = filepath.Join(home, filepath.FromSlash(GlobalAgentsSuffix))
	}
	c.GlobalAgents = expandHome(c.GlobalAgents)
	if strings.TrimSpace(c.Daemon.Socket) == "" {
		c.Daemon.Socket = DefaultSocket
	}
	if c.MaxRetries < 0 || c.MaxRetries > maxRetriesLimit {
		c.MaxRetries = DefaultMaxRetries
	}
	if c.Daemon.BusBuffer <= 0 {
		c.Daemon.BusBuffer = DefaultBusBuffer
	}
	if c.Daemon.MaxConcurrent <= 0 {
		c.Daemon.MaxConcurrent = DefaultMaxConcurrent
	}
	if c.Daemon.ShutdownTimeoutSeconds <= 0 {
		c.Daemon.ShutdownTimeoutSeconds = DefaultShutdownSeconds
	}
	if c.Daemon.SendTimeoutSeconds <= 0 {
		c.Daemon.SendTimeoutSeconds = DefaultSendSeconds
	}
	if c.Runner.HandoffHintTokens <= 0 {
		c.Runner.HandoffHintTokens = DefaultHandoffHintTokens
	}
	if c.Runner.HandoffForceTokens <= 0 {
		c.Runner.HandoffForceTokens = DefaultHandoffForceTokens
	}
	if c.Runner.MaxAgents <= 0 {
		c.Runner.MaxAgents = DefaultMaxAgents
	}
	if c.Runner.IdleTimeoutMinutes <= 0 {
		c.Runner.IdleTimeoutMinutes = DefaultIdleMinutes
	}
	if c.Runner.MaxTurnsPerTask <= 0 {
		c.Runner.MaxTurnsPerTask = 8
	}
	if c.Runner.StreamFirstSeconds < 0 {
		c.Runner.StreamFirstSeconds = DefaultStreamFirstSeconds
	}
	if c.Runner.StreamEverySeconds <= 0 {
		c.Runner.StreamEverySeconds = DefaultStreamEverySeconds
	}
	if c.Runner.StreamMaxChars <= 0 {
		c.Runner.StreamMaxChars = DefaultStreamMaxChars
	}
	if c.Channels.QQ.MaxMessageLen <= 0 {
		c.Channels.QQ.MaxMessageLen = 2048
	}
	if c.Channels.QQ.ReconnectMaxSec <= 0 {
		c.Channels.QQ.ReconnectMaxSec = 60
	}
	if c.Channels.QQ.StreamMaxLen <= 0 {
		c.Channels.QQ.StreamMaxLen = 2048
	}
	if strings.TrimSpace(c.Schedule.JobsDB) == "" {
		c.Schedule.JobsDB = DefaultJobsDB
	}
	if strings.TrimSpace(c.Schedule.MissedPolicy) == "" {
		c.Schedule.MissedPolicy = "run"
	}
	if c.Schedule.JobTimeoutSeconds <= 0 {
		c.Schedule.JobTimeoutSeconds = DefaultJobTimeoutSeconds
	}
	if strings.TrimSpace(c.HTTP.Listen) == "" {
		c.HTTP.Listen = DefaultHTTPListen
	}
	if c.HTTP.MaxBodyBytes <= 0 {
		c.HTTP.MaxBodyBytes = DefaultHTTPMaxBody
	}
	if c.HTTP.NotifyPerMinute < 0 {
		c.HTTP.NotifyPerMinute = DefaultHTTPNotifyPerMin
	}
	if c.HTTP.RequestPerMinute < 0 {
		c.HTTP.RequestPerMinute = DefaultHTTPRequestPerMin
	}
}

func (c *Config) SocketPath() string {
	p := strings.TrimSpace(c.Daemon.Socket)
	if p == "" {
		p = DefaultSocket
	}
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(c.DataDir, p)
}

func (c *Config) StatePath() string { return filepath.Join(c.DataDir, "state.json") }

func (c *Config) LockPath() string { return filepath.Join(c.DataDir, "ivan.lock") }

func (c *Config) EventsDir() string { return filepath.Join(c.DataDir, "events") }

func (c *Config) LogPath() string { return filepath.Join(c.DataDir, "logs", "ivan.log") }

func (c *Config) AgentsDir() string { return filepath.Join(c.DataDir, "agents") }

func (c *Config) AgentDir(slug string) string { return filepath.Join(c.AgentsDir(), slug) }

func (c *Config) HTTPEnabled() bool { return c.HTTP.Enabled }

func (c *Config) SendTimeout() time.Duration {
	return time.Duration(c.Daemon.SendTimeoutSeconds) * time.Second
}

func (c *Config) ShutdownTimeout() time.Duration {
	return time.Duration(c.Daemon.ShutdownTimeoutSeconds) * time.Second
}

func (c *Config) IdleTimeout() time.Duration {
	return time.Duration(c.Runner.IdleTimeoutMinutes) * time.Minute
}

func (c *Config) CLIEnabled() bool { return c.Channels.CLI.Enabled }

// StreamEnabled 按通道决定是否启用中间态投递（原地更新的流式消息）。
func (c *Config) StreamEnabled(channel string) bool {
	if !c.Runner.Stream {
		return false
	}
	switch channel {
	case "cli":
		return c.Channels.CLI.Stream
	case "qq":
		return c.Channels.QQ.Stream
	default:
		return false
	}
}

func (c *Config) StreamFirst() time.Duration {
	return time.Duration(c.Runner.StreamFirstSeconds) * time.Second
}

func (c *Config) StreamEvery() time.Duration {
	return time.Duration(c.Runner.StreamEverySeconds) * time.Second
}

func (c *Config) QQEnabled() bool {
	return c.Channels.QQ.Enabled
}

func (c *Config) MediaDir() string { return filepath.Join(c.DataDir, "media") }

// QQStatePath 是 QQ 通道自持的状态文件（通知认领令牌与已认领的通知目标）。
func (c *Config) QQStatePath() string { return filepath.Join(c.DataDir, "qq", "channel.json") }

func (c *Config) JobsDBPath() string {
	p := strings.TrimSpace(c.Schedule.JobsDB)
	if p == "" {
		p = DefaultJobsDB
	}
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(c.DataDir, p)
}

func (c *Config) MemoryDir() string { return filepath.Join(c.DataDir, "memory") }

func (c *Config) TasksDir() string { return filepath.Join(c.DataDir, "tasks") }

func (c *Config) TaskPath(id string) string { return filepath.Join(c.TasksDir(), taskSlug(id)+".md") }

func taskSlug(id string) string {
	out := make([]rune, 0, len(id))
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	s := strings.Trim(string(out), "-")
	if s == "" {
		return "task"
	}
	if utf8.RuneCountInString(s) > 64 {
		sum := sha256.Sum256([]byte(id))
		s = string([]rune(s)[:64]) + "-" + hex.EncodeToString(sum[:6])
	}
	return s
}

func (c *Config) GlobalAgentsPath() string {
	switch strings.ToLower(strings.TrimSpace(c.GlobalAgents)) {
	case "off", "none", "-":
		return ""
	}
	return c.GlobalAgents
}

func (c *Config) JobTimeout() time.Duration {
	return time.Duration(c.Schedule.JobTimeoutSeconds) * time.Second
}

func expandHome(p string) string {
	p = strings.TrimSpace(p)
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return p
		}
		if p == "~" {
			return home
		}
		return filepath.Join(home, p[2:])
	}
	return p
}

func borrowTanya(cfg *Config) {
	home, _ := os.UserHomeDir()
	path := filepath.Join(home, filepath.FromSlash(TanyaConfigPathSuffix))
	if v := os.Getenv(EnvPrefix + "TANYA_CONFIG"); v != "" {
		path = v
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var src struct {
		BaseURL string `yaml:"base_url"`
		APIKey  string `yaml:"api_key"`
		Model   string `yaml:"model"`
	}
	if err := yaml.Unmarshal(raw, &src); err != nil {
		return
	}
	if cfg.APIKey == "" {
		cfg.APIKey = strings.TrimSpace(src.APIKey)
	}
	if cfg.BaseURL == "" || cfg.BaseURL == "https://api.openai.com/v1" {
		if src.BaseURL != "" {
			cfg.BaseURL = src.BaseURL
		}
	}
	if cfg.Model == "" || cfg.Model == "deepseek-v4-flash" {
		if src.Model != "" {
			cfg.Model = src.Model
		}
	}
}
