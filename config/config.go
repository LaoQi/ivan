package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/LaoQi/tanya/agent"
)

var Version = "dev"

const (
	DefaultSessionMode       = "global"
	DataDirSuffix            = ".local/share/ivan"
	ConfigPathSuffix         = ".config/ivan/config.yaml"
	TanyaConfigPathSuffix    = ".config/tanya/config.yaml"
	UserAgentURL             = "https://github.com/LaoQi/ivan"
	EnvPrefix                = "IVAN_"
	DefaultSocket            = "ivan.sock"
	DefaultBusBuffer         = 64
	DefaultMaxConcurrent     = 2
	DefaultShutdownSeconds   = 30
	DefaultSendSeconds       = 60
	DefaultMaxAgents         = 8
	DefaultIdleMinutes       = 30
	DefaultJobsDB            = "jobs.db"
	DefaultJobTimeoutSeconds = 900
)

type RunnerConfig struct {
	ContextWindowTokens int     `yaml:"context_window_tokens"`
	HandoffThreshold    float64 `yaml:"handoff_threshold"`
	AutoContinue        bool    `yaml:"auto_continue"`
	MaxTurnsPerTask     int     `yaml:"max_turns_per_task"`
	MaxTokensPerTask    int     `yaml:"max_tokens_per_task"`
	MaxAgents           int     `yaml:"max_agents"`
	IdleTimeoutMinutes  int     `yaml:"idle_timeout_minutes"`
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
	AllowProactive  bool     `yaml:"allow_proactive"`
	ReconnectMaxSec int      `yaml:"reconnect_max_sec"`
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
	MaxTokensPerDay   int    `yaml:"max_tokens_per_day"`
}

type Config struct {
	agent.Config  `yaml:",inline"`
	Runner        RunnerConfig   `yaml:"runner"`
	Daemon        DaemonConfig   `yaml:"daemon"`
	Channels      ChannelsConfig `yaml:"channels"`
	Schedule      ScheduleConfig `yaml:"schedule"`
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
			SessionMode:      DefaultSessionMode,
			UserAgent:        UserAgent(Version),
			DataDir:          filepath.Join(home, filepath.FromSlash(DataDirSuffix)),
			ArchiveThreshold: agent.DefaultArchiveThreshold,
			ArchiveKeep:      agent.DefaultArchiveKeep,
		},
		Runner: RunnerConfig{
			ContextWindowTokens: 128000,
			HandoffThreshold:    0.7,
			AutoContinue:        false,
			MaxTurnsPerTask:     8,
			MaxAgents:           DefaultMaxAgents,
			IdleTimeoutMinutes:  DefaultIdleMinutes,
		},
		Daemon: DaemonConfig{
			Socket:                 DefaultSocket,
			BusBuffer:              DefaultBusBuffer,
			MaxConcurrent:          DefaultMaxConcurrent,
			ShutdownTimeoutSeconds: DefaultShutdownSeconds,
			SendTimeoutSeconds:     DefaultSendSeconds,
		},
		Channels: ChannelsConfig{
			CLI: CLIConfig{Enabled: true},
			QQ:  QQConfig{MaxMessageLen: 1200, ReconnectMaxSec: 60},
		},
		Schedule: ScheduleConfig{
			Enabled:           true,
			JobsDB:            DefaultJobsDB,
			MissedPolicy:      "run",
			JobTimeoutSeconds: DefaultJobTimeoutSeconds,
		},
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
	set("SOCKET", &cfg.Daemon.Socket)
	set("QQ_APP_ID", &cfg.Channels.QQ.AppID)
	set("QQ_APP_SECRET", &cfg.Channels.QQ.AppSecret)
}

func (c *Config) normalize() {
	c.DataDir = expandHome(c.DataDir)
	if strings.TrimSpace(c.Daemon.Socket) == "" {
		c.Daemon.Socket = DefaultSocket
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
	if c.Runner.MaxAgents <= 0 {
		c.Runner.MaxAgents = DefaultMaxAgents
	}
	if c.Runner.IdleTimeoutMinutes <= 0 {
		c.Runner.IdleTimeoutMinutes = DefaultIdleMinutes
	}
	if c.Runner.MaxTurnsPerTask <= 0 {
		c.Runner.MaxTurnsPerTask = 8
	}
	if c.Channels.QQ.MaxMessageLen <= 0 {
		c.Channels.QQ.MaxMessageLen = 1200
	}
	if c.Channels.QQ.ReconnectMaxSec <= 0 {
		c.Channels.QQ.ReconnectMaxSec = 60
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

func (c *Config) QQEnabled() bool {
	return c.Channels.QQ.Enabled
}

func (c *Config) MediaDir() string { return filepath.Join(c.DataDir, "media") }

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
