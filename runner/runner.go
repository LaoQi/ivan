package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/LaoQi/tanya/agent"
	"github.com/LaoQi/tanya/tools"

	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/prompt"
)

type ToolView interface {
	Args(ev agent.Event) (string, bool)
	Result(ev agent.Event) (string, bool)
}

type Runner struct {
	cfg    *config.Config
	ag     *agent.Agent
	log    *data.EventLog
	out    io.Writer
	views  map[string]ToolView
	failed bool
}

func New(cfg *config.Config, log *data.EventLog, out io.Writer) (*Runner, error) {
	ws, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	home, _ := os.UserHomeDir()
	var ag *agent.Agent
	list, err := tools.Standard(tools.Options{
		ShellOverride: os.Getenv("IVAN_SHELL"),
		Home:          home,
		Workspace:     func() string { return currentWorkspace(ag, ws) },
		LookPath:      exec.LookPath,
		ImageMaxBytes: cfg.ImageMaxBytes,
		ImageResize:   cfg.ImageResize,
		ImageDetail:   cfg.ImageDetail,
	})
	if err != nil {
		return nil, err
	}
	ag, err = agent.New(&cfg.Config,
		agent.WithSystemPrompt(prompt.System(ws, cfg.DataDir)),
		agent.WithTools(list...),
	)
	if err != nil {
		return nil, err
	}
	return &Runner{cfg: cfg, ag: ag, log: log, out: out, views: map[string]ToolView{}}, nil
}

func currentWorkspace(ag *agent.Agent, fallback string) string {
	if ag != nil {
		if ws := ag.Workspace(); ws != "" {
			return ws
		}
	}
	return fallback
}

func (r *Runner) Agent() *agent.Agent { return r.ag }

func (r *Runner) Ask(ctx context.Context, input string) error {
	sink := agent.Sinks(r.display, r.log.Emit)
	return r.ag.Ask(ctx, input, sink)
}

func (r *Runner) display(e agent.Event) {
	switch e.Kind {
	case agent.EventContent:
		fmt.Fprint(r.out, e.Text)
	case agent.EventToolStart:
		if v, ok := r.views[e.ToolName]; ok {
			if text, ok := v.Args(e); ok {
				fmt.Fprintf(r.out, "\n%s\n", text)
				return
			}
		}
		fmt.Fprintf(r.out, "\n▸ %s %s\n", e.ToolName, oneLine(e.ToolArgs, 200))
	case agent.EventToolEnd:
		if v, ok := r.views[e.ToolName]; ok {
			if text, ok := v.Result(e); ok {
				fmt.Fprint(r.out, text)
				return
			}
		}
		if text := strings.TrimSpace(e.Result.Text); text != "" {
			fmt.Fprintf(r.out, "%s\n", oneLine(text, 400))
		}
	case agent.EventTurnEnd:
		st := r.ag.Stats()
		if e.Turn.Failed {
			r.failed = true
			fmt.Fprintf(r.out, "error: 回合失败 %s\n", e.Turn.Duration.Round(1e6))
		}
		fmt.Fprintf(r.out, "[回合 %.1fs · 上下文 %d · 累计 %d tokens · 会话 %s]\n",
			e.Turn.Duration.Seconds(), st.ContextTokens, st.TotalTokens, sessionName(st.Session))
	}
}

func sessionName(path string) string {
	if path == "" {
		return "(未落盘)"
	}
	base := path
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	return strings.TrimSuffix(base, ".jsonl")
}

func oneLine(s string, limit int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > limit {
		s = s[:limit] + "…"
	}
	return s
}

func (r *Runner) Stats() agent.Stats { return r.ag.Stats() }

func (r *Runner) Close() error { return nil }
