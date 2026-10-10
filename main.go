package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/LaoQi/tanya/ctty"

	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/runner"
)

var (
	version   = "dev"
	buildTime = ""
)

//go:embed config.example.yaml
var configExample string

const envAllowRoot = "IVAN_ALLOW_ROOT"

func main() {
	ctty.EnsureUTF8()
	defer ctty.RestoreUTF8()
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	config.Version = version
	if code, done := dispatchMeta(args); done {
		return code
	}
	if err := rootRefusal(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	switch args[0] {
	case "run":
		return runOnce(args[1:])
	}
	usage()
	return 1
}

func dispatchMeta(args []string) (int, bool) {
	if len(args) == 0 {
		usage()
		return 1, true
	}
	switch args[0] {
	case "-v", "--version", "version":
		if buildTime != "" {
			fmt.Printf("ivan %s (%s)\n", version, buildTime)
		} else {
			fmt.Printf("ivan %s\n", version)
		}
		return 0, true
	case "help", "-h", "--help":
		usage()
		return 0, true
	case "config":
		fmt.Print(configExample)
		return 0, true
	}
	return 0, false
}

func rootRefusal() error {
	if !ctty.IsRoot() {
		return nil
	}
	if os.Getenv(envAllowRoot) == "1" {
		return nil
	}
	return fmt.Errorf("ivan 拒绝以 root 运行（如确需，设置 %s=1）", envAllowRoot)
}

func usage() {
	fmt.Fprint(os.Stderr, `ivan - 常驻的极简 agent

用法:
  ivan run "指令"      前台执行一轮（调试与无通道时使用）
  ivan config          输出默认配置示例
  ivan -v              版本
`)
}

func runOnce(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: ivan run \"指令\"")
		return 1
	}
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	log, err := data.NewEventLog(filepath.Join(cfg.DataDir, "events"), 256)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	defer log.Close()
	r, err := runner.New(cfg, log, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctty.WatchSignals()
	go func() {
		select {
		case <-ctty.Interrupted():
			cancel()
		case <-ctx.Done():
		}
	}()
	if err := r.Ask(ctx, strings.Join(args, " ")); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	return ctty.ExitStatus()
}
