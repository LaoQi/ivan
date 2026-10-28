package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"

	"github.com/LaoQi/battlelab/ctty"

	"github.com/LaoQi/ivan/config"
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
		return cmdRun(args[1:])
	case "daemon":
		return cmdDaemon(args[1:])
	case "send":
		return cmdSend(args[1:])
	case "status":
		return cmdStatus(args[1:])
	case "notify":
		return cmdNotify(args[1:])
	case "jobs":
		return cmdJobs(args[1:])
	case "cancel":
		return cmdCancel(args[1:])
	case "pause":
		return cmdPause(args[1:])
	case "resume":
		return cmdResume(args[1:])
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
  ivan run [--session=键] "指令"        前台执行一轮（不起 daemon）
  ivan daemon                          常驻运行（unix socket + 通道）
  ivan send [--session=键] [--timeout=秒] [--no-wait] "指令"
                                       经 socket 投给 daemon 并等回复
  ivan status [--json]                 查询 daemon 状态（未运行时读磁盘状态）
  ivan jobs [list] [--json]            列出计划任务（未运行时读作业库）
  ivan jobs rm <id> | ivan jobs run <id>
                                       取消/立即执行作业（需 daemon 运行）
  ivan notify [--title=标题] [--severity=级别] [--source=来源] "正文"
                                       发一条通知给用户（走 HTTP 入口的同一投递管线）
  ivan pause | ivan resume             暂停/恢复无人值守触发（定时作业与自动续跑）
  ivan cancel <会话键>                 取消某会话正在跑的回合
  ivan config                          输出默认配置示例
  ivan -v                              版本
`)
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ctty.WatchSignals()
	go func() {
		select {
		case <-ctty.Interrupted():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
