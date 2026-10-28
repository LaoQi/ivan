package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/ivan/channel/cli"
	"github.com/LaoQi/ivan/config"
	"github.com/LaoQi/ivan/data"
	"github.com/LaoQi/ivan/internal/fakellm"
)

func httpConfig(t *testing.T, srv *fakellm.Server) *config.Config {
	t.Helper()
	cfg := testConfig(t, srv)
	cfg.HTTP.Enabled = true
	cfg.HTTP.Listen = "127.0.0.1:0"
	cfg.HTTP.NotifyPerMinute = 100
	cfg.HTTP.RequestPerMinute = 100
	cfg.HTTP.Targets = []config.HTTPTargetConfig{{Name: "main", Channel: "cli", ChatID: "unused"}}
	return cfg
}

func httpListen(t *testing.T, cfg *config.Config) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := fetchStatus(t, cfg)
		if st.HTTP != nil && st.HTTP.Running && st.HTTP.Listen != "" {
			return st.HTTP.Listen
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("HTTP 入口未就绪")
	return ""
}

func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if s, ok := body.(string); ok {
		buf.WriteString(s)
	} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode: %v", err)
	}
	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// 通知经 HTTP 入口投递到真实通道（cli 目标无在等请求 → 计 unmatched，回执里如实反映）。
func TestHTTPNotifyFanoutReportsPerTargetResult(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "unused"})
	defer srv.Close()
	cfg := httpConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	base := "http://" + httpListen(t, cfg)
	code, out := postJSON(t, base+"/notify", map[string]string{
		"title": "构建失败", "body": "job #123 exit 1", "severity": "error", "source": "ci",
	})
	if code != http.StatusServiceUnavailable {
		t.Fatalf("cli 目标无在等请求应报全部失败: %d %v", code, out)
	}
	targets, _ := out["targets"].([]any)
	if len(targets) != 1 {
		t.Fatalf("targets = %v", out["targets"])
	}
	first := targets[0].(map[string]any)
	if first["name"] != "main" || first["result"] != "failed" || !strings.Contains(fmt.Sprint(first["error"]), "没有可投递目标") {
		t.Fatalf("逐目标结果未如实回报: %v", first)
	}
	st := fetchStatus(t, cfg)
	if st.HTTP == nil || st.HTTP.NotifyAccepted != 1 || st.HTTP.NotifyFailed != 1 {
		t.Fatalf("HTTP 统计异常: %+v", st.HTTP)
	}
}

// /request 异步：提交即返回会话键，轮询查询端点拿到回合终局正文。
func TestHTTPRequestAsyncPollResult(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "已部署，冒烟通过"})
	defer srv.Close()
	cfg := httpConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)

	base := "http://" + httpListen(t, cfg)
	code, out := postJSON(t, base+"/request", map[string]string{
		"source": "ci", "identity": "build-bot@runner-1", "purpose": "部署 staging", "text": "部署 release-2.3.1",
	})
	if code != http.StatusAccepted || out["status"] != "accepted" {
		t.Fatalf("提交应立即受理: %d %v", code, out)
	}
	session, _ := out["session"].(string)
	if session != "http:ci" {
		t.Fatalf("会话键 = %v", session)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		qr, err := http.Get(base + "/request?session=" + session)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		var q map[string]any
		_ = json.NewDecoder(qr.Body).Decode(&q)
		qr.Body.Close()
		if q["status"] == "done" {
			if q["reply"] != "已部署，冒烟通过" {
				t.Fatalf("查询应拿到回合终局正文: %v", q["reply"])
			}
			if q["finished_at"] == nil {
				t.Fatalf("应带完成时间: %v", q)
			}
			break
		}
		if q["status"] != "running" && q["status"] != "unknown" {
			t.Fatalf("中间态应为 running/unknown: %v", q)
		}
		time.Sleep(30 * time.Millisecond)
	}
	st := fetchStatus(t, cfg)
	if st.Runner.Turns != 1 || st.HTTP.RequestAccepted != 1 {
		t.Fatalf("回合或计数异常: turns=%d http=%+v", st.Runner.Turns, st.HTTP)
	}
	stt, err := data.LoadState(cfg.StatePath())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if _, ok := stt.Get("http:ci"); !ok {
		t.Fatalf("外部请求会话映射未落盘: %+v", stt.Snapshot())
	}
}

// 回合里的 send_message 广播到全部目标；不可达目标（cli 无在等请求）如实进入 partial 汇总，
// 模型看到的是真实投递结果而不是假 ok。
func TestHTTPSendMessageToolFanout(t *testing.T) {
	srv := fakellm.New(
		fakellm.Step{Tools: []fakellm.ToolCall{{ID: "c1", Name: "send_message", Args: `{"text":"构建失败已确认，请指示"}`}}},
		fakellm.Step{Content: "已通知用户"},
	)
	defer srv.Close()
	cfg := httpConfig(t, srv)
	cfg.HTTP.Targets = []config.HTTPTargetConfig{{Name: "main", Channel: "cli", ChatID: "user-cli"}}
	run := start(t, cfg)
	defer run.stop(t)

	base := "http://" + httpListen(t, cfg)
	code, out := postJSON(t, base+"/request", map[string]string{
		"source": "ci", "purpose": "通知用户", "text": "部署失败，告知用户",
	})
	if code != http.StatusAccepted {
		t.Fatalf("提交应受理: %d %v", code, out)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reqs := srv.Requests()
		if len(reqs) >= 2 {
			toolReceipt := ""
			for _, m := range reqs[len(reqs)-1].Messages {
				if strings.Contains(string(m), `"role":"tool"`) {
					toolReceipt = string(m)
				}
			}
			if toolReceipt != "" {
				if !strings.Contains(toolReceipt, "error: 全部目标投递失败") || !strings.Contains(toolReceipt, "main") {
					t.Fatalf("投递失败应如实回报给模型: %s", toolReceipt)
				}
				return
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("未观察到 send_message 的工具回执")
}

// 本地入口 ivan notify 走 socket，复用同一 fan-out 管线（这里用挂起的 cli 请求当投递目标）。
func TestNotifyControlOp(t *testing.T) {
	// 用慢回复把 cli 请求的 pending 留住，通知才有可投递目标。
	srv := fakellm.NewDelay(2*time.Second, fakellm.Step{Content: "占位回复"})
	defer srv.Close()
	cfg := httpConfig(t, srv)
	cfg.HTTP.Targets = []config.HTTPTargetConfig{{Name: "main", Channel: "cli", ChatID: "notify-target"}}
	run := start(t, cfg)
	defer run.stop(t)

	holder, err := cli.Dial(cfg.SocketPath(), 5*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer holder.Close()
	if err := holder.Do(cli.Frame{ID: "notify-target", Op: cli.OpSend, Text: "占位请求"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	payload := `{"title":"磁盘告警","body":"根分区 92%","severity":"warn","source":"node-exporter"}`
	raw, err := cli.Call(cfg.SocketPath(), cli.OpNotify, payload, 5*time.Second)
	if err != nil {
		t.Fatalf("notify op: %v", err)
	}
	var res struct {
		Status  string `json:"status"`
		Targets []struct {
			Name   string `json:"name"`
			Result string `json:"result"`
		} `json:"targets"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("解析回执: %v", err)
	}
	if res.Status != "delivered" || len(res.Targets) != 1 || res.Targets[0].Name != "main" || res.Targets[0].Result != "sent" {
		t.Fatalf("回执缺少逐目标结果: %s", raw)
	}
	st := fetchStatus(t, cfg)
	if st.HTTP.NotifyAccepted != 1 {
		t.Fatalf("本地入口未计数: %+v", st.HTTP)
	}
}

func TestNotifyControlOpUnavailable(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "unused"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	cfg.HTTP.Enabled = false
	run := start(t, cfg)
	defer run.stop(t)
	if _, err := cli.Call(cfg.SocketPath(), cli.OpNotify, `{"body":"x"}`, 5*time.Second); err == nil ||
		!strings.Contains(err.Error(), "未启用") {
		t.Fatalf("未启用时应明确报错: %v", err)
	}
}

func TestHTTPEnabledByDefault(t *testing.T) {
	srv := fakellm.New(fakellm.Step{Content: "unused"})
	defer srv.Close()
	cfg := testConfig(t, srv)
	run := start(t, cfg)
	defer run.stop(t)
	st := fetchStatus(t, cfg)
	if st.HTTP == nil || !st.HTTP.Running || st.HTTP.Targets != 0 {
		t.Fatalf("默认应启用 HTTP 入口（targets 可为空）: %+v", st.HTTP)
	}
	base := "http://" + httpListen(t, cfg)
	code, out := postJSON(t, base+"/notify", map[string]string{"title": "t", "body": "b"})
	if code != http.StatusServiceUnavailable || !strings.Contains(fmt.Sprint(out["message"]), "未配置通知目标") {
		t.Fatalf("无 targets 时 /notify 应明确失败: %d %v", code, out)
	}
	code, out = postJSON(t, base+"/request", map[string]string{"source": "ci", "purpose": "p", "text": "只读检查磁盘"})
	if code != http.StatusAccepted {
		t.Fatalf("无 targets 不影响 /request: %d %v", code, out)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		qr, err := http.Get(base + "/request?session=http:ci")
		if err == nil {
			var q map[string]any
			_ = json.NewDecoder(qr.Body).Decode(&q)
			qr.Body.Close()
			if q["status"] == "done" {
				return
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("/request 回合未完成")
}
