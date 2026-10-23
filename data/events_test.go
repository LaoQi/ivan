package data

import (
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/battlelab/agent"
)

func TestToRecordRetry(t *testing.T) {
	r := toRecord(agent.Event{Kind: agent.EventRetry, Retry: agent.RetryInfo{
		Attempt: 2, Max: 3, Delay: 1500 * time.Millisecond, Err: "API 错误 503: boom",
	}})
	if r.Kind != "retry" {
		t.Errorf("事件类型应为 retry: %q", r.Kind)
	}
	if r.Attempt != 2 || r.MaxRetries != 3 || r.DelayMS != 1500 {
		t.Errorf("重试字段未落盘: %+v", r)
	}
	if !strings.Contains(r.Text, "503") {
		t.Errorf("错误原文应完整记入: %q", r.Text)
	}
}

func TestKindNameCoversRetry(t *testing.T) {
	if got := kindName(agent.EventRetry); got != "retry" {
		t.Errorf("EventRetry 应映射为 retry: %q", got)
	}
}
