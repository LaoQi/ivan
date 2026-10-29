package qq

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LaoQi/ivan/bus"
	"github.com/LaoQi/ivan/channel"
)

func newStateChannel(t *testing.T, f *fakeAPI, mutate func(*Config)) (*Channel, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "qq", "channel.json")
	ch := newTestChannel(t, f, func(c *Config) {
		c.StatePath = path
		if mutate != nil {
			mutate(c)
		}
	})
	return ch, path
}

func readState(t *testing.T, path string) channelState {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取状态文件: %v", err)
	}
	var doc channelState
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("解析状态文件: %v", err)
	}
	return doc
}

func TestInitStateGeneratesTokenOnce(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	first := readState(t, path)
	if len(first.ClaimToken) != tokenHalf*2+1 {
		t.Fatalf("令牌形态异常: %q", first.ClaimToken)
	}
	if len(first.NoticeTo) != 0 {
		t.Fatalf("初始不应有目标: %v", first.NoticeTo)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("状态文件权限应为 0600，得到 %o", perm)
	}
	ch.initState()
	second := readState(t, path)
	if second.ClaimToken != first.ClaimToken {
		t.Fatalf("重启不应轮换令牌: %q -> %q", first.ClaimToken, second.ClaimToken)
	}
}

func TestClaimTokenClaimsTargetAndRotates(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	tok := readState(t, path).ClaimToken

	if !ch.claimToken(directEvent("m-1", "u-1", tok)) {
		t.Fatal("令牌应被识别")
	}
	doc := readState(t, path)
	if len(doc.NoticeTo) != 1 || doc.NoticeTo[0] != "u-1" {
		t.Fatalf("认领后目标不对: %v", doc.NoticeTo)
	}
	if doc.ClaimToken == tok || doc.ClaimToken == "" {
		t.Fatalf("认领后应轮换令牌: %q", doc.ClaimToken)
	}
	if doc.ClaimedAt.IsZero() {
		t.Fatal("应记录认领时间")
	}
	calls := f.calls()
	if len(calls) != 1 {
		t.Fatalf("应回复一条确认，得到 %d 次调用", len(calls))
	}
	if calls[0]["msg_id"] != "m-1" {
		t.Fatalf("确认走被动回复应带 msg_id: %+v", calls[0])
	}
	if content, _ := calls[0]["content"].(string); !strings.Contains(content, doc.ClaimToken) {
		t.Fatalf("确认回复应带新令牌: %q", content)
	}
}

func TestClaimTokenIgnoresWrongOrGroupToken(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	tok := readState(t, path).ClaimToken

	if ch.claimToken(directEvent("m-1", "u-1", "错误令牌")) {
		t.Fatal("错误令牌不应被识别")
	}
	if ch.claimToken(groupEvent("m-2", "g-1", "u-1", tok)) {
		t.Fatal("群聊令牌不应被识别")
	}
	if got := readState(t, path).NoticeTo; len(got) != 0 {
		t.Fatalf("不应产生目标: %v", got)
	}
}

func TestClaimTokenRejectsSameLengthWrongToken(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	tok := readState(t, path).ClaimToken

	wrong := []byte(tok)
	for i, b := range wrong {
		if b == '-' {
			continue
		}
		if b == 'A' {
			wrong[i] = 'B'
		} else {
			wrong[i] = 'A'
		}
		break
	}
	if string(wrong) == tok {
		t.Fatal("构造的错误令牌与原令牌相同")
	}
	if len(wrong) != len(tok) {
		t.Fatalf("错误令牌长度应与真令牌一致: %q vs %q", wrong, tok)
	}
	if ch.claimToken(directEvent("m-1", "u-1", string(wrong))) {
		t.Fatal("同长度不同内容的令牌不应被接受")
	}
	if got := readState(t, path).NoticeTo; len(got) != 0 {
		t.Fatalf("不应产生目标: %v", got)
	}
	if len(f.calls()) != 0 {
		t.Fatalf("不应有任何发送: %v", f.calls())
	}
}

func TestClaimTokenAcceptsSpacesAndLowercase(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	tok := readState(t, path).ClaimToken

	if !ch.claimToken(directEvent("m-1", "u-2", "  "+strings.ToLower(tok)+"  ")) {
		t.Fatal("应容忍大小写与空白")
	}
	if got := readState(t, path).NoticeTo; len(got) != 1 || got[0] != "u-2" {
		t.Fatalf("目标不对: %v", got)
	}
}

func TestClaimTokenRespectsAllowFrom(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, func(c *Config) { c.AllowFrom = []string{"u-ok"} })
	ch.initState()
	tok := readState(t, path).ClaimToken

	if !ch.claimToken(directEvent("m-1", "u-bad", tok)) {
		t.Fatal("白名单外的令牌消息应被通道吞掉，不应进入 agent")
	}
	if got := readState(t, path).NoticeTo; len(got) != 0 {
		t.Fatalf("白名单外不应认领成功: %v", got)
	}
	if len(f.calls()) != 0 {
		t.Fatalf("白名单外不应回确认: %v", f.calls())
	}
}

func TestClaimTokenOverridesExistingTarget(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	ch.claimToken(directEvent("m-1", "u-1", readState(t, path).ClaimToken))
	next := readState(t, path).ClaimToken
	ch.claimToken(directEvent("m-2", "u-2", next))
	doc := readState(t, path)
	if len(doc.NoticeTo) != 1 || doc.NoticeTo[0] != "u-2" {
		t.Fatalf("新账号应覆盖旧目标: %v", doc.NoticeTo)
	}
}

func TestNoticeEmptyWithoutTargets(t *testing.T) {
	f := newFakeAPI(t)
	ch, _ := newStateChannel(t, f, nil)
	ch.initState()
	if got := ch.Notice(context.Background(), "有通知"); len(got) != 0 {
		t.Fatalf("无目标应返回空结果: %v", got)
	}
	if len(f.calls()) != 0 {
		t.Fatalf("无目标不应发请求: %v", f.calls())
	}
	if got := ch.Notice(context.Background(), "   "); len(got) != 0 {
		t.Fatalf("空内容应返回空结果: %v", got)
	}
}

func TestNoticeSendsProactiveText(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	ch.claimToken(directEvent("m-1", "u-1", readState(t, path).ClaimToken))
	f.sent = nil

	results := ch.Notice(context.Background(), "构建失败 — ci")
	if len(results) != 1 || results[0].Target != "u-1" || results[0].Err != nil {
		t.Fatalf("结果不对: %+v", results)
	}
	calls := f.calls()
	if len(calls) != 1 {
		t.Fatalf("应发一条: %d", len(calls))
	}
	if _, ok := calls[0]["msg_id"]; ok {
		t.Fatalf("通知应走主动消息，不带 msg_id: %+v", calls[0])
	}
	if mt, _ := calls[0]["msg_type"].(float64); int(mt) != 0 {
		t.Fatalf("通知应为纯文本 msg_type=0: %+v", calls[0])
	}
	if calls[0]["content"] != "构建失败 — ci" {
		t.Fatalf("正文不对: %+v", calls[0])
	}
	if path := f.paths[0]; path != "/v2/users/u-1/messages" {
		t.Fatalf("应打单聊接口，得到 %q", path)
	}
}

func TestNoticeSplitsLongContent(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, func(c *Config) { c.MaxMessageLen = 10 })
	ch.initState()
	ch.claimToken(directEvent("m-1", "u-1", readState(t, path).ClaimToken))
	f.sent = nil

	long := strings.Repeat("字", 25)
	if got := ch.Notice(context.Background(), long); len(got) != 1 || got[0].Err != nil {
		t.Fatalf("结果不对: %+v", got)
	}
	calls := f.calls()
	if len(calls) < 3 {
		t.Fatalf("超长正文应切分，实际 %d 片", len(calls))
	}
	total := 0
	for _, c := range calls {
		s, _ := c["content"].(string)
		if n := len([]rune(s)); n > 10 {
			t.Fatalf("单片超过上限: %d %q", n, s)
		}
		total += len([]rune(s))
	}
	if total != 25 {
		t.Fatalf("切分后总字符数应不变，得到 %d", total)
	}
}

func TestNoticeFailureIsReported(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	ch.claimToken(directEvent("m-1", "u-1", readState(t, path).ClaimToken))
	if err := f.deny("u-1"); err != nil {
		t.Fatal(err)
	}
	results := ch.Notice(context.Background(), "告警")
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("失败应如实回报: %+v", results)
	}
}

func TestStateCorruptedNotRebuilt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channel.json")
	if err := os.WriteFile(path, []byte("{坏掉"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadState(path); err == nil {
		t.Fatal("损坏的状态文件应报错")
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "{坏掉" {
		t.Fatalf("不应重建原文件: %q %v", raw, err)
	}
}

func TestNoticerCapability(t *testing.T) {
	f := newFakeAPI(t)
	ch, _ := newStateChannel(t, f, nil)
	var _ channel.Noticer = ch
	if _, ok := any(ch).(channel.Noticer); !ok {
		t.Fatal("qq 通道应实现 channel.Noticer")
	}
	_ = time.Now()
}

// Manager 按 Noticer 能力遍历通道时能发现 qq 通道，认领后的目标真的收到主动消息。
func TestNoticeViaManager(t *testing.T) {
	f := newFakeAPI(t)
	ch, path := newStateChannel(t, f, nil)
	ch.initState()
	ch.claimToken(directEvent("m-1", "u-9", readState(t, path).ClaimToken))
	f.sent = nil

	mgr := channel.New(bus.New(4, 4))
	if err := mgr.Register(ch); err != nil {
		t.Fatalf("register: %v", err)
	}
	var found channel.Noticer
	for _, c := range mgr.All() {
		if n, ok := c.(channel.Noticer); ok {
			found = n
		}
	}
	if found == nil {
		t.Fatal("Manager.All 应能发现实现 Noticer 的 qq 通道")
	}
	results := found.Notice(context.Background(), "部署完成")
	if len(results) != 1 || results[0].Target != "u-9" || results[0].Err != nil {
		t.Fatalf("结果不对: %+v", results)
	}
	if len(f.calls()) != 1 {
		t.Fatalf("应真实发出: %v", f.calls())
	}
}
