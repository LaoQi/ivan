package qqbot

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestTokenExpiresInString(t *testing.T) {
	var v tokenResult
	if err := json.Unmarshal([]byte(`{"access_token":"x","expires_in":"6468"}`), &v); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if v.ExpiresIn.Int() != 6468 {
		t.Fatalf("expires_in 期望 6468，得到 %d", v.ExpiresIn.Int())
	}
	if v.AccessToken != "x" {
		t.Fatalf("access_token 解析错误")
	}
}

func TestTokenExpiresInNumber(t *testing.T) {
	var v tokenResult
	if err := json.Unmarshal([]byte(`{"access_token":"y","expires_in":3600}`), &v); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if v.ExpiresIn.Int() != 3600 {
		t.Fatalf("expires_in 期望 3600，得到 %d", v.ExpiresIn.Int())
	}
}

func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   Class
		code   int
	}{
		{"限流", http.StatusTooManyRequests, `{"message":"too many"}`, ClassRateLimit, 0},
		{"认证", http.StatusUnauthorized, `{"err_code":11244,"message":"token 校验失败"}`, ClassAuth, 11244},
		{"无权限", http.StatusForbidden, `{"err_code":11253,"message":"无权限"}`, ClassPermission, 11253},
		{"窗口过期", http.StatusBadRequest, `{"err_code":40034005,"message":"回复消息msg_id已过期"}`, ClassExpired, 40034005},
		{"参数错误", http.StatusBadRequest, `{"err_code":10001,"message":"参数错误"}`, ClassBadRequest, 10001},
		{"服务端", http.StatusInternalServerError, ``, ClassServer, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := errorFromResponse("send", tc.status, []byte(tc.body))
			if err.Class != tc.want {
				t.Fatalf("分类期望 %s，得到 %s", tc.want, err.Class)
			}
			if tc.code != 0 && err.Code != tc.code {
				t.Fatalf("错误码期望 %d，得到 %d", tc.code, err.Code)
			}
			if err.Op != "send" {
				t.Fatalf("Op 丢失")
			}
		})
	}
}

func TestErrorRedactsSecret(t *testing.T) {
	err := errorFromResponse("getAppAccessToken", http.StatusBadRequest,
		[]byte(`{"message":"invalid clientSecret: super-secret-value"}`))
	if got := err.Error(); strings.Contains(got, "super-secret-value") {
		t.Fatalf("错误信息泄漏了凭证: %s", got)
	}
}

func TestErrorHelpers(t *testing.T) {
	rate := errorFromResponse("send", http.StatusTooManyRequests, nil)
	if !IsRetryable(rate) {
		t.Fatalf("429 应可重试")
	}
	expired := errorFromResponse("send", http.StatusBadRequest, []byte(`{"err_code":40034005}`))
	if !IsWindowExpired(expired) {
		t.Fatalf("40034005 应判定窗口过期")
	}
	auth := errorFromResponse("send", http.StatusUnauthorized, nil)
	if !IsAuthFailed(auth) {
		t.Fatalf("401 应判定认证失败")
	}
	bad := errorFromResponse("send", http.StatusBadRequest, []byte(`{"err_code":10001}`))
	if IsRetryable(bad) {
		t.Fatalf("参数错误不应重试")
	}
	var nilErr error
	if IsRetryable(nilErr) || IsAuthFailed(nilErr) {
		t.Fatalf("nil 错误不应命中")
	}
}

func TestDedupCache(t *testing.T) {
	c := newMsgCache(time.Minute, 4)
	if c.Seen("a") {
		t.Fatalf("首次不应命中")
	}
	if !c.Seen("a") {
		t.Fatalf("重复应命中")
	}
	if c.Seen("") {
		t.Fatalf("空 id 不应命中")
	}
	for _, id := range []string{"b", "c", "d", "e", "f"} {
		c.Seen(id)
	}
	if c.Len() > 4 {
		t.Fatalf("容量上限失效: %d", c.Len())
	}
}

func TestDedupExpiry(t *testing.T) {
	c := newMsgCache(20*time.Millisecond, 100)
	c.Seen("x")
	time.Sleep(40 * time.Millisecond)
	if c.Seen("x") {
		t.Fatalf("TTL 过后不应命中")
	}
}

func TestConnectReadyAndMessage(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)

	select {
	case r := <-h.ready:
		if r.SessionID != "sess-1" || r.Username != "测试机器人" {
			t.Fatalf("READY 信息错误: %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("未收到 READY")
	}

	select {
	case d := <-f.identified:
		if d.Intents != DefaultIntents {
			t.Fatalf("intents 期望 %d，得到 %d", DefaultIntents, d.Intents)
		}
		if d.Shard != [2]int{0, 1} {
			t.Fatalf("shard 期望 [0 1]，得到 %v", d.Shard)
		}
		if d.Token != wsToken("tok-abc") {
			t.Fatalf("identify token 期望带 QQBot 前缀，得到 %q", d.Token)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未收到 identify")
	}
	if !c.Stats().Connected {
		t.Fatalf("应处于已连接状态")
	}
}

func TestMessageRoutingDirectAndGroup(t *testing.T) {
	f := newFakeGateway(t)
	_, h := startClient(t, f)
	<-h.ready

	// 直接通过服务端连接推送两条消息
	f.mu.Lock()
	f.pending = []map[string]any{
		{"kind": EventC2CMessage, "d": map[string]any{
			"id": "m-c2c-1", "content": " 你好 ", "timestamp": "2026-10-20T10:00:00Z",
			"author": map[string]any{"user_openid": "u-1"},
		}},
		{"kind": EventGroupAt, "d": map[string]any{
			"id": "m-grp-1", "content": "hi group", "group_openid": "g-1",
			"author": map[string]any{"member_openid": "mem-1", "union_openid": "un-1"},
		}},
	}
	f.mu.Unlock()
	f.flush(t)

	got := map[string]MessageEvent{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-h.msgs:
			got[ev.ID] = ev
		case <-time.After(3 * time.Second):
			t.Fatalf("只收到 %d 条消息", i)
		}
	}
	c2c := got["m-c2c-1"]
	if c2c.UserOpenID != "u-1" || c2c.Content != " 你好 " || c2c.Kind != EventC2CMessage {
		t.Fatalf("单聊消息映射错误: %+v", c2c)
	}
	grp := got["m-grp-1"]
	if grp.GroupOpenID != "g-1" || grp.UserOpenID != "mem-1" || grp.Kind != EventGroupAt {
		t.Fatalf("群消息映射错误: %+v", grp)
	}
}

func TestDuplicateMessageDropped(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready

	f.mu.Lock()
	f.pending = []map[string]any{
		{"kind": EventC2CMessage, "d": map[string]any{"id": "dup-1", "content": "a", "author": map[string]any{"user_openid": "u"}}},
		{"kind": EventC2CMessage, "d": map[string]any{"id": "dup-1", "content": "a", "author": map[string]any{"user_openid": "u"}}},
	}
	f.mu.Unlock()
	f.flush(t)

	select {
	case <-h.msgs:
	case <-time.After(2 * time.Second):
		t.Fatal("未收到消息")
	}
	select {
	case ev := <-h.msgs:
		t.Fatalf("重复消息被投递: %+v", ev)
	case <-time.After(300 * time.Millisecond):
	}
	if c.Stats().Duplicates != 1 {
		t.Fatalf("去重计数期望 1，得到 %d", c.Stats().Duplicates)
	}
}

func TestHeartbeatSentAndAcked(t *testing.T) {
	f := newFakeGateway(t)
	f.mu.Lock()
	f.helloMs = 120
	f.mu.Unlock()
	c, h := startClient(t, f)
	<-h.ready

	select {
	case seq := <-f.heartbeats:
		_ = seq
	case <-time.After(2 * time.Second):
		t.Fatal("未收到心跳")
	}
	waitFor(t, time.Second, func() bool { return !c.Stats().LastHeartbeat.IsZero() }, "心跳确认")
}

func TestSendTextUsesAuthorizationAndSeq(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready

	ctx := context.Background()
	id, err := c.SendText(ctx, Target{UserOpenID: "u-1"}, "hello", "m-1", 2)
	if err != nil {
		t.Fatalf("SendText: %v", err)
	}
	if id != "msg-out-1" {
		t.Fatalf("返回消息 id 错误: %s", id)
	}
	call, ok := f.lastSend()
	if !ok {
		t.Fatal("服务端未收到发送请求")
	}
	if call.Path != "/v2/users/u-1/messages" {
		t.Fatalf("路径错误: %s", call.Path)
	}
	if call.Body["content"] != "hello" || call.Body["msg_id"] != "m-1" {
		t.Fatalf("请求体错误: %+v", call.Body)
	}
	if n, _ := call.Body["msg_seq"].(float64); int(n) != 2 {
		t.Fatalf("msg_seq 期望 2，得到 %v", call.Body["msg_seq"])
	}
	if mt, _ := call.Body["msg_type"].(float64); int(mt) != MsgTypeText {
		t.Fatalf("msg_type 期望 0")
	}
	if c.Stats().Sent != 1 {
		t.Fatalf("发送计数期望 1，得到 %d", c.Stats().Sent)
	}
}

func TestSendGroupPath(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready

	if _, err := c.SendText(context.Background(), Target{GroupOpenID: "g-9"}, "群消息", "m-2", 1); err != nil {
		t.Fatalf("SendText: %v", err)
	}
	call, _ := f.lastSend()
	if call.Path != "/v2/groups/g-9/messages" {
		t.Fatalf("群路径错误: %s", call.Path)
	}
}

func TestSendWithoutTarget(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready

	_, err := c.SendText(context.Background(), Target{}, "x", "m-1", 1)
	if !errors.Is(err, errNoTarget) {
		t.Fatalf("无目标应返回 errNoTarget，得到 %v", err)
	}
}

func TestSendWindowExpired(t *testing.T) {
	f := newFakeGateway(t)
	f.mu.Lock()
	f.sendStatus = http.StatusBadRequest
	f.sendBody = `{"err_code":40034005,"message":"回复消息msg_id已过期"}`
	f.mu.Unlock()
	c, h := startClient(t, f)
	<-h.ready

	_, err := c.SendText(context.Background(), Target{UserOpenID: "u-1"}, "x", "m-1", 5)
	if !IsWindowExpired(err) {
		t.Fatalf("应判定窗口过期，得到 %v", err)
	}
	if c.Stats().SendErrors != 1 {
		t.Fatalf("发送错误计数期望 1，得到 %d", c.Stats().SendErrors)
	}
}

func TestSendAuthFailure(t *testing.T) {
	f := newFakeGateway(t)
	f.mu.Lock()
	f.sendStatus = http.StatusUnauthorized
	f.sendBody = `{"err_code":11244,"message":"token 校验失败"}`
	f.mu.Unlock()
	c, h := startClient(t, f)
	<-h.ready

	_, err := c.SendText(context.Background(), Target{UserOpenID: "u-1"}, "x", "m-1", 5)
	if !IsAuthFailed(err) {
		t.Fatalf("应判定认证失败，得到 %v", err)
	}
}

func TestStopClosesConnection(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if c.Stats().Connected {
		t.Fatalf("停止后不应仍标记已连接")
	}
}

func TestNewRequiresCredentials(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatalf("缺凭证应报错")
	}
}

func TestTargetHelpers(t *testing.T) {
	cases := []struct {
		t       Target
		group   bool
		msgPath string
	}{
		{Target{UserOpenID: "u"}, false, "/v2/users/u/messages"},
		{Target{GroupOpenID: "g"}, true, "/v2/groups/g/messages"},
	}
	for _, tc := range cases {
		if tc.t.IsGroup() != tc.group {
			t.Fatalf("IsGroup 错误: %+v", tc.t)
		}
		if tc.t.messagesPath() != tc.msgPath {
			t.Fatalf("messagesPath 错误: %s", tc.t.messagesPath())
		}
	}
}

func TestResumeAfterDisconnect(t *testing.T) {
	f := newFakeGateway(t)
	f.mu.Lock()
	f.helloMs = 200
	f.mu.Unlock()
	c, h := startClient(t, f)
	<-h.ready

	// 确认已进入 resume 的候选状态（有 session_id）
	if c.SessionID() != "sess-1" {
		t.Fatalf("session_id 期望 sess-1，得到 %q", c.SessionID())
	}
	f.killConn()

	select {
	case r := <-f.resumed:
		if r.SessionID != "sess-1" {
			t.Fatalf("resume session 期望 sess-1，得到 %q", r.SessionID)
		}
		if r.Token != wsToken("tok-abc") {
			t.Fatalf("resume token 期望带 QQBot 前缀，得到 %q", r.Token)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("断线后未尝试 resume")
	}
}

func TestInvalidSessionReidentifies(t *testing.T) {
	f := newFakeGateway(t)
	f.mu.Lock()
	f.helloMs = 150
	f.mu.Unlock()
	_, h := startClient(t, f)
	<-h.ready

	// 服务端下发 op9，客户端应丢弃 session 并重新 identify
	f.connWrite(map[string]any{"op": opInvalidSession, "d": true})

	select {
	case <-f.identified:
		// 重新 identify 完成（首个 identify 已被消费）
	case <-time.After(3 * time.Second):
		t.Fatal("op9 后未重新 identify")
	}
}

func TestHeartbeatTimeoutTriggersReconnect(t *testing.T) {
	f := newFakeGateway(t)
	f.mu.Lock()
	f.helloMs = 100
	f.ackHeartbeat = false
	f.mu.Unlock()
	c, h := startClient(t, f)
	<-h.ready

	waitFor(t, 5*time.Second, func() bool {
		return c.Stats().Reconnects > 0
	}, "心跳未确认应触发重连")
}

func TestWSTokenPrefix(t *testing.T) {
	if got := wsToken("abc"); got != "QQBot abc" {
		t.Fatalf("应加前缀，得到 %q", got)
	}
	if got := wsToken("QQBot abc"); got != "QQBot abc" {
		t.Fatalf("已有前缀不应重复，得到 %q", got)
	}
	if got := wsToken(""); got != "" {
		t.Fatalf("空 token 不应加前缀，得到 %q", got)
	}
}

func TestIdentifyPropertiesAnnounceClient(t *testing.T) {
	f := newFakeGateway(t)
	cfg := testConfig(f)
	cfg.UserAgent = "ivan/v9.9.9 (+https://example.invalid)"
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := newRecordingHandler()
	if err := c.Start(context.Background(), h); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	<-h.ready

	select {
	case d := <-f.identified:
		if d.Properties["$browser"] != "ivan/v9.9.9" {
			t.Fatalf("$browser 期望 ivan/v9.9.9，得到 %q", d.Properties["$browser"])
		}
		if d.Properties["$os"] != runtime.GOOS {
			t.Fatalf("$os 期望 %s，得到 %q", runtime.GOOS, d.Properties["$os"])
		}
		if d.Properties["$device"] != "ivan" {
			t.Fatalf("$device 期望 ivan，得到 %q", d.Properties["$device"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未收到 identify")
	}
}

func TestIdentifyPropertiesDefaultBrowser(t *testing.T) {
	c := &Client{cfg: Config{}}
	props := c.identifyProperties()
	if props["$browser"] != "ivan" {
		t.Fatalf("无 UA 时 $browser 应退回 ivan，得到 %q", props["$browser"])
	}
}

func TestRESTUserAgent(t *testing.T) {
	f := newFakeGateway(t)
	cfg := testConfig(f)
	cfg.UserAgent = "ivan/v1.2.3 (+https://example.invalid)"
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := newRecordingHandler()
	if err := c.Start(context.Background(), h); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	<-h.ready

	if _, err := c.SendText(context.Background(), Target{UserOpenID: "u-1"}, "hi", "m-1", 1); err != nil {
		t.Fatalf("SendText: %v", err)
	}
	if got := f.lastUserAgent(); got != "ivan/v1.2.3 (+https://example.invalid)" {
		t.Fatalf("REST User-Agent 期望透传，得到 %q", got)
	}
}

func TestUploadInlineBelowLimit(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready

	data := []byte("small-png-bytes")
	info, err := c.UploadImage(context.Background(), Target{UserOpenID: "u-1"}, "a.png", data)
	if err != nil {
		t.Fatalf("UploadImage: %v", err)
	}
	if info != "fi-1" {
		t.Fatalf("file_info 期望 fi-1，得到 %q", info)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.prepares) != 0 {
		t.Fatalf("小于 5MB 不应走分片")
	}
	if len(f.fileUploads) != 1 {
		t.Fatalf("应有一次 files 上传，得到 %d", len(f.fileUploads))
	}
	body := f.fileUploads[0]
	if body["file_data"] == nil || body["file_name"] != "a.png" {
		t.Fatalf("内联上传请求体错误: %+v", body)
	}
	if body["srv_send_msg"] != false {
		t.Fatalf("应为 srv_send_msg=false")
	}
}

func TestUploadChunkedAboveLimit(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready

	data := make([]byte, inlineUploadLimit+5)
	for i := range data {
		data[i] = byte(i % 251)
	}
	info, err := c.UploadImage(context.Background(), Target{UserOpenID: "u-1"}, "big.png", data)
	if err != nil {
		t.Fatalf("UploadImage: %v", err)
	}
	if info != "fi-1" {
		t.Fatalf("file_info 期望 fi-1，得到 %q", info)
	}

	f.mu.Lock()
	prepares := append([]map[string]any(nil), f.prepares...)
	finishes := append([]map[string]any(nil), f.partFinishes...)
	bodies := append([]string(nil), f.partBodies...)
	uploads := append([]map[string]any(nil), f.fileUploads...)
	f.mu.Unlock()

	if len(prepares) != 1 {
		t.Fatalf("应调用一次 upload_prepare，得到 %d", len(prepares))
	}
	if prepares[0]["file_size"] != fmt.Sprintf("%d", len(data)) {
		t.Fatalf("file_size 错误: %v", prepares[0]["file_size"])
	}
	for _, k := range []string{"md5", "sha1", "md5_10m", "file_name", "file_type"} {
		if prepares[0][k] == nil {
			t.Fatalf("upload_prepare 缺字段 %s: %+v", k, prepares[0])
		}
	}
	if len(bodies) < 2 {
		t.Fatalf("应至少 PUT 两个分片，得到 %d", len(bodies))
	}
	if len(finishes) != len(bodies) {
		t.Fatalf("每个分片都应 part_finish（PUT=%d finish=%d）", len(bodies), len(finishes))
	}
	joined := strings.Join(bodies, "")
	if len(joined) != len(data) {
		t.Fatalf("分片合起来应等于原文件长度 %d，得到 %d", len(data), len(joined))
	}
	if len(uploads) != 1 || uploads[0]["upload_id"] != "up-1" {
		t.Fatalf("合并请求应带 upload_id: %+v", uploads)
	}
}

func TestUploadPreparePathPerTarget(t *testing.T) {
	if got := preparePath(Target{UserOpenID: "u"}); got != "/v2/users/u/upload_prepare" {
		t.Fatalf("单聊预上传路径错误: %s", got)
	}
	if got := preparePath(Target{GroupOpenID: "g"}); got != "/v2/groups/g/upload_prepare" {
		t.Fatalf("群聊预上传路径错误: %s", got)
	}
	if got := partFinishPath(Target{GroupOpenID: "g"}); got != "/v2/groups/g/upload_part_finish" {
		t.Fatalf("群聊分片完成路径错误: %s", got)
	}
}

func TestUploadChecksums(t *testing.T) {
	data := []byte("hello-qqbot")
	sum := md5.Sum(data)
	sha := sha1.Sum(data)
	if hex.EncodeToString(sum[:]) == "" || hex.EncodeToString(sha[:]) == "" {
		t.Fatal("校验值计算失败")
	}
	if md5WindowBytes != 10002432 {
		t.Fatalf("md5_10m 窗口应为 10002432 字节，得到 %d", md5WindowBytes)
	}
}

func TestSendMarkdownFields(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready

	ctx := context.Background()
	if _, err := c.SendMarkdown(ctx, Target{UserOpenID: "u-1"}, "## 标题\n正文", "m-1", 3); err != nil {
		t.Fatalf("SendMarkdown: %v", err)
	}
	call, ok := f.lastSend()
	if !ok {
		t.Fatal("服务端未收到发送请求")
	}
	if mt, _ := call.Body["msg_type"].(float64); int(mt) != MsgTypeMarkdown {
		t.Fatalf("msg_type 期望 2，得到 %v", call.Body["msg_type"])
	}
	if _, has := call.Body["content"]; has {
		t.Fatalf("markdown 消息不应带 content: %+v", call.Body)
	}
	md, _ := call.Body["markdown"].(map[string]any)
	if md == nil || md["content"] != "## 标题\n正文" {
		t.Fatalf("markdown 字段错误: %+v", call.Body["markdown"])
	}
	if call.Body["msg_id"] != "m-1" {
		t.Fatalf("msg_id 未透传: %+v", call.Body)
	}
	if n, _ := call.Body["msg_seq"].(float64); int(n) != 3 {
		t.Fatalf("msg_seq 期望 3，得到 %v", call.Body["msg_seq"])
	}
}

func TestSendMarkdownEmptyRejected(t *testing.T) {
	f := newFakeGateway(t)
	c, h := startClient(t, f)
	<-h.ready
	if _, err := c.SendMarkdown(context.Background(), Target{UserOpenID: "u-1"}, "   ", "m-1", 1); err == nil {
		t.Fatal("空 markdown 应报错")
	}
}

func TestMarkdownRejectedClassification(t *testing.T) {
	cases := []struct {
		code int
		want bool
	}{
		{40034008, true},
		{40034011, true},
		{50055, true},
		{50056, true},
		{304036, true},
		{40034005, false},
		{40054007, false},
	}
	for _, c := range cases {
		err := &Error{Code: c.code, Op: "send"}
		if got := err.MarkdownRejected(); got != c.want {
			t.Fatalf("code %d MarkdownRejected=%v，期望 %v", c.code, got, c.want)
		}
	}
	if !IsMarkdownRejected(&Error{Code: 40034011}) {
		t.Fatal("IsMarkdownRejected 应命中")
	}
	if IsMarkdownRejected(&Error{Code: 40034005}) {
		t.Fatal("窗口过期不应算 markdown 被拒")
	}
}
