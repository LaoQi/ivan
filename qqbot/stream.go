package qqbot

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

const (
	InputStateGenerating = 1
	InputStateFinished   = 10

	InputModeAppend  = "append"
	InputModeReplace = "replace"
)

type StreamInfo struct {
	ID           string `json:"id"`
	RemainMsgLen int    `json:"remain_msg_len"`
}

type streamChunk struct {
	InputMode   string `json:"input_mode,omitempty"`
	InputState  int    `json:"input_state"`
	Index       int    `json:"index"`
	ContentType string `json:"content_type,omitempty"`
	ContentRaw  string `json:"content_raw"`
	MsgID       string `json:"msg_id,omitempty"`
	EventID     string `json:"event_id,omitempty"`
	StreamMsgID string `json:"stream_msg_id,omitempty"`
	MsgSeq      int    `json:"msg_seq,omitempty"`
}

// Stream 承载一条流式消息的分片状态：首片成功后拿到 stream_msg_id，后续分片原位更新。
// 群聊不支持流式（官方限制），调用方应改用 Typing + 普通消息。
type Stream struct {
	client *Client
	target Target

	mu      sync.Mutex
	id      string
	index   int
	last    string
	msgID   string
	msgSeq  int
	remain  int
	started bool
	closed  bool
}

func (c *Client) streamPath(target Target) string {
	return "/v2/users/" + target.UserOpenID + "/stream_messages"
}

// OpenStream 发送首片并返回流句柄；replyTo/seq 用于被动回复（可空）。
func (c *Client) OpenStream(ctx context.Context, target Target, text, contentType, replyTo string, seq int) (*Stream, error) {
	if !target.valid() {
		return nil, errNoTarget
	}
	if target.IsGroup() {
		return nil, fmt.Errorf("qqbot 群聊不支持流式消息")
	}
	s := &Stream{client: c, target: target, msgID: replyTo, msgSeq: seq}
	if err := s.send(ctx, text, InputStateGenerating, InputModeReplace); err != nil {
		return nil, err
	}
	return s, nil
}

// Update 追加/替换流式内容；done 为真时标记生成结束。
func (s *Stream) Update(ctx context.Context, text string, done bool) error {
	state := InputStateGenerating
	if done {
		state = InputStateFinished
	}
	return s.send(ctx, text, state, InputModeReplace)
}

// Typing 显示“输入中”状态（msg_type=6），不产生消息条数；second 上限 60 秒。
func (c *Client) Typing(ctx context.Context, target Target, replyTo string, seq, second int) error {
	if !target.valid() {
		return errNoTarget
	}
	if second <= 0 || second > 60 {
		second = 60
	}
	data := map[string]any{
		"msg_type":     MsgTypeInput,
		"input_notify": map[string]int{"input_type": 1, "input_second": second},
	}
	if replyTo != "" {
		data["msg_id"] = replyTo
		data["msg_seq"] = seq
	}
	_, err := c.doREST(ctx, "typing", http.MethodPost, target.messagesPath(), data)
	if err != nil {
		c.countSendErr()
		c.recordError(err)
		return err
	}
	return nil
}

func (s *Stream) send(ctx context.Context, text string, state int, mode string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if state == InputStateGenerating && text == s.last && s.started {
		s.mu.Unlock()
		return nil
	}
	chunk := streamChunk{
		InputMode:   mode,
		InputState:  state,
		Index:       s.index,
		ContentType: "text",
		ContentRaw:  text,
		MsgID:       s.msgID,
		StreamMsgID: s.id,
	}
	if s.msgSeq > 0 {
		chunk.MsgSeq = s.msgSeq
	}
	s.mu.Unlock()

	raw, err := s.client.doREST(ctx, "stream", http.MethodPost, s.client.streamPath(s.target), chunk)
	if err != nil {
		return err
	}
	var info StreamInfo
	_ = json.Unmarshal(raw, &info)

	s.mu.Lock()
	defer s.mu.Unlock()
	if info.ID != "" {
		s.id = info.ID
	}
	s.remain = info.RemainMsgLen
	s.last = text
	s.index++
	s.started = true
	if state == InputStateFinished {
		s.closed = true
	}
	return nil
}

// Close 在不指定终局文本时收尾（已定稿则无操作）。
func (s *Stream) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed || !s.started {
		s.mu.Unlock()
		return nil
	}
	last := s.last
	s.mu.Unlock()
	return s.Update(ctx, last, true)
}
