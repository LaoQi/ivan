package qqbot

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxBodyRead = 1 << 20

func readLimited(r io.Reader) []byte {
	b, _ := io.ReadAll(io.LimitReader(r, maxBodyRead))
	return b
}

func (c *Client) doREST(ctx context.Context, op, method, path string, payload any) ([]byte, error) {
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if payload != nil {
		raw, merr := json.Marshal(payload)
		if merr != nil {
			return nil, merr
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.APIBase+path, body)
	if err != nil {
		return nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "QQBot "+token)
	req.Header.Set("Accept", "application/json")
	if c.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", c.cfg.UserAgent)
	}
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("qqbot %s 请求失败: %w", op, err)
	}
	defer resp.Body.Close()
	raw := readLimited(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return raw, nil
	}
	return nil, errorFromResponse(op, resp.StatusCode, raw)
}

func (c *Client) Gateway(ctx context.Context) (*GatewayInfo, error) {
	raw, err := c.doREST(ctx, "gateway", http.MethodGet, "/gateway/bot", nil)
	if err != nil {
		return nil, err
	}
	var info GatewayInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("解析 gateway 响应失败: %w", err)
	}
	if info.URL == "" {
		return nil, fmt.Errorf("qqbot gateway 响应缺少 url")
	}
	return &info, nil
}

func (c *Client) gatewayURL(ctx context.Context) (string, error) {
	if c.cfg.GatewayURL != "" {
		return c.cfg.GatewayURL, nil
	}
	info, err := c.Gateway(ctx)
	if err != nil {
		return "", err
	}
	return info.URL, nil
}

func (c *Client) SendText(ctx context.Context, target Target, content string, replyTo string, seq int) (string, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", fmt.Errorf("qqbot 文本内容为空")
	}
	if !target.valid() {
		return "", errNoTarget
	}
	data := sendTextData{Content: content, MsgType: MsgTypeText, MsgID: replyTo, MsgSeq: seq}
	return c.sendMessage(ctx, target, data)
}

func (c *Client) SendMarkdown(ctx context.Context, target Target, markdown string, replyTo string, seq int) (string, error) {
	if strings.TrimSpace(markdown) == "" {
		return "", fmt.Errorf("qqbot markdown 内容为空")
	}
	if !target.valid() {
		return "", errNoTarget
	}
	data := map[string]any{
		"msg_type": MsgTypeMarkdown,
		"markdown": map[string]string{"content": markdown},
		"msg_id":   replyTo,
		"msg_seq":  seq,
	}
	return c.sendMessage(ctx, target, data)
}

func (c *Client) SendImage(ctx context.Context, target Target, fileInfo string, replyTo string, seq int) (string, error) {
	if strings.TrimSpace(fileInfo) == "" {
		return "", fmt.Errorf("qqbot file_info 为空")
	}
	if !target.valid() {
		return "", errNoTarget
	}
	data := sendMediaData{MsgType: MsgTypeMedia, MsgID: replyTo, MsgSeq: seq, Media: mediaField{FileInfo: fileInfo}}
	return c.sendMessage(ctx, target, data)
}

func (c *Client) sendMessage(ctx context.Context, target Target, payload any) (string, error) {
	raw, err := c.doREST(ctx, "send", http.MethodPost, target.messagesPath(), payload)
	if err != nil {
		c.countSendErr()
		c.recordError(err)
		return "", err
	}
	c.countSent()
	var out sendResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", nil
	}
	return out.ID, nil
}

func (c *Client) SendTextReply(ctx context.Context, target Target, content string, replyTo string, seq int) (string, error) {
	if replyTo == "" {
		return "", fmt.Errorf("qqbot 缺少 msg_id，无法被动回复")
	}
	return c.SendText(ctx, target, content, replyTo, seq)
}

func (c *Client) UploadImageBytes(ctx context.Context, target Target, data []byte) (string, error) {
	if !target.valid() {
		return "", errNoTarget
	}
	payload := map[string]any{
		"file_type":    1,
		"file_data":    base64.StdEncoding.EncodeToString(data),
		"srv_send_msg": false,
	}
	raw, err := c.doREST(ctx, "upload", http.MethodPost, target.filesPath(), payload)
	if err != nil {
		return "", err
	}
	var out uploadResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("解析上传响应失败: %w", err)
	}
	if out.FileInfo == "" {
		return "", fmt.Errorf("qqbot 上传响应缺少 file_info")
	}
	return out.FileInfo, nil
}

func (c *Client) UploadImageURL(ctx context.Context, target Target, url string) (string, error) {
	if !target.valid() {
		return "", errNoTarget
	}
	payload := map[string]any{
		"file_type":    1,
		"url":          url,
		"srv_send_msg": false,
	}
	raw, err := c.doREST(ctx, "upload", http.MethodPost, target.filesPath(), payload)
	if err != nil {
		return "", err
	}
	var out uploadResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("解析上传响应失败: %w", err)
	}
	if out.FileInfo == "" {
		return "", fmt.Errorf("qqbot 上传响应缺少 file_info")
	}
	return out.FileInfo, nil
}
