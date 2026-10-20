package qqbot

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

const (
	inlineUploadLimit = 5 << 20
	md5WindowBytes    = 10002432
)

type uploadPart struct {
	Index        int     `json:"index"`
	PresignedURL string  `json:"presigned_url"`
	BlockSize    flexInt `json:"block_size"`
}

type prepareResult struct {
	UploadID  string       `json:"upload_id"`
	BlockSize flexInt      `json:"block_size"`
	Parts     []uploadPart `json:"parts"`
}

func (c *Client) UploadImage(ctx context.Context, target Target, name string, data []byte) (string, error) {
	if !target.valid() {
		return "", errNoTarget
	}
	if len(data) == 0 {
		return "", fmt.Errorf("qqbot 图片内容为空")
	}
	if len(data) <= inlineUploadLimit {
		return c.uploadInline(ctx, target, name, data)
	}
	return c.uploadChunked(ctx, target, name, data)
}

func (c *Client) uploadInline(ctx context.Context, target Target, name string, data []byte) (string, error) {
	raw, err := c.uploadRequest(ctx, target, map[string]any{
		"file_type":    1,
		"file_name":    name,
		"file_data":    encodeBase64(data),
		"srv_send_msg": false,
	})
	if err != nil {
		return "", err
	}
	return fileInfoOf(raw)
}

func (c *Client) uploadChunked(ctx context.Context, target Target, name string, data []byte) (string, error) {
	uploadID, err := c.uploadPrepare(ctx, target, name, data)
	if err != nil {
		return "", err
	}
	raw, err := c.uploadRequest(ctx, target, map[string]any{
		"file_type":    1,
		"file_name":    name,
		"upload_id":    uploadID,
		"srv_send_msg": false,
	})
	if err != nil {
		return "", err
	}
	return fileInfoOf(raw)
}

func (c *Client) uploadPrepare(ctx context.Context, target Target, name string, data []byte) (string, error) {
	whole := md5.Sum(data)
	sum := sha1.Sum(data)
	window := data
	if len(window) > md5WindowBytes {
		window = window[:md5WindowBytes]
	}
	part10 := md5.Sum(window)
	payload := map[string]any{
		"file_type": 1,
		"file_size": fmt.Sprintf("%d", len(data)),
		"file_name": name,
		"md5":       hex.EncodeToString(whole[:]),
		"sha1":      hex.EncodeToString(sum[:]),
		"md5_10m":   hex.EncodeToString(part10[:]),
	}
	rawBytes, err := c.doREST(ctx, "upload_prepare", http.MethodPost, preparePath(target), payload)
	if err != nil {
		return "", err
	}
	var prep prepareResult
	if err := json.Unmarshal(rawBytes, &prep); err != nil {
		return "", fmt.Errorf("解析预上传响应失败: %w", err)
	}
	if prep.UploadID == "" || len(prep.Parts) == 0 {
		return "", fmt.Errorf("qqbot 预上传响应缺少 upload_id 或 parts")
	}
	parts := append([]uploadPart(nil), prep.Parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].Index < parts[j].Index })
	cursor := 0
	for _, part := range parts {
		if cursor >= len(data) {
			break
		}
		chunk := data[cursor:]
		if size := part.BlockSize.Int(); size > 0 && size < len(chunk) {
			chunk = chunk[:size]
		}
		if err := c.uploadOnePart(ctx, target, prep.UploadID, part, chunk); err != nil {
			return "", err
		}
		cursor += len(chunk)
	}
	if cursor < len(data) {
		return "", fmt.Errorf("qqbot 分片未覆盖全部数据（已传 %d/%d 字节）", cursor, len(data))
	}
	return prep.UploadID, nil
}

func (c *Client) uploadOnePart(ctx context.Context, target Target, uploadID string, part uploadPart, chunk []byte) error {
	if len(chunk) == 0 {
		return fmt.Errorf("qqbot 分片 %d 内容为空", part.Index)
	}
	if err := c.putPresigned(ctx, part.PresignedURL, chunk); err != nil {
		return err
	}
	sum := md5.Sum(chunk)
	_, err := c.doREST(ctx, "upload_part_finish", http.MethodPost, partFinishPath(target), map[string]any{
		"upload_id":  uploadID,
		"part_index": part.Index,
		"block_size": fmt.Sprintf("%d", len(chunk)),
		"md5":        hex.EncodeToString(sum[:]),
	})
	return err
}

func (c *Client) putPresigned(ctx context.Context, url string, chunk []byte) error {
	if strings.TrimSpace(url) == "" {
		return fmt.Errorf("qqbot 分片缺少预签名 URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(chunk))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(chunk))
	resp, err := c.presignClient().Do(req)
	if err != nil {
		return fmt.Errorf("qqbot 分片 PUT 失败: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errorFromResponse("upload_part_put", resp.StatusCode, body)
	}
	return nil
}

func (c *Client) presignClient() *http.Client {
	if c.presign != nil {
		return c.presign
	}
	return c.cfg.HTTPClient
}

func (c *Client) uploadRequest(ctx context.Context, target Target, payload map[string]any) ([]byte, error) {
	raw, err := c.doREST(ctx, "upload", http.MethodPost, target.filesPath(), payload)
	if err != nil {
		c.countSendErr()
		return nil, err
	}
	return raw, nil
}

func fileInfoOf(raw []byte) (string, error) {
	var out uploadResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("解析上传响应失败: %w", err)
	}
	if out.FileInfo == "" {
		return "", fmt.Errorf("qqbot 上传响应缺少 file_info")
	}
	return out.FileInfo, nil
}

func preparePath(t Target) string {
	if t.IsGroup() {
		return "/v2/groups/" + t.GroupOpenID + "/upload_prepare"
	}
	return "/v2/users/" + t.UserOpenID + "/upload_prepare"
}

func partFinishPath(t Target) string {
	if t.IsGroup() {
		return "/v2/groups/" + t.GroupOpenID + "/upload_part_finish"
	}
	return "/v2/users/" + t.UserOpenID + "/upload_part_finish"
}
