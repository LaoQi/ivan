package qqbot

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Class int

const (
	ClassUnknown Class = iota
	ClassRateLimit
	ClassAuth
	ClassPermission
	ClassBadRequest
	ClassExpired
	ClassServer
)

func (c Class) String() string {
	switch c {
	case ClassRateLimit:
		return "rate_limit"
	case ClassAuth:
		return "auth"
	case ClassPermission:
		return "permission"
	case ClassBadRequest:
		return "bad_request"
	case ClassExpired:
		return "expired"
	case ClassServer:
		return "server"
	default:
		return "unknown"
	}
}

type Error struct {
	Status  int
	Code    int
	Message string
	TraceID string
	Class   Class
	Op      string
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "qqbot %s 失败", e.Op)
	if e.Status != 0 {
		fmt.Fprintf(&b, " http=%d", e.Status)
	}
	if e.Code != 0 {
		fmt.Fprintf(&b, " code=%d", e.Code)
	}
	fmt.Fprintf(&b, " (%s)", e.Class)
	if e.Message != "" {
		fmt.Fprintf(&b, ": %s", e.Message)
	}
	if e.TraceID != "" {
		fmt.Fprintf(&b, " trace_id=%s", e.TraceID)
	}
	return b.String()
}

func (e *Error) Retryable() bool {
	switch e.Class {
	case ClassRateLimit, ClassServer:
		return true
	default:
		return false
	}
}

func (e *Error) AuthFailed() bool {
	return e.Class == ClassAuth
}

func (e *Error) WindowExpired() bool {
	return e.Code == ErrCodeMsgExpired
}

const (
	ErrCodeMsgExpired = 40034005
)

func errorFromResponse(op string, status int, body []byte) *Error {
	e := &Error{Status: status, Op: op}
	var payload struct {
		ErrCode int    `json:"err_code"`
		Code    int    `json:"code"`
		Message string `json:"message"`
		ErrMsg  string `json:"err_msg"`
		TraceID string `json:"trace_id"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		if payload.ErrCode != 0 {
			e.Code = payload.ErrCode
		} else {
			e.Code = payload.Code
		}
		e.Message = strings.TrimSpace(payload.Message)
		if e.Message == "" {
			e.Message = strings.TrimSpace(payload.ErrMsg)
		}
		e.TraceID = payload.TraceID
	}
	if e.Message == "" && len(body) > 0 && len(body) <= 512 {
		e.Message = strings.TrimSpace(string(body))
	}
	e.Message = redact(e.Message)
	e.Class = classify(status, e.Code)
	return e
}

func classify(status, code int) Class {
	switch {
	case status == 429:
		return ClassRateLimit
	case status == 401:
		return ClassAuth
	case status == 403:
		return ClassPermission
	case status >= 500:
		return ClassServer
	}
	switch code {
	case ErrCodeMsgExpired:
		return ClassExpired
	case 11244, 11252, 40034006:
		return ClassAuth
	case 11253, 11254, 11251, 11261, 11241:
		return ClassPermission
	case 10001, 10002, 40034, 40054:
		return ClassBadRequest
	}
	if status >= 400 {
		return ClassBadRequest
	}
	return ClassUnknown
}

var secretMarkers = []string{"secret", "access_token", "access-token", "authorization", "token="}

func redact(s string) string {
	lower := strings.ToLower(s)
	for _, m := range secretMarkers {
		if strings.Contains(lower, m) {
			return "[已隐藏响应内容]"
		}
	}
	return s
}

type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		return nil
	}
	s = strings.Trim(s, `"`)
	if s == "" {
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		fl, ferr := strconv.ParseFloat(s, 64)
		if ferr != nil {
			return fmt.Errorf("无法解析整数 %q", s)
		}
		n = int(fl)
	}
	*f = flexInt(n)
	return nil
}

func (f flexInt) Int() int { return int(f) }

func IsWindowExpired(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.WindowExpired()
}

func IsRetryable(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Retryable()
}

func IsAuthFailed(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.AuthFailed()
}
