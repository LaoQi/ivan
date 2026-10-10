package bus

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	ChannelCLI      = "cli"
	ChannelSchedule = "schedule"
	ChannelQQ       = "qq"
)

func SessionKey(m InboundMessage) string {
	if key := strings.TrimSpace(m.SessionKey); key != "" {
		return key
	}
	ch := strings.TrimSpace(m.Channel)
	if ch == "" {
		ch = "unknown"
	}
	if ch == ChannelCLI {
		return ChannelCLI + ":default"
	}
	id := strings.TrimSpace(m.ChatID)
	switch m.ChatType {
	case ChatDirect:
		if id != "" {
			return ch + ":c2c:" + id
		}
	case ChatGroup:
		if id != "" {
			return ch + ":group:" + id
		}
	}
	if id != "" {
		return ch + ":" + id
	}
	return ch + ":default"
}

const slugLimit = 64

func KeySlug(key string) string {
	var b strings.Builder
	dash := false
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
			dash = false
		default:
			if !dash && b.Len() > 0 {
				b.WriteByte('-')
				dash = true
			}
		}
	}
	name := strings.Trim(b.String(), "-._")
	if name == "" {
		name = "session"
	}
	if len(name) > slugLimit {
		sum := sha256.Sum256([]byte(key))
		name = name[:slugLimit] + "-" + hex.EncodeToString(sum[:4])
	}
	return name
}
