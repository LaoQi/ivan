package qqbot

import (
	"sync"
	"time"
)

type msgCache struct {
	ttl    time.Duration
	max    int
	mu     sync.Mutex
	seen   map[string]time.Time
	lastGC time.Time
}

func newMsgCache(ttl time.Duration, max int) *msgCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if max <= 0 {
		max = 4096
	}
	return &msgCache{ttl: ttl, max: max, seen: map[string]time.Time{}}
}

func (c *msgCache) Seen(id string) bool {
	if id == "" {
		return false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gcLocked(now)
	if at, ok := c.seen[id]; ok && now.Sub(at) < c.ttl {
		return true
	}
	c.seen[id] = now
	return false
}

func (c *msgCache) gcLocked(now time.Time) {
	if c.max > 0 && len(c.seen) < c.max && now.Sub(c.lastGC) < c.ttl {
		return
	}
	for id, at := range c.seen {
		if now.Sub(at) >= c.ttl {
			delete(c.seen, id)
		}
	}
	c.lastGC = now
	if c.max > 0 && len(c.seen) >= c.max {
		oldestID := ""
		var oldest time.Time
		for id, at := range c.seen {
			if oldestID == "" || at.Before(oldest) {
				oldestID, oldest = id, at
			}
		}
		if oldestID != "" {
			delete(c.seen, oldestID)
		}
	}
}

func (c *msgCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}
