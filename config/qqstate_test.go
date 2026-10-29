package config

import (
	"path/filepath"
	"testing"
)

func TestQQStatePathUnderDataDir(t *testing.T) {
	cfg := Default()
	cfg.DataDir = "/tmp/ivan-data"
	want := filepath.Join("/tmp/ivan-data", "qq", "channel.json")
	if got := cfg.QQStatePath(); got != want {
		t.Fatalf("QQStatePath = %q，期望 %q", got, want)
	}
}
