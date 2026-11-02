package config

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func mapLookup(env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg, err := FromLookup(mapLookup(nil))
	if err != nil {
		t.Fatalf("unexpected error with defaults: %v", err)
	}
	if cfg.Path != "/data" {
		t.Errorf("Path = %q, want /data", cfg.Path)
	}
	if cfg.StateDir != "/var/lib/mirror" {
		t.Errorf("StateDir = %q, want /var/lib/mirror", cfg.StateDir)
	}
	if cfg.NodeID != "" {
		t.Errorf("NodeID = %q, want empty", cfg.NodeID)
	}
	if cfg.ListenAddr != ":22001" {
		t.Errorf("ListenAddr = %q, want :22001", cfg.ListenAddr)
	}
	if cfg.Secret != "" {
		t.Errorf("Secret = %q, want empty", cfg.Secret)
	}
	if !cfg.DiscoveryLAN {
		t.Errorf("DiscoveryLAN = false, want true")
	}
	if cfg.DiscoveryPort != 22002 {
		t.Errorf("DiscoveryPort = %d, want 22002", cfg.DiscoveryPort)
	}
	if cfg.ConflictStrategy != ConflictStrategyLWW {
		t.Errorf("ConflictStrategy = %q, want lww", cfg.ConflictStrategy)
	}
	if cfg.RescanInterval != 60*time.Second {
		t.Errorf("RescanInterval = %v, want 60s", cfg.RescanInterval)
	}
	if cfg.DebounceDelay != 500*time.Millisecond {
		t.Errorf("DebounceDelay = %v, want 500ms", cfg.DebounceDelay)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	}
}

func TestCustomConfigParsing(t *testing.T) {
	env := map[string]string{
		"MIRROR_PATH":              "/mnt/sync",
		"MIRROR_STATE_DIR":         "/tmp/mirror-state",
		"MIRROR_NODE_ID":           "node-1",
		"MIRROR_LISTEN_ADDR":       "0.0.0.0:23001",
		"MIRROR_SECRET":            "my-super-secret",
		"MIRROR_PEERS":             "10.0.0.1:22001, 10.0.0.2:22001 , ",
		"MIRROR_DISCOVERY_DNS":     "tasks.mirror, tasks.mirror-backup",
		"MIRROR_DISCOVERY_LAN":     "false",
		"MIRROR_DISCOVERY_PORT":    "23002",
		"MIRROR_EXCLUDE":           "*.tmp, .git, node_modules",
		"MIRROR_CONFLICT_STRATEGY": "keep-copy",
		"MIRROR_RESCAN_INTERVAL":   "2m",
		"MIRROR_DEBOUNCE_DELAY":    "250ms",
		"MIRROR_LOG_LEVEL":         "DEBUG",
	}

	cfg, err := FromLookup(mapLookup(env))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Path != "/mnt/sync" {
		t.Errorf("Path = %q", cfg.Path)
	}
	if cfg.StateDir != "/tmp/mirror-state" {
		t.Errorf("StateDir = %q", cfg.StateDir)
	}
	if cfg.NodeID != "node-1" {
		t.Errorf("NodeID = %q", cfg.NodeID)
	}
	if cfg.ListenAddr != "0.0.0.0:23001" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.Secret != "my-super-secret" {
		t.Errorf("Secret = %q", cfg.Secret)
	}
	if !reflect.DeepEqual(cfg.Peers, []string{"10.0.0.1:22001", "10.0.0.2:22001"}) {
		t.Errorf("Peers = %v", cfg.Peers)
	}
	if !reflect.DeepEqual(cfg.DiscoveryDNS, []string{"tasks.mirror", "tasks.mirror-backup"}) {
		t.Errorf("DiscoveryDNS = %v", cfg.DiscoveryDNS)
	}
	if cfg.DiscoveryLAN != false {
		t.Errorf("DiscoveryLAN = %v", cfg.DiscoveryLAN)
	}
	if cfg.DiscoveryPort != 23002 {
		t.Errorf("DiscoveryPort = %d", cfg.DiscoveryPort)
	}
	if !reflect.DeepEqual(cfg.Exclude, []string{"*.tmp", ".git", "node_modules"}) {
		t.Errorf("Exclude = %v", cfg.Exclude)
	}
	if cfg.ConflictStrategy != ConflictStrategyKeepCopy {
		t.Errorf("ConflictStrategy = %q", cfg.ConflictStrategy)
	}
	if cfg.RescanInterval != 2*time.Minute {
		t.Errorf("RescanInterval = %v", cfg.RescanInterval)
	}
	if cfg.DebounceDelay != 250*time.Millisecond {
		t.Errorf("DebounceDelay = %v", cfg.DebounceDelay)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
}

func TestConfigValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "empty path",
			env:  map[string]string{"MIRROR_PATH": "   "},
		},
		{
			name: "empty state dir",
			env:  map[string]string{"MIRROR_STATE_DIR": ""},
		},
		{
			name: "empty listen addr",
			env:  map[string]string{"MIRROR_LISTEN_ADDR": ""},
		},
		{
			name: "invalid bool for discovery lan",
			env:  map[string]string{"MIRROR_DISCOVERY_LAN": "not-a-bool"},
		},
		{
			name: "non-numeric discovery port",
			env:  map[string]string{"MIRROR_DISCOVERY_PORT": "abc"},
		},
		{
			name: "out-of-range discovery port",
			env:  map[string]string{"MIRROR_DISCOVERY_PORT": "70000"},
		},
		{
			name: "invalid conflict strategy",
			env:  map[string]string{"MIRROR_CONFLICT_STRATEGY": "manual"},
		},
		{
			name: "invalid rescan interval",
			env:  map[string]string{"MIRROR_RESCAN_INTERVAL": "not-a-duration"},
		},
		{
			name: "negative rescan interval",
			env:  map[string]string{"MIRROR_RESCAN_INTERVAL": "-5s"},
		},
		{
			name: "zero debounce delay",
			env:  map[string]string{"MIRROR_DEBOUNCE_DELAY": "0s"},
		},
		{
			name: "invalid log level",
			env:  map[string]string{"MIRROR_LOG_LEVEL": "trace"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := FromLookup(mapLookup(tc.env)); err == nil {
				t.Fatalf("expected error for %s (%v), got nil", tc.name, tc.env)
			}
		})
	}
}

func TestLoadOrCreateNodeID(t *testing.T) {
	tmpDir := t.TempDir()
	stateDir := filepath.Join(tmpDir, "state")

	id1, err := LoadOrCreateNodeID(stateDir)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeID failed: %v", err)
	}
	if len(id1) != 16 {
		t.Fatalf("expected 16-char hex ID, got %q", id1)
	}

	// Second call should return the exact same persisted ID.
	id2, err := LoadOrCreateNodeID(stateDir)
	if err != nil {
		t.Fatalf("second LoadOrCreateNodeID failed: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("expected persisted ID %q, got %q", id1, id2)
	}

	cfg := Default()
	cfg.StateDir = stateDir
	if err := cfg.EnsureNodeID(); err != nil {
		t.Fatalf("EnsureNodeID failed: %v", err)
	}
	if cfg.NodeID != id1 {
		t.Fatalf("expected cfg.NodeID=%q, got %q", id1, cfg.NodeID)
	}
}
