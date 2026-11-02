package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	ConflictStrategyLWW      = "lww"
	ConflictStrategyKeepCopy = "keep-copy"

	ConflictLWW      = ConflictStrategyLWW
	ConflictKeepCopy = ConflictStrategyKeepCopy

	DefaultPath             = "/data"
	DefaultStateDir         = "/var/lib/mirror"
	DefaultListenAddr       = ":22001"
	DefaultDiscoveryLAN     = true
	DefaultDiscoveryPort    = 22002
	DefaultConflictStrategy = ConflictStrategyLWW
	DefaultRescanInterval   = 60 * time.Second
	DefaultDebounceDelay    = 500 * time.Millisecond
	DefaultLogLevel         = "info"
)

// Config holds all runtime settings parsed from environment variables.
type Config struct {
	Path             string
	StateDir         string
	NodeID           string
	ListenAddr       string
	Secret           string
	Peers            []string
	DiscoveryDNS     []string
	DiscoveryLAN     bool
	DiscoveryPort    int
	Exclude          []string
	ConflictStrategy string
	RescanInterval   time.Duration
	DebounceDelay    time.Duration
	LogLevel         string
}

// Default returns a Config populated with default settings.
func Default() *Config {
	return &Config{
		Path:             DefaultPath,
		StateDir:         DefaultStateDir,
		NodeID:           "",
		ListenAddr:       DefaultListenAddr,
		Secret:           "",
		Peers:            nil,
		DiscoveryDNS:     nil,
		DiscoveryLAN:     DefaultDiscoveryLAN,
		DiscoveryPort:    DefaultDiscoveryPort,
		Exclude:          nil,
		ConflictStrategy: DefaultConflictStrategy,
		RescanInterval:   DefaultRescanInterval,
		DebounceDelay:    DefaultDebounceDelay,
		LogLevel:         DefaultLogLevel,
	}
}

// FromEnv loads and validates configuration from process environment variables.
func FromEnv() (*Config, error) {
	return FromLookup(os.LookupEnv)
}

// FromLookup loads and validates configuration using the provided environment lookup function.
func FromLookup(lookup func(string) (string, bool)) (*Config, error) {
	cfg := Default()

	if v, ok := lookup("MIRROR_PATH"); ok {
		cfg.Path = strings.TrimSpace(v)
	}
	if v, ok := lookup("MIRROR_STATE_DIR"); ok {
		cfg.StateDir = strings.TrimSpace(v)
	}
	if v, ok := lookup("MIRROR_NODE_ID"); ok {
		cfg.NodeID = strings.TrimSpace(v)
	}
	if v, ok := lookup("MIRROR_LISTEN_ADDR"); ok {
		cfg.ListenAddr = strings.TrimSpace(v)
	}
	if v, ok := lookup("MIRROR_SECRET"); ok {
		cfg.Secret = v
	}
	if v, ok := lookup("MIRROR_PEERS"); ok {
		cfg.Peers = splitCSV(v)
	}
	if v, ok := lookup("MIRROR_DISCOVERY_DNS"); ok {
		cfg.DiscoveryDNS = splitCSV(v)
	}
	if v, ok := lookup("MIRROR_DISCOVERY_LAN"); ok && strings.TrimSpace(v) != "" {
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("invalid MIRROR_DISCOVERY_LAN %q: %w", v, err)
		}
		cfg.DiscoveryLAN = b
	}
	if v, ok := lookup("MIRROR_DISCOVERY_PORT"); ok && strings.TrimSpace(v) != "" {
		port, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("invalid MIRROR_DISCOVERY_PORT %q: %w", v, err)
		}
		cfg.DiscoveryPort = port
	}
	if v, ok := lookup("MIRROR_EXCLUDE"); ok {
		cfg.Exclude = splitCSV(v)
	}
	if v, ok := lookup("MIRROR_CONFLICT_STRATEGY"); ok && strings.TrimSpace(v) != "" {
		cfg.ConflictStrategy = strings.ToLower(strings.TrimSpace(v))
	}
	if v, ok := lookup("MIRROR_RESCAN_INTERVAL"); ok && strings.TrimSpace(v) != "" {
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("invalid MIRROR_RESCAN_INTERVAL %q: %w", v, err)
		}
		cfg.RescanInterval = d
	}
	if v, ok := lookup("MIRROR_DEBOUNCE_DELAY"); ok && strings.TrimSpace(v) != "" {
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("invalid MIRROR_DEBOUNCE_DELAY %q: %w", v, err)
		}
		cfg.DebounceDelay = d
	}
	if v, ok := lookup("MIRROR_LOG_LEVEL"); ok && strings.TrimSpace(v) != "" {
		cfg.LogLevel = strings.ToLower(strings.TrimSpace(v))
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks that all configuration values are within valid bounds.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Path) == "" {
		return errors.New("MIRROR_PATH must not be empty")
	}
	if strings.TrimSpace(c.StateDir) == "" {
		return errors.New("MIRROR_STATE_DIR must not be empty")
	}
	if strings.TrimSpace(c.ListenAddr) == "" {
		return errors.New("MIRROR_LISTEN_ADDR must not be empty")
	}
	if c.DiscoveryPort <= 0 || c.DiscoveryPort > 65535 {
		return fmt.Errorf("MIRROR_DISCOVERY_PORT must be between 1 and 65535, got %d", c.DiscoveryPort)
	}
	switch c.ConflictStrategy {
	case ConflictStrategyLWW, ConflictStrategyKeepCopy:
	default:
		return fmt.Errorf("MIRROR_CONFLICT_STRATEGY must be %q or %q, got %q",
			ConflictStrategyLWW, ConflictStrategyKeepCopy, c.ConflictStrategy)
	}
	if c.RescanInterval <= 0 {
		return fmt.Errorf("MIRROR_RESCAN_INTERVAL must be positive, got %v", c.RescanInterval)
	}
	if c.DebounceDelay <= 0 {
		return fmt.Errorf("MIRROR_DEBOUNCE_DELAY must be positive, got %v", c.DebounceDelay)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("MIRROR_LOG_LEVEL must be one of debug, info, warn, error; got %q", c.LogLevel)
	}
	return nil
}

// LoadOrCreateNodeID reads a persisted node ID from stateDir/node_id, or generates
// and atomically persists a new 16-hex-character random node ID if none exists.
func LoadOrCreateNodeID(stateDir string) (string, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return "", fmt.Errorf("create state dir %q: %w", stateDir, err)
	}
	idFile := filepath.Join(stateDir, "node_id")
	if data, err := os.ReadFile(idFile); err == nil {
		id := strings.TrimSpace(string(data))
		if id != "" {
			return id, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read node_id file %q: %w", idFile, err)
	}

	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate random node_id: %w", err)
	}
	id := hex.EncodeToString(buf[:])

	tmpFile := idFile + ".tmp"
	if err := os.WriteFile(tmpFile, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write temp node_id file: %w", err)
	}
	if err := os.Rename(tmpFile, idFile); err != nil {
		_ = os.Remove(tmpFile)
		return "", fmt.Errorf("rename node_id file: %w", err)
	}
	return id, nil
}

// EnsureNodeID populates c.NodeID using LoadOrCreateNodeID(c.StateDir) if c.NodeID is empty.
func (c *Config) EnsureNodeID() error {
	if c.NodeID != "" {
		return nil
	}
	id, err := LoadOrCreateNodeID(c.StateDir)
	if err != nil {
		return err
	}
	c.NodeID = id
	return nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	var out []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
