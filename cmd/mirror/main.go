package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mirror-sync/mirror/internal/config"
	"github.com/mirror-sync/mirror/internal/store"
	"github.com/mirror-sync/mirror/internal/sync"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mirror: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return fmt.Errorf("create sync path %q: %w", cfg.Path, err)
	}
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return fmt.Errorf("create state dir %q: %w", cfg.StateDir, err)
	}

	if cfg.NodeID == "" {
		nodeID, err := config.LoadOrCreateNodeID(cfg.StateDir)
		if err != nil {
			return fmt.Errorf("load or create node_id: %w", err)
		}
		cfg.NodeID = nodeID
	}

	st, err := store.Open(cfg.StateDir)
	if err != nil {
		return fmt.Errorf("open state store %q: %w", cfg.StateDir, err)
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			logger.Warn("failed to close state store cleanly", "err", closeErr)
		}
	}()

	engine, err := sync.NewEngine(*cfg, st, logger)
	if err != nil {
		return fmt.Errorf("init sync engine: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := engine.Run(ctx); err != nil {
		return fmt.Errorf("run sync engine: %w", err)
	}

	logger.Info("mirror shut down cleanly", "node_id", cfg.NodeID)
	return nil
}

func newLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	return slog.New(handler)
}
