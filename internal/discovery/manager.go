package discovery

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mirror-sync/mirror/internal/config"
)

const (
	SourceStatic = "static"
	SourceDNS    = "dns"
	SourceLAN    = "lan"

	DefaultStaticInterval = 5 * time.Second
)

// OnPeerDiscovered is invoked whenever a peer endpoint (`host:port` or `ip:port`) is discovered.
type OnPeerDiscovered func(addr string, source string)

// PeerEvent represents a discovered peer endpoint and its discovery source.
type PeerEvent struct {
	Addr   string
	Source string
}

// Manager coordinates Static, Docker Swarm DNS, and UDP LAN broadcast discovery routines.
type Manager struct {
	cfg                *config.Config
	listenPort         int
	onPeer             OnPeerDiscovered
	logger             *slog.Logger
	resolver           Resolver
	pollInterval       time.Duration
	lanBroadcastTarget []string
}

// NewManager creates a new discovery Manager from cfg.
func NewManager(cfg *config.Config, onPeer OnPeerDiscovered, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	listenPort := ExtractListenPort(cfg.ListenAddr, 22001)
	return &Manager{
		cfg:          cfg,
		listenPort:   listenPort,
		onPeer:       onPeer,
		logger:       logger,
		resolver:     net.DefaultResolver,
		pollInterval: DefaultStaticInterval,
	}
}

// WithResolver sets a custom DNS Resolver on the Manager (used by unit tests).
func (m *Manager) WithResolver(r Resolver) *Manager {
	if r != nil {
		m.resolver = r
	}
	return m
}

// WithPollInterval configures the polling/announce interval for Static, DNS, and LAN discovery.
func (m *Manager) WithPollInterval(d time.Duration) *Manager {
	if d > 0 {
		m.pollInterval = d
	}
	return m
}

// WithLANBroadcastTargets overrides the UDP target addresses for LAN broadcast beacons.
func (m *Manager) WithLANBroadcastTargets(targets []string) *Manager {
	m.lanBroadcastTarget = targets
	return m
}

// Run starts all configured discovery workers (Static, DNS, and LAN) and blocks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	// 1. Static peer announcer.
	if len(m.cfg.Peers) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.runStatic(ctx)
		}()
	}

	// 2. Docker Swarm DNS discovery (`tasks.<service>`).
	if len(m.cfg.DiscoveryDNS) > 0 {
		dnsDisc := NewDNSDiscoverer(
			m.cfg.DiscoveryDNS,
			m.listenPort,
			m.listenPort,
			m.onPeer,
			m.logger,
		).WithResolver(m.resolver).WithInterval(m.pollInterval)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dnsDisc.Run(ctx)
		}()
	}

	// 3. UDP LAN broadcast discovery.
	if m.cfg.DiscoveryLAN {
		lanDisc := NewLANDiscoverer(
			m.cfg.NodeID,
			m.cfg.Secret,
			m.listenPort,
			m.cfg.DiscoveryPort,
			m.onPeer,
			m.logger,
		).WithInterval(m.pollInterval)
		if len(m.lanBroadcastTarget) > 0 {
			lanDisc.WithBroadcastTargets(m.lanBroadcastTarget)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lanDisc.Run(ctx); err != nil && ctx.Err() == nil {
				m.logger.Warn("LAN discovery stopped with error", "err", err)
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

func (m *Manager) runStatic(ctx context.Context) {
	m.emitStaticPeers()

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.emitStaticPeers()
		}
	}
}

func (m *Manager) emitStaticPeers() {
	for _, raw := range m.cfg.Peers {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		host, port := splitHostPortWithDefault(entry, m.listenPort)
		if host == "" {
			continue
		}
		addr := net.JoinHostPort(host, strconv.Itoa(port))
		if m.onPeer != nil {
			m.onPeer(addr, SourceStatic)
		}
	}
}

// ExtractListenPort parses the numeric port from a listen address like ":22001" or "0.0.0.0:22001".
func ExtractListenPort(listenAddr string, fallback int) int {
	if fallback <= 0 {
		fallback = 22001
	}
	_, portStr, err := net.SplitHostPort(strings.TrimSpace(listenAddr))
	if err != nil {
		return fallback
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return fallback
	}
	return port
}
