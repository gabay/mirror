package discovery

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultDNSPollInterval is the default interval between Docker Swarm DNS lookups.
	DefaultDNSPollInterval = 5 * time.Second
)

// Resolver abstracts DNS IP lookups so unit tests can mock Docker Swarm tasks.<service> responses.
type Resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// DNSDiscoverer periodically resolves DNS names (such as Docker Swarm `tasks.<service>`)
// and emits every resolved container IP:port endpoint.
type DNSDiscoverer struct {
	dnsNames         []string
	defaultPort      int
	localListenPort  int
	interval         time.Duration
	resolver         Resolver
	localIPsProvider func() map[string]bool
	onPeer           OnPeerDiscovered
	logger           *slog.Logger
}

// NewDNSDiscoverer creates a new DNSDiscoverer for the given DNS names and default port.
func NewDNSDiscoverer(
	dnsNames []string,
	defaultPort int,
	localListenPort int,
	onPeer OnPeerDiscovered,
	logger *slog.Logger,
) *DNSDiscoverer {
	if logger == nil {
		logger = slog.Default()
	}
	if defaultPort <= 0 {
		defaultPort = 22001
	}
	return &DNSDiscoverer{
		dnsNames:         dnsNames,
		defaultPort:      defaultPort,
		localListenPort:  localListenPort,
		interval:         DefaultDNSPollInterval,
		resolver:         net.DefaultResolver,
		localIPsProvider: localInterfaceIPs,
		onPeer:           onPeer,
		logger:           logger,
	}
}

// WithResolver overrides the DNS resolver (useful for unit tests).
func (d *DNSDiscoverer) WithResolver(r Resolver) *DNSDiscoverer {
	if r != nil {
		d.resolver = r
	}
	return d
}

// WithInterval overrides the DNS polling interval.
func (d *DNSDiscoverer) WithInterval(interval time.Duration) *DNSDiscoverer {
	if interval > 0 {
		d.interval = interval
	}
	return d
}

// WithLocalIPsProvider overrides the function used to determine local interface IPs.
func (d *DNSDiscoverer) WithLocalIPsProvider(fn func() map[string]bool) *DNSDiscoverer {
	if fn != nil {
		d.localIPsProvider = fn
	}
	return d
}

// PollOnce resolves all configured DNS names once, filters out local interface IPs
// pointing to the same port, invokes onPeer for each discovered address, and returns
// the list of discovered `ip:port` endpoints.
func (d *DNSDiscoverer) PollOnce(ctx context.Context) []string {
	localIPs := d.localIPsProvider()
	var discovered []string
	seen := make(map[string]bool)

	for _, rawEntry := range d.dnsNames {
		entry := strings.TrimSpace(rawEntry)
		if entry == "" {
			continue
		}
		host, port := splitHostPortWithDefault(entry, d.defaultPort)
		if host == "" {
			continue
		}

		addrs, err := d.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			d.logger.Debug("dns discovery lookup failed", "host", host, "err", err)
			continue
		}

		var resolvedIPs []string
		for _, ipAddr := range addrs {
			if ipAddr.IP == nil {
				continue
			}
			ipStr := ipAddr.IP.String()
			resolvedIPs = append(resolvedIPs, ipStr)
			// Filter out local interface IPs when targeting our own listen port.
			if port == d.localListenPort && localIPs[ipStr] {
				continue
			}

			endpoint := net.JoinHostPort(ipStr, strconv.Itoa(port))
			if seen[endpoint] {
				continue
			}
			seen[endpoint] = true
			discovered = append(discovered, endpoint)
			if d.onPeer != nil {
				d.onPeer(endpoint, SourceDNS)
			}
		}
		d.logger.Debug("dns discovery lookup succeeded",
			"host", host,
			"resolved_ips", resolvedIPs,
			"remote_endpoints", discovered,
		)
	}

	return discovered
}

// Run polls DNS immediately and then periodically until ctx is cancelled.
func (d *DNSDiscoverer) Run(ctx context.Context) error {
	if len(d.dnsNames) == 0 {
		<-ctx.Done()
		return nil
	}

	d.logger.Info("DNS peer discovery started",
		"dns_names", d.dnsNames,
		"default_port", d.defaultPort,
		"interval", d.interval.String(),
	)

	d.PollOnce(ctx)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			d.PollOnce(ctx)
		}
	}
}

// splitHostPortWithDefault extracts host and port from an entry, falling back to defaultPort
// when no port is specified.
func splitHostPortWithDefault(entry string, defaultPort int) (string, int) {
	if h, pStr, err := net.SplitHostPort(entry); err == nil {
		if p, err := strconv.Atoi(pStr); err == nil && p > 0 && p <= 65535 {
			return h, p
		}
	}
	return strings.Trim(entry, "[]"), defaultPort
}

// localInterfaceIPs returns a set of IP strings assigned to local network interfaces.
func localInterfaceIPs() map[string]bool {
	out := make(map[string]bool)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return out
	}
	for _, a := range addrs {
		switch v := a.(type) {
		case *net.IPNet:
			if v.IP != nil {
				out[v.IP.String()] = true
			}
		case *net.IPAddr:
			if v.IP != nil {
				out[v.IP.String()] = true
			}
		}
	}
	return out
}
