package discovery

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	// BeaconMagic is the 8-byte header magic identifying a mirror LAN UDP beacon.
	BeaconMagic = "MIRRLAN1"

	// DefaultLANInterval is the default interval between UDP LAN broadcast beacons.
	DefaultLANInterval = 3 * time.Second

	// DefaultMaxBeaconAge is the maximum allowed clock skew / age for a received LAN beacon.
	DefaultMaxBeaconAge = 60 * time.Second

	// maxBeaconPacketSize caps UDP beacon packet reads.
	maxBeaconPacketSize = 512

	// Linux SO_REUSEPORT socket option constant (15 on asm-generic/socket.h).
	soReusePort = 15
)

var (
	// ErrInvalidBeacon is returned when a UDP packet does not match the mirror beacon wire format.
	ErrInvalidBeacon = errors.New("discovery: invalid LAN beacon packet")
	// ErrSelfBeacon is returned when a received UDP beacon originated from the local NodeID.
	ErrSelfBeacon = errors.New("discovery: ignoring beacon from self")
	// ErrBeaconHMACMismatch is returned when a beacon's HMAC-SHA256 tag does not match our secret.
	ErrBeaconHMACMismatch = errors.New("discovery: LAN beacon HMAC mismatch")
	// ErrStaleBeacon is returned when a beacon's timestamp is outside the allowed clock skew window.
	ErrStaleBeacon = errors.New("discovery: stale LAN beacon timestamp")
)

// Beacon represents a decoded UDP LAN discovery broadcast packet.
//
// Wire layout:
//
//	[8B  Magic "MIRRLAN1"]
//	[2B  uint16 ListenPort]
//	[8B  int64  TimestampNs]
//	[16B Nonce]
//	[2B  uint16 len(NodeID)]
//	[NB  NodeID]
//	[32B HMAC-SHA256(key = secret, data = header_through_NodeID)]
type Beacon struct {
	Magic       [8]byte
	NodeID      string
	ListenPort  uint16
	TimestampNs int64
	Nonce       [16]byte
	HMAC        [32]byte
}

// EncodeBeacon builds and signs a compact UDP LAN discovery beacon packet.
func EncodeBeacon(nodeID string, listenPort int, secret string, now time.Time) ([]byte, error) {
	if nodeID == "" {
		return nil, errors.New("discovery: nodeID must not be empty")
	}
	if len(nodeID) > 256 {
		return nil, errors.New("discovery: nodeID too long")
	}
	if listenPort <= 0 || listenPort > 65535 {
		return nil, fmt.Errorf("discovery: invalid listenPort %d", listenPort)
	}

	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("discovery: generate beacon nonce: %w", err)
	}

	bodyLen := 8 + 2 + 8 + 16 + 2 + len(nodeID)
	pkt := make([]byte, bodyLen+32)

	copy(pkt[0:8], BeaconMagic)
	binary.BigEndian.PutUint16(pkt[8:10], uint16(listenPort))
	binary.BigEndian.PutUint64(pkt[10:18], uint64(now.UnixNano()))
	copy(pkt[18:34], nonce[:])
	binary.BigEndian.PutUint16(pkt[34:36], uint16(len(nodeID)))
	copy(pkt[36:bodyLen], nodeID)

	mac := computeBeaconHMAC(secret, pkt[:bodyLen])
	copy(pkt[bodyLen:], mac[:])

	return pkt, nil
}

// DecodeBeacon parses a raw UDP packet into a Beacon struct without verifying its HMAC or timestamp.
func DecodeBeacon(packet []byte) (*Beacon, error) {
	const minPacketLen = 8 + 2 + 8 + 16 + 2 + 1 + 32
	if len(packet) < minPacketLen || len(packet) > maxBeaconPacketSize {
		return nil, ErrInvalidBeacon
	}
	if string(packet[0:8]) != BeaconMagic {
		return nil, ErrInvalidBeacon
	}

	listenPort := binary.BigEndian.Uint16(packet[8:10])
	if listenPort == 0 {
		return nil, ErrInvalidBeacon
	}
	tsNs := int64(binary.BigEndian.Uint64(packet[10:18]))

	var nonce [16]byte
	copy(nonce[:], packet[18:34])

	nodeIDLen := int(binary.BigEndian.Uint16(packet[34:36]))
	bodyLen := 36 + nodeIDLen
	if nodeIDLen <= 0 || bodyLen+32 != len(packet) {
		return nil, ErrInvalidBeacon
	}

	var b Beacon
	copy(b.Magic[:], packet[0:8])
	b.ListenPort = listenPort
	b.TimestampNs = tsNs
	b.Nonce = nonce
	b.NodeID = string(packet[36:bodyLen])
	copy(b.HMAC[:], packet[bodyLen:bodyLen+32])

	return &b, nil
}

// VerifyBeacon decodes a raw UDP beacon packet, verifies its HMAC-SHA256 tag in constant time
// using secret (matching empty or non-empty secrets identically), rejects beacons from localNodeID,
// and validates that TimestampNs is within maxAge of now.
func VerifyBeacon(
	packet []byte,
	localNodeID string,
	secret string,
	now time.Time,
	maxAge time.Duration,
) (*Beacon, error) {
	b, err := DecodeBeacon(packet)
	if err != nil {
		return nil, err
	}

	bodyLen := len(packet) - 32
	expectedMAC := computeBeaconHMAC(secret, packet[:bodyLen])
	if !hmac.Equal(b.HMAC[:], expectedMAC[:]) {
		return nil, ErrBeaconHMACMismatch
	}

	if b.NodeID == localNodeID {
		return nil, ErrSelfBeacon
	}

	if maxAge <= 0 {
		maxAge = DefaultMaxBeaconAge
	}
	diff := now.UnixNano() - b.TimestampNs
	if diff < 0 {
		diff = -diff
	}
	if diff > maxAge.Nanoseconds() {
		return nil, ErrStaleBeacon
	}

	return b, nil
}

func computeBeaconHMAC(secret string, body []byte) [32]byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("mirror-v1-lan-beacon"))
	mac.Write(body)
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

// LANDiscoverer listens for and broadcasts HMAC-authenticated UDP LAN discovery beacons.
type LANDiscoverer struct {
	nodeID           string
	secret           string
	listenPort       int
	discoveryPort    int
	interval         time.Duration
	maxAge           time.Duration
	broadcastTargets []string
	onPeer           OnPeerDiscovered
	logger           *slog.Logger
}

// NewLANDiscoverer creates a new UDP LAN broadcast discoverer.
func NewLANDiscoverer(
	nodeID string,
	secret string,
	listenPort int,
	discoveryPort int,
	onPeer OnPeerDiscovered,
	logger *slog.Logger,
) *LANDiscoverer {
	if logger == nil {
		logger = slog.Default()
	}
	if discoveryPort <= 0 {
		discoveryPort = 22002
	}
	return &LANDiscoverer{
		nodeID:        nodeID,
		secret:        secret,
		listenPort:    listenPort,
		discoveryPort: discoveryPort,
		interval:      DefaultLANInterval,
		maxAge:        DefaultMaxBeaconAge,
		onPeer:        onPeer,
		logger:        logger,
	}
}

// WithInterval configures the beacon broadcast interval.
func (l *LANDiscoverer) WithInterval(interval time.Duration) *LANDiscoverer {
	if interval > 0 {
		l.interval = interval
	}
	return l
}

// WithBroadcastTargets overrides the target UDP `host:port` addresses used when sending beacons.
func (l *LANDiscoverer) WithBroadcastTargets(targets []string) *LANDiscoverer {
	l.broadcastTargets = targets
	return l
}

// Run binds the UDP discovery socket with SO_REUSEADDR + SO_REUSEPORT, starts the receiver loop,
// and broadcasts beacons periodically until ctx is cancelled.
func (l *LANDiscoverer) Run(ctx context.Context) error {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var sockErr error
			_ = c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
			})
			return sockErr
		},
	}

	bindAddr := fmt.Sprintf(":%d", l.discoveryPort)
	pc, err := lc.ListenPacket(ctx, "udp4", bindAddr)
	if err != nil {
		return fmt.Errorf("discovery: bind UDP %s: %w", bindAddr, err)
	}
	defer pc.Close()

	l.logger.Info("LAN UDP discovery listener started",
		"bind_addr", bindAddr,
		"announce_listen_port", l.listenPort,
		"interval", l.interval.String(),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		l.readLoop(ctx, pc)
	}()

	// Send initial beacon immediately, then periodically.
	l.broadcastOnce()

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = pc.Close()
			wg.Wait()
			return nil
		case <-ticker.C:
			l.broadcastOnce()
		}
	}
}

func (l *LANDiscoverer) readLoop(ctx context.Context, pc net.PacketConn) {
	buf := make([]byte, maxBeaconPacketSize)
	for {
		n, srcAddr, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}

		beacon, err := VerifyBeacon(buf[:n], l.nodeID, l.secret, time.Now(), l.maxAge)
		if err != nil {
			if !errors.Is(err, ErrSelfBeacon) {
				l.logger.Debug("ignoring LAN beacon",
					"sender_addr", srcAddr.String(),
					"err", err,
				)
			}
			continue
		}

		senderIP := extractIPFromAddr(srcAddr)
		if senderIP == "" {
			continue
		}

		endpoint := net.JoinHostPort(senderIP, strconv.Itoa(int(beacon.ListenPort)))
		l.logger.Debug("received valid LAN discovery beacon",
			"remote_node_id", beacon.NodeID,
			"endpoint", endpoint,
		)
		if l.onPeer != nil {
			l.onPeer(endpoint, SourceLAN)
		}
	}
}

func (l *LANDiscoverer) broadcastOnce() {
	pkt, err := EncodeBeacon(l.nodeID, l.listenPort, l.secret, time.Now())
	if err != nil {
		l.logger.Debug("encode LAN beacon failed", "err", err)
		return
	}

	targets := l.broadcastTargets
	if len(targets) == 0 {
		targets = discoverBroadcastTargets(l.discoveryPort)
	}

	// Use an ephemeral UDP socket with SO_BROADCAST enabled so each transmission
	// uses a fresh source port (allowing Linux SO_REUSEPORT to distribute across
	// multiple local listeners when testing on loopback).
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			_ = c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
			})
			return nil
		},
	}
	sendConn, err := lc.ListenPacket(context.Background(), "udp4", ":0")
	if err != nil {
		return
	}
	defer sendConn.Close()

	for _, target := range targets {
		udpAddr, err := net.ResolveUDPAddr("udp4", target)
		if err != nil {
			continue
		}
		_, _ = sendConn.WriteTo(pkt, udpAddr)
	}
}

func extractIPFromAddr(addr net.Addr) string {
	switch a := addr.(type) {
	case *net.UDPAddr:
		if a.IP != nil {
			return a.IP.String()
		}
	default:
		if host, _, err := net.SplitHostPort(addr.String()); err == nil {
			return host
		}
	}
	return ""
}

// discoverBroadcastTargets returns `255.255.255.255:<port>`, `127.0.0.1:<port>`,
// and all active IPv4 subnet broadcast addresses.
func discoverBroadcastTargets(port int) []string {
	portStr := strconv.Itoa(port)
	seen := make(map[string]bool)
	var targets []string

	addTarget := func(ip string) {
		addr := net.JoinHostPort(ip, portStr)
		if !seen[addr] {
			seen[addr] = true
			targets = append(targets, addr)
		}
	}

	addTarget("255.255.255.255")
	addTarget("127.0.0.1")

	ifaces, err := net.Interfaces()
	if err != nil {
		return targets
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagBroadcast == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok || ipNet.IP == nil {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 == nil || len(ipNet.Mask) != 4 {
				continue
			}
			var bcast [4]byte
			for i := range 4 {
				bcast[i] = ip4[i] | ^ipNet.Mask[i]
			}
			addTarget(net.IPv4(bcast[0], bcast[1], bcast[2], bcast[3]).String())
		}
	}
	return targets
}
