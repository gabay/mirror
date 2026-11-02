package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mirror-sync/mirror/internal/config"
)

type mockResolver struct {
	records map[string][]net.IPAddr
}

func (m *mockResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	ips, ok := m.records[host]
	if !ok {
		return nil, fmt.Errorf("no such host: %s", host)
	}
	return ips, nil
}

func TestDNSDiscoverer_DockerSwarmMultiIP(t *testing.T) {
	resolver := &mockResolver{
		records: map[string][]net.IPAddr{
			"tasks.mirror": {
				{IP: net.ParseIP("10.0.0.2")}, // local container IP -> should be filtered on same port
				{IP: net.ParseIP("10.0.0.3")},
				{IP: net.ParseIP("10.0.0.4")},
			},
			"mirror-remote.internal": {
				{IP: net.ParseIP("192.168.10.50")},
			},
		},
	}

	var mu sync.Mutex
	var got []PeerEvent
	onPeer := func(addr string, source string) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, PeerEvent{Addr: addr, Source: source})
	}

	disc := NewDNSDiscoverer(
		[]string{"tasks.mirror", "mirror-remote.internal:22005"},
		22001,
		22001,
		onPeer,
		nil,
	).WithResolver(resolver).WithLocalIPsProvider(func() map[string]bool {
		return map[string]bool{"10.0.0.2": true}
	})

	endpoints := disc.PollOnce(context.Background())

	expected := map[string]bool{
		"10.0.0.3:22001":      true,
		"10.0.0.4:22001":      true,
		"192.168.10.50:22005": true,
	}
	if len(endpoints) != len(expected) {
		t.Fatalf("expected %d endpoints, got %v", len(expected), endpoints)
	}
	for _, ep := range endpoints {
		if !expected[ep] {
			t.Fatalf("unexpected endpoint %q", ep)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("expected 3 callback events, got %+v", got)
	}
	for _, ev := range got {
		if ev.Source != SourceDNS {
			t.Fatalf("expected source %q, got %q", SourceDNS, ev.Source)
		}
	}
}

func TestBeacon_EncodeDecodeAndVerifyHMAC(t *testing.T) {
	now := time.Now()

	// 1. Matching non-empty secret.
	pkt, err := EncodeBeacon("node-alpha", 22001, "cluster-secret", now)
	if err != nil {
		t.Fatalf("EncodeBeacon: %v", err)
	}
	b, err := VerifyBeacon(pkt, "node-beta", "cluster-secret", now.Add(2*time.Second), DefaultMaxBeaconAge)
	if err != nil {
		t.Fatalf("VerifyBeacon matching secret failed: %v", err)
	}
	if b.NodeID != "node-alpha" || b.ListenPort != 22001 {
		t.Fatalf("unexpected decoded beacon: %+v", b)
	}

	// 2. Matching empty secret ("").
	emptyPkt, err := EncodeBeacon("node-alpha", 22001, "", now)
	if err != nil {
		t.Fatalf("EncodeBeacon empty secret: %v", err)
	}
	if _, err := VerifyBeacon(emptyPkt, "node-beta", "", now, DefaultMaxBeaconAge); err != nil {
		t.Fatalf("expected empty secret to match empty secret, got: %v", err)
	}

	// 3. Mismatched secret: empty vs non-empty & secret1 vs secret2.
	if _, err := VerifyBeacon(emptyPkt, "node-beta", "non-empty", now, DefaultMaxBeaconAge); !errors.Is(err, ErrBeaconHMACMismatch) {
		t.Fatalf("expected ErrBeaconHMACMismatch for empty vs non-empty, got: %v", err)
	}
	if _, err := VerifyBeacon(pkt, "node-beta", "", now, DefaultMaxBeaconAge); !errors.Is(err, ErrBeaconHMACMismatch) {
		t.Fatalf("expected ErrBeaconHMACMismatch for non-empty vs empty, got: %v", err)
	}
	if _, err := VerifyBeacon(pkt, "node-beta", "wrong-secret", now, DefaultMaxBeaconAge); !errors.Is(err, ErrBeaconHMACMismatch) {
		t.Fatalf("expected ErrBeaconHMACMismatch for wrong secret, got: %v", err)
	}

	// 4. Ignore self NodeID.
	if _, err := VerifyBeacon(pkt, "node-alpha", "cluster-secret", now, DefaultMaxBeaconAge); !errors.Is(err, ErrSelfBeacon) {
		t.Fatalf("expected ErrSelfBeacon when localNodeID == beacon.NodeID, got: %v", err)
	}

	// 5. Reject stale timestamp.
	if _, err := VerifyBeacon(pkt, "node-beta", "cluster-secret", now.Add(5*time.Minute), DefaultMaxBeaconAge); !errors.Is(err, ErrStaleBeacon) {
		t.Fatalf("expected ErrStaleBeacon, got: %v", err)
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer pc.Close()
	return pc.LocalAddr().(*net.UDPAddr).Port
}

func TestLANDiscoverer_LiveUDPLoopback(t *testing.T) {
	discPort := freeUDPPort(t)
	target := fmt.Sprintf("127.0.0.1:%d", discPort)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node1Found := make(chan string, 8)
	node2Found := make(chan string, 8)
	intruderFound := make(chan string, 8)

	lan1 := NewLANDiscoverer("node-1", "lan-secret", 22101, discPort, func(addr, source string) {
		if source == SourceLAN {
			select {
			case node1Found <- addr:
			default:
			}
		}
	}, nil).WithInterval(40 * time.Millisecond).WithBroadcastTargets([]string{target})

	lan2 := NewLANDiscoverer("node-2", "lan-secret", 22102, discPort, func(addr, source string) {
		if source == SourceLAN {
			select {
			case node2Found <- addr:
			default:
			}
		}
	}, nil).WithInterval(40 * time.Millisecond).WithBroadcastTargets([]string{target})

	// Intruder uses a different secret and should neither be accepted by node-1/node-2 nor accept them.
	lanIntruder := NewLANDiscoverer("node-intruder", "wrong-secret", 22999, discPort, func(addr, source string) {
		select {
		case intruderFound <- addr:
		default:
		}
	}, nil).WithInterval(40 * time.Millisecond).WithBroadcastTargets([]string{target})

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		_ = lan1.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = lan2.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = lanIntruder.Run(ctx)
	}()

	var gotFromNode1, gotFromNode2 string
	for gotFromNode1 == "" || gotFromNode2 == "" {
		select {
		case addr := <-node1Found:
			if addr == "127.0.0.1:22999" {
				t.Fatalf("node-1 accepted beacon from intruder with wrong secret")
			}
			if addr == "127.0.0.1:22101" {
				t.Fatalf("node-1 accepted beacon from self")
			}
			if addr == "127.0.0.1:22102" {
				gotFromNode1 = addr
			}
		case addr := <-node2Found:
			if addr == "127.0.0.1:22999" {
				t.Fatalf("node-2 accepted beacon from intruder with wrong secret")
			}
			if addr == "127.0.0.1:22102" {
				t.Fatalf("node-2 accepted beacon from self")
			}
			if addr == "127.0.0.1:22101" {
				gotFromNode2 = addr
			}
		case addr := <-intruderFound:
			t.Fatalf("intruder unexpectedly verified beacon from %s", addr)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for LAN UDP peer discovery (got1=%q, got2=%q)", gotFromNode1, gotFromNode2)
		}
	}

	cancel()
	wg.Wait()
}

func TestManager_CombinedStaticAndDNS(t *testing.T) {
	cfg := config.Default()
	cfg.NodeID = "node-mgr-1"
	cfg.ListenAddr = ":22001"
	cfg.Peers = []string{"10.1.0.10:22001", "10.1.0.11"}
	cfg.DiscoveryDNS = []string{"tasks.mirror"}
	cfg.DiscoveryLAN = false

	resolver := &mockResolver{
		records: map[string][]net.IPAddr{
			"tasks.mirror": {
				{IP: net.ParseIP("10.0.9.2")},
			},
		},
	}

	var mu sync.Mutex
	seen := make(map[string]string)

	mgr := NewManager(cfg, func(addr, source string) {
		mu.Lock()
		defer mu.Unlock()
		seen[addr] = source
	}, nil).WithResolver(resolver).WithPollInterval(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = mgr.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if seen["10.1.0.10:22001"] != SourceStatic {
		t.Fatalf("expected static peer 10.1.0.10:22001, got %v", seen)
	}
	if seen["10.1.0.11:22001"] != SourceStatic {
		t.Fatalf("expected static peer 10.1.0.11:22001 with default port, got %v", seen)
	}
	if seen["10.0.9.2:22001"] != SourceDNS {
		t.Fatalf("expected DNS peer 10.0.9.2:22001, got %v", seen)
	}
}
