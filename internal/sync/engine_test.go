package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mirror-sync/mirror/internal/config"
	"github.com/mirror-sync/mirror/internal/discovery"
	"github.com/mirror-sync/mirror/internal/store"
)

type safeLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeLogBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeLogBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

type testNode struct {
	t        *testing.T
	nodeID   string
	dataDir  string
	stateDir string
	store    *store.Store
	engine   *Engine
	logs     *safeLogBuffer
}

func newTestNode(
	t *testing.T,
	nodeID string,
	secret string,
	strategy string,
	excludes []string,
	peers []string,
) *testNode {
	t.Helper()

	dataDir := t.TempDir()
	stateDir := t.TempDir()

	return newTestNodeWithDirs(t, nodeID, dataDir, stateDir, secret, strategy, excludes, peers)
}

func newTestNodeWithDirs(
	t *testing.T,
	nodeID string,
	dataDir string,
	stateDir string,
	secret string,
	strategy string,
	excludes []string,
	peers []string,
) *testNode {
	t.Helper()

	st, err := store.Open(stateDir)
	if err != nil {
		t.Fatalf("store.Open(%s): %v", nodeID, err)
	}

	logBuf := &safeLogBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	if strategy == "" {
		strategy = config.ConflictStrategyLWW
	}

	cfg := config.Config{
		Path:             dataDir,
		StateDir:         stateDir,
		NodeID:           nodeID,
		ListenAddr:       "127.0.0.1:0",
		Secret:           secret,
		Peers:            peers,
		DiscoveryLAN:     false,
		DiscoveryPort:    22002,
		Exclude:          excludes,
		ConflictStrategy: strategy,
		RescanInterval:   5 * time.Second,
		DebounceDelay:    50 * time.Millisecond,
		LogLevel:         "debug",
	}

	eng, err := NewEngine(cfg, st, logger)
	if err != nil {
		_ = st.Close()
		t.Fatalf("NewEngine(%s): %v", nodeID, err)
	}
	eng.WithDiscoveryInterval(100 * time.Millisecond)

	return &testNode{
		t:        t,
		nodeID:   nodeID,
		dataDir:  dataDir,
		stateDir: stateDir,
		store:    st,
		engine:   eng,
		logs:     logBuf,
	}
}

func (n *testNode) start(ctx context.Context) {
	n.t.Helper()
	if err := n.engine.Start(ctx); err != nil {
		n.t.Fatalf("engine.Start(%s): %v", n.nodeID, err)
	}
}

func (n *testNode) close() {
	_ = n.engine.Close()
	_ = n.store.Close()
}

func waitForCondition(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, desc)
}

func TestEngine_TwoNodesInitialAndLiveSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	nodeA := newTestNode(t, "node-a", "cluster-secret", config.ConflictLWW, []string{"*.ignored"}, nil)
	defer nodeA.close()

	// Pre-populate files on nodeA before starting:
	// 1. Nested directory and small file
	if err := os.MkdirAll(filepath.Join(nodeA.dataDir, "sub", "nested"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	helloContent := []byte("hello distributed world\n")
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "sub", "nested", "hello.txt"), helloContent, 0o640); err != nil {
		t.Fatalf("write hello.txt: %v", err)
	}

	// 2. Multi-chunk file (~300 KB) to exercise FastCDC chunking & block transfer
	largeContent := make([]byte, 300*1024)
	for i := range largeContent {
		largeContent[i] = byte((i*31 + 7) & 0xff)
	}
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "large.bin"), largeContent, 0o750); err != nil {
		t.Fatalf("write large.bin: %v", err)
	}

	// 3. Symlink
	if err := os.Symlink("sub/nested/hello.txt", filepath.Join(nodeA.dataDir, "hello-link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// 4. Ignored file (should not sync)
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "secret.ignored"), []byte("do not sync"), 0o644); err != nil {
		t.Fatalf("write ignored: %v", err)
	}

	nodeA.start(ctx)

	nodeB := newTestNode(
		t,
		"node-b",
		"cluster-secret",
		config.ConflictLWW,
		[]string{"*.ignored"},
		[]string{nodeA.engine.ListenAddr()},
	)
	defer nodeB.close()
	nodeB.start(ctx)

	// Verify initial sync to nodeB.
	waitForCondition(t, 5*time.Second, "initial files synced to node-b", func() bool {
		gotHello, err := os.ReadFile(filepath.Join(nodeB.dataDir, "sub", "nested", "hello.txt"))
		if err != nil || !bytes.Equal(gotHello, helloContent) {
			return false
		}
		gotLarge, err := os.ReadFile(filepath.Join(nodeB.dataDir, "large.bin"))
		if err != nil || !bytes.Equal(gotLarge, largeContent) {
			return false
		}
		linkTarget, err := os.Readlink(filepath.Join(nodeB.dataDir, "hello-link"))
		if err != nil || linkTarget != "sub/nested/hello.txt" {
			return false
		}
		return true
	})

	// Verify ignored file was not synced to nodeB.
	if _, err := os.Lstat(filepath.Join(nodeB.dataDir, "secret.ignored")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected secret.ignored to be excluded on node-b, got err=%v", err)
	}

	// Verify permissions on large.bin were preserved.
	if info, err := os.Lstat(filepath.Join(nodeB.dataDir, "large.bin")); err != nil {
		t.Fatalf("stat large.bin on node-b: %v", err)
	} else if info.Mode().Perm() != 0o750 {
		t.Fatalf("expected large.bin mode 0750 on node-b, got %04o", info.Mode().Perm())
	}

	// Live update 1: Create a new file on nodeB and verify it propagates to nodeA via watcher.
	liveContent := []byte("created live on node-b")
	if err := os.WriteFile(filepath.Join(nodeB.dataDir, "from-b.txt"), liveContent, 0o644); err != nil {
		t.Fatalf("write from-b.txt: %v", err)
	}

	waitForCondition(t, 5*time.Second, "live file from-b.txt synced to node-a", func() bool {
		got, err := os.ReadFile(filepath.Join(nodeA.dataDir, "from-b.txt"))
		return err == nil && bytes.Equal(got, liveContent)
	})

	// Live update 2: Modify middle of large.bin on nodeA and verify nodeB updates it.
	modifiedLarge := append([]byte(nil), largeContent...)
	copy(modifiedLarge[100*1024:], []byte("UPDATED_MIDDLE_SECTION_DATA"))
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "large.bin"), modifiedLarge, 0o750); err != nil {
		t.Fatalf("modify large.bin on node-a: %v", err)
	}

	waitForCondition(t, 5*time.Second, "modified large.bin synced to node-b", func() bool {
		got, err := os.ReadFile(filepath.Join(nodeB.dataDir, "large.bin"))
		return err == nil && bytes.Equal(got, modifiedLarge)
	})

	// Live update 3: Delete hello.txt on nodeB and verify deletion propagates to nodeA via tombstone.
	if err := os.Remove(filepath.Join(nodeB.dataDir, "sub", "nested", "hello.txt")); err != nil {
		t.Fatalf("remove hello.txt on node-b: %v", err)
	}

	waitForCondition(t, 5*time.Second, "deletion of hello.txt synced to node-a", func() bool {
		_, err := os.Lstat(filepath.Join(nodeA.dataDir, "sub", "nested", "hello.txt"))
		return errors.Is(err, os.ErrNotExist)
	})
}

func TestEngine_EmptyNodeJoiningDoesNotDeleteExistingClusterData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Node 1 has existing cluster files.
	existingNode := newTestNode(t, "node-existing", "", config.ConflictLWW, nil, nil)
	defer existingNode.close()

	files := map[string][]byte{
		"alpha.txt":       []byte("alpha data"),
		"dir1/beta.txt":   []byte("beta data"),
		"dir1/dir2/gamma": []byte("gamma data"),
	}
	for rel, content := range files {
		abs := filepath.Join(existingNode.dataDir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(abs, content, 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	existingNode.start(ctx)

	// Node 2 starts completely empty (empty folder, empty state store) and connects to existingNode.
	emptyNode := newTestNode(
		t,
		"node-empty-newcomer",
		"",
		config.ConflictLWW,
		nil,
		[]string{existingNode.engine.ListenAddr()},
	)
	defer emptyNode.close()
	emptyNode.start(ctx)

	// Verify emptyNode pulls all files from existingNode.
	waitForCondition(t, 5*time.Second, "empty node pulls all files from existing node", func() bool {
		for rel, want := range files {
			got, err := os.ReadFile(filepath.Join(emptyNode.dataDir, filepath.FromSlash(rel)))
			if err != nil || !bytes.Equal(got, want) {
				return false
			}
		}
		return true
	})

	// Verify existingNode still has all of its files intact (nothing was deleted).
	for rel, want := range files {
		got, err := os.ReadFile(filepath.Join(existingNode.dataDir, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("existingNode lost file %s: %v", rel, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("existingNode file %s corrupted: got %q, want %q", rel, got, want)
		}
	}
}

func TestEngine_PrePopulatedConcurrentConflict_LWWAndKeepCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Both nodes are pre-populated offline with conflicting versions of "config.yaml"
	// and configured with MIRROR_CONFLICT_STRATEGY=keep-copy.
	node1 := newTestNode(t, "node-11111111", "shared-secret", config.ConflictKeepCopy, nil, nil)
	defer node1.close()

	node2 := newTestNode(t, "node-22222222", "shared-secret", config.ConflictKeepCopy, nil, nil)
	defer node2.close()

	loserContent := []byte("version: 1\nowner: node-1-older\n")
	winnerContent := []byte("version: 2\nowner: node-2-newer\n")

	tOlder := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	tNewer := time.Date(2026, 5, 10, 12, 5, 0, 0, time.UTC)

	file1 := filepath.Join(node1.dataDir, "config.yaml")
	if err := os.WriteFile(file1, loserContent, 0o644); err != nil {
		t.Fatalf("write node1 file: %v", err)
	}
	if err := os.Chtimes(file1, tOlder, tOlder); err != nil {
		t.Fatalf("chtimes node1: %v", err)
	}

	file2 := filepath.Join(node2.dataDir, "config.yaml")
	if err := os.WriteFile(file2, winnerContent, 0o644); err != nil {
		t.Fatalf("write node2 file: %v", err)
	}
	if err := os.Chtimes(file2, tNewer, tNewer); err != nil {
		t.Fatalf("chtimes node2: %v", err)
	}

	// Also add unique non-conflicting files on each node to verify union merge.
	if err := os.WriteFile(filepath.Join(node1.dataDir, "only-1.txt"), []byte("from-1"), 0o644); err != nil {
		t.Fatalf("write only-1.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(node2.dataDir, "only-2.txt"), []byte("from-2"), 0o644); err != nil {
		t.Fatalf("write only-2.txt: %v", err)
	}

	// Start both nodes and dial each other simultaneously to also exercise duplicate connection tie-breaking.
	node1.start(ctx)
	node2.start(ctx)

	if err := node1.engine.DialPeer(ctx, node2.engine.ListenAddr()); err != nil {
		t.Fatalf("node1 DialPeer node2: %v", err)
	}
	if err := node2.engine.DialPeer(ctx, node1.engine.ListenAddr()); err != nil {
		t.Fatalf("node2 DialPeer node1: %v", err)
	}

	expectedConflictFile := "config.sync-conflict-20260510-120000-node-111.yaml"

	waitForCondition(t, 6*time.Second, "both nodes converge on LWW winner and conflict copy", func() bool {
		// Check config.yaml on both nodes matches winnerContent.
		c1, err1 := os.ReadFile(filepath.Join(node1.dataDir, "config.yaml"))
		c2, err2 := os.ReadFile(filepath.Join(node2.dataDir, "config.yaml"))
		if err1 != nil || err2 != nil || !bytes.Equal(c1, winnerContent) || !bytes.Equal(c2, winnerContent) {
			return false
		}

		// Check conflict copy exists on node1 (where local file lost) and synced to node2.
		cc1, err1 := os.ReadFile(filepath.Join(node1.dataDir, expectedConflictFile))
		cc2, err2 := os.ReadFile(filepath.Join(node2.dataDir, expectedConflictFile))
		if err1 != nil || err2 != nil || !bytes.Equal(cc1, loserContent) || !bytes.Equal(cc2, loserContent) {
			return false
		}

		// Check unique files merged on both nodes.
		u1On2, err1 := os.ReadFile(filepath.Join(node2.dataDir, "only-1.txt"))
		u2On1, err2 := os.ReadFile(filepath.Join(node1.dataDir, "only-2.txt"))
		if err1 != nil || err2 != nil || string(u1On2) != "from-1" || string(u2On1) != "from-2" {
			return false
		}

		// Check VersionVectors in Store have converged on both nodes.
		fi1, ok1 := node1.store.GetFile("config.yaml")
		fi2, ok2 := node2.store.GetFile("config.yaml")
		if !ok1 || !ok2 {
			return false
		}
		return fi1.Version["node-11111111"] == 1 &&
			fi1.Version["node-22222222"] == 1 &&
			fi2.Version["node-11111111"] == 1 &&
			fi2.Version["node-22222222"] == 1
	})

	// Verify conflict warning was logged.
	if !strings.Contains(node1.logs.String(), "conflict detected, resolved via LWW") &&
		!strings.Contains(node2.logs.String(), "conflict detected, resolved via LWW") {
		t.Fatalf("expected conflict warning in logs, got node1=%q, node2=%q",
			node1.logs.String(), node2.logs.String())
	}
}

func TestEngine_SecretMismatchRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodeA := newTestNode(t, "node-a", "secret-one", config.ConflictLWW, nil, nil)
	defer nodeA.close()

	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "top-secret.txt"), []byte("classified"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	nodeA.start(ctx)

	nodeB := newTestNode(
		t,
		"node-b",
		"secret-two",
		config.ConflictLWW,
		nil,
		[]string{nodeA.engine.ListenAddr()},
	)
	defer nodeB.close()
	nodeB.start(ctx)

	waitForCondition(t, 3*time.Second, "secret mismatch warning logged", func() bool {
		return strings.Contains(nodeA.logs.String(), "MIRROR_SECRET mismatch") ||
			strings.Contains(nodeB.logs.String(), "MIRROR_SECRET mismatch")
	})

	if nodeA.engine.PeerCount() != 0 || nodeB.engine.PeerCount() != 0 {
		t.Fatalf("expected 0 connected peers on secret mismatch, got A=%d, B=%d",
			nodeA.engine.PeerCount(), nodeB.engine.PeerCount())
	}

	if _, err := os.Lstat(filepath.Join(nodeB.dataDir, "top-secret.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nodeB with wrong secret should not have synced top-secret.txt")
	}

	// Also verify empty secret ("") vs non-empty secret is rejected.
	nodeC := newTestNode(
		t,
		"node-c-empty-secret",
		"",
		config.ConflictLWW,
		nil,
		[]string{nodeA.engine.ListenAddr()},
	)
	defer nodeC.close()
	nodeC.start(ctx)

	waitForCondition(t, 3*time.Second, "empty vs non-empty secret mismatch logged", func() bool {
		return strings.Contains(nodeC.logs.String(), "MIRROR_SECRET mismatch")
	})
	if nodeC.engine.PeerCount() != 0 {
		t.Fatalf("expected 0 connected peers on empty vs non-empty secret mismatch, got C=%d", nodeC.engine.PeerCount())
	}
}

func TestEngine_PrePopulatedNewNodeJoiningCluster_DefaultLWW(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Existing node in the cluster with default LWW strategy.
	clusterNode := newTestNode(t, "node-cluster", "lww-secret", config.ConflictLWW, nil, nil)
	defer clusterNode.close()

	tOlder := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	tMiddle := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	tNewer := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// 1. Overlapping file where joining node has newer mtime.
	clusterOverlapA := filepath.Join(clusterNode.dataDir, "overlap-joiner-wins.txt")
	if err := os.WriteFile(clusterOverlapA, []byte("cluster-older-content"), 0o644); err != nil {
		t.Fatalf("write clusterOverlapA: %v", err)
	}
	if err := os.Chtimes(clusterOverlapA, tOlder, tOlder); err != nil {
		t.Fatalf("chtimes clusterOverlapA: %v", err)
	}

	// 2. Overlapping file where existing cluster node has newer mtime.
	clusterOverlapB := filepath.Join(clusterNode.dataDir, "overlap-cluster-wins.txt")
	if err := os.WriteFile(clusterOverlapB, []byte("cluster-newer-content"), 0o644); err != nil {
		t.Fatalf("write clusterOverlapB: %v", err)
	}
	if err := os.Chtimes(clusterOverlapB, tNewer, tNewer); err != nil {
		t.Fatalf("chtimes clusterOverlapB: %v", err)
	}

	// 3. Non-overlapping file only on cluster node.
	if err := os.WriteFile(filepath.Join(clusterNode.dataDir, "cluster-only.txt"), []byte("from-cluster"), 0o644); err != nil {
		t.Fatalf("write cluster-only.txt: %v", err)
	}

	clusterNode.start(ctx)

	// Pre-populate new joining node before starting it.
	joinerDataDir := t.TempDir()
	joinerStateDir := t.TempDir()

	joinerOverlapA := filepath.Join(joinerDataDir, "overlap-joiner-wins.txt")
	if err := os.WriteFile(joinerOverlapA, []byte("joiner-newer-content"), 0o644); err != nil {
		t.Fatalf("write joinerOverlapA: %v", err)
	}
	if err := os.Chtimes(joinerOverlapA, tMiddle, tMiddle); err != nil {
		t.Fatalf("chtimes joinerOverlapA: %v", err)
	}

	joinerOverlapB := filepath.Join(joinerDataDir, "overlap-cluster-wins.txt")
	if err := os.WriteFile(joinerOverlapB, []byte("joiner-older-content"), 0o644); err != nil {
		t.Fatalf("write joinerOverlapB: %v", err)
	}
	if err := os.Chtimes(joinerOverlapB, tOlder, tOlder); err != nil {
		t.Fatalf("chtimes joinerOverlapB: %v", err)
	}

	if err := os.WriteFile(filepath.Join(joinerDataDir, "joiner-only.txt"), []byte("from-joiner"), 0o644); err != nil {
		t.Fatalf("write joiner-only.txt: %v", err)
	}

	joinerNode := newTestNodeWithDirs(
		t,
		"node-joiner",
		joinerDataDir,
		joinerStateDir,
		"lww-secret",
		config.ConflictLWW,
		nil,
		[]string{clusterNode.engine.ListenAddr()},
	)
	defer joinerNode.close()
	joinerNode.start(ctx)

	waitForCondition(t, 6*time.Second, "pre-populated joiner and cluster converge via LWW", func() bool {
		for _, n := range []*testNode{clusterNode, joinerNode} {
			gotA, err := os.ReadFile(filepath.Join(n.dataDir, "overlap-joiner-wins.txt"))
			if err != nil || string(gotA) != "joiner-newer-content" {
				return false
			}
			gotB, err := os.ReadFile(filepath.Join(n.dataDir, "overlap-cluster-wins.txt"))
			if err != nil || string(gotB) != "cluster-newer-content" {
				return false
			}
			gotClusterOnly, err := os.ReadFile(filepath.Join(n.dataDir, "cluster-only.txt"))
			if err != nil || string(gotClusterOnly) != "from-cluster" {
				return false
			}
			gotJoinerOnly, err := os.ReadFile(filepath.Join(n.dataDir, "joiner-only.txt"))
			if err != nil || string(gotJoinerOnly) != "from-joiner" {
				return false
			}
		}
		return true
	})

	// Verify conflict warning was logged on both or either node.
	if !strings.Contains(clusterNode.logs.String(), "conflict detected, resolved via LWW") &&
		!strings.Contains(joinerNode.logs.String(), "conflict detected, resolved via LWW") {
		t.Fatalf("expected conflict warning in logs")
	}

	// Verify no .sync-conflict-* files were created when using default LWW strategy.
	for _, n := range []*testNode{clusterNode, joinerNode} {
		entries, err := os.ReadDir(n.dataDir)
		if err != nil {
			t.Fatalf("readdir %s: %v", n.nodeID, err)
		}
		for _, e := range entries {
			if strings.Contains(e.Name(), ".sync-conflict-") {
				t.Fatalf("unexpected conflict copy %q on %s with lww strategy", e.Name(), n.nodeID)
			}
		}
	}
}

func TestEngine_ThreeNodeMeshConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	node1 := newTestNode(t, "node-1", "mesh-secret", config.ConflictLWW, nil, nil)
	defer node1.close()
	node1.start(ctx)

	node2 := newTestNode(t, "node-2", "mesh-secret", config.ConflictLWW, nil, []string{node1.engine.ListenAddr()})
	defer node2.close()
	node2.start(ctx)

	node3 := newTestNode(t, "node-3", "mesh-secret", config.ConflictLWW, nil, []string{
		node1.engine.ListenAddr(),
		node2.engine.ListenAddr(),
	})
	defer node3.close()
	node3.start(ctx)

	nodes := []*testNode{node1, node2, node3}

	// 1. Create distinct files on each of the 3 nodes, plus a directory with custom permissions & file with explicit mtime on node1.
	dirPath1 := filepath.Join(node1.dataDir, "shared-dir")
	if err := os.MkdirAll(dirPath1, 0o750); err != nil {
		t.Fatalf("mkdir shared-dir: %v", err)
	}
	if err := os.Chmod(dirPath1, 0o750); err != nil {
		t.Fatalf("chmod shared-dir: %v", err)
	}

	fixedMtime := time.Date(2026, 7, 15, 8, 30, 0, 0, time.UTC)
	metaFile1 := filepath.Join(dirPath1, "meta.txt")
	if err := os.WriteFile(metaFile1, []byte("metadata test"), 0o640); err != nil {
		t.Fatalf("write meta.txt: %v", err)
	}
	if err := os.Chmod(metaFile1, 0o640); err != nil {
		t.Fatalf("chmod meta.txt: %v", err)
	}
	if err := os.Chtimes(metaFile1, fixedMtime, fixedMtime); err != nil {
		t.Fatalf("chtimes meta.txt: %v", err)
	}

	// 2. Create a > 200KB multi-chunk file (260 KB) on node1.
	largeData := make([]byte, 260*1024)
	for i := range largeData {
		largeData[i] = byte((i*17 + 13) & 0xff)
	}
	if err := os.WriteFile(filepath.Join(node1.dataDir, "mesh-large.bin"), largeData, 0o644); err != nil {
		t.Fatalf("write mesh-large.bin: %v", err)
	}

	// 3. Create symlink on node3.
	if err := os.Symlink("shared-dir/meta.txt", filepath.Join(node3.dataDir, "meta-link")); err != nil {
		t.Fatalf("symlink on node3: %v", err)
	}

	for i, n := range nodes {
		name := fmt.Sprintf("file-from-%d.txt", i+1)
		content := []byte(fmt.Sprintf("payload from node %d", i+1))
		if err := os.WriteFile(filepath.Join(n.dataDir, name), content, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	waitForCondition(t, 8*time.Second, "all 3 nodes converge on initial files, large file, symlink, and metadata", func() bool {
		for _, n := range nodes {
			for i := range nodes {
				name := fmt.Sprintf("file-from-%d.txt", i+1)
				want := fmt.Sprintf("payload from node %d", i+1)
				got, err := os.ReadFile(filepath.Join(n.dataDir, name))
				if err != nil || string(got) != want {
					return false
				}
			}
			gotLarge, err := os.ReadFile(filepath.Join(n.dataDir, "mesh-large.bin"))
			if err != nil || !bytes.Equal(gotLarge, largeData) {
				return false
			}
			linkTarget, err := os.Readlink(filepath.Join(n.dataDir, "meta-link"))
			if err != nil || linkTarget != "shared-dir/meta.txt" {
				return false
			}
			dirInfo, err := os.Lstat(filepath.Join(n.dataDir, "shared-dir"))
			if err != nil || dirInfo.Mode().Perm() != 0o750 {
				return false
			}
			fileInfo, err := os.Lstat(filepath.Join(n.dataDir, "shared-dir", "meta.txt"))
			if err != nil || fileInfo.Mode().Perm() != 0o640 || !fileInfo.ModTime().Equal(fixedMtime) {
				return false
			}
		}
		return true
	})

	// 4. Perform a partial block update in the middle of the 260 KB file on node2 and verify node1 & node3 converge.
	updatedLarge := append([]byte(nil), largeData...)
	copy(updatedLarge[120*1024:], []byte("PARTIAL_FASTCDC_BLOCK_PATCH_IN_MESH"))
	if err := os.WriteFile(filepath.Join(node2.dataDir, "mesh-large.bin"), updatedLarge, 0o644); err != nil {
		t.Fatalf("update mesh-large.bin on node2: %v", err)
	}

	waitForCondition(t, 8*time.Second, "partial block update on mesh-large.bin propagates to all 3 nodes", func() bool {
		for _, n := range nodes {
			got, err := os.ReadFile(filepath.Join(n.dataDir, "mesh-large.bin"))
			if err != nil || !bytes.Equal(got, updatedLarge) {
				return false
			}
		}
		return true
	})

	// 5. Delete file-from-1.txt on node3 and verify deletion propagates via tombstone to node1 and node2.
	if err := os.Remove(filepath.Join(node3.dataDir, "file-from-1.txt")); err != nil {
		t.Fatalf("remove file-from-1.txt on node3: %v", err)
	}

	waitForCondition(t, 8*time.Second, "tombstone deletion propagates across all 3 nodes", func() bool {
		for _, n := range nodes {
			if _, err := os.Lstat(filepath.Join(n.dataDir, "file-from-1.txt")); !errors.Is(err, os.ErrNotExist) {
				return false
			}
		}
		return true
	})
}

func TestEngine_ExclusionsAndSpecialFileFIFO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	excludes := []string{"*.ignored", ".git", "node_modules", "cache/**"}

	nodeA := newTestNode(t, "node-a", "fifo-secret", config.ConflictLWW, excludes, nil)
	defer nodeA.close()

	// Pre-populate excluded files and directories on nodeA.
	excludedDirs := []string{
		filepath.Join(nodeA.dataDir, ".git", "objects"),
		filepath.Join(nodeA.dataDir, "node_modules", "left-pad"),
		filepath.Join(nodeA.dataDir, "cache", "shards"),
		filepath.Join(nodeA.dataDir, "included"),
	}
	for _, d := range excludedDirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	excludedFiles := map[string]string{
		"root.ignored":                   "ignored root file",
		"included/nested.ignored":        "ignored nested file",
		".git/HEAD":                      "ref: refs/heads/main",
		"node_modules/left-pad/index.js": "module.exports = {}",
		"cache/shards/01.bin":            "cached shard",
	}
	for rel, content := range excludedFiles {
		if err := os.WriteFile(filepath.Join(nodeA.dataDir, filepath.FromSlash(rel)), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	// Pre-populate a valid included file.
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "included", "valid.txt"), []byte("sync-me"), 0o644); err != nil {
		t.Fatalf("write valid.txt: %v", err)
	}

	// Pre-populate a named pipe (FIFO special file) on nodeA.
	fifoPath := filepath.Join(nodeA.dataDir, "included", "special.fifo")
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	nodeA.start(ctx)

	nodeB := newTestNode(
		t,
		"node-b",
		"fifo-secret",
		config.ConflictLWW,
		excludes,
		[]string{nodeA.engine.ListenAddr()},
	)
	defer nodeB.close()
	nodeB.start(ctx)

	waitForCondition(t, 5*time.Second, "valid.txt synced to nodeB", func() bool {
		got, err := os.ReadFile(filepath.Join(nodeB.dataDir, "included", "valid.txt"))
		return err == nil && string(got) == "sync-me"
	})

	// Verify warning was logged for the FIFO special file on nodeA.
	if !strings.Contains(nodeA.logs.String(), "ignoring unsupported special file") ||
		!strings.Contains(nodeA.logs.String(), "included/special.fifo") {
		t.Fatalf("expected warning log for special.fifo on nodeA, got logs:\n%s", nodeA.logs.String())
	}

	// Verify FIFO and all excluded paths were NOT synced to nodeB.
	for rel := range excludedFiles {
		if _, err := os.Lstat(filepath.Join(nodeB.dataDir, filepath.FromSlash(rel))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected excluded path %q not to exist on nodeB, got err=%v", rel, err)
		}
	}
	for _, excludedDir := range []string{".git", "node_modules", "cache/shards"} {
		if _, err := os.Lstat(filepath.Join(nodeB.dataDir, filepath.FromSlash(excludedDir))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected excluded directory %q not to exist on nodeB, got err=%v", excludedDir, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(nodeB.dataDir, "included", "special.fifo")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected special.fifo not to exist on nodeB, got err=%v", err)
	}

	// Live test: create a new FIFO, a new excluded file, and a new normal file while watcher is active.
	liveFIFO := filepath.Join(nodeA.dataDir, "included", "live.fifo")
	if err := syscall.Mkfifo(liveFIFO, 0o600); err != nil {
		t.Fatalf("mkfifo live.fifo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "live.ignored"), []byte("ignore-live"), 0o644); err != nil {
		t.Fatalf("write live.ignored: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "included", "live-valid.txt"), []byte("live-ok"), 0o644); err != nil {
		t.Fatalf("write live-valid.txt: %v", err)
	}

	waitForCondition(t, 5*time.Second, "live-valid.txt synced to nodeB and live.fifo warning logged", func() bool {
		got, err := os.ReadFile(filepath.Join(nodeB.dataDir, "included", "live-valid.txt"))
		if err != nil || string(got) != "live-ok" {
			return false
		}
		return strings.Contains(nodeA.logs.String(), "included/live.fifo")
	})

	if _, err := os.Lstat(filepath.Join(nodeB.dataDir, "live.ignored")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected live.ignored not to sync to nodeB")
	}
	if _, err := os.Lstat(filepath.Join(nodeB.dataDir, "included", "live.fifo")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected live.fifo not to sync to nodeB")
	}
}

type staticDNSResolver struct {
	mu      sync.RWMutex
	records map[string][]net.IPAddr
}

func (r *staticDNSResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ips, ok := r.records[host]; ok {
		out := make([]net.IPAddr, len(ips))
		copy(out, ips)
		return out, nil
	}
	return nil, fmt.Errorf("host not found: %s", host)
}

func (r *staticDNSResolver) SetRecords(host string, ips []net.IPAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.records == nil {
		r.records = make(map[string][]net.IPAddr)
	}
	r.records[host] = ips
}

func freeTestUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer pc.Close()
	return pc.LocalAddr().(*net.UDPAddr).Port
}

func TestEngine_LANDiscoveryAndSwarmDNSDiscoveryIntegration(t *testing.T) {
	t.Run("LAN_UDP_AutoDiscovery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		discPort := freeTestUDPPort(t)
		lanTarget := fmt.Sprintf("127.0.0.1:%d", discPort)

		createLANNode := func(nodeID string) *testNode {
			dataDir := t.TempDir()
			stateDir := t.TempDir()
			st, err := store.Open(stateDir)
			if err != nil {
				t.Fatalf("store.Open: %v", err)
			}
			logBuf := &safeLogBuffer{}
			logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			cfg := config.Config{
				Path:             dataDir,
				StateDir:         stateDir,
				NodeID:           nodeID,
				ListenAddr:       "127.0.0.1:0",
				Secret:           "lan-auto-secret",
				DiscoveryLAN:     true,
				DiscoveryPort:    discPort,
				ConflictStrategy: config.ConflictLWW,
				RescanInterval:   5 * time.Second,
				DebounceDelay:    50 * time.Millisecond,
				LogLevel:         "debug",
			}
			eng, err := NewEngine(cfg, st, logger)
			if err != nil {
				_ = st.Close()
				t.Fatalf("NewEngine: %v", err)
			}
			eng.WithDiscoveryInterval(50 * time.Millisecond).
				WithLANBroadcastTargets([]string{lanTarget})
			return &testNode{
				t:        t,
				nodeID:   nodeID,
				dataDir:  dataDir,
				stateDir: stateDir,
				store:    st,
				engine:   eng,
				logs:     logBuf,
			}
		}

		node1 := createLANNode("lan-node-1")
		defer node1.close()
		if err := os.WriteFile(filepath.Join(node1.dataDir, "lan-discovered.txt"), []byte("hello via UDP LAN beacon"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		node1.start(ctx)

		node2 := createLANNode("lan-node-2")
		defer node2.close()
		node2.start(ctx)

		waitForCondition(t, 6*time.Second, "nodes auto-discover via UDP LAN and sync file", func() bool {
			got, err := os.ReadFile(filepath.Join(node2.dataDir, "lan-discovered.txt"))
			return err == nil && string(got) == "hello via UDP LAN beacon"
		})
	})

	t.Run("Swarm_DNS_AutoDiscovery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		node1 := newTestNode(t, "swarm-node-1", "swarm-secret", config.ConflictLWW, nil, nil)
		defer node1.close()
		if err := os.WriteFile(filepath.Join(node1.dataDir, "swarm-task.txt"), []byte("hello from tasks.mirror"), 0o644); err != nil {
			t.Fatalf("write swarm-task.txt: %v", err)
		}
		node1.start(ctx)

		_, portStr, err := net.SplitHostPort(node1.engine.ListenAddr())
		if err != nil {
			t.Fatalf("SplitHostPort: %v", err)
		}

		resolver := &staticDNSResolver{
			records: map[string][]net.IPAddr{
				"tasks.mirror": {
					{IP: net.ParseIP("127.0.0.1")},
				},
			},
		}

		dataDir2 := t.TempDir()
		stateDir2 := t.TempDir()
		st2, err := store.Open(stateDir2)
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		logBuf2 := &safeLogBuffer{}
		logger2 := slog.New(slog.NewTextHandler(logBuf2, &slog.HandlerOptions{Level: slog.LevelDebug}))
		cfg2 := config.Config{
			Path:             dataDir2,
			StateDir:         stateDir2,
			NodeID:           "swarm-node-2",
			ListenAddr:       "127.0.0.1:0",
			Secret:           "swarm-secret",
			DiscoveryDNS:     []string{fmt.Sprintf("tasks.mirror:%s", portStr)},
			DiscoveryLAN:     false,
			DiscoveryPort:    22002,
			ConflictStrategy: config.ConflictLWW,
			RescanInterval:   5 * time.Second,
			DebounceDelay:    50 * time.Millisecond,
			LogLevel:         "debug",
		}
		eng2, err := NewEngine(cfg2, st2, logger2)
		if err != nil {
			_ = st2.Close()
			t.Fatalf("NewEngine: %v", err)
		}
		eng2.WithResolver(discovery.Resolver(resolver)).
			WithDiscoveryInterval(50 * time.Millisecond)

		node2 := &testNode{
			t:        t,
			nodeID:   "swarm-node-2",
			dataDir:  dataDir2,
			stateDir: stateDir2,
			store:    st2,
			engine:   eng2,
			logs:     logBuf2,
		}
		defer node2.close()
		node2.start(ctx)

		waitForCondition(t, 6*time.Second, "node2 discovers node1 via tasks.mirror DNS and syncs file", func() bool {
			got, err := os.ReadFile(filepath.Join(node2.dataDir, "swarm-task.txt"))
			return err == nil && string(got) == "hello from tasks.mirror"
		})
	})
}

// blackholeTCPProxy forwards TCP connections to a backend address and allows blackholing
// active connections (dropping all subsequent bytes without sending TCP FIN/RST) to simulate
// half-open Docker Swarm overlay network connections.
type blackholeTCPProxy struct {
	ln          net.Listener
	targetAddr  atomic.Value
	blackholed  atomic.Bool
	mu          sync.Mutex
	clientConns []net.Conn
	serverConns []net.Conn
}

func newBlackholeTCPProxy(t *testing.T, listenAddr string, initialTarget string) *blackholeTCPProxy {
	t.Helper()
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		t.Fatalf("newBlackholeTCPProxy listen %s: %v", listenAddr, err)
	}
	p := &blackholeTCPProxy{ln: ln}
	p.targetAddr.Store(initialTarget)

	go p.acceptLoop()
	return p
}

func (p *blackholeTCPProxy) Addr() string {
	return p.ln.Addr().String()
}

func (p *blackholeTCPProxy) SetBlackhole(enabled bool) {
	p.blackholed.Store(enabled)
}

// SeverBackendOnly closes the backend side while keeping the client side open & blackholed,
// creating a true half-open socket on the client (no FIN/RST delivered to client).
func (p *blackholeTCPProxy) SeverBackendOnly() {
	p.blackholed.Store(true)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, sc := range p.serverConns {
		_ = sc.Close()
	}
	p.serverConns = nil
}

func (p *blackholeTCPProxy) Close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clientConns {
		_ = c.Close()
	}
	for _, s := range p.serverConns {
		_ = s.Close()
	}
}

func (p *blackholeTCPProxy) acceptLoop() {
	for {
		cConn, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.blackholed.Load() {
			// Accept and hold open silently when blackholed.
			p.mu.Lock()
			p.clientConns = append(p.clientConns, cConn)
			p.mu.Unlock()
			continue
		}
		target := p.targetAddr.Load().(string)
		sConn, err := net.DialTimeout("tcp", target, 2*time.Second)
		if err != nil {
			_ = cConn.Close()
			continue
		}
		p.mu.Lock()
		p.clientConns = append(p.clientConns, cConn)
		p.serverConns = append(p.serverConns, sConn)
		p.mu.Unlock()

		go p.pipe(cConn, sConn)
		go p.pipe(sConn, cConn)
	}
}

func (p *blackholeTCPProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.blackholed.Load() {
			if _, wErr := dst.Write(buf[:n]); wErr != nil {
				return
			}
		}
		if err != nil {
			if !p.blackholed.Load() {
				_ = dst.Close()
			}
			return
		}
	}
}

func TestEngine_SwarmDNS_HalfOpenCanonicalSessionAndContainerRestartE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Allocate a shared Swarm listen port on distinct loopback IPs (127.0.0.2, 127.0.0.3, 127.0.0.4)
	// to faithfully model Docker Swarm containers on an overlay network discovered via tasks.mirror.
	probeLn, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Fatalf("listen 127.0.0.2: %v", err)
	}
	swarmPort := probeLn.Addr().(*net.TCPAddr).Port
	_ = probeLn.Close()

	dnsEntry := fmt.Sprintf("tasks.mirror:%d", swarmPort)
	resolver := &staticDNSResolver{}
	resolver.SetRecords("tasks.mirror", []net.IPAddr{
		{IP: net.ParseIP("127.0.0.2")}, // Node 1 (smaller NodeID: "1111-node-alpha")
		{IP: net.ParseIP("127.0.0.3")}, // Proxy to Node 2's initial container
	})

	createSwarmNode := func(nodeID, bindIP, dataDir, stateDir string) *testNode {
		st, err := store.Open(stateDir)
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		logBuf := &safeLogBuffer{}
		logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		cfg := config.Config{
			Path:             dataDir,
			StateDir:         stateDir,
			NodeID:           nodeID,
			ListenAddr:       fmt.Sprintf("%s:%d", bindIP, swarmPort),
			Secret:           "swarm-e2e-secret",
			DiscoveryDNS:     []string{dnsEntry},
			DiscoveryLAN:     false,
			DiscoveryPort:    22002,
			ConflictStrategy: config.ConflictLWW,
			RescanInterval:   2 * time.Second,
			DebounceDelay:    50 * time.Millisecond,
			LogLevel:         "debug",
		}
		eng, err := NewEngine(cfg, st, logger)
		if err != nil {
			_ = st.Close()
			t.Fatalf("NewEngine: %v", err)
		}
		eng.WithResolver(resolver).
			WithDiscoveryInterval(100 * time.Millisecond).
			WithPingInterval(250 * time.Millisecond).
			WithProbeTimeout(150 * time.Millisecond)

		return &testNode{
			t:        t,
			nodeID:   nodeID,
			dataDir:  dataDir,
			stateDir: stateDir,
			store:    st,
			engine:   eng,
			logs:     logBuf,
		}
	}

	// Node 1 has smaller NodeID ("1111-node-alpha"), so outbound connections from Node 1 -> Node 2
	// are canonical (isCanonical = true).
	node1 := createSwarmNode("1111-node-alpha", "127.0.0.2", t.TempDir(), t.TempDir())
	defer node1.close()

	// Node 2 has larger NodeID ("9999-node-omega"). Bind Node 2's first container on 127.0.0.13
	// and front it with a blackhole proxy at 127.0.0.3:<swarmPort> so we can leave Node 1's
	// outbound canonical socket half-open when Node 2 restarts on a new overlay IP (127.0.0.4).
	node2DataDir := t.TempDir()
	node2StateDir := t.TempDir()
	node2v1 := createSwarmNode("9999-node-omega", "127.0.0.13", node2DataDir, node2StateDir)
	proxy := newBlackholeTCPProxy(t, fmt.Sprintf("127.0.0.3:%d", swarmPort), fmt.Sprintf("127.0.0.13:%d", swarmPort))
	defer proxy.Close()

	node1.start(ctx)
	node2v1.start(ctx)

	// Phase 1: Verify initial Swarm DNS discovery and live watcher sync in both directions.
	if err := os.WriteFile(filepath.Join(node1.dataDir, "phase1-from-1.txt"), []byte("phase1-ok"), 0o644); err != nil {
		t.Fatalf("write phase1-from-1.txt: %v", err)
	}
	waitForCondition(t, 5*time.Second, "phase 1 live sync from node1 to node2v1", func() bool {
		got, err := os.ReadFile(filepath.Join(node2DataDir, "phase1-from-1.txt"))
		return err == nil && string(got) == "phase1-ok"
	})

	// Phase 2: Simulate Docker Swarm container restart / reschedule of Node 2 onto a new overlay IP (127.0.0.4)
	// while Node 1's old TCP connection to 127.0.0.3 remains half-open (blackholed in overlay network, no FIN/RST).
	proxy.SeverBackendOnly()
	node2v1.close()

	// Update Swarm DNS tasks.mirror to point to Node 2's new container IP 127.0.0.4.
	resolver.SetRecords("tasks.mirror", []net.IPAddr{
		{IP: net.ParseIP("127.0.0.2")},
		{IP: net.ParseIP("127.0.0.4")},
	})

	// Start Node 2's new container with the same persisted stateDir & dataDir (same NodeID "9999-node-omega").
	node2v2 := createSwarmNode("9999-node-omega", "127.0.0.4", node2DataDir, node2StateDir)
	defer node2v2.close()
	node2v2.start(ctx)

	// Write new files on BOTH Node 1 and Node 2 and verify the watcher detects and syncs them across.
	if err := os.WriteFile(filepath.Join(node2DataDir, "after-restart-from-2.txt"), []byte("synced-from-restarted-node2"), 0o644); err != nil {
		t.Fatalf("write after-restart-from-2.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(node1.dataDir, "after-restart-from-1.txt"), []byte("synced-from-surviving-node1"), 0o644); err != nil {
		t.Fatalf("write after-restart-from-1.txt: %v", err)
	}

	waitForCondition(t, 6*time.Second, "bidirectional live sync recovers after half-open canonical session and Swarm IP change", func() bool {
		gotFrom2, err1 := os.ReadFile(filepath.Join(node1.dataDir, "after-restart-from-2.txt"))
		gotFrom1, err2 := os.ReadFile(filepath.Join(node2DataDir, "after-restart-from-1.txt"))
		return err1 == nil && string(gotFrom2) == "synced-from-restarted-node2" &&
			err2 == nil && string(gotFrom1) == "synced-from-surviving-node1"
	})

	// Phase 3: Simulate silent overlay network blackhole where NEITHER container restarts
	// (no FIN/RST on either side), files are modified while the connection is dead, and then
	// the overlay network heals. Heartbeat ping/read deadlines must evict the dead session
	// and DNS discovery must re-establish sync automatically.
	proxy2 := newBlackholeTCPProxy(t, fmt.Sprintf("127.0.0.5:%d", swarmPort), fmt.Sprintf("127.0.0.4:%d", swarmPort))
	defer proxy2.Close()

	// Route Node 1 -> Node 2 via proxy2 (127.0.0.5) and blackhole it after connection is active.
	resolver.SetRecords("tasks.mirror", []net.IPAddr{
		{IP: net.ParseIP("127.0.0.2")},
		{IP: net.ParseIP("127.0.0.5")},
	})

	// Silently blackhole all traffic through proxy2 and sever backend to force reconnect through proxy2.
	proxy2.SetBlackhole(true)
	// Also close current active session's underlying net.Conn on node2v2 side or wait for blackhole:
	// To simulate an overlay conntrack drop between node1 and node2v2, blackhole proxy2, force
	// the switchover to proxy2, then un-blackhole proxy2 and verify automatic recovery.
	node2v2.engine.peersMu.RLock()
	for _, s := range node2v2.engine.peers {
		_ = s.Close()
	}
	node2v2.engine.peersMu.RUnlock()

	// While proxy2 is blackholed, write a file on node1 (watcher indexes it while peer is unreachable).
	if err := os.WriteFile(filepath.Join(node1.dataDir, "during-blackhole.txt"), []byte("healed-after-blackhole"), 0o644); err != nil {
		t.Fatalf("write during-blackhole.txt: %v", err)
	}

	// Heal the overlay network proxy.
	proxy2.SetBlackhole(false)

	waitForCondition(t, 6*time.Second, "sync automatically recovers after silent overlay blackhole heals", func() bool {
		got, err := os.ReadFile(filepath.Join(node2DataDir, "during-blackhole.txt"))
		return err == nil && string(got) == "healed-after-blackhole"
	})
}

func TestEngine_LargeIndexExchange_NoFullDuplexDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	nodeA := newTestNode(t, "node-large-a", "large-idx-secret", config.ConflictLWW, nil, nil)
	defer nodeA.close()

	nodeB := newTestNode(t, "node-large-b", "large-idx-secret", config.ConflictLWW, nil, nil)
	defer nodeB.close()

	// Pre-populate 300 files on each node (> maxFilesPerIndexBatch = 256) so both nodes
	// send multi-batch IndexExchange frames simultaneously upon connecting.
	const filesPerNode = 300
	for i := range filesPerNode {
		nameA := fmt.Sprintf("dir-a/file-%04d.txt", i)
		absA := filepath.Join(nodeA.dataDir, filepath.FromSlash(nameA))
		if err := os.MkdirAll(filepath.Dir(absA), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(absA, []byte(fmt.Sprintf("content-a-%d", i)), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}

		nameB := fmt.Sprintf("dir-b/file-%04d.txt", i)
		absB := filepath.Join(nodeB.dataDir, filepath.FromSlash(nameB))
		if err := os.MkdirAll(filepath.Dir(absB), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(absB, []byte(fmt.Sprintf("content-b-%d", i)), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	nodeA.start(ctx)
	nodeB.start(ctx)

	if err := nodeA.engine.DialPeer(ctx, nodeB.engine.ListenAddr()); err != nil {
		t.Fatalf("DialPeer: %v", err)
	}

	waitForCondition(t, 10*time.Second, "both nodes exchange 300+ files without deadlocking", func() bool {
		gotBOnA, err1 := os.ReadFile(filepath.Join(nodeA.dataDir, "dir-b", "file-0299.txt"))
		gotAOnB, err2 := os.ReadFile(filepath.Join(nodeB.dataDir, "dir-a", "file-0299.txt"))
		return err1 == nil && string(gotBOnA) == "content-b-299" &&
			err2 == nil && string(gotAOnB) == "content-a-299"
	})

	// Verify live watcher change immediately propagates after large index exchange.
	if err := os.WriteFile(filepath.Join(nodeA.dataDir, "live-after-large.txt"), []byte("live-ok"), 0o644); err != nil {
		t.Fatalf("write live-after-large.txt: %v", err)
	}
	waitForCondition(t, 5*time.Second, "live watcher change propagates after large index exchange", func() bool {
		got, err := os.ReadFile(filepath.Join(nodeB.dataDir, "live-after-large.txt"))
		return err == nil && string(got) == "live-ok"
	})
}
