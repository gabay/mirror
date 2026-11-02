package fs

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mirror-sync/mirror/internal/chunker"
	"github.com/mirror-sync/mirror/internal/ignore"
	"github.com/mirror-sync/mirror/internal/model"
	"github.com/mirror-sync/mirror/internal/store"
)

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func newTestLogger(buf *safeBuffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestScannerEmptyFolderAndEmptyStore(t *testing.T) {
	rootDir := t.TempDir()
	stateDir := t.TempDir()

	st, err := store.Open(stateDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	var logBuf safeBuffer
	scanner := NewScanner(rootDir, "node-a", st, ignore.NewMatcher(nil), newTestLogger(&logBuf))

	changed, err := scanner.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll on empty dir: %v", err)
	}
	if len(changed) != 0 {
		t.Fatalf("expected 0 changes on empty dir + empty store, got %d: %+v", len(changed), changed)
	}
	if all := st.ListFiles(); len(all) != 0 {
		t.Fatalf("expected 0 files in store (0 tombstones), got %d: %+v", len(all), all)
	}
}

func TestScannerLifecycleAndFastPath(t *testing.T) {
	rootDir := t.TempDir()
	stateDir := t.TempDir()

	st, err := store.Open(stateDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	var logBuf safeBuffer
	matcher := ignore.NewMatcher([]string{"*.tmp", "ignored_dir/", "node_modules"})
	scanner := NewScanner(rootDir, "node-a", st, matcher, newTestLogger(&logBuf))

	// 1. Create a directory, a regular file, a symlink, and excluded files/dirs.
	subDir := filepath.Join(rootDir, "subdir")
	if err := os.MkdirAll(subDir, 0o750); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}

	regFile := filepath.Join(subDir, "hello.txt")
	regContent := []byte("hello world from mirror scanner test\n")
	if err := os.WriteFile(regFile, regContent, 0o640); err != nil {
		t.Fatalf("write hello.txt: %v", err)
	}
	fixedMtime := time.Date(2026, 5, 10, 12, 30, 45, 123456789, time.UTC)
	if err := os.Chtimes(regFile, fixedMtime, fixedMtime); err != nil {
		t.Fatalf("chtimes hello.txt: %v", err)
	}

	symPath := filepath.Join(rootDir, "hello-link")
	if err := os.Symlink("subdir/hello.txt", symPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// Excluded entries
	if err := os.WriteFile(filepath.Join(rootDir, "skip.tmp"), []byte("ignore me"), 0o644); err != nil {
		t.Fatalf("write skip.tmp: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, ".mirror.tmp.staging.1234"), []byte("staging"), 0o644); err != nil {
		t.Fatalf("write staging file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(rootDir, "ignored_dir", "nested"), 0o755); err != nil {
		t.Fatalf("mkdir ignored_dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "ignored_dir", "nested", "secret.txt"), []byte("no"), 0o644); err != nil {
		t.Fatalf("write ignored nested file: %v", err)
	}

	// Special FIFO file -> must log WARN and be skipped.
	fifoPath := filepath.Join(rootDir, "test.fifo")
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	// 2. Run initial ScanAll.
	changed, err := scanner.ScanAll()
	if err != nil {
		t.Fatalf("initial ScanAll: %v", err)
	}

	if !strings.Contains(logBuf.String(), "ignoring unsupported special file") ||
		!strings.Contains(logBuf.String(), "test.fifo") {
		t.Fatalf("expected WARN log for FIFO special file, got logs:\n%s", logBuf.String())
	}

	if len(changed) != 3 {
		t.Fatalf("expected 3 scanned entries (hello-link, subdir, subdir/hello.txt), got %d: %+v", len(changed), changed)
	}

	// Verify symlink
	symFi, ok := st.GetFile("hello-link")
	if !ok {
		t.Fatalf("missing hello-link in store")
	}
	if symFi.Type != model.FileTypeSymlink || symFi.SymlinkTarget != "subdir/hello.txt" {
		t.Fatalf("unexpected symlink FileInfo: %+v", symFi)
	}
	if symFi.FileHash != chunker.HashBytes([]byte("subdir/hello.txt")) {
		t.Fatalf("unexpected symlink hash")
	}
	if symFi.Version["node-a"] != 1 {
		t.Fatalf("expected symlink version 1, got %v", symFi.Version)
	}

	// Verify directory
	dirFi, ok := st.GetFile("subdir")
	if !ok {
		t.Fatalf("missing subdir in store")
	}
	if dirFi.Type != model.FileTypeDir || dirFi.Deleted {
		t.Fatalf("unexpected subdir FileInfo: %+v", dirFi)
	}

	// Verify regular file
	regFi, ok := st.GetFile("subdir/hello.txt")
	if !ok {
		t.Fatalf("missing subdir/hello.txt in store")
	}
	if regFi.Type != model.FileTypeRegular || regFi.Size != int64(len(regContent)) {
		t.Fatalf("unexpected regular FileInfo: %+v", regFi)
	}
	if regFi.ModTimeNs != fixedMtime.UnixNano() {
		t.Fatalf("expected ModTimeNs %d, got %d", fixedMtime.UnixNano(), regFi.ModTimeNs)
	}
	if regFi.Mode&0o777 != 0o640 {
		t.Fatalf("expected mode 0640, got %o", regFi.Mode)
	}
	if regFi.FileHash != chunker.HashBytes(regContent) || len(regFi.Blocks) != 1 {
		t.Fatalf("unexpected hash/blocks on regular file: %+v", regFi)
	}
	if regFi.Version["node-a"] != 1 {
		t.Fatalf("expected version {node-a: 1}, got %v", regFi.Version)
	}

	// Verify excluded files and FIFO are not in store.
	for _, excluded := range []string{"skip.tmp", ".mirror.tmp.staging.1234", "ignored_dir", "ignored_dir/nested/secret.txt", "test.fifo"} {
		if _, exists := st.GetFile(excluded); exists {
			t.Fatalf("excluded/special path %q should not be in store", excluded)
		}
	}

	// 3. Unchanged rescan -> 0 changes emitted, fast-path hit count incremented.
	hitsBefore := scanner.FastPathHits()
	changed2, err := scanner.ScanAll()
	if err != nil {
		t.Fatalf("second ScanAll: %v", err)
	}
	if len(changed2) != 0 {
		t.Fatalf("expected 0 changes on unchanged rescan, got %d: %+v", len(changed2), changed2)
	}
	if scanner.FastPathHits() <= hitsBefore {
		t.Fatalf("expected fast-path hit counter to increment on unchanged regular file")
	}

	// 4. Modify regular file -> version incremented to 2.
	updatedContent := []byte("updated content for mirror scanner test!\n")
	if err := os.WriteFile(regFile, updatedContent, 0o640); err != nil {
		t.Fatalf("rewrite hello.txt: %v", err)
	}
	newMtime := fixedMtime.Add(10 * time.Second)
	if err := os.Chtimes(regFile, newMtime, newMtime); err != nil {
		t.Fatalf("chtimes updated hello.txt: %v", err)
	}

	changed3, err := scanner.ScanAll()
	if err != nil {
		t.Fatalf("third ScanAll: %v", err)
	}
	if len(changed3) != 1 || changed3[0].Path != "subdir/hello.txt" {
		t.Fatalf("expected only subdir/hello.txt changed, got %+v", changed3)
	}
	if changed3[0].Version["node-a"] != 2 {
		t.Fatalf("expected version {node-a: 2}, got %v", changed3[0].Version)
	}
	if changed3[0].FileHash != chunker.HashBytes(updatedContent) {
		t.Fatalf("expected updated FileHash")
	}

	// 5. Delete regular file -> tombstone emitted with Deleted=true and version incremented to 3.
	if err := os.Remove(regFile); err != nil {
		t.Fatalf("remove hello.txt: %v", err)
	}

	changed4, err := scanner.ScanAll()
	if err != nil {
		t.Fatalf("fourth ScanAll: %v", err)
	}
	if len(changed4) != 1 || changed4[0].Path != "subdir/hello.txt" {
		t.Fatalf("expected tombstone for subdir/hello.txt, got %+v", changed4)
	}
	tombstone := changed4[0]
	if !tombstone.Deleted || tombstone.Size != 0 || len(tombstone.Blocks) != 0 {
		t.Fatalf("invalid tombstone: %+v", tombstone)
	}
	if tombstone.Version["node-a"] != 3 {
		t.Fatalf("expected tombstone version {node-a: 3}, got %v", tombstone.Version)
	}

	// 6. Rescan after tombstone -> 0 new changes emitted (tombstone is not re-emitted).
	changed5, err := scanner.ScanAll()
	if err != nil {
		t.Fatalf("fifth ScanAll: %v", err)
	}
	if len(changed5) != 0 {
		t.Fatalf("expected 0 changes when tombstone already recorded, got %+v", changed5)
	}
}

func TestWriterPartialBlockReuseAndMetadata(t *testing.T) {
	rootDir := t.TempDir()
	stateDir := t.TempDir()

	st, err := store.Open(stateDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	var logBuf safeBuffer
	logger := newTestLogger(&logBuf)

	// Use smaller chunk sizes (Min 4KB, Avg 8KB, Max 16KB) so a ~120KB file splits into ~12-15 blocks.
	customChunker, err := chunker.New(chunker.Options{
		MinSize: 4 * 1024,
		AvgSize: 8 * 1024,
		MaxSize: 16 * 1024,
	})
	if err != nil {
		t.Fatalf("chunker.New: %v", err)
	}

	scanner := NewScanner(rootDir, "node-local", st, ignore.NewMatcher(nil), logger).WithChunker(customChunker)
	writer := NewWriter(rootDir, st, logger)

	// Create a deterministic 128KB original file on disk and scan it into local store.
	origData := make([]byte, 128*1024)
	if _, err := rand.Read(origData); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	relPath := "docs/large.bin"
	absPath := filepath.Join(rootDir, "docs", "large.bin")
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(absPath, origData, 0o644); err != nil {
		t.Fatalf("write origData: %v", err)
	}

	if _, err := scanner.ScanAll(); err != nil {
		t.Fatalf("ScanAll: %v", err)
	}

	localFi, ok := st.GetFile(relPath)
	if !ok {
		t.Fatalf("expected %s in store", relPath)
	}
	if len(localFi.Blocks) < 6 {
		t.Fatalf("expected at least 6 blocks, got %d", len(localFi.Blocks))
	}

	// Construct a modified version of the file on a simulated remote node by mutating bytes
	// inside the interior of 1 block (preserving its trailing 64-byte Gear cut boundary),
	// leaving >80% of blocks identical!
	targetBlockIdx := len(localFi.Blocks) / 2
	targetBlock := localFi.Blocks[targetBlockIdx]

	remoteData := make([]byte, len(origData))
	copy(remoteData, origData)
	for i := int(targetBlock.Offset) + 64; i < int(targetBlock.Offset)+128; i++ {
		remoteData[i] ^= 0xFF
	}

	remoteBlocks, remoteFileHash, remoteSize, err := customChunker.ChunkReader(bytes.NewReader(remoteData))
	if err != nil {
		t.Fatalf("chunk remoteData: %v", err)
	}

	// Build a map of remote block hash -> raw bytes so fetchBlock can serve requested blocks.
	remoteBlockMap := make(map[[32]byte][]byte)
	localBlockSet := make(map[[32]byte]bool)
	for _, b := range localFi.Blocks {
		localBlockSet[b.Hash] = true
	}

	missingCount := 0
	reusedCount := 0
	for _, b := range remoteBlocks {
		slice := remoteData[b.Offset : b.Offset+int64(b.Size)]
		remoteBlockMap[b.Hash] = slice
		if localBlockSet[b.Hash] {
			reusedCount++
		} else {
			missingCount++
		}
	}

	if reusedCount == 0 || missingCount == 0 {
		t.Fatalf("expected both reused (%d) and missing (%d) blocks", reusedCount, missingCount)
	}
	if float64(reusedCount)/float64(len(remoteBlocks)) < 0.80 {
		t.Fatalf("expected >=80%% block reuse, got %d/%d", reusedCount, len(remoteBlocks))
	}

	var fetchedBlocks atomic.Int32
	fetchBlock := func(ctx context.Context, hash [32]byte, size int32) ([]byte, error) {
		fetchedBlocks.Add(1)
		if localBlockSet[hash] {
			t.Errorf("fetchBlock was called for block %x that already exists locally!", hash[:4])
		}
		data, exists := remoteBlockMap[hash]
		if !exists {
			return nil, os.ErrNotExist
		}
		return data, nil
	}

	remoteMtime := time.Date(2026, 6, 15, 8, 0, 0, 987654321, time.UTC)
	remoteFi := model.FileInfo{
		Path:       relPath,
		Type:       model.FileTypeRegular,
		Size:       remoteSize,
		Mode:       0o750,
		Uid:        localFi.Uid,
		Gid:        localFi.Gid,
		ModTimeNs:  remoteMtime.UnixNano(),
		Deleted:    false,
		Version:    localFi.Version.Increment("node-remote"),
		ModifiedBy: "node-remote",
		FileHash:   remoteFileHash,
		Blocks:     remoteBlocks,
	}

	if err := writer.ApplyRemoteFile(context.Background(), remoteFi, fetchBlock); err != nil {
		t.Fatalf("ApplyRemoteFile: %v", err)
	}

	if int(fetchedBlocks.Load()) != missingCount {
		t.Fatalf("expected fetchBlock called %d times, got %d", missingCount, fetchedBlocks.Load())
	}

	// Verify reconstructed file content, permissions, and mtime on disk.
	gotData, err := os.ReadFile(absPath)
	if err != nil {
		t.Fatalf("read reconstructed file: %v", err)
	}
	if !bytes.Equal(gotData, remoteData) {
		t.Fatalf("reconstructed file content does not match remoteData")
	}

	meta, err := LstatMetadata(absPath)
	if err != nil {
		t.Fatalf("LstatMetadata: %v", err)
	}
	if meta.Mode&0o777 != 0o750 {
		t.Fatalf("expected mode 0750, got %o", meta.Mode)
	}
	if meta.ModTimeNs != remoteMtime.UnixNano() {
		t.Fatalf("expected mtime %d, got %d", remoteMtime.UnixNano(), meta.ModTimeNs)
	}

	// Verify Scanner sees 0 changes after Writer applied the remote file (no sync loop!).
	postChanges, err := scanner.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll after ApplyRemoteFile: %v", err)
	}
	if len(postChanges) != 0 {
		t.Fatalf("expected 0 scanner changes after ApplyRemoteFile, got %+v", postChanges)
	}
}

func TestWriterSymlinkDirDeleteAndConflictCopy(t *testing.T) {
	rootDir := t.TempDir()
	stateDir := t.TempDir()

	st, err := store.Open(stateDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	writer := NewWriter(rootDir, st, nil)
	ctx := context.Background()

	// 1. Directory creation
	dirFi := model.FileInfo{
		Path:       "nested/folder",
		Type:       model.FileTypeDir,
		Mode:       0o750,
		ModTimeNs:  time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC).UnixNano(),
		Version:    model.VersionVector{"node-1": 1},
		ModifiedBy: "node-1",
	}
	if err := writer.ApplyRemoteFile(ctx, dirFi, nil); err != nil {
		t.Fatalf("ApplyRemoteFile dir: %v", err)
	}
	dirAbs := filepath.Join(rootDir, "nested", "folder")
	if info, err := os.Lstat(dirAbs); err != nil || !info.IsDir() {
		t.Fatalf("expected directory created at %q, err=%v", dirAbs, err)
	}

	// 2. Symlink creation and atomic replacement
	symFi := model.FileInfo{
		Path:          "nested/link",
		Type:          model.FileTypeSymlink,
		SymlinkTarget: "../target.txt",
		ModTimeNs:     time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC).UnixNano(),
		Version:       model.VersionVector{"node-1": 1},
		ModifiedBy:    "node-1",
	}
	if err := writer.ApplyRemoteFile(ctx, symFi, nil); err != nil {
		t.Fatalf("ApplyRemoteFile symlink: %v", err)
	}
	symAbs := filepath.Join(rootDir, "nested", "link")
	target, err := os.Readlink(symAbs)
	if err != nil || target != "../target.txt" {
		t.Fatalf("expected symlink target ../target.txt, got %q (err=%v)", target, err)
	}

	// Atomically replace symlink with new target
	symFi.SymlinkTarget = "../other.txt"
	symFi.Version = symFi.Version.Increment("node-1")
	if err := writer.ApplyRemoteFile(ctx, symFi, nil); err != nil {
		t.Fatalf("ApplyRemoteFile symlink replacement: %v", err)
	}
	target2, err := os.Readlink(symAbs)
	if err != nil || target2 != "../other.txt" {
		t.Fatalf("expected replaced symlink target ../other.txt, got %q (err=%v)", target2, err)
	}

	// 3. SaveConflictCopy test
	conflictTargetRel := "nested/report.json"
	conflictTargetAbs := filepath.Join(rootDir, "nested", "report.json")
	loserContent := []byte(`{"status":"local-losing-version"}`)
	if err := os.WriteFile(conflictTargetAbs, loserContent, 0o640); err != nil {
		t.Fatalf("write conflict source: %v", err)
	}

	loserTime := time.Date(2026, 11, 2, 14, 22, 33, 0, time.UTC)
	conflictRel, err := writer.SaveConflictCopy(conflictTargetRel, "node-abcdef123456", loserTime.UnixNano())
	if err != nil {
		t.Fatalf("SaveConflictCopy: %v", err)
	}

	expectedConflictRel := "nested/report.sync-conflict-20261102-142233-node-abc.json"
	if conflictRel != expectedConflictRel {
		t.Fatalf("expected conflict relPath %q, got %q", expectedConflictRel, conflictRel)
	}

	conflictBytes, err := os.ReadFile(filepath.Join(rootDir, filepath.FromSlash(conflictRel)))
	if err != nil {
		t.Fatalf("read conflict copy: %v", err)
	}
	if !bytes.Equal(conflictBytes, loserContent) {
		t.Fatalf("conflict copy content mismatch: got %q, want %q", conflictBytes, loserContent)
	}

	// 4. Directory and symlink deletion via Tombstone
	symFi.Deleted = true
	symFi.Version = symFi.Version.Increment("node-1")
	if err := writer.ApplyRemoteFile(ctx, symFi, nil); err != nil {
		t.Fatalf("ApplyRemoteFile delete symlink: %v", err)
	}
	if _, err := os.Lstat(symAbs); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected symlink deleted, got err=%v", err)
	}

	dirFi.Deleted = true
	dirFi.Version = dirFi.Version.Increment("node-1")
	if err := writer.ApplyRemoteFile(ctx, dirFi, nil); err != nil {
		t.Fatalf("ApplyRemoteFile delete dir: %v", err)
	}
	if _, err := os.Lstat(dirAbs); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected folder deleted, got err=%v", err)
	}
}

func TestScannerIncrementalScanPath(t *testing.T) {
	rootDir := t.TempDir()
	stateDir := t.TempDir()

	st, err := store.Open(stateDir)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	scanner := NewScanner(rootDir, "node-inc", st, ignore.NewMatcher([]string{"*.bak"}), nil)

	// 1. Scan non-existent untracked path -> nil, false, nil
	fi, changed, err := scanner.ScanPath("missing.txt")
	if err != nil || changed || fi != nil {
		t.Fatalf("expected (nil, false, nil) for untracked missing path, got (%+v, %v, %v)", fi, changed, err)
	}

	// 2. Create file and ScanPath -> updated=true, version=1
	fileAbs := filepath.Join(rootDir, "notes.txt")
	if err := os.WriteFile(fileAbs, []byte("initial note"), 0o644); err != nil {
		t.Fatalf("write notes.txt: %v", err)
	}
	fi, changed, err = scanner.ScanPath("notes.txt")
	if err != nil || !changed || fi == nil {
		t.Fatalf("expected changed=true on new file, got (%+v, %v, %v)", fi, changed, err)
	}
	if fi.Version["node-inc"] != 1 {
		t.Fatalf("expected version 1, got %v", fi.Version)
	}

	// 3. ScanPath again without changes -> updated=false
	fi2, changed2, err := scanner.ScanPath("notes.txt")
	if err != nil || changed2 || fi2 == nil {
		t.Fatalf("expected changed=false on unchanged file, got (%+v, %v, %v)", fi2, changed2, err)
	}

	// 4. Delete file and ScanPath -> tombstone emitted with Deleted=true, version=2
	if err := os.Remove(fileAbs); err != nil {
		t.Fatalf("remove notes.txt: %v", err)
	}
	fi3, changed3, err := scanner.ScanPath("notes.txt")
	if err != nil || !changed3 || fi3 == nil || !fi3.Deleted {
		t.Fatalf("expected tombstone from ScanPath after delete, got (%+v, %v, %v)", fi3, changed3, err)
	}
	if fi3.Version["node-inc"] != 2 {
		t.Fatalf("expected tombstone version 2, got %v", fi3.Version)
	}
}

func TestWatcherDebounceAndDynamicSubdirs(t *testing.T) {
	rootDir := t.TempDir()
	matcher := ignore.NewMatcher([]string{"*.ignored", "ignored_folder"})

	var mu sync.Mutex
	var batches [][]string
	notifyCh := make(chan struct{}, 16)

	onChanges := func(paths []string) {
		mu.Lock()
		copied := make([]string, len(paths))
		copy(copied, paths)
		batches = append(batches, copied)
		mu.Unlock()

		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}

	watcher, err := NewWatcher(rootDir, matcher, 50*time.Millisecond, onChanges, nil)
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer watcher.Close()

	// 1. Perform rapid writes to multiple files and nested directories within the 50ms debounce window.
	if err := os.WriteFile(filepath.Join(rootDir, "a.txt"), []byte("1"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "a.txt"), []byte("2"), 0o644); err != nil {
		t.Fatalf("write a.txt 2: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "a.txt"), []byte("3"), 0o644); err != nil {
		t.Fatalf("write a.txt 3: %v", err)
	}
	// Write ignored files that should NOT trigger events.
	if err := os.WriteFile(filepath.Join(rootDir, "skip.ignored"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("write skip.ignored: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, ".mirror.tmp.a.123"), []byte("tmp"), 0o644); err != nil {
		t.Fatalf("write .mirror.tmp: %v", err)
	}

	// Create nested directory and file inside it.
	nestedDir := filepath.Join(rootDir, "sub", "deep")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatalf("mkdir nestedDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nestedDir, "b.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatalf("write sub/deep/b.txt: %v", err)
	}

	// Wait for debounced batch
	select {
	case <-notifyCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for first watcher debounce batch")
	}

	// Allow any secondary settle batch if nested mkdir raced the timer
	time.Sleep(80 * time.Millisecond)

	seenPaths := make(map[string]bool)
	mu.Lock()
	for _, b := range batches {
		for _, p := range b {
			seenPaths[p] = true
		}
	}
	mu.Unlock()

	if !seenPaths["a.txt"] {
		t.Fatalf("expected watcher to report a.txt, got %v", seenPaths)
	}
	if !seenPaths["sub/deep/b.txt"] {
		t.Fatalf("expected watcher to report sub/deep/b.txt, got %v", seenPaths)
	}
	if seenPaths["skip.ignored"] || seenPaths[".mirror.tmp.a.123"] {
		t.Fatalf("watcher should not emit ignored files, got %v", seenPaths)
	}

	// 2. Now modify a file inside the newly created nested directory to prove dynamic watch registration works.
	mu.Lock()
	batches = nil
	mu.Unlock()
	for len(notifyCh) > 0 {
		<-notifyCh
	}

	if err := os.WriteFile(filepath.Join(nestedDir, "c.txt"), []byte("dynamic watch test"), 0o644); err != nil {
		t.Fatalf("write sub/deep/c.txt: %v", err)
	}
	if err := os.Remove(filepath.Join(rootDir, "a.txt")); err != nil {
		t.Fatalf("remove a.txt: %v", err)
	}

	select {
	case <-notifyCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for second watcher debounce batch")
	}

	seenPhase2 := make(map[string]bool)
	mu.Lock()
	for _, b := range batches {
		for _, p := range b {
			seenPhase2[p] = true
		}
	}
	mu.Unlock()

	if !seenPhase2["sub/deep/c.txt"] {
		t.Fatalf("expected dynamic watch on sub/deep to detect sub/deep/c.txt, got %v", seenPhase2)
	}
	if !seenPhase2["a.txt"] {
		t.Fatalf("expected watcher to detect deletion of a.txt, got %v", seenPhase2)
	}
}
