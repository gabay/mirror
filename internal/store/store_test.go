package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mirror-sync/mirror/internal/model"
)

func makeHash(seed byte) [32]byte {
	var h [32]byte
	for i := range h {
		h[i] = seed + byte(i)
	}
	return h
}

func TestStorePersistenceAcrossReopen(t *testing.T) {
	stateDir := t.TempDir()

	s, err := Open(stateDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	b1 := makeHash(1)
	b2 := makeHash(2)
	fHash := makeHash(10)

	regFile := model.FileInfo{
		Path:       "docs/readme.txt",
		Type:       model.FileTypeRegular,
		Size:       100000,
		Mode:       0o644,
		Uid:        1000,
		Gid:        1000,
		ModTimeNs:  1700000000123456789,
		Deleted:    false,
		Version:    model.VersionVector{"node-a": 2, "node-b": 1},
		ModifiedBy: "node-a",
		FileHash:   fHash,
		Blocks: []model.BlockInfo{
			{Index: 0, Offset: 0, Size: 65536, Hash: b1},
			{Index: 1, Offset: 65536, Size: 34464, Hash: b2},
		},
	}

	symFile := model.FileInfo{
		Path:          "bin/latest",
		Type:          model.FileTypeSymlink,
		Mode:          0o777,
		ModTimeNs:     1700000000999999999,
		SymlinkTarget: "../docs/readme.txt",
		Version:       model.VersionVector{"node-a": 1},
		ModifiedBy:    "node-a",
	}

	tombstone := model.FileInfo{
		Path:       "old/removed.log",
		Type:       model.FileTypeRegular,
		Deleted:    true,
		ModTimeNs:  1700000001000000000,
		Version:    model.VersionVector{"node-b": 3},
		ModifiedBy: "node-b",
	}

	if err := s.PutFile(regFile); err != nil {
		t.Fatalf("PutFile(regFile) failed: %v", err)
	}
	if err := s.PutFilesBatch([]model.FileInfo{symFile, tombstone}); err != nil {
		t.Fatalf("PutFilesBatch failed: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Re-open from stateDir and verify all records and block locations were restored.
	s2, err := Open(stateDir)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer s2.Close()

	list := s2.ListFiles()
	if len(list) != 3 {
		t.Fatalf("expected 3 files after reopen, got %d", len(list))
	}
	// ListFiles must be sorted lexicographically by Path.
	if list[0].Path != "bin/latest" || list[1].Path != "docs/readme.txt" || list[2].Path != "old/removed.log" {
		t.Fatalf("unexpected sorted order: %q, %q, %q", list[0].Path, list[1].Path, list[2].Path)
	}

	gotReg, ok := s2.GetFile("docs/readme.txt")
	if !ok {
		t.Fatal("expected docs/readme.txt to exist")
	}
	if gotReg.FileHash != fHash || len(gotReg.Blocks) != 2 || gotReg.Version["node-a"] != 2 {
		t.Fatalf("restored FileInfo mismatch: %+v", gotReg)
	}

	gotTomb, ok := s2.GetFile("old/removed.log")
	if !ok || !gotTomb.Deleted {
		t.Fatalf("expected tombstone old/removed.log to be preserved, got %+v (ok=%v)", gotTomb, ok)
	}

	locs, ok := s2.LookupBlock(b1)
	if !ok || len(locs) != 1 || locs[0].Path != "docs/readme.txt" || locs[0].Offset != 0 || locs[0].Size != 65536 {
		t.Fatalf("unexpected LookupBlock(b1) after reopen: %+v (ok=%v)", locs, ok)
	}
}

func TestStoreBlockIndexUpdateAndDelete(t *testing.T) {
	stateDir := t.TempDir()
	s, err := Open(stateDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	hShared := makeHash(101)
	hOnlyA := makeHash(102)
	hNewA := makeHash(103)

	// File A and File B both contain hShared; File A also contains hOnlyA.
	fileA := model.FileInfo{
		Path:    "a.bin",
		Type:    model.FileTypeRegular,
		Size:    32768,
		Version: model.VersionVector{"n1": 1},
		Blocks: []model.BlockInfo{
			{Index: 0, Offset: 0, Size: 16384, Hash: hShared},
			{Index: 1, Offset: 16384, Size: 16384, Hash: hOnlyA},
		},
	}
	fileB := model.FileInfo{
		Path:    "b.bin",
		Type:    model.FileTypeRegular,
		Size:    16384,
		Version: model.VersionVector{"n1": 1},
		Blocks: []model.BlockInfo{
			{Index: 0, Offset: 0, Size: 16384, Hash: hShared},
		},
	}

	if err := s.PutFilesBatch([]model.FileInfo{fileA, fileB}); err != nil {
		t.Fatalf("PutFilesBatch failed: %v", err)
	}

	// Verify hShared has 2 locations and hOnlyA has 1 location.
	if locs, ok := s.LookupBlock(hShared); !ok || len(locs) != 2 {
		t.Fatalf("expected 2 locations for hShared, got %+v", locs)
	}
	if locs, ok := s.LookupBlock(hOnlyA); !ok || len(locs) != 1 || locs[0].Path != "a.bin" {
		t.Fatalf("expected 1 location for hOnlyA, got %+v", locs)
	}

	// Update File A so it now only has hNewA (no longer hShared or hOnlyA).
	fileAv2 := model.FileInfo{
		Path:    "a.bin",
		Type:    model.FileTypeRegular,
		Size:    16384,
		Version: model.VersionVector{"n1": 2},
		Blocks: []model.BlockInfo{
			{Index: 0, Offset: 0, Size: 16384, Hash: hNewA},
		},
	}
	if err := s.PutFile(fileAv2); err != nil {
		t.Fatalf("PutFile(fileAv2) failed: %v", err)
	}

	// hOnlyA must now be completely removed from block index.
	if locs, ok := s.LookupBlock(hOnlyA); ok || len(locs) != 0 {
		t.Fatalf("expected hOnlyA to be removed after fileA update, got %+v", locs)
	}
	// hShared must still have 1 location (from b.bin).
	if locs, ok := s.LookupBlock(hShared); !ok || len(locs) != 1 || locs[0].Path != "b.bin" {
		t.Fatalf("expected hShared to still resolve to b.bin, got %+v", locs)
	}
	// hNewA must resolve to a.bin.
	if locs, ok := s.LookupBlock(hNewA); !ok || len(locs) != 1 || locs[0].Path != "a.bin" {
		t.Fatalf("expected hNewA to resolve to a.bin, got %+v", locs)
	}

	// Now transition File A to Deleted: true (tombstone).
	fileADeleted := model.FileInfo{
		Path:    "a.bin",
		Type:    model.FileTypeRegular,
		Deleted: true,
		Version: model.VersionVector{"n1": 3},
	}
	if err := s.PutFile(fileADeleted); err != nil {
		t.Fatalf("PutFile(fileADeleted) failed: %v", err)
	}

	// hNewA must be removed from the block index.
	if locs, ok := s.LookupBlock(hNewA); ok || len(locs) != 0 {
		t.Fatalf("expected hNewA to be removed when a.bin is deleted, got %+v", locs)
	}
	// Tombstone for a.bin must remain in files.
	gotA, ok := s.GetFile("a.bin")
	if !ok || !gotA.Deleted || gotA.Version["n1"] != 3 {
		t.Fatalf("expected tombstone for a.bin in store, got %+v (ok=%v)", gotA, ok)
	}
}

func TestStoreWALCrashRecovery(t *testing.T) {
	stateDir := t.TempDir()
	s, err := Open(stateDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	h := makeHash(55)
	fi := model.FileInfo{
		Path:    "crash/test.dat",
		Type:    model.FileTypeRegular,
		Size:    4096,
		Version: model.VersionVector{"n1": 1},
		Blocks: []model.BlockInfo{
			{Index: 0, Offset: 0, Size: 4096, Hash: h},
		},
	}
	if err := s.PutFile(fi); err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	// Simulate an ungraceful crash by closing only the underlying walFile handle
	// without calling s.Close(), and appending a corrupted partial WAL tail.
	s.mu.Lock()
	if s.walFile != nil {
		_, _ = s.walFile.Write([]byte{0x10, 0x00, 0x00, 0x00, 0xde, 0xad})
		_ = s.walFile.Close()
		s.walFile = nil
	}
	s.closed = true
	s.mu.Unlock()

	// Re-open and verify committed WAL entry was recovered despite trailing partial frame.
	s2, err := Open(stateDir)
	if err != nil {
		t.Fatalf("Open after simulated crash failed: %v", err)
	}
	defer s2.Close()

	got, ok := s2.GetFile("crash/test.dat")
	if !ok || got.Size != 4096 {
		t.Fatalf("expected crash/test.dat to be recovered from WAL, got %+v (ok=%v)", got, ok)
	}
	if locs, ok := s2.LookupBlock(h); !ok || len(locs) != 1 || locs[0].Path != "crash/test.dat" {
		t.Fatalf("expected block location to be recovered from WAL, got %+v", locs)
	}

	// Also verify snapshot file exists in stateDir.
	if _, err := os.Stat(filepath.Join(stateDir, snapshotFileName)); err != nil {
		t.Fatalf("expected snapshot file to exist: %v", err)
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	stateDir := t.TempDir()
	s, err := Open(stateDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	const workers = 8
	const opsPerWorker = 40

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				h := makeHash(byte(workerID*31 + i))
				p := fmt.Sprintf("dir-%d/file-%d.bin", workerID, i%10)
				deleted := i%7 == 6

				fi := model.FileInfo{
					Path:    p,
					Type:    model.FileTypeRegular,
					Size:    int64(1024 * (i + 1)),
					Deleted: deleted,
					Version: model.VersionVector{fmt.Sprintf("node-%d", workerID): uint64(i + 1)},
				}
				if !deleted {
					fi.Blocks = []model.BlockInfo{
						{Index: 0, Offset: 0, Size: int32(1024 * (i + 1)), Hash: h},
					}
				}

				if err := s.PutFile(fi); err != nil {
					t.Errorf("concurrent PutFile failed: %v", err)
					return
				}

				_, _ = s.GetFile(p)
				_, _ = s.LookupBlock(h)
				if i%10 == 0 {
					_ = s.ListFiles()
				}
			}
		}(w)
	}

	wg.Wait()

	all := s.ListFiles()
	if len(all) != workers*10 {
		t.Fatalf("expected %d tracked files, got %d", workers*10, len(all))
	}
}

func BenchmarkStoreLookupBlock(b *testing.B) {
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	targetHash := makeHash(42)
	fi := model.FileInfo{
		Path:    "db/state.sqlite",
		Type:    model.FileTypeRegular,
		Size:    65536,
		Version: model.VersionVector{"n1": 1},
		Blocks: []model.BlockInfo{
			{Index: 0, Offset: 0, Size: 65536, Hash: targetHash},
		},
	}
	if err := s.PutFile(fi); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.LookupBlock(targetHash)
	}
}
