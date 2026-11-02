package chunker

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestBlake3OfficialTestVectors(t *testing.T) {
	// Official BLAKE3 test vectors use an input pattern where byte[i] = byte(i % 251).
	makeInput := func(length int) []byte {
		buf := make([]byte, length)
		for i := range buf {
			buf[i] = byte(i % 251)
		}
		return buf
	}

	cases := []struct {
		length   int
		expected string
	}{
		{0, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"},
		{1, "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"},
		{64, "4eed7141ea4a5cd4b788606bd23f46e212af9cacebacdc7d1f4c6dc7f2511b98"},
		{65, "de1e5fa0be70df6d2be8fffd0e99ceaa8eb6e8c93a63f2d8d1c30ecb6b263dee"},
		{1024, "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7"},
		{1025, "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"},
		{2048, "e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a"},
		{65536, "68d647e619a930e7b1082f74f334b0c65a315725569bdc123f0ee11881717bfe"},
	}

	for _, tc := range cases {
		data := makeInput(tc.length)
		sum := HashBytes(data)
		got := hex.EncodeToString(sum[:])
		if got != tc.expected {
			t.Errorf("HashBytes(len=%d) = %s, want %s", tc.length, got, tc.expected)
		}

		// Verify incremental streaming writes (7 bytes at a time) yield identical BLAKE3 digest.
		h := newBlake3Hasher()
		for i := 0; i < len(data); i += 7 {
			end := i + 7
			if end > len(data) {
				end = len(data)
			}
			h.Write(data[i:end])
		}
		if streamSum := h.Sum256(); streamSum != sum {
			t.Errorf("streaming blake3(len=%d) = %x, want %x", tc.length, streamSum, sum)
		}
	}
}

func TestChunkEmpty(t *testing.T) {
	blocks, fileHash, totalSize, err := ChunkReader(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("ChunkReader(empty) unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks for empty input, got %d", len(blocks))
	}
	if totalSize != 0 {
		t.Fatalf("expected totalSize 0, got %d", totalSize)
	}
	wantEmptyHash := HashBytes(nil)
	if fileHash != wantEmptyHash {
		t.Fatalf("empty fileHash = %x, want %x", fileHash, wantEmptyHash)
	}
}

func TestChunkSmallFile(t *testing.T) {
	data := make([]byte, 7*1024) // 7 KB < 16 KB MinSize
	rng := rand.New(rand.NewSource(42))
	_, _ = rng.Read(data)

	blocks, fileHash, totalSize, err := ChunkReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ChunkReader(small) unexpected error: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block for <16KB input, got %d", len(blocks))
	}
	if totalSize != int64(len(data)) {
		t.Fatalf("totalSize = %d, want %d", totalSize, len(data))
	}

	wantHash := HashBytes(data)
	if fileHash != wantHash {
		t.Fatalf("fileHash = %x, want %x", fileHash, wantHash)
	}

	b := blocks[0]
	if b.Index != 0 || b.Offset != 0 || b.Size != int32(len(data)) || b.Hash != wantHash {
		t.Fatalf("unexpected block metadata: %+v", b)
	}
}

func TestChunkLargeRandomData(t *testing.T) {
	const size = 2 * 1024 * 1024 // 2 MB
	data := make([]byte, size)
	rng := rand.New(rand.NewSource(1337))
	_, _ = rng.Read(data)

	blocks, fileHash, totalSize, err := ChunkReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ChunkReader(2MB) unexpected error: %v", err)
	}
	if totalSize != size {
		t.Fatalf("totalSize = %d, want %d", totalSize, size)
	}
	if len(blocks) < 8 {
		t.Fatalf("expected multiple blocks for 2MB random payload, got %d", len(blocks))
	}

	wantFileHash := HashBytes(data)
	if fileHash != wantFileHash {
		t.Fatalf("fileHash mismatch: got %x, want %x", fileHash, wantFileHash)
	}

	var reassembled bytes.Buffer
	var expectedOffset int64

	for i, b := range blocks {
		if b.Index != i {
			t.Fatalf("block %d has Index=%d", i, b.Index)
		}
		if b.Offset != expectedOffset {
			t.Fatalf("block %d has Offset=%d, want %d", i, b.Offset, expectedOffset)
		}

		isLast := i == len(blocks)-1
		if !isLast && int(b.Size) < DefaultMinSize {
			t.Fatalf("non-final block %d has Size=%d < DefaultMinSize=%d", i, b.Size, DefaultMinSize)
		}
		if int(b.Size) > DefaultMaxSize {
			t.Fatalf("block %d has Size=%d > DefaultMaxSize=%d", i, b.Size, DefaultMaxSize)
		}

		slice := data[b.Offset : b.Offset+int64(b.Size)]
		if gotHash := HashBytes(slice); b.Hash != gotHash {
			t.Fatalf("block %d hash mismatch: got %x, want %x", i, b.Hash, gotHash)
		}
		reassembled.Write(slice)
		expectedOffset += int64(b.Size)
	}

	if expectedOffset != size {
		t.Fatalf("final offset = %d, want %d", expectedOffset, size)
	}
	if !bytes.Equal(reassembled.Bytes(), data) {
		t.Fatal("reassembled chunks do not match original payload")
	}
}

func TestContentShiftResilience(t *testing.T) {
	const originalSize = 1024 * 1024 // 1 MB
	const insertOffset = 500
	const insertSize = 500

	rng := rand.New(rand.NewSource(9999))
	original := make([]byte, originalSize)
	_, _ = rng.Read(original)

	inserted := make([]byte, insertSize)
	_, _ = rng.Read(inserted)

	modified := make([]byte, 0, originalSize+insertSize)
	modified = append(modified, original[:insertOffset]...)
	modified = append(modified, inserted...)
	modified = append(modified, original[insertOffset:]...)

	origBlocks, _, _, err := ChunkReader(bytes.NewReader(original))
	if err != nil {
		t.Fatalf("chunk original: %v", err)
	}

	modBlocks, _, _, err := ChunkReader(bytes.NewReader(modified))
	if err != nil {
		t.Fatalf("chunk modified: %v", err)
	}

	origSet := make(map[[32]byte]struct{}, len(origBlocks))
	for _, b := range origBlocks {
		origSet[b.Hash] = struct{}{}
	}

	matched := 0
	for _, b := range modBlocks {
		if _, ok := origSet[b.Hash]; ok {
			matched++
		}
	}

	ratio := float64(matched) / float64(len(origBlocks))
	t.Logf("original chunks: %d, modified chunks: %d, identical chunks: %d (%.1f%%)",
		len(origBlocks), len(modBlocks), matched, ratio*100)

	if ratio <= 0.70 {
		t.Fatalf("expected >70%% chunk reuse after 500B insertion near start, got %.1f%% (%d/%d)",
			ratio*100, matched, len(origBlocks))
	}
}

func TestChunkFileAndCustomOptions(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "sample.bin")

	rng := rand.New(rand.NewSource(2026))
	payload := make([]byte, 128*1024)
	_, _ = rng.Read(payload)

	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		t.Fatalf("write sample file: %v", err)
	}

	blocks, fileHash, totalSize, err := ChunkFile(filePath)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}
	if totalSize != int64(len(payload)) {
		t.Fatalf("ChunkFile totalSize = %d, want %d", totalSize, len(payload))
	}
	if fileHash != HashBytes(payload) {
		t.Fatalf("ChunkFile fileHash mismatch")
	}
	if len(blocks) == 0 {
		t.Fatal("expected at least 1 block from ChunkFile")
	}

	// Custom smaller options
	custom, err := New(Options{
		MinSize: 2 * 1024,
		AvgSize: 8 * 1024,
		MaxSize: 32 * 1024,
	})
	if err != nil {
		t.Fatalf("New custom chunker failed: %v", err)
	}
	customBlocks, customHash, customTotal, err := custom.ChunkFile(filePath)
	if err != nil {
		t.Fatalf("custom.ChunkFile failed: %v", err)
	}
	if customTotal != int64(len(payload)) || customHash != fileHash {
		t.Fatalf("custom chunker hash/size mismatch")
	}
	if len(customBlocks) <= len(blocks) {
		t.Fatalf("expected more blocks with 8KB avg (%d) than 64KB avg (%d)", len(customBlocks), len(blocks))
	}
}

func BenchmarkHashBytes64KB(b *testing.B) {
	data := make([]byte, 64*1024)
	rng := rand.New(rand.NewSource(1))
	_, _ = rng.Read(data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = HashBytes(data)
	}
}

func BenchmarkChunkReader1MB(b *testing.B) {
	data := make([]byte, 1024*1024)
	rng := rand.New(rand.NewSource(2))
	_, _ = rng.Read(data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := ChunkReader(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}
