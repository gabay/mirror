package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/mirror-sync/mirror/internal/model"
)

var (
	// ErrStoreClosed is returned when a mutating operation is called on a closed Store.
	ErrStoreClosed = errors.New("store: database is closed")
	// ErrEmptyPath is returned when a FileInfo has an empty or invalid relative path.
	ErrEmptyPath = errors.New("store: file path must not be empty")
)

const (
	snapshotFileName    = "state.db"
	walFileName         = "state.wal"
	snapshotMagic       = "MIRRDB01"
	walCompactThreshold = 512
	maxFrameBytes       = 128 * 1024 * 1024 // 128 MB sanity limit
)

// BlockLocation identifies where a specific content block resides on the local filesystem.
type BlockLocation struct {
	Path   string
	Offset int64
	Size   int32
}

// Store manages durable persistence of FileInfo state (via atomic snapshots + CRC32 WAL)
// and maintains a thread-safe in-memory index of files and local block locations.
type Store struct {
	mu       sync.RWMutex
	stateDir string
	walFile  *os.File
	walOps   int
	closed   bool

	files  map[string]model.FileInfo
	blocks map[[32]byte][]BlockLocation
}

// Open opens or initializes the persistent state store in stateDir.
// It loads the latest compacted snapshot, replays any uncommitted WAL frames,
// rebuilds the in-memory file and block index, and compacts the state on disk.
func Open(stateDir string) (*Store, error) {
	if strings.TrimSpace(stateDir) == "" {
		return nil, errors.New("store: stateDir must not be empty")
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create stateDir %q: %w", stateDir, err)
	}

	s := &Store{
		stateDir: stateDir,
		files:    make(map[string]model.FileInfo),
		blocks:   make(map[[32]byte][]BlockLocation),
	}

	if err := s.recoverFromDisk(); err != nil {
		return nil, err
	}

	return s, nil
}

// Close flushes and compacts the current state to the snapshot file, removes the WAL,
// and closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var firstErr error
	if s.walFile != nil {
		if err := s.walFile.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.walFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.walFile = nil
	}

	if err := s.writeSnapshotLocked(); err != nil && firstErr == nil {
		firstErr = err
	} else if err == nil {
		_ = os.Remove(filepath.Join(s.stateDir, walFileName))
	}

	return firstErr
}

// GetFile retrieves a cloned FileInfo for the given relative path.
func (s *Store) GetFile(relPath string) (model.FileInfo, bool) {
	norm := normalizePath(relPath)
	if norm == "" {
		return model.FileInfo{}, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	fi, ok := s.files[norm]
	if !ok {
		return model.FileInfo{}, false
	}
	return fi.Clone(), true
}

// PutFile persists fi to disk and updates the in-memory file and block location indexes.
// If fi.Deleted is true, any previous block locations for fi.Path are removed from the
// block index while the tombstone FileInfo remains tracked in files.
func (s *Store) PutFile(fi model.FileInfo) error {
	return s.PutFilesBatch([]model.FileInfo{fi})
}

// PutFilesBatch atomically persists a batch of FileInfo records to disk and updates
// the in-memory file and block location indexes.
func (s *Store) PutFilesBatch(fis []model.FileInfo) error {
	if len(fis) == 0 {
		return nil
	}

	normalized := make([]model.FileInfo, len(fis))
	for i, fi := range fis {
		norm := normalizePath(fi.Path)
		if norm == "" {
			return ErrEmptyPath
		}
		cloned := fi.Clone()
		cloned.Path = norm
		normalized[i] = cloned
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	if err := s.appendWALLocked(normalized); err != nil {
		return err
	}

	for _, fi := range normalized {
		s.applyInMemoryLocked(fi)
	}

	s.walOps += len(normalized)
	if s.walOps >= walCompactThreshold {
		if err := s.compactLocked(); err != nil {
			return err
		}
	}

	return nil
}

// ListFiles returns a deep-copied slice of all tracked FileInfo entries (including tombstones),
// sorted lexicographically by Path.
func (s *Store) ListFiles() []model.FileInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]model.FileInfo, 0, len(s.files))
	for _, fi := range s.files {
		out = append(out, fi.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	return out
}

// LookupBlock returns a copy of all known active local BlockLocations for the given block hash.
func (s *Store) LookupBlock(hash [32]byte) ([]BlockLocation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	locs, ok := s.blocks[hash]
	if !ok || len(locs) == 0 {
		return nil, false
	}
	out := make([]BlockLocation, len(locs))
	copy(out, locs)
	return out, true
}

// applyInMemoryLocked updates s.files and s.blocks for fi. Caller must hold s.mu.
func (s *Store) applyInMemoryLocked(fi model.FileInfo) {
	if old, exists := s.files[fi.Path]; exists {
		s.removeFileBlocksLocked(old)
	}

	cloned := fi.Clone()
	s.files[cloned.Path] = cloned

	if !cloned.Deleted && cloned.Type == model.FileTypeRegular {
		for _, b := range cloned.Blocks {
			s.blocks[b.Hash] = append(s.blocks[b.Hash], BlockLocation{
				Path:   cloned.Path,
				Offset: b.Offset,
				Size:   b.Size,
			})
		}
	}
}

// removeFileBlocksLocked removes all BlockLocations associated with old.Path from s.blocks.
func (s *Store) removeFileBlocksLocked(old model.FileInfo) {
	if old.Deleted || old.Type != model.FileTypeRegular || len(old.Blocks) == 0 {
		return
	}

	for _, b := range old.Blocks {
		locs, exists := s.blocks[b.Hash]
		if !exists {
			continue
		}
		kept := locs[:0]
		for _, loc := range locs {
			if loc.Path != old.Path {
				kept = append(kept, loc)
			}
		}
		if len(kept) == 0 {
			delete(s.blocks, b.Hash)
		} else {
			s.blocks[b.Hash] = kept
		}
	}
}

func (s *Store) recoverFromDisk() error {
	snapPath := filepath.Join(s.stateDir, snapshotFileName)
	if data, err := os.ReadFile(snapPath); err == nil {
		records, err := decodeSnapshot(data)
		if err != nil {
			return fmt.Errorf("store: decode snapshot %q: %w", snapPath, err)
		}
		for _, fi := range records {
			if norm := normalizePath(fi.Path); norm != "" {
				fi.Path = norm
				s.applyInMemoryLocked(fi)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: read snapshot %q: %w", snapPath, err)
	}

	walPath := filepath.Join(s.stateDir, walFileName)
	if f, err := os.Open(walPath); err == nil {
		replayed, replayErr := replayWAL(f)
		_ = f.Close()
		if replayErr != nil {
			return fmt.Errorf("store: replay WAL %q: %w", walPath, replayErr)
		}
		for _, fi := range replayed {
			if norm := normalizePath(fi.Path); norm != "" {
				fi.Path = norm
				s.applyInMemoryLocked(fi)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: open WAL %q: %w", walPath, err)
	}

	// Compact recovered state into snapshot and open a clean WAL.
	if err := s.compactLocked(); err != nil {
		return err
	}
	return nil
}

func (s *Store) compactLocked() error {
	if s.walFile != nil {
		_ = s.walFile.Close()
		s.walFile = nil
	}
	if err := s.writeSnapshotLocked(); err != nil {
		return err
	}
	walPath := filepath.Join(s.stateDir, walFileName)
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("store: open fresh WAL %q: %w", walPath, err)
	}
	s.walFile = f
	s.walOps = 0
	return nil
}

func (s *Store) writeSnapshotLocked() error {
	records := make([]model.FileInfo, 0, len(s.files))
	for _, fi := range s.files {
		records = append(records, fi)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Path < records[j].Path
	})

	data, err := encodeSnapshot(records)
	if err != nil {
		return fmt.Errorf("store: encode snapshot: %w", err)
	}

	snapPath := filepath.Join(s.stateDir, snapshotFileName)
	tmpPath := snapPath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("store: create snapshot tmp %q: %w", tmpPath, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("store: write snapshot tmp %q: %w", tmpPath, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("store: sync snapshot tmp %q: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("store: close snapshot tmp %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, snapPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("store: rename snapshot %q -> %q: %w", tmpPath, snapPath, err)
	}
	return nil
}

func (s *Store) appendWALLocked(batch []model.FileInfo) error {
	if s.walFile == nil {
		walPath := filepath.Join(s.stateDir, walFileName)
		f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("store: open WAL %q: %w", walPath, err)
		}
		s.walFile = f
	}

	frame, err := encodeFrame(batch)
	if err != nil {
		return fmt.Errorf("store: encode WAL frame: %w", err)
	}
	if _, err := s.walFile.Write(frame); err != nil {
		return fmt.Errorf("store: write WAL frame: %w", err)
	}
	if err := s.walFile.Sync(); err != nil {
		return fmt.Errorf("store: sync WAL: %w", err)
	}
	return nil
}

func encodeSnapshot(records []model.FileInfo) ([]byte, error) {
	frame, err := encodeFrame(records)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(snapshotMagic)+len(frame))
	copy(out, snapshotMagic)
	copy(out[len(snapshotMagic):], frame)
	return out, nil
}

func decodeSnapshot(data []byte) ([]model.FileInfo, error) {
	if len(data) < len(snapshotMagic)+8 {
		return nil, errors.New("snapshot file too short")
	}
	if string(data[:len(snapshotMagic)]) != snapshotMagic {
		return nil, errors.New("invalid snapshot magic header")
	}
	r := bytes.NewReader(data[len(snapshotMagic):])
	records, err := readNextFrame(r)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func encodeFrame(records []model.FileInfo) ([]byte, error) {
	var payloadBuf bytes.Buffer
	if err := gob.NewEncoder(&payloadBuf).Encode(records); err != nil {
		return nil, err
	}
	payload := payloadBuf.Bytes()
	checksum := crc32.ChecksumIEEE(payload)

	frame := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], checksum)
	copy(frame[8:], payload)
	return frame, nil
}

func readNextFrame(r io.Reader) ([]model.FileInfo, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(header[0:4])
	expectedCRC := binary.LittleEndian.Uint32(header[4:8])
	if length > maxFrameBytes {
		return nil, fmt.Errorf("frame length %d exceeds maximum %d", length, maxFrameBytes)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("frame CRC mismatch: got %08x, want %08x", actualCRC, expectedCRC)
	}

	var records []model.FileInfo
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&records); err != nil {
		return nil, err
	}
	return records, nil
}

// replayWAL reads sequential WAL frames until EOF. If the final frame was partially written
// or corrupted due to an abrupt crash, replayWAL stops cleanly at the last valid frame.
func replayWAL(r io.Reader) ([]model.FileInfo, error) {
	var all []model.FileInfo
	for {
		batch, err := readNextFrame(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			// Stop at corrupted trailing WAL record (crash tolerance).
			break
		}
		all = append(all, batch...)
	}
	return all, nil
}

func normalizePath(p string) string {
	s := strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	s = strings.TrimPrefix(s, "./")
	s = strings.Trim(s, "/")
	if s == "" || s == "." {
		return ""
	}
	cleaned := path.Clean(s)
	if cleaned == "." || cleaned == "" || strings.HasPrefix(cleaned, "..") {
		return ""
	}
	return cleaned
}
