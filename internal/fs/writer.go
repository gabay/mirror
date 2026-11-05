package fs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mirror-sync/mirror/internal/chunker"
	"github.com/mirror-sync/mirror/internal/model"
	"github.com/mirror-sync/mirror/internal/store"
)

// FetchBlockFunc fetches a single content block by its 32-byte BLAKE3 hash and expected byte size
// from a remote peer.
type FetchBlockFunc func(ctx context.Context, hash [32]byte, size int32) ([]byte, error)

// BlockSource is an optional interface wrapping FetchBlock.
type BlockSource interface {
	FetchBlock(ctx context.Context, hash [32]byte, size int32) ([]byte, error)
}

// Writer performs atomic file, directory, and symlink updates in rootDir, reusing matching
// local blocks via Store before requesting missing blocks from remote peers.
type Writer struct {
	rootDir string
	store   *store.Store
	logger  *slog.Logger
}

// NewWriter creates a new atomic filesystem Writer rooted at rootDir.
func NewWriter(rootDir string, st *store.Store, logger *slog.Logger) *Writer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Writer{
		rootDir: filepath.Clean(rootDir),
		store:   st,
		logger:  logger,
	}
}

// SaveConflictCopy creates a backup copy of the existing local file or symlink at relPath
// using the naming convention `<dir>/<base>.sync-conflict-<YYYYMMDD-HHMMSS>-<shortNodeID><ext>`.
// It returns the relative path of the conflict copy (or "" if relPath does not exist on disk).
func (w *Writer) SaveConflictCopy(relPath string, loserNodeID string, loserModTimeNs int64) (string, error) {
	norm := normalizeRelPath(relPath)
	if norm == "" {
		return "", store.ErrEmptyPath
	}

	srcAbs := filepath.Join(w.rootDir, filepath.FromSlash(norm))
	info, err := os.Lstat(srcAbs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("writer: lstat conflict source %q: %w", srcAbs, err)
	}
	if info.Mode().IsDir() {
		return "", nil
	}

	if loserModTimeNs <= 0 {
		loserModTimeNs = info.ModTime().UnixNano()
	}
	ts := time.Unix(0, loserModTimeNs).UTC().Format("20060102-150405")

	shortNode := strings.TrimSpace(loserNodeID)
	if shortNode == "" {
		shortNode = "local"
	}
	if len(shortNode) > 8 {
		shortNode = shortNode[:8]
	}

	conflictRel := FormatConflictPath(norm, ts, shortNode)
	dstAbs := filepath.Join(w.rootDir, filepath.FromSlash(conflictRel))

	if err := os.MkdirAll(filepath.Dir(dstAbs), 0o755); err != nil {
		return "", fmt.Errorf("writer: mkdir for conflict copy %q: %w", dstAbs, err)
	}

	meta := ExtractMetadata(info)
	metaFi := model.FileInfo{
		Path:      conflictRel,
		Type:      meta.Type,
		Mode:      meta.Mode,
		Uid:       meta.Uid,
		Gid:       meta.Gid,
		ModTimeNs: meta.ModTimeNs,
	}

	stagingPath, err := makeStagingPath(dstAbs)
	if err != nil {
		return "", err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(stagingPath)
		}
	}()

	if meta.Type == model.FileTypeSymlink {
		target, err := os.Readlink(srcAbs)
		if err != nil {
			return "", fmt.Errorf("writer: readlink conflict source %q: %w", srcAbs, err)
		}
		if err := os.Symlink(target, stagingPath); err != nil {
			return "", fmt.Errorf("writer: create conflict symlink %q: %w", stagingPath, err)
		}
		if err := ApplyMetadata(stagingPath, metaFi, w.logger); err != nil {
			return "", err
		}
		if err := os.Rename(stagingPath, dstAbs); err != nil {
			return "", fmt.Errorf("writer: rename conflict symlink %q -> %q: %w", stagingPath, dstAbs, err)
		}
		renamed = true
		return conflictRel, nil
	}

	srcFile, err := os.Open(srcAbs)
	if err != nil {
		return "", fmt.Errorf("writer: open conflict source %q: %w", srcAbs, err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(stagingPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", fmt.Errorf("writer: create conflict staging %q: %w", stagingPath, err)
	}

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		_ = dstFile.Close()
		return "", fmt.Errorf("writer: copy conflict data to %q: %w", stagingPath, err)
	}
	if err := dstFile.Sync(); err != nil {
		_ = dstFile.Close()
		return "", fmt.Errorf("writer: sync conflict staging %q: %w", stagingPath, err)
	}
	if err := dstFile.Close(); err != nil {
		return "", fmt.Errorf("writer: close conflict staging %q: %w", stagingPath, err)
	}

	if err := ApplyMetadata(stagingPath, metaFi, w.logger); err != nil {
		return "", err
	}
	if err := os.Rename(stagingPath, dstAbs); err != nil {
		return "", fmt.Errorf("writer: rename conflict staging %q -> %q: %w", stagingPath, dstAbs, err)
	}
	renamed = true
	return conflictRel, nil
}

// FormatConflictPath constructs `<dir>/<base>.sync-conflict-<timestamp>-<shortNodeID><ext>`.
func FormatConflictPath(relPath, timestamp, shortNodeID string) string {
	norm := normalizeRelPath(relPath)
	dir := path.Dir(norm)
	fileName := path.Base(norm)
	ext := path.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	if base == "" {
		// Dotfile with no other extension (e.g. ".bashrc")
		base = fileName
		ext = ""
	}
	conflictName := fmt.Sprintf("%s.sync-conflict-%s-%s%s", base, timestamp, shortNodeID, ext)
	if dir == "." || dir == "" {
		return conflictName
	}
	return dir + "/" + conflictName
}

// ApplyRemoteFile applies a remote FileInfo state (deletion, directory, symlink, or regular file)
// to disk atomically and records the updated state in Store.
func (w *Writer) ApplyRemoteFile(ctx context.Context, fi model.FileInfo, fetchBlock FetchBlockFunc) error {
	norm := normalizeRelPath(fi.Path)
	if norm == "" {
		return store.ErrEmptyPath
	}
	fi = fi.Clone()
	fi.Path = norm

	absPath := filepath.Join(w.rootDir, filepath.FromSlash(norm))

	if fi.Deleted {
		if info, err := os.Lstat(absPath); err == nil {
			if info.IsDir() {
				if err := os.RemoveAll(absPath); err != nil {
					return fmt.Errorf("writer: remove deleted dir %q: %w", absPath, err)
				}
			} else {
				if err := os.Remove(absPath); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("writer: remove deleted file %q: %w", absPath, err)
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("writer: lstat deleted path %q: %w", absPath, err)
		}

		if w.store != nil {
			if err := w.store.PutFile(fi); err != nil {
				return fmt.Errorf("writer: store tombstone %q: %w", norm, err)
			}
		}
		return nil
	}

	switch fi.Type {
	case model.FileTypeDir:
		return w.applyDir(absPath, fi)
	case model.FileTypeSymlink:
		return w.applySymlink(absPath, fi)
	case model.FileTypeRegular:
		return w.applyRegularFile(ctx, absPath, fi, fetchBlock)
	default:
		return fmt.Errorf("writer: unsupported file type %v for %q", fi.Type, norm)
	}
}

func (w *Writer) applyDir(absPath string, fi model.FileInfo) error {
	if info, err := os.Lstat(absPath); err == nil && !info.IsDir() {
		if err := os.Remove(absPath); err != nil {
			return fmt.Errorf("writer: remove non-dir at %q: %w", absPath, err)
		}
	}

	if err := os.MkdirAll(absPath, 0o755); err != nil {
		return fmt.Errorf("writer: mkdir %q: %w", absPath, err)
	}

	if err := ApplyMetadata(absPath, fi, w.logger); err != nil {
		return err
	}

	storedFi := syncOnDiskOwnership(absPath, fi)
	if w.store != nil {
		if err := w.store.PutFile(storedFi); err != nil {
			return fmt.Errorf("writer: store dir %q: %w", fi.Path, err)
		}
	}
	return nil
}

func (w *Writer) applySymlink(absPath string, fi model.FileInfo) error {
	parentDir := filepath.Dir(absPath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return fmt.Errorf("writer: mkdir parent for symlink %q: %w", absPath, err)
	}

	stagingPath, err := makeStagingPath(absPath)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(stagingPath)
		}
	}()

	if err := os.Symlink(fi.SymlinkTarget, stagingPath); err != nil {
		return fmt.Errorf("writer: create staging symlink %q: %w", stagingPath, err)
	}

	if err := ApplyMetadata(stagingPath, fi, w.logger); err != nil {
		return err
	}

	if info, err := os.Lstat(absPath); err == nil && info.IsDir() {
		if err := os.RemoveAll(absPath); err != nil {
			return fmt.Errorf("writer: remove existing directory at symlink target %q: %w", absPath, err)
		}
	}

	storedFi := syncOnDiskOwnership(stagingPath, fi)
	if storedFi.FileHash == ([32]byte{}) {
		storedFi.FileHash = chunker.HashBytes([]byte(fi.SymlinkTarget))
	}
	if w.store != nil {
		if err := w.store.PutFile(storedFi); err != nil {
			return fmt.Errorf("writer: store symlink %q: %w", fi.Path, err)
		}
	}

	if err := os.Rename(stagingPath, absPath); err != nil {
		return fmt.Errorf("writer: rename staging symlink %q -> %q: %w", stagingPath, absPath, err)
	}
	renamed = true
	return nil
}

func (w *Writer) applyRegularFile(
	ctx context.Context,
	absPath string,
	fi model.FileInfo,
	fetchBlock FetchBlockFunc,
) error {
	stagingPath, err := w.StageRegularFile(ctx, fi, fetchBlock)
	if err != nil {
		return err
	}
	return w.CommitStagedRegularFile(stagingPath, fi)
}

// StageRegularFile downloads or reuses blocks for a regular file into a hidden staging file,
// verifies its total size and full-file BLAKE3 digest, and applies its file metadata.
// The caller must subsequently call CommitStagedRegularFile or remove the returned stagingPath.
func (w *Writer) StageRegularFile(
	ctx context.Context,
	fi model.FileInfo,
	fetchBlock FetchBlockFunc,
) (string, error) {
	norm := normalizeRelPath(fi.Path)
	if norm == "" {
		return "", store.ErrEmptyPath
	}
	fi = fi.Clone()
	fi.Path = norm

	absPath := filepath.Join(w.rootDir, filepath.FromSlash(norm))
	parentDir := filepath.Dir(absPath)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return "", fmt.Errorf("writer: mkdir parent for file %q: %w", absPath, err)
	}

	stagingPath, err := makeStagingPath(absPath)
	if err != nil {
		return "", err
	}
	stagedOK := false
	defer func() {
		if !stagedOK {
			_ = os.Remove(stagingPath)
		}
	}()

	stagingFile, err := os.OpenFile(stagingPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return "", fmt.Errorf("writer: create staging file %q: %w", stagingPath, err)
	}
	defer func() {
		if stagingFile != nil {
			_ = stagingFile.Close()
		}
	}()

	writtenInStaging := make(map[[32]byte]int64, len(fi.Blocks))

	for _, block := range fi.Blocks {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if block.Size < 0 || block.Offset < 0 {
			return "", fmt.Errorf("writer: invalid block bounds (offset=%d, size=%d) for %q",
				block.Offset, block.Size, fi.Path)
		}

		var data []byte
		var ok bool

		// 1. Check if this exact block hash was already written earlier in this staging file.
		if prevOffset, exists := writtenInStaging[block.Hash]; exists {
			buf := make([]byte, block.Size)
			if n, err := stagingFile.ReadAt(buf, prevOffset); err == nil && int32(n) == block.Size {
				if chunker.HashBytes(buf) == block.Hash {
					data = buf
					ok = true
				}
			}
		}

		// 2. Try local block reuse from Store block index (or existing target file at absPath).
		if !ok {
			data, ok = w.readLocalBlock(block, absPath)
		}

		// 3. Fetch missing block from remote peer.
		if !ok {
			if fetchBlock == nil {
				return "", fmt.Errorf("writer: missing local block %x for %q and fetchBlock is nil",
					block.Hash[:4], fi.Path)
			}
			fetched, err := fetchBlock(ctx, block.Hash, block.Size)
			if err != nil {
				return "", fmt.Errorf("writer: fetch block %x (index %d) for %q: %w",
					block.Hash[:4], block.Index, fi.Path, err)
			}
			if int32(len(fetched)) != block.Size {
				return "", fmt.Errorf("writer: block %x size mismatch: got %d, want %d",
					block.Hash[:4], len(fetched), block.Size)
			}
			actualHash := chunker.HashBytes(fetched)
			if actualHash != block.Hash {
				return "", fmt.Errorf("writer: block %x hash mismatch: got %x",
					block.Hash[:8], actualHash[:8])
			}
			data = fetched
		}

		if _, err := stagingFile.WriteAt(data, block.Offset); err != nil {
			return "", fmt.Errorf("writer: write block %d at offset %d in %q: %w",
				block.Index, block.Offset, stagingPath, err)
		}
		writtenInStaging[block.Hash] = block.Offset
	}

	// Verify total size and full-file BLAKE3 hash of the assembled staging file.
	if _, err := stagingFile.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("writer: seek staging file %q: %w", stagingPath, err)
	}
	_, computedFileHash, totalSize, err := chunker.ChunkReader(stagingFile)
	if err != nil {
		return "", fmt.Errorf("writer: verify assembled file %q: %w", stagingPath, err)
	}
	if totalSize != fi.Size {
		return "", fmt.Errorf("writer: assembled file %q size mismatch: got %d, want %d",
			fi.Path, totalSize, fi.Size)
	}
	if computedFileHash != fi.FileHash {
		return "", fmt.Errorf("writer: assembled file %q hash mismatch: got %x, want %x",
			fi.Path, computedFileHash[:8], fi.FileHash[:8])
	}

	if err := stagingFile.Sync(); err != nil {
		return "", fmt.Errorf("writer: sync staging file %q: %w", stagingPath, err)
	}
	if err := stagingFile.Close(); err != nil {
		return "", fmt.Errorf("writer: close staging file %q: %w", stagingPath, err)
	}
	stagingFile = nil

	if err := ApplyMetadata(stagingPath, fi, w.logger); err != nil {
		return "", err
	}

	stagedOK = true
	return stagingPath, nil
}

// CommitStagedRegularFile records fi in Store and atomically renames stagingPath to its target path.
// If committing fails, stagingPath is removed automatically.
func (w *Writer) CommitStagedRegularFile(stagingPath string, fi model.FileInfo) error {
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(stagingPath)
		}
	}()

	norm := normalizeRelPath(fi.Path)
	if norm == "" {
		return store.ErrEmptyPath
	}
	fi = fi.Clone()
	fi.Path = norm
	absPath := filepath.Join(w.rootDir, filepath.FromSlash(norm))

	if info, err := os.Lstat(absPath); err == nil && info.IsDir() {
		if err := os.RemoveAll(absPath); err != nil {
			return fmt.Errorf("writer: remove existing directory at file target %q: %w", absPath, err)
		}
	}

	// Update Store right before atomic rename so any concurrent watcher/scanner event
	// sees the file on disk matches Store and does not trigger a spurious version increment.
	storedFi := syncOnDiskOwnership(stagingPath, fi)
	if w.store != nil {
		if err := w.store.PutFile(storedFi); err != nil {
			return fmt.Errorf("writer: store file %q: %w", fi.Path, err)
		}
	}

	if err := os.Rename(stagingPath, absPath); err != nil {
		return fmt.Errorf("writer: rename staging file %q -> %q: %w", stagingPath, absPath, err)
	}
	renamed = true
	return nil
}

// readLocalBlock searches the Store's local block index (and targetAbsPath as fallback)
// for a verified local slice matching block.Hash and block.Size.
func (w *Writer) readLocalBlock(block model.BlockInfo, targetAbsPath string) ([]byte, bool) {
	if w.store != nil {
		if locs, ok := w.store.LookupBlock(block.Hash); ok {
			for _, loc := range locs {
				if loc.Size != block.Size {
					continue
				}
				candidateAbs := filepath.Join(w.rootDir, filepath.FromSlash(loc.Path))
				if buf, ok := readAndVerifySlice(candidateAbs, loc.Offset, block.Size, block.Hash); ok {
					return buf, true
				}
			}
		}
	}

	// Fallback: check the existing file at targetAbsPath at block.Offset.
	if buf, ok := readAndVerifySlice(targetAbsPath, block.Offset, block.Size, block.Hash); ok {
		return buf, true
	}

	return nil, false
}

func readAndVerifySlice(absPath string, offset int64, size int32, expectedHash [32]byte) ([]byte, bool) {
	if size < 0 || offset < 0 {
		return nil, false
	}
	f, err := os.Open(absPath)
	if err != nil {
		return nil, false
	}
	defer f.Close()

	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	if int32(n) != size {
		return nil, false
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false
	}
	if chunker.HashBytes(buf) != expectedHash {
		return nil, false
	}
	return buf, true
}

func makeStagingPath(targetAbsPath string) (string, error) {
	dir := filepath.Dir(targetAbsPath)
	base := filepath.Base(targetAbsPath)

	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", fmt.Errorf("writer: generate random staging suffix: %w", err)
	}
	stagingName := fmt.Sprintf(".mirror.tmp.%s.%s", base, hex.EncodeToString(rnd[:]))
	return filepath.Join(dir, stagingName), nil
}

// syncOnDiskOwnership reads back the actual on-disk uid/gid/mtime of path so that if
// os.Lchown or lutimes was skipped due to insufficient privileges (EPERM), the stored
// FileInfo matches the actual filesystem state and prevents false-positive rescan loops.
func syncOnDiskOwnership(absPath string, fi model.FileInfo) model.FileInfo {
	out := fi.Clone()
	if meta, err := LstatMetadata(absPath); err == nil {
		out.Uid = meta.Uid
		out.Gid = meta.Gid
		if out.Type == model.FileTypeSymlink {
			out.ModTimeNs = meta.ModTimeNs
		}
	}
	return out
}
