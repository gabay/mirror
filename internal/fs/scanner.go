package fs

import (
	"errors"
	"fmt"
	iofs "io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirror-sync/mirror/internal/chunker"
	"github.com/mirror-sync/mirror/internal/ignore"
	"github.com/mirror-sync/mirror/internal/model"
	"github.com/mirror-sync/mirror/internal/store"
)

// Scanner inspects the local filesystem, chunks regular files with FastCDC + BLAKE3,
// tracks version vectors in Store, and generates tombstones for deleted tracked entries.
type Scanner struct {
	mu           sync.Mutex
	rootDir      string
	nodeID       string
	store        *store.Store
	matcher      *ignore.Matcher
	chunker      *chunker.Chunker
	logger       *slog.Logger
	fastPathHits atomic.Int64
}

// NewScanner creates a new Scanner for rootDir.
func NewScanner(
	rootDir string,
	nodeID string,
	st *store.Store,
	matcher *ignore.Matcher,
	logger *slog.Logger,
) *Scanner {
	if logger == nil {
		logger = slog.Default()
	}
	if matcher == nil {
		matcher = ignore.NewMatcher(nil)
	}
	c, _ := chunker.New(chunker.DefaultOptions())
	return &Scanner{
		rootDir: filepath.Clean(rootDir),
		nodeID:  nodeID,
		store:   st,
		matcher: matcher,
		chunker: c,
		logger:  logger,
	}
}

// WithChunker configures a custom Chunker on the Scanner and returns the Scanner.
func (s *Scanner) WithChunker(c *chunker.Chunker) *Scanner {
	if c != nil {
		s.chunker = c
	}
	return s
}

// FastPathHits returns the total number of regular files that matched the unchanged metadata fast-path.
func (s *Scanner) FastPathHits() int64 {
	return s.fastPathHits.Load()
}

// ScanAll walks rootDir without following symlinks, chunks new or modified files,
// emits tombstones for previously tracked files that were removed from disk,
// persists all updates to the Store, and returns the list of changed FileInfo entries.
func (s *Scanner) ScanAll() ([]model.FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := make(map[string]struct{})
	var changed []model.FileInfo

	err := filepath.WalkDir(s.rootDir, func(absPath string, d iofs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}

		rel, err := filepath.Rel(s.rootDir, absPath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		relPath := normalizeRelPath(rel)
		if relPath == "" {
			return nil
		}

		info, err := os.Lstat(absPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		isDir := info.Mode().IsDir()
		if s.matcher != nil && s.matcher.Match(relPath, isDir) {
			if isDir {
				return filepath.SkipDir
			}
			return nil
		}

		meta := ExtractMetadata(info)
		if meta.IsSpecial {
			s.logger.Warn("ignoring unsupported special file",
				"path", relPath,
				"mode", info.Mode().String(),
			)
			return nil
		}

		seen[relPath] = struct{}{}

		existing, exists := s.store.GetFile(relPath)
		fi, updated, err := s.inspectEntryLocked(absPath, relPath, meta, existing, exists)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				delete(seen, relPath)
				return nil
			}
			return err
		}
		if updated {
			changed = append(changed, fi)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scanner: walk %q: %w", s.rootDir, err)
	}

	// Tombstone detection: check all non-deleted tracked files in Store.
	// If store is empty on a brand-new node, ListFiles() is empty and 0 tombstones are emitted.
	tracked := s.store.ListFiles()
	for _, existing := range tracked {
		if existing.Deleted {
			continue
		}
		if s.matcher != nil && s.matcher.Match(existing.Path, existing.Type == model.FileTypeDir) {
			continue
		}
		if _, ok := seen[existing.Path]; !ok {
			tombstone := s.makeTombstone(existing)
			changed = append(changed, tombstone)
		}
	}

	if len(changed) > 0 {
		sort.Slice(changed, func(i, j int) bool {
			return changed[i].Path < changed[j].Path
		})
		if err := s.store.PutFilesBatch(changed); err != nil {
			return nil, fmt.Errorf("scanner: persist batch: %w", err)
		}
	}

	return changed, nil
}

// ScanPath performs an incremental scan of a single relative path on disk.
// If the entry was created, modified, or deleted (and previously tracked), it updates
// the Store and returns (updatedFileInfo, true, nil).
// If the entry is unchanged, ignored, or untrackable, it returns (existingOrNil, false, nil).
func (s *Scanner) ScanPath(relPath string) (*model.FileInfo, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	norm := normalizeRelPath(relPath)
	if norm == "" {
		return nil, false, nil
	}

	return s.scanSinglePathLocked(norm)
}

// ScanPaths performs an incremental scan for a batch of relative paths (e.g. from Watcher).
// For directories that exist on disk, it also walks their subtree to pick up nested creations;
// for removed directories, it also marks tracked descendants as tombstones.
func (s *Scanner) ScanPaths(relPaths []string) ([]model.FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	visited := make(map[string]struct{})
	var changed []model.FileInfo

	for _, raw := range relPaths {
		norm := normalizeRelPath(raw)
		if norm == "." {
			// Full rescan requested (e.g., queue overflow).
			s.mu.Unlock()
			allChanged, err := s.ScanAll()
			s.mu.Lock()
			return allChanged, err
		}
		if norm == "" {
			continue
		}
		if _, ok := visited[norm]; ok {
			continue
		}

		absPath := filepath.Join(s.rootDir, filepath.FromSlash(norm))
		info, err := os.Lstat(absPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				visited[norm] = struct{}{}
				if fi, updated, err := s.scanSinglePathLocked(norm); err != nil {
					return nil, err
				} else if updated && fi != nil {
					changed = append(changed, *fi)
				}
				// Also check if any tracked descendants under norm/ were deleted.
				prefix := norm + "/"
				for _, tracked := range s.store.ListFiles() {
					if tracked.Deleted || !strings.HasPrefix(tracked.Path, prefix) {
						continue
					}
					if _, done := visited[tracked.Path]; done {
						continue
					}
					visited[tracked.Path] = struct{}{}
					if fi, updated, err := s.scanSinglePathLocked(tracked.Path); err != nil {
						return nil, err
					} else if updated && fi != nil {
						changed = append(changed, *fi)
					}
				}
				continue
			}
			return nil, err
		}

		if info.Mode().IsDir() {
			// Walk subtree rooted at absPath in case files were created inside a new directory.
			walkErr := filepath.WalkDir(absPath, func(subAbs string, d iofs.DirEntry, wErr error) error {
				if wErr != nil {
					if errors.Is(wErr, os.ErrNotExist) {
						return nil
					}
					return wErr
				}
				rel, err := filepath.Rel(s.rootDir, subAbs)
				if err != nil {
					return err
				}
				subNorm := normalizeRelPath(rel)
				if subNorm == "" {
					return nil
				}
				if s.matcher != nil && s.matcher.Match(subNorm, d.IsDir()) {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				if _, done := visited[subNorm]; done {
					return nil
				}
				visited[subNorm] = struct{}{}
				fi, updated, err := s.scanSinglePathLocked(subNorm)
				if err != nil {
					return err
				}
				if updated && fi != nil {
					changed = append(changed, *fi)
				}
				return nil
			})
			if walkErr != nil {
				return nil, walkErr
			}
			continue
		}

		visited[norm] = struct{}{}
		fi, updated, err := s.scanSinglePathLocked(norm)
		if err != nil {
			return nil, err
		}
		if updated && fi != nil {
			changed = append(changed, *fi)
		}
	}

	sort.Slice(changed, func(i, j int) bool {
		return changed[i].Path < changed[j].Path
	})
	return changed, nil
}

func (s *Scanner) scanSinglePathLocked(relPath string) (*model.FileInfo, bool, error) {
	absPath := filepath.Join(s.rootDir, filepath.FromSlash(relPath))
	info, err := os.Lstat(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			existing, exists := s.store.GetFile(relPath)
			if !exists || existing.Deleted {
				return nil, false, nil
			}
			if s.matcher != nil && s.matcher.Match(relPath, existing.Type == model.FileTypeDir) {
				return nil, false, nil
			}
			tombstone := s.makeTombstone(existing)
			if err := s.store.PutFile(tombstone); err != nil {
				return nil, false, err
			}
			return &tombstone, true, nil
		}
		return nil, false, err
	}

	isDir := info.Mode().IsDir()
	if s.matcher != nil && s.matcher.Match(relPath, isDir) {
		return nil, false, nil
	}

	meta := ExtractMetadata(info)
	if meta.IsSpecial {
		s.logger.Warn("ignoring unsupported special file",
			"path", relPath,
			"mode", info.Mode().String(),
		)
		return nil, false, nil
	}

	existing, exists := s.store.GetFile(relPath)
	fi, updated, err := s.inspectEntryLocked(absPath, relPath, meta, existing, exists)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !updated {
		return &existing, false, nil
	}

	if err := s.store.PutFile(fi); err != nil {
		return nil, false, err
	}
	return &fi, true, nil
}

func (s *Scanner) inspectEntryLocked(
	absPath string,
	relPath string,
	meta StatMetadata,
	existing model.FileInfo,
	exists bool,
) (model.FileInfo, bool, error) {
	// Fast-path check for regular files: if size, mtime, mode, uid, and gid are identical
	// to an active regular file in the store, reuse its Blocks and FileHash without reading disk.
	if exists && !existing.Deleted &&
		meta.Type == model.FileTypeRegular &&
		existing.Type == model.FileTypeRegular &&
		meta.Size == existing.Size &&
		meta.ModTimeNs == existing.ModTimeNs &&
		meta.Mode == existing.Mode &&
		meta.Uid == existing.Uid &&
		meta.Gid == existing.Gid {
		s.fastPathHits.Add(1)
		return existing, false, nil
	}

	switch meta.Type {
	case model.FileTypeRegular:
		blocks, fileHash, totalSize, err := s.chunker.ChunkFile(absPath)
		if err != nil {
			return model.FileInfo{}, false, err
		}

		// Check if content or metadata meaningfully changed vs existing.
		if exists && !existing.Deleted &&
			existing.Type == model.FileTypeRegular &&
			existing.Size == totalSize &&
			existing.FileHash == fileHash &&
			existing.Mode == meta.Mode &&
			existing.Uid == meta.Uid &&
			existing.Gid == meta.Gid &&
			existing.ModTimeNs == meta.ModTimeNs {
			return existing, false, nil
		}

		fi := model.FileInfo{
			Path:       relPath,
			Type:       model.FileTypeRegular,
			Size:       totalSize,
			Mode:       meta.Mode,
			Uid:        meta.Uid,
			Gid:        meta.Gid,
			ModTimeNs:  meta.ModTimeNs,
			Deleted:    false,
			Version:    existing.Version.Increment(s.nodeID),
			ModifiedBy: s.nodeID,
			FileHash:   fileHash,
			Blocks:     blocks,
		}
		return fi, true, nil

	case model.FileTypeSymlink:
		target, err := os.Readlink(absPath)
		if err != nil {
			return model.FileInfo{}, false, err
		}
		fileHash := chunker.HashBytes([]byte(target))

		if exists && !existing.Deleted &&
			existing.Type == model.FileTypeSymlink &&
			existing.SymlinkTarget == target &&
			existing.FileHash == fileHash &&
			existing.Uid == meta.Uid &&
			existing.Gid == meta.Gid &&
			existing.ModTimeNs == meta.ModTimeNs {
			return existing, false, nil
		}

		fi := model.FileInfo{
			Path:          relPath,
			Type:          model.FileTypeSymlink,
			Size:          int64(len(target)),
			Mode:          meta.Mode,
			Uid:           meta.Uid,
			Gid:           meta.Gid,
			ModTimeNs:     meta.ModTimeNs,
			SymlinkTarget: target,
			Deleted:       false,
			Version:       existing.Version.Increment(s.nodeID),
			ModifiedBy:    s.nodeID,
			FileHash:      fileHash,
			Blocks:        nil,
		}
		return fi, true, nil

	case model.FileTypeDir:
		// For directories, child file creations/deletions or temporary staging files update
		// the directory's kernel mtime. A directory entry itself is only considered updated
		// when newly created, resurrected from Deleted, or its mode/uid/gid changes.
		if exists && !existing.Deleted &&
			existing.Type == model.FileTypeDir &&
			existing.Mode == meta.Mode &&
			existing.Uid == meta.Uid &&
			existing.Gid == meta.Gid {
			return existing, false, nil
		}

		fi := model.FileInfo{
			Path:       relPath,
			Type:       model.FileTypeDir,
			Size:       0,
			Mode:       meta.Mode,
			Uid:        meta.Uid,
			Gid:        meta.Gid,
			ModTimeNs:  meta.ModTimeNs,
			Deleted:    false,
			Version:    existing.Version.Increment(s.nodeID),
			ModifiedBy: s.nodeID,
			FileHash:   [32]byte{},
			Blocks:     nil,
		}
		return fi, true, nil

	default:
		return model.FileInfo{}, false, fmt.Errorf("scanner: unsupported file type %v for %q", meta.Type, relPath)
	}
}

func (s *Scanner) makeTombstone(existing model.FileInfo) model.FileInfo {
	nowNs := time.Now().UnixNano()
	if nowNs <= existing.ModTimeNs {
		nowNs = existing.ModTimeNs + 1
	}
	return model.FileInfo{
		Path:          existing.Path,
		Type:          existing.Type,
		Size:          0,
		Mode:          existing.Mode,
		Uid:           existing.Uid,
		Gid:           existing.Gid,
		ModTimeNs:     nowNs,
		SymlinkTarget: "",
		Deleted:       true,
		Version:       existing.Version.Increment(s.nodeID),
		ModifiedBy:    s.nodeID,
		FileHash:      [32]byte{},
		Blocks:        nil,
	}
}

func normalizeRelPath(p string) string {
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
