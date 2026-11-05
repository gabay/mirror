package fs

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/mirror-sync/mirror/internal/ignore"
)

const (
	// DefaultDebounceDelay is the default quiet window before coalesced fs events are emitted.
	DefaultDebounceDelay = 500 * time.Millisecond

	inotifyWatchMask = syscall.IN_CREATE |
		syscall.IN_MODIFY |
		syscall.IN_CLOSE_WRITE |
		syscall.IN_ATTRIB |
		syscall.IN_DELETE |
		syscall.IN_MOVED_FROM |
		syscall.IN_MOVED_TO |
		syscall.IN_DELETE_SELF |
		syscall.IN_MOVE_SELF
)

// Watcher recursively monitors rootDir and all non-ignored subdirectories using Linux inotify,
// coalescing rapid filesystem events over a configurable debounce window.
type Watcher struct {
	rootDir       string
	matcher       *ignore.Matcher
	debounceDelay time.Duration
	onChanges     func(paths []string)
	logger        *slog.Logger

	mu          sync.Mutex
	fd          int
	inotifyFile *os.File
	wdToPath    map[int]string
	pathToWd    map[string]int
	closed      bool

	eventCh chan string
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewWatcher creates and starts a recursive filesystem Watcher rooted at rootDir.
// Initial directory watches are registered synchronously before NewWatcher returns so no
// subsequent filesystem events can be missed.
func NewWatcher(
	rootDir string,
	matcher *ignore.Matcher,
	debounceDelay time.Duration,
	onChanges func(paths []string),
	logger *slog.Logger,
) (*Watcher, error) {
	if strings.TrimSpace(rootDir) == "" {
		return nil, errors.New("watcher: rootDir must not be empty")
	}
	if debounceDelay <= 0 {
		debounceDelay = DefaultDebounceDelay
	}
	if matcher == nil {
		matcher = ignore.NewMatcher(nil)
	}
	if logger == nil {
		logger = slog.Default()
	}

	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("watcher: inotify_init1: %w", err)
	}

	w := &Watcher{
		rootDir:       filepath.Clean(rootDir),
		matcher:       matcher,
		debounceDelay: debounceDelay,
		onChanges:     onChanges,
		logger:        logger,
		fd:            fd,
		inotifyFile:   os.NewFile(uintptr(fd), "inotify"),
		wdToPath:      make(map[int]string),
		pathToWd:      make(map[string]int),
		eventCh:       make(chan string, 1024),
		stopCh:        make(chan struct{}),
	}

	// Register initial watches across rootDir and all non-ignored subdirectories.
	if _, err := w.addSubtreeWatches(w.rootDir); err != nil {
		_ = w.inotifyFile.Close()
		return nil, fmt.Errorf("watcher: register initial watches on %q: %w", w.rootDir, err)
	}

	w.logger.Info("filesystem watcher started",
		"root_dir", w.rootDir,
		"watched_dirs", w.WatchCount(),
		"debounce_delay", w.debounceDelay.String(),
	)

	w.wg.Add(2)
	go w.readLoop()
	go w.debounceLoop()

	return w, nil
}

// Start binds the Watcher's lifecycle to ctx so that cancelling ctx automatically closes the Watcher.
func (w *Watcher) Start(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			_ = w.Close()
		case <-w.stopCh:
		}
	}()
}

// Close stops the Watcher, removes all inotify watches, and waits for background goroutines to exit.
func (w *Watcher) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.wg.Wait()
		return nil
	}
	w.closed = true
	close(w.stopCh)
	err := w.inotifyFile.Close()
	w.mu.Unlock()

	w.wg.Wait()
	return err
}

// WatchCount returns the number of directories currently watched.
func (w *Watcher) WatchCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.wdToPath)
}

func (w *Watcher) addSubtreeWatches(startAbs string) ([]string, error) {
	var discovered []string

	err := filepath.WalkDir(startAbs, func(absPath string, d iofs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}

		rel, err := filepath.Rel(w.rootDir, absPath)
		if err != nil {
			return err
		}

		var relPath string
		if rel == "." {
			relPath = "."
		} else {
			relPath = normalizeRelPath(rel)
			if relPath == "" {
				return nil
			}
		}

		isDir := d.IsDir()
		if relPath != "." && w.matcher != nil && w.matcher.Match(relPath, isDir) {
			if isDir {
				return filepath.SkipDir
			}
			return nil
		}

		if relPath != "." {
			discovered = append(discovered, relPath)
		}

		if isDir {
			if err := w.addSingleWatch(absPath, relPath); err != nil {
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) {
					return nil
				}
				return err
			}
		}
		return nil
	})

	return discovered, err
}

func (w *Watcher) addSingleWatch(absPath, relPath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	wd, err := syscall.InotifyAddWatch(w.fd, absPath, uint32(inotifyWatchMask))
	if err != nil {
		return err
	}

	if oldWd, exists := w.pathToWd[relPath]; exists && oldWd != wd {
		delete(w.wdToPath, oldWd)
	}
	w.wdToPath[wd] = relPath
	w.pathToWd[relPath] = wd
	return nil
}

func (w *Watcher) removeWatchByWd(wd int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if relPath, ok := w.wdToPath[wd]; ok {
		delete(w.wdToPath, wd)
		if curWd, exists := w.pathToWd[relPath]; exists && curWd == wd {
			delete(w.pathToWd, relPath)
		}
	}
}

func (w *Watcher) removeSubtreeWatches(prefixRel string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	slashPrefix := prefixRel + "/"
	for p, wd := range w.pathToWd {
		if p == prefixRel || strings.HasPrefix(p, slashPrefix) {
			delete(w.pathToWd, p)
			delete(w.wdToPath, wd)
			if !w.closed {
				_, _ = syscall.InotifyRmWatch(w.fd, uint32(wd))
			}
		}
	}
}

func (w *Watcher) lookupWatchPath(wd int) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.wdToPath[wd]
	return p, ok
}

func (w *Watcher) readLoop() {
	defer w.wg.Done()

	buf := make([]byte, 64*1024)
	for {
		n, err := w.inotifyFile.Read(buf)
		if n > 0 {
			w.parseEvents(buf[:n])
		}
		if err != nil {
			select {
			case <-w.stopCh:
				return
			default:
			}
			if errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EBADF) {
				return
			}
			if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) {
				continue
			}
			w.logger.Warn("watcher read error", "err", err)
			return
		}
	}
}

func (w *Watcher) parseEvents(buf []byte) {
	offset := 0
	n := len(buf)

	for offset+syscall.SizeofInotifyEvent <= n {
		raw := (*syscall.InotifyEvent)(unsafe.Pointer(&buf[offset]))
		mask := uint32(raw.Mask)
		wd := int(raw.Wd)
		nameLen := int(raw.Len)
		offset += syscall.SizeofInotifyEvent

		var name string
		if nameLen > 0 && offset+nameLen <= n {
			nameBytes := buf[offset : offset+nameLen]
			name = strings.TrimRight(string(nameBytes), "\x00")
			offset += nameLen
		}

		if mask&syscall.IN_Q_OVERFLOW != 0 {
			w.enqueuePath(".")
			continue
		}

		if mask&syscall.IN_IGNORED != 0 {
			w.removeWatchByWd(wd)
			continue
		}

		dirRel, ok := w.lookupWatchPath(wd)
		if !ok {
			continue
		}

		var evRel string
		switch {
		case name == "":
			evRel = dirRel
		case dirRel == "." || dirRel == "":
			evRel = normalizeRelPath(name)
		default:
			evRel = normalizeRelPath(dirRel + "/" + name)
		}

		if evRel == "" || evRel == "." {
			continue
		}

		isDir := (mask & syscall.IN_ISDIR) != 0
		if !isDir && mask&(syscall.IN_CREATE|syscall.IN_MOVED_TO) != 0 {
			// Check Lstat in case a directory was created and IN_ISDIR was not checked yet.
			if info, err := os.Lstat(filepath.Join(w.rootDir, filepath.FromSlash(evRel))); err == nil && info.IsDir() {
				isDir = true
			}
		}

		if w.matcher != nil && w.matcher.Match(evRel, isDir) {
			continue
		}

		w.logger.Debug("watcher event detected",
			"path", evRel,
			"is_dir", isDir,
			"mask", fmt.Sprintf("0x%x", mask),
		)

		if isDir && mask&(syscall.IN_CREATE|syscall.IN_MOVED_TO) != 0 {
			subAbs := filepath.Join(w.rootDir, filepath.FromSlash(evRel))
			discovered, err := w.addSubtreeWatches(subAbs)
			if err != nil {
				w.logger.Debug("watcher failed adding subtree watch", "path", evRel, "err", err)
			}
			w.enqueuePath(evRel)
			for _, subRel := range discovered {
				w.enqueuePath(subRel)
			}
			continue
		}

		if isDir && mask&(syscall.IN_DELETE|syscall.IN_MOVED_FROM|syscall.IN_DELETE_SELF|syscall.IN_MOVE_SELF) != 0 {
			w.removeSubtreeWatches(evRel)
		}

		w.enqueuePath(evRel)
	}
}

func (w *Watcher) enqueuePath(relPath string) {
	select {
	case <-w.stopCh:
		return
	case w.eventCh <- relPath:
	}
}

func (w *Watcher) debounceLoop() {
	defer w.wg.Done()

	pending := make(map[string]struct{})
	var timer *time.Timer
	var timerCh <-chan time.Time

	stopTimer := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerCh = nil
		}
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		paths := make([]string, 0, len(pending))
		for p := range pending {
			paths = append(paths, p)
		}
		clear(pending)
		sort.Strings(paths)

		w.logger.Debug("watcher flushing debounced changes", "paths", paths)
		if w.onChanges != nil {
			w.onChanges(paths)
		}
	}

	for {
		select {
		case <-w.stopCh:
			stopTimer()
			return

		case p := <-w.eventCh:
			pending[p] = struct{}{}
			if timer == nil {
				timer = time.NewTimer(w.debounceDelay)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(w.debounceDelay)
			}
			timerCh = timer.C

		case <-timerCh:
			timerCh = nil
			flush()
		}
	}
}
