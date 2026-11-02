package fs

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/mirror-sync/mirror/internal/model"
)

const (
	// SpecialModeMask matches device nodes, FIFOs/named pipes, sockets, and irregular files.
	SpecialModeMask = os.ModeDevice | os.ModeNamedPipe | os.ModeSocket | os.ModeCharDevice | os.ModeIrregular

	// PermissionMask includes standard Unix permission bits (0777) plus setuid, setgid, and sticky bits.
	PermissionMask = os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky

	atFDCWD           = -0x64
	atSymlinkNoFollow = 0x100
)

var chownWarnedOnce atomic.Bool

// StatMetadata holds normalized OS metadata extracted from an os.FileInfo (via os.Lstat).
type StatMetadata struct {
	Type        model.FileType
	Size        int64
	Mode        uint32
	Uid         uint32
	Gid         uint32
	ModTimeNs   int64
	IsSpecial   bool
	RawFileMode os.FileMode
}

// IsSpecialFile returns true if mode represents a device node, FIFO, socket, or other non-syncable file type.
func IsSpecialFile(mode os.FileMode) bool {
	if mode&SpecialModeMask != 0 {
		return true
	}
	if mode&os.ModeSymlink != 0 || mode.IsDir() || mode.IsRegular() {
		return false
	}
	return true
}

// ExtractMetadata extracts file type, permissions, ownership (uid/gid), size, and nanosecond mtime
// from an os.FileInfo obtained via os.Lstat.
func ExtractMetadata(info os.FileInfo) StatMetadata {
	mode := info.Mode()
	meta := StatMetadata{
		Mode:        uint32(mode & PermissionMask),
		ModTimeNs:   info.ModTime().UnixNano(),
		RawFileMode: mode,
	}

	if st, ok := info.Sys().(*syscall.Stat_t); ok && st != nil {
		meta.Uid = st.Uid
		meta.Gid = st.Gid
	}

	switch {
	case IsSpecialFile(mode):
		meta.IsSpecial = true
	case mode&os.ModeSymlink != 0:
		meta.Type = model.FileTypeSymlink
		meta.Size = info.Size()
	case mode.IsDir():
		meta.Type = model.FileTypeDir
		meta.Size = 0
	case mode.IsRegular():
		meta.Type = model.FileTypeRegular
		meta.Size = info.Size()
	default:
		meta.IsSpecial = true
	}

	return meta
}

// LstatMetadata performs os.Lstat on absPath and extracts normalized StatMetadata.
func LstatMetadata(absPath string) (StatMetadata, error) {
	info, err := os.Lstat(absPath)
	if err != nil {
		return StatMetadata{}, err
	}
	return ExtractMetadata(info), nil
}

// ApplyMetadata applies ownership (uid/gid), permissions (mode), and modification time (nanoseconds)
// to absPath without following symlinks.
// If os.Lchown fails with EPERM or EACCES (e.g., running as non-root), it logs once and does not fail.
func ApplyMetadata(absPath string, fi model.FileInfo, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	if err := os.Lchown(absPath, int(fi.Uid), int(fi.Gid)); err != nil {
		if isPermissionError(err) {
			if chownWarnedOnce.CompareAndSwap(false, true) {
				logger.Warn("unable to lchown path (insufficient privileges); continuing without ownership sync",
					"path", absPath,
					"uid", fi.Uid,
					"gid", fi.Gid,
					"err", err,
				)
			} else {
				logger.Debug("lchown skipped due to insufficient privileges",
					"path", absPath,
					"uid", fi.Uid,
					"gid", fi.Gid,
				)
			}
		} else {
			return fmt.Errorf("lchown %q: %w", absPath, err)
		}
	}

	if fi.Type == model.FileTypeSymlink {
		if err := lutimesNano(absPath, fi.ModTimeNs); err != nil {
			if !isUnsupportedOrPermError(err) {
				return fmt.Errorf("lutimes %q: %w", absPath, err)
			}
			logger.Debug("lutimes skipped on symlink", "path", absPath, "err", err)
		}
		return nil
	}

	// Regular file or Directory: apply mode and nanosecond mtime.
	permMode := os.FileMode(fi.Mode) & PermissionMask
	if permMode == 0 {
		if fi.Type == model.FileTypeDir {
			permMode = 0o755
		} else {
			permMode = 0o644
		}
	}
	if err := os.Chmod(absPath, permMode); err != nil {
		return fmt.Errorf("chmod %q: %w", absPath, err)
	}

	mtime := time.Unix(0, fi.ModTimeNs)
	if err := os.Chtimes(absPath, mtime, mtime); err != nil {
		return fmt.Errorf("chtimes %q: %w", absPath, err)
	}

	return nil
}

// lutimesNano sets access and modification times on a symlink itself (AT_SYMLINK_NOFOLLOW)
// with nanosecond precision using Linux utimensat.
func lutimesNano(absPath string, modTimeNs int64) error {
	pathPtr, err := syscall.BytePtrFromString(absPath)
	if err != nil {
		return err
	}
	ts := syscall.NsecToTimespec(modTimeNs)
	times := [2]syscall.Timespec{ts, ts}

	fdcwd := ^uintptr(99) // AT_FDCWD (-100)
	_, _, errno := syscall.Syscall6(
		syscall.SYS_UTIMENSAT,
		fdcwd,
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&times[0])),
		uintptr(atSymlinkNoFollow),
		0,
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func isPermissionError(err error) bool {
	return errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EACCES) ||
		errors.Is(err, os.ErrPermission)
}

func isUnsupportedOrPermError(err error) bool {
	return isPermissionError(err) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, syscall.ENOSYS)
}
