package model

// FileType represents the kind of filesystem entry tracked by mirror.
type FileType uint8

const (
	FileTypeRegular FileType = iota
	FileTypeDir
	FileTypeSymlink
)

func (t FileType) String() string {
	switch t {
	case FileTypeRegular:
		return "regular"
	case FileTypeDir:
		return "dir"
	case FileTypeSymlink:
		return "symlink"
	default:
		return "unknown"
	}
}

// BlockInfo describes a single content-defined chunk of a regular file.
type BlockInfo struct {
	Index  int      `json:"index"`
	Offset int64    `json:"offset"`
	Size   int32    `json:"size"`
	Hash   [32]byte `json:"hash"`
}

// FileInfo holds the complete synchronized metadata and block list for a path.
type FileInfo struct {
	Path          string        `json:"path"`
	Type          FileType      `json:"type"`
	Size          int64         `json:"size"`
	Mode          uint32        `json:"mode"`
	Uid           uint32        `json:"uid"`
	Gid           uint32        `json:"gid"`
	ModTimeNs     int64         `json:"mod_time_ns"`
	SymlinkTarget string        `json:"symlink_target,omitempty"`
	Deleted       bool          `json:"deleted"`
	Version       VersionVector `json:"version"`
	ModifiedBy    string        `json:"modified_by"`
	FileHash      [32]byte      `json:"file_hash"`
	Blocks        []BlockInfo   `json:"blocks,omitempty"`
}

// Clone returns a deep copy of FileInfo including its VersionVector and Blocks slice.
func (f FileInfo) Clone() FileInfo {
	out := f
	out.Version = f.Version.Clone()
	if len(f.Blocks) > 0 {
		out.Blocks = make([]BlockInfo, len(f.Blocks))
		copy(out.Blocks, f.Blocks)
	} else {
		out.Blocks = nil
	}
	return out
}
