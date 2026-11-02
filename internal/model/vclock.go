package model

import (
	"bytes"
)

// Comparison represents the causal relationship between two VersionVectors.
type Comparison int

const (
	// CmpEqual indicates both version vectors are identical.
	CmpEqual Comparison = iota
	// CmpDominates indicates the receiver strictly dominates the other vector (receiver > other).
	CmpDominates
	// CmpDominated indicates the receiver is strictly dominated by the other vector (receiver < other).
	CmpDominated
	// CmpConcurrent indicates neither vector dominates the other (receiver || other).
	CmpConcurrent
)

func (c Comparison) String() string {
	switch c {
	case CmpEqual:
		return "Equal"
	case CmpDominates:
		return "Dominates"
	case CmpDominated:
		return "Dominated"
	case CmpConcurrent:
		return "Concurrent"
	default:
		return "Unknown"
	}
}

// VersionVector tracks causal history across nodes in the cluster.
type VersionVector map[string]uint64

// NewVersionVector returns an empty initialized VersionVector.
func NewVersionVector() VersionVector {
	return make(VersionVector)
}

// Clone returns a deep copy of the VersionVector, omitting zero-valued entries.
func (v VersionVector) Clone() VersionVector {
	if len(v) == 0 {
		return make(VersionVector)
	}
	out := make(VersionVector, len(v))
	for k, val := range v {
		if val > 0 {
			out[k] = val
		}
	}
	return out
}

// Increment returns a new VersionVector with the counter for nodeID incremented by 1.
func (v VersionVector) Increment(nodeID string) VersionVector {
	out := v.Clone()
	out[nodeID] = out[nodeID] + 1
	return out
}

// Merge returns a new VersionVector taking the element-wise maximum of v and other.
func (v VersionVector) Merge(other VersionVector) VersionVector {
	out := v.Clone()
	for k, val := range other {
		if val > out[k] {
			out[k] = val
		}
	}
	return out
}

// Compare determines the causal ordering between v and other.
func (v VersionVector) Compare(other VersionVector) Comparison {
	var hasGreater, hasLess bool

	for k, vVal := range v {
		oVal := other[k]
		if vVal > oVal {
			hasGreater = true
		} else if vVal < oVal {
			hasLess = true
		}
		if hasGreater && hasLess {
			return CmpConcurrent
		}
	}

	for k, oVal := range other {
		if _, exists := v[k]; exists {
			continue
		}
		if oVal > 0 {
			hasLess = true
			if hasGreater {
				return CmpConcurrent
			}
		}
	}

	switch {
	case hasGreater && !hasLess:
		return CmpDominates
	case !hasGreater && hasLess:
		return CmpDominated
	case !hasGreater && !hasLess:
		return CmpEqual
	default:
		return CmpConcurrent
	}
}

// ResolveConflict compares local and remote FileInfo versions and resolves any conflict
// deterministically so that ResolveConflict(a, b) and ResolveConflict(b, a) always pick
// the exact same winner and produce the exact same merged VersionVector.
func ResolveConflict(local, remote FileInfo) (winner FileInfo, loser FileInfo, isConflict bool) {
	cmp := local.Version.Compare(remote.Version)
	switch cmp {
	case CmpDominates:
		return local.Clone(), remote.Clone(), false
	case CmpDominated:
		return remote.Clone(), local.Clone(), false
	case CmpEqual:
		// Even when versions are equal, pick deterministically if metadata differs slightly.
		if compareDeterministic(local, remote) >= 0 {
			return local.Clone(), remote.Clone(), false
		}
		return remote.Clone(), local.Clone(), false
	case CmpConcurrent:
		mergedVersion := local.Version.Merge(remote.Version)

		// Check if both states represent the exact same logical file state
		// (either both deleted, or same type, hash, symlink target, and mode).
		if sameLogicalState(local, remote) {
			if compareDeterministic(local, remote) >= 0 {
				w := local.Clone()
				l := remote.Clone()
				w.Version = mergedVersion
				return w, l, false
			}
			w := remote.Clone()
			l := local.Clone()
			w.Version = mergedVersion
			return w, l, false
		}

		// True concurrent conflict: pick deterministic LWW winner.
		if compareDeterministic(local, remote) >= 0 {
			w := local.Clone()
			l := remote.Clone()
			w.Version = mergedVersion
			return w, l, true
		}
		w := remote.Clone()
		l := local.Clone()
		w.Version = mergedVersion
		return w, l, true
	default:
		return local.Clone(), remote.Clone(), false
	}
}

// sameLogicalState returns true if two concurrent FileInfo entries need no conflict warning
// because they converged to the same state independently.
func sameLogicalState(a, b FileInfo) bool {
	if a.Deleted && b.Deleted {
		return true
	}
	if a.Deleted != b.Deleted {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if a.Mode != b.Mode {
		return false
	}
	switch a.Type {
	case FileTypeRegular:
		return a.Size == b.Size && a.FileHash == b.FileHash
	case FileTypeSymlink:
		return a.SymlinkTarget == b.SymlinkTarget
	case FileTypeDir:
		return true
	default:
		return a.FileHash == b.FileHash && a.SymlinkTarget == b.SymlinkTarget
	}
}

// compareDeterministic returns >0 if a wins over b, <0 if b wins over a, and 0 if identical.
// This ordering is strictly symmetric and independent of argument order.
func compareDeterministic(a, b FileInfo) int {
	// 1. Higher ModTimeNs wins (Last-Write-Wins).
	if a.ModTimeNs != b.ModTimeNs {
		if a.ModTimeNs > b.ModTimeNs {
			return 1
		}
		return -1
	}

	// 2. Non-deleted wins over deleted when timestamps are identical.
	if a.Deleted != b.Deleted {
		if !a.Deleted && b.Deleted {
			return 1
		}
		return -1
	}

	// 3. Compare FileType.
	if a.Type != b.Type {
		if a.Type > b.Type {
			return 1
		}
		return -1
	}

	// 4. Compare FileHash lexicographically.
	if cmp := bytes.Compare(a.FileHash[:], b.FileHash[:]); cmp != 0 {
		return cmp
	}

	// 5. Compare SymlinkTarget.
	if a.SymlinkTarget != b.SymlinkTarget {
		if a.SymlinkTarget > b.SymlinkTarget {
			return 1
		}
		return -1
	}

	// 6. Compare Size.
	if a.Size != b.Size {
		if a.Size > b.Size {
			return 1
		}
		return -1
	}

	// 7. Compare Mode.
	if a.Mode != b.Mode {
		if a.Mode > b.Mode {
			return 1
		}
		return -1
	}

	// 8. Compare ModifiedBy lexicographically.
	if a.ModifiedBy != b.ModifiedBy {
		if a.ModifiedBy > b.ModifiedBy {
			return 1
		}
		return -1
	}

	// 9. Compare Uid and Gid.
	if a.Uid != b.Uid {
		if a.Uid > b.Uid {
			return 1
		}
		return -1
	}
	if a.Gid != b.Gid {
		if a.Gid > b.Gid {
			return 1
		}
		return -1
	}

	return 0
}
