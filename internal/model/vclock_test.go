package model

import (
	"reflect"
	"testing"
)

func TestVersionVectorOperations(t *testing.T) {
	v1 := NewVersionVector()
	if len(v1) != 0 {
		t.Fatalf("expected empty vector, got %v", v1)
	}

	v2 := v1.Increment("node-a")
	if v1["node-a"] != 0 {
		t.Fatalf("Increment mutated original vector: %v", v1)
	}
	if v2["node-a"] != 1 {
		t.Fatalf("expected node-a=1, got %v", v2)
	}

	v3 := v2.Increment("node-a").Increment("node-b")
	if v3["node-a"] != 2 || v3["node-b"] != 1 {
		t.Fatalf("unexpected counts in v3: %v", v3)
	}

	v4 := NewVersionVector().Increment("node-b").Increment("node-b").Increment("node-c")
	merged := v3.Merge(v4)
	expected := VersionVector{
		"node-a": 2,
		"node-b": 2,
		"node-c": 1,
	}
	if !reflect.DeepEqual(merged, expected) {
		t.Fatalf("Merge mismatch: got %v, want %v", merged, expected)
	}
}

func TestVersionVectorCompare(t *testing.T) {
	tests := []struct {
		name string
		a    VersionVector
		b    VersionVector
		want Comparison
	}{
		{
			name: "both empty",
			a:    VersionVector{},
			b:    VersionVector{},
			want: CmpEqual,
		},
		{
			name: "nil and zero entry",
			a:    nil,
			b:    VersionVector{"node-a": 0},
			want: CmpEqual,
		},
		{
			name: "identical non-empty",
			a:    VersionVector{"node-a": 2, "node-b": 1},
			b:    VersionVector{"node-a": 2, "node-b": 1},
			want: CmpEqual,
		},
		{
			name: "a dominates b (same keys)",
			a:    VersionVector{"node-a": 3, "node-b": 1},
			b:    VersionVector{"node-a": 2, "node-b": 1},
			want: CmpDominates,
		},
		{
			name: "a dominates b (extra key in a)",
			a:    VersionVector{"node-a": 1, "node-b": 1},
			b:    VersionVector{"node-a": 1},
			want: CmpDominates,
		},
		{
			name: "a dominated by b (same keys)",
			a:    VersionVector{"node-a": 1, "node-b": 1},
			b:    VersionVector{"node-a": 2, "node-b": 1},
			want: CmpDominated,
		},
		{
			name: "a dominated by b (extra key in b)",
			a:    VersionVector{"node-a": 1},
			b:    VersionVector{"node-a": 1, "node-b": 2},
			want: CmpDominated,
		},
		{
			name: "concurrent disjoint keys",
			a:    VersionVector{"node-a": 1},
			b:    VersionVector{"node-b": 1},
			want: CmpConcurrent,
		},
		{
			name: "concurrent overlapping keys",
			a:    VersionVector{"node-a": 2, "node-b": 1},
			b:    VersionVector{"node-a": 1, "node-b": 2},
			want: CmpConcurrent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.a.Compare(tc.b)
			if got != tc.want {
				t.Fatalf("a.Compare(b) = %v, want %v (a=%v, b=%v)", got, tc.want, tc.a, tc.b)
			}
		})
	}
}

func TestResolveConflictCausalOrdering(t *testing.T) {
	base := FileInfo{
		Path:       "docs/readme.txt",
		Type:       FileTypeRegular,
		Size:       10,
		Mode:       0o644,
		ModTimeNs:  1000,
		Version:    VersionVector{"node-a": 1},
		ModifiedBy: "node-a",
		FileHash:   [32]byte{1},
	}
	newer := base.Clone()
	newer.Size = 20
	newer.ModTimeNs = 900 // Even if clock is behind, causal dominance wins!
	newer.Version = VersionVector{"node-a": 1, "node-b": 1}
	newer.ModifiedBy = "node-b"
	newer.FileHash = [32]byte{2}

	winner, loser, isConflict := ResolveConflict(base, newer)
	if isConflict {
		t.Fatalf("expected no conflict when remote dominates local")
	}
	if winner.ModifiedBy != "node-b" || loser.ModifiedBy != "node-a" {
		t.Fatalf("unexpected winner/loser: winner=%v, loser=%v", winner.ModifiedBy, loser.ModifiedBy)
	}

	// Reversed arguments
	winner2, loser2, isConflict2 := ResolveConflict(newer, base)
	if isConflict2 {
		t.Fatalf("expected no conflict when local dominates remote")
	}
	if !reflect.DeepEqual(winner, winner2) || !reflect.DeepEqual(loser, loser2) {
		t.Fatalf("ResolveConflict not symmetric on causal dominance")
	}
}

func TestResolveConflictConcurrentIdenticalContent(t *testing.T) {
	a := FileInfo{
		Path:       "config.yaml",
		Type:       FileTypeRegular,
		Size:       42,
		Mode:       0o644,
		ModTimeNs:  1000,
		Version:    VersionVector{"node-a": 1},
		ModifiedBy: "node-a",
		FileHash:   [32]byte{9, 9, 9},
	}
	b := FileInfo{
		Path:       "config.yaml",
		Type:       FileTypeRegular,
		Size:       42,
		Mode:       0o644,
		ModTimeNs:  2000,
		Version:    VersionVector{"node-b": 1},
		ModifiedBy: "node-b",
		FileHash:   [32]byte{9, 9, 9},
	}

	w1, l1, conflict1 := ResolveConflict(a, b)
	w2, l2, conflict2 := ResolveConflict(b, a)

	if conflict1 || conflict2 {
		t.Fatalf("expected no conflict when concurrent edits produce identical content")
	}
	if !reflect.DeepEqual(w1, w2) || !reflect.DeepEqual(l1, l2) {
		t.Fatalf("symmetry failed: w1=%+v, w2=%+v", w1, w2)
	}
	if w1.ModTimeNs != 2000 {
		t.Fatalf("expected higher ModTimeNs 2000, got %d", w1.ModTimeNs)
	}
	expectedVer := VersionVector{"node-a": 1, "node-b": 1}
	if !reflect.DeepEqual(w1.Version, expectedVer) {
		t.Fatalf("expected merged version %v, got %v", expectedVer, w1.Version)
	}
}

func TestResolveConflictBothDeleted(t *testing.T) {
	a := FileInfo{
		Path:       "old.txt",
		Deleted:    true,
		ModTimeNs:  1000,
		Version:    VersionVector{"node-a": 2},
		ModifiedBy: "node-a",
	}
	b := FileInfo{
		Path:       "old.txt",
		Deleted:    true,
		ModTimeNs:  1500,
		Version:    VersionVector{"node-b": 3},
		ModifiedBy: "node-b",
	}

	w1, _, conflict1 := ResolveConflict(a, b)
	w2, _, conflict2 := ResolveConflict(b, a)
	if conflict1 || conflict2 {
		t.Fatalf("both deleted should not be flagged as a conflict")
	}
	if !reflect.DeepEqual(w1, w2) {
		t.Fatalf("both deleted resolution must be symmetric: %+v vs %+v", w1, w2)
	}
	if !w1.Deleted || w1.ModTimeNs != 1500 {
		t.Fatalf("unexpected winner state: %+v", w1)
	}
}

func TestResolveConflictLWWAndTieBreakingSymmetry(t *testing.T) {
	// Case 1: Different ModTimeNs
	a := FileInfo{
		Path:       "app.db",
		Type:       FileTypeRegular,
		Size:       100,
		Mode:       0o644,
		ModTimeNs:  1000,
		Version:    VersionVector{"node-a": 1},
		ModifiedBy: "node-a",
		FileHash:   [32]byte{1},
	}
	b := FileInfo{
		Path:       "app.db",
		Type:       FileTypeRegular,
		Size:       200,
		Mode:       0o644,
		ModTimeNs:  2000,
		Version:    VersionVector{"node-b": 1},
		ModifiedBy: "node-b",
		FileHash:   [32]byte{2},
	}

	wAB, lAB, cAB := ResolveConflict(a, b)
	wBA, lBA, cBA := ResolveConflict(b, a)
	if !cAB || !cBA {
		t.Fatalf("expected true conflict for divergent concurrent versions")
	}
	if !reflect.DeepEqual(wAB, wBA) || !reflect.DeepEqual(lAB, lBA) {
		t.Fatalf("asymmetric conflict resolution: (a,b)->%+v vs (b,a)->%+v", wAB, wBA)
	}
	if wAB.ModifiedBy != "node-b" || wAB.Size != 200 {
		t.Fatalf("expected node-b to win by higher ModTimeNs, got %+v", wAB)
	}
	if !reflect.DeepEqual(wAB.Version, VersionVector{"node-a": 1, "node-b": 1}) {
		t.Fatalf("winner version vector was not merged: %v", wAB.Version)
	}

	// Case 2: Equal ModTimeNs, Deleted vs Non-Deleted (non-deleted wins)
	del := a.Clone()
	del.Deleted = true
	del.ModTimeNs = 5000
	live := b.Clone()
	live.Deleted = false
	live.ModTimeNs = 5000

	wDel1, lDel1, cDel1 := ResolveConflict(del, live)
	wDel2, lDel2, cDel2 := ResolveConflict(live, del)
	if !cDel1 || !cDel2 {
		t.Fatalf("expected conflict between concurrent delete and modification")
	}
	if !reflect.DeepEqual(wDel1, wDel2) || !reflect.DeepEqual(lDel1, lDel2) {
		t.Fatalf("asymmetric delete vs live tie-break")
	}
	if wDel1.Deleted {
		t.Fatalf("expected non-deleted file to win tie-break when ModTimeNs are equal")
	}

	// Case 3: Equal ModTimeNs, tie-break by FileHash
	h1 := a.Clone()
	h1.ModTimeNs = 3000
	h1.FileHash = [32]byte{0x10}
	h2 := b.Clone()
	h2.ModTimeNs = 3000
	h2.FileHash = [32]byte{0x20}

	wH1, lH1, _ := ResolveConflict(h1, h2)
	wH2, lH2, _ := ResolveConflict(h2, h1)
	if !reflect.DeepEqual(wH1, wH2) || !reflect.DeepEqual(lH1, lH2) {
		t.Fatalf("asymmetric hash tie-break")
	}
	if wH1.FileHash != h2.FileHash {
		t.Fatalf("expected higher hash to win tie-break")
	}

	// Case 4: Equal ModTimeNs and Mode diff only, tie-break by Mode/ModifiedBy
	m1 := a.Clone()
	m1.ModTimeNs = 4000
	m1.FileHash = [32]byte{0x55}
	m1.Mode = 0o600
	m2 := b.Clone()
	m2.ModTimeNs = 4000
	m2.FileHash = [32]byte{0x55}
	m2.Size = m1.Size
	m2.Mode = 0o755

	wM1, lM1, cM1 := ResolveConflict(m1, m2)
	wM2, lM2, cM2 := ResolveConflict(m2, m1)
	if !cM1 || !cM2 {
		t.Fatalf("expected conflict when Mode differs concurrently")
	}
	if !reflect.DeepEqual(wM1, wM2) || !reflect.DeepEqual(lM1, lM2) {
		t.Fatalf("asymmetric mode tie-break")
	}
	if wM1.Mode != 0o755 {
		t.Fatalf("expected 0755 to win tie-break over 0600")
	}
}
