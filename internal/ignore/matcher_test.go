package ignore

import "testing"

func TestMatcherBuiltInRules(t *testing.T) {
	m := NewMatcher(nil)

	tests := []struct {
		path  string
		isDir bool
		want  bool
	}{
		{path: ".mirror.tmp.file.txt.1234", isDir: false, want: true},
		{path: "sub/dir/.mirror.tmp.app.db.9999", isDir: false, want: true},
		{path: ".mirror.db", isDir: false, want: true},
		{path: ".mirror.db-lock", isDir: false, want: true},
		{path: "sub/.mirror.db", isDir: false, want: true},
		{path: "normal.txt", isDir: false, want: false},
		{path: "sub/normal.txt", isDir: false, want: false},
		{path: "", isDir: false, want: false},
		{path: ".", isDir: true, want: false},
	}

	for _, tc := range tests {
		if got := m.Match(tc.path, tc.isDir); got != tc.want {
			t.Errorf("Match(%q, %v) = %v, want %v", tc.path, tc.isDir, got, tc.want)
		}
	}
}

func TestMatcherCustomPatterns(t *testing.T) {
	m := NewMatcher([]string{
		"*.tmp",
		".git",
		"node_modules",
		"cache/**",
		"logs/*.log",
		"/root-only",
		"build/",
		"a/**/z/*.bak",
	})

	tests := []struct {
		path  string
		isDir bool
		want  bool
	}{
		// File glob anywhere
		{path: "foo.tmp", isDir: false, want: true},
		{path: "deep/nested/bar.tmp", isDir: false, want: true},
		{path: "deep/nested/bar.txt", isDir: false, want: false},

		// Directory/file name anywhere (.git, node_modules)
		{path: ".git", isDir: true, want: true},
		{path: ".git/config", isDir: false, want: true},
		{path: "sub/.git/HEAD", isDir: false, want: true},
		{path: "node_modules", isDir: true, want: true},
		{path: "web/node_modules/pkg/index.js", isDir: false, want: true},

		// Double-star glob `cache/**`
		{path: "cache/item.bin", isDir: false, want: true},
		{path: "cache/a/b/c.bin", isDir: false, want: true},

		// Multi-segment glob `logs/*.log`
		{path: "logs/app.log", isDir: false, want: true},
		{path: "service/logs/error.log", isDir: false, want: true},
		{path: "logs/app.txt", isDir: false, want: false},

		// Root-anchored `/root-only`
		{path: "root-only", isDir: false, want: true},
		{path: "root-only/child.txt", isDir: false, want: true},
		{path: "sub/root-only", isDir: false, want: false},

		// Directory-only `build/`
		{path: "build", isDir: true, want: true},
		{path: "build/output.bin", isDir: false, want: true},
		{path: "sub/build", isDir: true, want: true},
		{path: "build", isDir: false, want: false}, // regular file named "build" is not matched by "build/"

		// Middle `**` (`a/**/z/*.bak`)
		{path: "a/z/file.bak", isDir: false, want: true},
		{path: "a/b/c/z/file.bak", isDir: false, want: true},
		{path: "a/b/c/z/file.txt", isDir: false, want: false},
	}

	for _, tc := range tests {
		if got := m.Match(tc.path, tc.isDir); got != tc.want {
			t.Errorf("Match(%q, isDir=%v) = %v, want %v", tc.path, tc.isDir, got, tc.want)
		}
	}
}

func BenchmarkMatcher(b *testing.B) {
	m := NewMatcher([]string{
		"*.tmp",
		".git",
		"node_modules",
		"cache/**",
		"logs/*.log",
		"/root-only",
		"build/",
		"a/**/z/*.bak",
	})
	paths := []string{
		"config/app.yaml",
		"deep/nested/dir/sqlite.db",
		"web/node_modules/pkg/index.js",
		"cache/a/b/c.bin",
		"service/logs/error.log",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Match(paths[i%len(paths)], false)
	}
}
