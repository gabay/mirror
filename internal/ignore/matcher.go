package ignore

import (
	"path"
	"strings"
)

type rule struct {
	pattern  string
	anchored bool // starts with '/' -> only matches relative to sync root
	dirOnly  bool // ends with '/' -> only matches directories (and their descendants)
	hasSlash bool // contains '/' -> matched against path rather than single basename
}

// Matcher evaluates file and directory relative paths against exclusion rules.
type Matcher struct {
	rules []rule
}

// NewMatcher compiles a list of glob exclusion patterns alongside built-in internal patterns.
// Built-in patterns always exclude internal staging files (`.mirror.tmp.*`) and state files (`.mirror.db*`).
func NewMatcher(patterns []string) *Matcher {
	all := make([]string, 0, len(patterns)+2)
	all = append(all, ".mirror.tmp.*", ".mirror.db*")
	all = append(all, patterns...)

	var rules []rule
	for _, raw := range all {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		// Normalize Windows-style backslashes if any.
		p = strings.ReplaceAll(p, "\\", "/")

		var r rule
		if strings.HasPrefix(p, "/") {
			r.anchored = true
			p = strings.TrimLeft(p, "/")
		}
		if strings.HasSuffix(p, "/") {
			r.dirOnly = true
			p = strings.TrimRight(p, "/")
		}
		if p == "" {
			continue
		}
		r.pattern = path.Clean(p)
		r.hasSlash = strings.Contains(r.pattern, "/")
		rules = append(rules, r)
	}
	return &Matcher{rules: rules}
}

// Match returns true if relPath (or any of its parent directories) matches an exclusion rule.
// relPath should be a path relative to the synchronized root directory.
func (m *Matcher) Match(relPath string, isDir bool) bool {
	if m == nil {
		return false
	}

	normalized := strings.ReplaceAll(strings.TrimSpace(relPath), "\\", "/")
	normalized = strings.TrimPrefix(normalized, "./")
	normalized = strings.Trim(normalized, "/")
	if normalized == "" || normalized == "." {
		return false
	}

	cleaned := path.Clean(normalized)
	if cleaned == "." || cleaned == "" {
		return false
	}

	segments := strings.Split(cleaned, "/")
	// Check each prefix from top-level parent down to the full path itself.
	// Every intermediate prefix is a parent directory (segIsDir = true).
	for i := range segments {
		subPath := strings.Join(segments[:i+1], "/")
		segName := segments[i]
		segIsDir := true
		if i == len(segments)-1 {
			segIsDir = isDir
		}
		if m.matchSingle(subPath, segName, segIsDir) {
			return true
		}
	}

	return false
}

func (m *Matcher) matchSingle(subPath, baseName string, isDir bool) bool {
	for _, r := range m.rules {
		if r.dirOnly && !isDir {
			continue
		}

		if !r.hasSlash {
			if r.anchored {
				// Anchored single-segment pattern (e.g., `/root-only`) only matches at root level.
				if !strings.Contains(subPath, "/") && matchGlob(r.pattern, subPath) {
					return true
				}
			} else {
				// Unanchored single-segment pattern (e.g., `*.tmp`, `.git`, `node_modules`)
				// matches any path segment name.
				if matchGlob(r.pattern, baseName) {
					return true
				}
			}
			continue
		}

		// Pattern contains '/': match against subPath (or any suffix of subPath if not anchored).
		if r.anchored {
			if matchPathSegments(strings.Split(r.pattern, "/"), strings.Split(subPath, "/")) {
				return true
			}
		} else {
			patParts := strings.Split(r.pattern, "/")
			pathParts := strings.Split(subPath, "/")
			for start := 0; start < len(pathParts); start++ {
				if matchPathSegments(patParts, pathParts[start:]) {
					return true
				}
			}
		}
	}
	return false
}

// matchPathSegments matches a slash-split glob pattern (supporting `**`) against slash-split path segments.
func matchPathSegments(patParts, pathParts []string) bool {
	if len(patParts) == 0 {
		return len(pathParts) == 0
	}

	if patParts[0] == "**" {
		// Collapse consecutive `**`
		for len(patParts) > 1 && patParts[1] == "**" {
			patParts = patParts[1:]
		}
		// Trailing `**` matches 0 or more remaining segments (e.g. `cache/**` matches `cache/a` and `cache/a/b`).
		// Note: `cache/**` requires at least 1 segment under `cache` unless `cache` itself is matched;
		// wait: if pattern is `cache/**`, `patParts` at `**` has `len(patParts) == 1`.
		// Matching 1 or more segments for trailing `**` ensures `cache/**` matches everything inside `cache/`,
		// or 0 or more segments when `**` is in the middle (`a/**/b` matches `a/b` and `a/x/y/b`).
		if len(patParts) == 1 {
			return len(pathParts) >= 1
		}
		for i := 0; i <= len(pathParts); i++ {
			if matchPathSegments(patParts[1:], pathParts[i:]) {
				return true
			}
		}
		return false
	}

	if len(pathParts) == 0 {
		return false
	}

	if !matchGlob(patParts[0], pathParts[0]) {
		return false
	}

	return matchPathSegments(patParts[1:], pathParts[1:])
}

func matchGlob(pattern, name string) bool {
	matched, err := path.Match(pattern, name)
	return err == nil && matched
}
