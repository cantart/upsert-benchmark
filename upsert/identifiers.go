package upsert

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

// quoteIdentifier quotes a SQL identifier, ensuring internal quotes are escaped.
func quoteIdentifier(name string) (string, error) {
	if !isSafeIdentifier(name) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\"")), nil
}

// isSafeIdentifier reports whether the identifier meets simple SQL safety rules.
func isSafeIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r == '_' {
			continue
		}
		if unicode.IsLetter(r) {
			continue
		}
		if unicode.IsDigit(r) {
			if i == 0 {
				return false
			}
			continue
		}
		return false
	}
	return true
}

// deriveIndexName builds a safe deterministic name for indexes over the given table and keys.
func deriveIndexName(table string, uniqueKeys []string, suffix string) string {
	h := sha1.New()
	keys := append([]string(nil), uniqueKeys...)
	sort.Strings(keys)

	writePart := func(part string) {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{'|'})
	}

	writePart(strings.ToLower(table))
	for _, key := range keys {
		writePart(strings.ToLower(key))
	}
	writePart(strings.ToLower(suffix))

	digest := fmt.Sprintf("%x", h.Sum(nil))
	return fmt.Sprintf("idx_%s", digest[:16])
}
