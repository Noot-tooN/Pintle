package pintle

import (
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"
)

const maxAuthorityTime uint64 = 1<<53 - 1

func identifier(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}
func validResource(r Resource) bool {
	if !identifier(r.Namespace, 64) || len(r.Key) == 0 || len(r.Key) > 128 || !utf8.ValidString(r.Key) {
		return false
	}
	for _, c := range r.Key {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}
func decimal(s string, positive bool) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != s || positive && n == 0 {
		return 0, fmt.Errorf("invalid canonical counter")
	}
	return n, nil
}
