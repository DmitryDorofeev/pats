package pats

import (
	"fmt"
	"strings"
)

// ValidateSubject reports whether s is a valid publish subject: dot-separated,
// non-empty tokens without whitespace or wildcards.
func ValidateSubject(s string) error {
	return validate(s, false)
}

// ValidatePattern reports whether p is a valid subscription pattern. A token
// may be "*" (matches exactly one token), and the last token may be ">"
// (matches one or more remaining tokens).
func ValidatePattern(p string) error {
	return validate(p, true)
}

func validate(s string, wildcards bool) error {
	if s == "" {
		return fmt.Errorf("%w: empty", ErrBadSubject)
	}
	tokens := strings.Split(s, ".")
	for i, t := range tokens {
		if t == "" {
			return fmt.Errorf("%w: %q has an empty token", ErrBadSubject, s)
		}
		if strings.ContainsAny(t, " \t\r\n") {
			return fmt.Errorf("%w: %q contains whitespace", ErrBadSubject, s)
		}
		if t == "*" || t == ">" {
			if !wildcards {
				return fmt.Errorf("%w: %q contains a wildcard", ErrBadSubject, s)
			}
			if t == ">" && i != len(tokens)-1 {
				return fmt.Errorf("%w: %q has '>' before the last token", ErrBadSubject, s)
			}
			continue
		}
		if strings.ContainsAny(t, "*>") {
			return fmt.Errorf("%w: %q has a wildcard inside a token", ErrBadSubject, s)
		}
	}
	return nil
}

// patternColumns returns the consumers columns that publish matches pattern
// against: whether it has wildcards, its tokens without a trailing ">"
// (dot-joined), and whether it ends in ">".
func patternColumns(pattern string) (wild bool, tokens string, tail bool) {
	tokens, tail = strings.CutSuffix(pattern, ">")
	if tail {
		tokens = strings.TrimSuffix(tokens, ".")
	}
	wild = tail || strings.Contains(pattern, "*")
	return wild, tokens, tail
}

// SubjectMatches reports whether subject matches pattern. Publish applies the
// same rule in SQL to the columns from patternColumns.
func SubjectMatches(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, pt := range p {
		if pt == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if pt != "*" && pt != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}
