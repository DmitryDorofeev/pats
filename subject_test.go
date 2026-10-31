package pats

import "testing"

// matchCases is mirrored by the SQL function test in internal/testsuite.
var matchCases = []struct {
	pattern, subject string
	want             bool
}{
	{"a", "a", true},
	{"a", "b", false},
	{"a.b", "a.b", true},
	{"a.b", "a", false},
	{"a", "a.b", false},
	{"a.*", "a.b", true},
	{"a.*", "a", false},
	{"a.*", "a.b.c", false},
	{"*.b", "a.b", true},
	{"*", "a", true},
	{"*", "a.b", false},
	{"a.>", "a.b", true},
	{"a.>", "a.b.c", true},
	{"a.>", "a", false},
	{">", "a", true},
	{">", "a.b.c", true},
	{"a.*.>", "a.b.c", true},
	{"a.*.>", "a.b", false},
	{"a.*.c", "a.x.c", true},
	{"a.*.c", "a.x.d", false},
}

func TestSubjectMatches(t *testing.T) {
	for _, c := range matchCases {
		if got := SubjectMatches(c.pattern, c.subject); got != c.want {
			t.Errorf("SubjectMatches(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

func TestValidate(t *testing.T) {
	good := []string{"a", "a.b", "orders.created", "a-b.c_d"}
	for _, s := range good {
		if err := ValidateSubject(s); err != nil {
			t.Errorf("ValidateSubject(%q): %v", s, err)
		}
	}
	badSubj := []string{"", "a.", ".a", "a..b", "a.*", "a.>", "a b", "a*"}
	for _, s := range badSubj {
		if err := ValidateSubject(s); err == nil {
			t.Errorf("ValidateSubject(%q): expected error", s)
		}
	}
	goodPat := []string{"a.*", "a.>", ">", "*", "*.b.>"}
	for _, s := range goodPat {
		if err := ValidatePattern(s); err != nil {
			t.Errorf("ValidatePattern(%q): %v", s, err)
		}
	}
	badPat := []string{"a.>.b", "a*", "a.b>", ""}
	for _, s := range badPat {
		if err := ValidatePattern(s); err == nil {
			t.Errorf("ValidatePattern(%q): expected error", s)
		}
	}
}
