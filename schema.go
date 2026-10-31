package pats

import (
	"fmt"
	"regexp"
)

// AdvisoryLockClass is the first key of every advisory lock pats takes
// (pg_advisory_xact_lock(AdvisoryLockClass, ...)). It is "pats" in ASCII.
// Avoid it as a lock class in your own code.
const AdvisoryLockClass = 0x70617473

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ValidateSchema reports whether schema is usable as a pats schema name: a
// lowercase PostgreSQL identifier. The name is interpolated into SQL, so
// anything else is rejected.
func ValidateSchema(schema string) error {
	if !identRe.MatchString(schema) {
		return fmt.Errorf("pats: invalid schema name %q (want lowercase identifier)", schema)
	}
	return nil
}
