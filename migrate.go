package pats

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
)

//go:embed sql/*.sql
var migrationsFS embed.FS

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func validateSchema(schema string) error {
	if !identRe.MatchString(schema) {
		return fmt.Errorf("pats: invalid schema name %q (want lowercase identifier)", schema)
	}
	return nil
}

// SchemaSQL returns the full DDL for the given schema, for callers that apply
// migrations with their own tooling (combine with WithoutMigrate).
func SchemaSQL(schema string) (string, error) {
	if err := validateSchema(schema); err != nil {
		return "", err
	}
	files, err := migrationFiles()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, f := range files {
		src, err := renderMigration(f, schema)
		if err != nil {
			return "", err
		}
		b.WriteString(src)
		b.WriteString("\n")
	}
	return b.String(), nil
}

// Migrate installs or upgrades the pats schema. It is safe to run
// concurrently from several processes. Connect calls it unless
// WithoutMigrate is given.
func Migrate(ctx context.Context, q Querier, schema string) error {
	if err := validateSchema(schema); err != nil {
		return err
	}
	files, err := migrationFiles()
	if err != nil {
		return err
	}
	for _, f := range files {
		src, err := renderMigration(f, schema)
		if err != nil {
			return err
		}
		// A multi-statement script without arguments runs as one implicit
		// transaction, so the xact lock serialises concurrent migrators.
		script := fmt.Sprintf("SELECT pg_advisory_xact_lock(hashtext('pats.migrate.%s'));\n%s", schema, src)
		if err := q.Exec(ctx, script); err != nil {
			return fmt.Errorf("pats: migration %s: %w", f, err)
		}
	}
	return nil
}

func migrationFiles() ([]string, error) {
	files, err := fs.Glob(migrationsFS, "sql/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func renderMigration(name, schema string) (string, error) {
	b, err := migrationsFS.ReadFile(name)
	if err != nil {
		return "", err
	}
	return strings.NewReplacer("{{schema}}", schema, "{{channel}}", schema).Replace(string(b)), nil
}
