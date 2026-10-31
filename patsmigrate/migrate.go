// Package patsmigrate installs, upgrades and removes the pats schema.
//
// It is modelled on River's rivermigrate. Migrations are numbered up/down SQL
// pairs embedded in the binary. Applied versions are recorded in the
// pats_migration table, and each version is applied in its own transaction
// together with its version row. pats.Connect never migrates; run Migrate
// yourself (or the pats CLI) before connecting:
//
//	migrator, err := patsmigrate.New(pgxdriver.New(pool), nil)
//	if err != nil {
//		// handle error
//	}
//	res, err := migrator.Migrate(ctx, patsmigrate.DirectionUp, nil)
//
// To use another migration tool, take Migration.SQLUp and SQLDown from
// AllVersions.
package patsmigrate

import (
	"cmp"
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dmitrydorofeev/pats"
)

//go:embed migration/*/*.sql
var migrationFS embed.FS

const (
	// migrationLine is the only migration line pats has. The directory
	// layout matches River's in case more are ever needed.
	migrationLine = "main"

	// migrateVersionTableAdded is the version that creates pats_migration.
	// Below it there is no table to record versions in.
	migrateVersionTableAdded = 1

	defaultSchema = "pats"
)

// Migration is a bundled migration containing a version (e.g. 1, 2, 3), and
// SQL for up and down directions. The SQL still contains the {{schema}} and
// {{channel}} placeholders; MigrateVersion.SQL holds the rendered form.
type Migration struct {
	// Name is a human-friendly name for the migration derived from its
	// filename.
	Name string

	// SQLDown is the SQL for the migration's down direction.
	SQLDown string

	// SQLUp is the SQL for the migration's up direction.
	SQLUp string

	// Version is the integer version number of this migration.
	Version int
}

// Config contains configuration for Migrator.
type Config struct {
	// Logger is the structured logger to use. Defaults to slog.Default().
	Logger *slog.Logger

	// Schema is the target schema to migrate. Defaults to "pats". It must
	// match the schema given to pats.WithSchema.
	Schema string
}

// Migrator runs up or down migrations to establish the schema that pats
// needs.
type Migrator struct {
	logger     *slog.Logger
	migrations map[int]Migration
	q          pats.Querier
	schema     string
}

// New returns a migrator that runs statements with q, typically the same
// driver passed to pats.Connect (pgxdriver.New or sqldriver.New). The config
// parameter may be nil.
func New(q pats.Querier, config *Config) (*Migrator, error) {
	if config == nil {
		config = &Config{}
	}
	schema := cmp.Or(config.Schema, defaultSchema)
	if err := pats.ValidateSchema(schema); err != nil {
		return nil, err
	}
	logger := cmp.Or(config.Logger, slog.Default())

	migrations, err := migrationsFromFS(migrationFS, migrationLine)
	if err != nil {
		// A problem here is in pats's own embedded files.
		panic(err)
	}
	return &Migrator{
		logger:     logger,
		migrations: validateAndInit(migrations),
		q:          q,
		schema:     schema,
	}, nil
}

// MigrateOpts are options for a migrate operation.
type MigrateOpts struct {
	// DryRun computes and renders the migrations that would be applied
	// without executing them.
	DryRun bool

	// MaxSteps is the maximum number of migrations to apply either up or
	// down. When migrating up, migrates an unlimited number of steps by
	// default. When migrating down, migrates only a single step by default
	// (set TargetVersion to -1 to apply unlimited steps down). Set to -1 to
	// apply no migrations (for testing/checking purposes).
	MaxSteps int

	// TargetVersion is a specific migration version to migrate to. Up
	// migrations are applied including the target version; down migrations
	// are applied excluding it, leaving the schema at the target version.
	//
	// When migrating down, TargetVersion can be set to -1 to apply all down
	// migrations (the pats schema is removed completely).
	TargetVersion int
}

// MigrateResult is the result of a migrate operation.
type MigrateResult struct {
	// Direction is the direction that migration occurred (up or down).
	Direction Direction

	// Versions are migration versions that were added (for up migrations)
	// or removed (for down migrations) for this run.
	Versions []MigrateVersion
}

// MigrateVersion is the result for a single applied migration.
type MigrateVersion struct {
	// Duration is the amount of time it took to apply the migration.
	Duration time.Duration

	// Name is a human-friendly name for the migration derived from its
	// filename.
	Name string

	// SQL is the rendered SQL that was applied for the migration.
	SQL string

	// Version is the version of the migration applied.
	Version int
}

// Direction is the direction of a migration.
type Direction string

const (
	DirectionDown Direction = "down"
	DirectionUp   Direction = "up"
)

// AllVersions gets information on all known migration versions, in order.
func (m *Migrator) AllVersions() []Migration {
	migrations := slices.Collect(maps.Values(m.migrations))
	slices.SortFunc(migrations, func(a, b Migration) int { return a.Version - b.Version })
	return migrations
}

// GetVersion gets information about a specific migration version.
func (m *Migrator) GetVersion(version int) (Migration, error) {
	migration, ok := m.migrations[version]
	if !ok {
		available := slices.Sorted(maps.Keys(m.migrations))
		return Migration{}, fmt.Errorf("migration %d not found (available versions: %v)", version, available)
	}
	return migration, nil
}

// ExistingVersions gets the versions that have been applied to the
// database, ordered by version.
func (m *Migrator) ExistingVersions(ctx context.Context) ([]Migration, error) {
	versions, err := m.existingVersions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Migration, len(versions))
	for i, v := range versions {
		migration, ok := m.migrations[v]
		if !ok {
			return nil, fmt.Errorf("migration %d not found in migrator bundle", v)
		}
		out[i] = migration
	}
	return out, nil
}

// Migrate migrates the database in the given direction. The opts parameter
// may be nil.
//
// By default, applies all outstanding migrations when moving up, but for
// safety only one step when moving down. MigrateOpts.MaxSteps and
// MigrateOpts.TargetVersion change that; TargetVersion -1 with DirectionDown
// removes the pats schema completely.
//
// It is safe to run concurrently from several processes: each version is
// applied under an advisory lock, and a version another process applied
// first is skipped.
func (m *Migrator) Migrate(ctx context.Context, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	if opts == nil {
		opts = &MigrateOpts{}
	}
	existing, err := m.existingVersions(ctx)
	if err != nil {
		return nil, err
	}

	var targets []Migration
	switch direction {
	case DirectionUp:
		targets = m.AllVersions()
		targets = slices.DeleteFunc(targets, func(x Migration) bool { return slices.Contains(existing, x.Version) })
	case DirectionDown:
		targets = m.AllVersions()
		targets = slices.DeleteFunc(targets, func(x Migration) bool { return !slices.Contains(existing, x.Version) })
		slices.Reverse(targets)
	default:
		return nil, fmt.Errorf("patsmigrate: invalid direction %q", direction)
	}

	targets, err = m.selectSteps(direction, opts, targets)
	if err != nil {
		return nil, err
	}

	res := &MigrateResult{Direction: direction, Versions: make([]MigrateVersion, 0, len(targets))}
	if len(targets) == 0 {
		m.logger.InfoContext(ctx, "patsmigrate: no migrations to apply")
		return res, nil
	}

	for _, mig := range targets {
		sql := mig.SQLUp
		if direction == DirectionDown {
			sql = mig.SQLDown
		}
		sql = m.Render(sql)

		var duration time.Duration
		if !opts.DryRun {
			start := time.Now()
			applied, err := m.apply(ctx, direction, mig.Version, sql)
			if err != nil {
				return nil, err
			}
			if !applied {
				m.logger.InfoContext(ctx, "patsmigrate: migration already applied by another process",
					slog.String("direction", string(direction)), slog.Int("version", mig.Version))
				continue
			}
			duration = time.Since(start)
		}

		m.logger.InfoContext(ctx, "patsmigrate: applied migration",
			slog.String("direction", string(direction)),
			slog.Bool("dry_run", opts.DryRun),
			slog.Duration("duration", duration),
			slog.Int("version", mig.Version),
		)
		res.Versions = append(res.Versions, MigrateVersion{Duration: duration, Name: mig.Name, SQL: sql, Version: mig.Version})
	}
	return res, nil
}

// selectSteps trims targets (already sorted in the direction of travel)
// according to MaxSteps and TargetVersion.
func (m *Migrator) selectSteps(direction Direction, opts *MigrateOpts, targets []Migration) ([]Migration, error) {
	var maxSteps int
	switch {
	case opts.MaxSteps != 0:
		maxSteps = opts.MaxSteps
	case direction == DirectionDown && opts.TargetVersion == 0:
		maxSteps = 1
	}
	switch {
	case maxSteps < 0:
		targets = nil
	case maxSteps > 0:
		targets = targets[:min(maxSteps, len(targets))]
	}

	if opts.TargetVersion > 0 {
		if _, ok := m.migrations[opts.TargetVersion]; !ok {
			return nil, fmt.Errorf("version %d is not a valid pats migration version", opts.TargetVersion)
		}
		i := slices.IndexFunc(targets, func(x Migration) bool { return x.Version == opts.TargetVersion })
		switch {
		case i == -1 && direction == DirectionDown:
			return nil, fmt.Errorf("version %d is not in target list of valid migrations to apply", opts.TargetVersion)
		case i == -1:
			// Up to a version that's already applied: no-op.
			targets = nil
		case direction == DirectionUp:
			targets = targets[:i+1]
		default:
			// Down migrations stop above the target version.
			targets = targets[:i]
		}
	}
	return targets, nil
}

// apply runs one migration and records it. The statements are sent as a
// single argument-less script, which PostgreSQL runs as one implicit
// transaction under every supported driver, so the advisory lock, the DDL
// and the version row commit or roll back together.
//
// It reports false if the version turned out to be applied (or removed) by
// another process in the meantime.
func (m *Migrator) apply(ctx context.Context, direction Direction, version int, sql string) (bool, error) {
	var record string
	switch {
	case direction == DirectionUp:
		record = fmt.Sprintf("INSERT INTO %s.pats_migration (version) VALUES (%d);", m.schema, version)
	case version > migrateVersionTableAdded:
		// The DELETE must remove a row, otherwise another process already
		// downmigrated this version and the transaction is aborted.
		record = fmt.Sprintf(`DO $$ BEGIN
    DELETE FROM %s.pats_migration WHERE version = %d;
    IF NOT FOUND THEN RAISE EXCEPTION 'pats migration %d is not applied'; END IF;
END $$;`, m.schema, version, version)
	}
	script := fmt.Sprintf("SELECT pg_advisory_xact_lock(%d, hashtext('migrate.%s'));\n%s\n%s",
		pats.AdvisoryLockClass, m.schema, sql, record)

	execErr := m.q.Exec(ctx, script)
	if execErr == nil {
		return true, nil
	}
	// A concurrent migrator may have applied this version while we waited
	// for the lock, making our script fail. Check before reporting an error.
	existing, err := m.existingVersions(ctx)
	if err == nil {
		has := slices.Contains(existing, version)
		if (direction == DirectionUp && has) || (direction == DirectionDown && !has) {
			return false, nil
		}
	}
	return false, fmt.Errorf("patsmigrate: error applying version %03d [%s]: %w",
		version, strings.ToUpper(string(direction)), execErr)
}

// ValidateResult is the result of a validation operation.
type ValidateResult struct {
	// Messages contain informational messages of what wasn't valid in case
	// of a failed validation. Always empty if OK is true.
	Messages []string

	// OK is true if validation completed with no problems.
	OK bool
}

// ValidateOpts are options for a validate operation.
type ValidateOpts struct {
	// TargetVersion is a specific migration version to validate up to. When
	// set, validation only checks that migrations up to and including
	// TargetVersion have been applied.
	TargetVersion int
}

// Validate reports whether all migrations (or all up to
// opts.TargetVersion) have been applied. The opts parameter may be nil.
func (m *Migrator) Validate(ctx context.Context, opts *ValidateOpts) (*ValidateResult, error) {
	if opts == nil {
		opts = &ValidateOpts{}
	}
	if opts.TargetVersion > 0 {
		if _, ok := m.migrations[opts.TargetVersion]; !ok {
			return nil, fmt.Errorf("version %d is not a valid pats migration version", opts.TargetVersion)
		}
	}
	existing, err := m.existingVersions(ctx)
	if err != nil {
		return nil, err
	}
	var missing []int
	for _, mig := range m.AllVersions() {
		if opts.TargetVersion > 0 && mig.Version > opts.TargetVersion {
			break
		}
		if !slices.Contains(existing, mig.Version) {
			missing = append(missing, mig.Version)
		}
	}
	if len(missing) > 0 {
		msg := fmt.Sprintf("Unapplied migrations: %v", missing)
		m.logger.InfoContext(ctx, "patsmigrate: "+msg)
		return &ValidateResult{Messages: []string{msg}}, nil
	}
	return &ValidateResult{OK: true}, nil
}

// existingVersions returns applied versions in ascending order. A missing
// pats_migration table means nothing has been applied yet.
func (m *Migrator) existingVersions(ctx context.Context) ([]int, error) {
	rows, err := m.q.Query(ctx,
		"SELECT to_regclass($1) IS NOT NULL", m.schema+".pats_migration")
	if err != nil {
		return nil, fmt.Errorf("patsmigrate: checking for pats_migration: %w", err)
	}
	var exists bool
	if rows.Next() {
		err = rows.Scan(&exists)
	}
	if cerr := rows.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("patsmigrate: checking for pats_migration: %w", err)
	}
	if !exists {
		return nil, nil
	}

	rows, err = m.q.Query(ctx, "SELECT version FROM "+m.schema+".pats_migration ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("patsmigrate: reading migrations: %w", err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		versions = append(versions, int(v))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return versions, nil
}

// Render replaces the {{schema}} and {{channel}} placeholders in a
// Migration's SQL with this migrator's schema, for use with other migration
// tools.
func (m *Migrator) Render(sql string) string {
	return strings.NewReplacer("{{schema}}", m.schema, "{{channel}}", m.schema).Replace(sql)
}

// migrationsFromFS reads migration bundles from migration/<line>/ in fsys.
// Files are named like 001_initial_schema.up.sql.
func migrationsFromFS(fsys fs.FS, line string) ([]Migration, error) {
	dir := "migration/" + line
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("error reading migrations: %w", err)
	}

	var (
		lastBundle *Migration
		migrations []Migration
	)
	// ReadDir returns entries sorted by filename, so all 001* files come
	// before all 002* files.
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()

		versionStr, name, ok := strings.Cut(filename, "_")
		if !ok {
			return nil, fmt.Errorf("expected name to start with version string like '001_': %q", filename)
		}
		version, err := strconv.Atoi(versionStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing version %q: %w", versionStr, err)
		}
		// For 001_initial_schema.up.sql the name is "initial schema".
		name, _, _ = strings.Cut(name, ".")
		name = strings.ReplaceAll(name, "_", " ")

		if lastBundle == nil || lastBundle.Version != version {
			migrations = append(migrations, Migration{Name: name, Version: version})
			lastBundle = &migrations[len(migrations)-1]
		}

		contents, err := fs.ReadFile(fsys, dir+"/"+filename)
		if err != nil {
			return nil, fmt.Errorf("error reading file %q: %w", filename, err)
		}
		switch {
		case strings.HasSuffix(filename, ".down.sql"):
			lastBundle.SQLDown = string(contents)
		case strings.HasSuffix(filename, ".up.sql"):
			lastBundle.SQLUp = string(contents)
		default:
			return nil, fmt.Errorf("file %q should end with either '.down.sql' or '.up.sql'", filename)
		}
	}
	if len(migrations) == 0 {
		return nil, fmt.Errorf("no migrations found for line: %q", line)
	}
	return migrations, nil
}

// validateAndInit checks a set of migrations for missing SQL, duplicate or
// skipped version numbers, and indexes them by version.
func validateAndInit(versions []Migration) map[int]Migration {
	lastVersion := 0
	migrations := make(map[int]Migration, len(versions))
	for _, b := range versions {
		if b.SQLDown == "" {
			panic(fmt.Sprintf("version bundle should specify Down: %+v", b))
		}
		if b.SQLUp == "" {
			panic(fmt.Sprintf("version bundle should specify Up: %+v", b))
		}
		if b.Version == 0 {
			panic(fmt.Sprintf("version bundle should specify Version: %+v", b))
		}
		if _, ok := migrations[b.Version]; ok {
			panic(fmt.Sprintf("duplicate version: %03d", b.Version))
		}
		if b.Version <= lastVersion {
			panic(fmt.Sprintf("versions should be ascending; current: %03d, last: %03d", b.Version, lastVersion))
		}
		if b.Version > lastVersion+1 {
			panic(fmt.Sprintf("versions shouldn't skip a sequence number; current: %03d, last: %03d", b.Version, lastVersion))
		}
		lastVersion = b.Version
		migrations[b.Version] = b
	}
	return migrations
}
