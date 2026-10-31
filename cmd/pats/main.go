// Command pats manages the pats schema in a PostgreSQL database.
//
//	pats migrate-up     --database-url URL [--schema pats] [--max-steps N] [--target-version N] [--dry-run]
//	pats migrate-down   --database-url URL [--schema pats] [--max-steps N] [--target-version N] [--dry-run]
//	pats migrate-list   --database-url URL [--schema pats]
//	pats migrate-get    --version N (--up | --down) [--schema pats]
//	pats validate       --database-url URL [--schema pats]
//
// --database-url defaults to $DATABASE_URL. migrate-get needs no database;
// it prints a migration's SQL for use with other migration tools.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dmitrydorofeev/pats/patsmigrate"
	"github.com/dmitrydorofeev/pats/pgxdriver"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "pats:", err)
		os.Exit(1)
	}
}

const usage = `usage: pats <command> [flags]

commands:
  migrate-up     apply pending migrations
  migrate-down   roll back migrations (one step by default)
  migrate-list   list migrations and whether they are applied
  migrate-get    print a migration's SQL
  validate       exit non-zero if migrations are pending

run "pats <command> -h" for flags`

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	cmd := args[0]

	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	var (
		databaseURL   = fs.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection string")
		schema        = fs.String("schema", "pats", "schema holding the pats tables")
		maxSteps      = fs.Int("max-steps", 0, "maximum number of migrations to apply")
		targetVersion = fs.Int("target-version", 0, "version to migrate to (-1 with migrate-down removes everything)")
		dryRun        = fs.Bool("dry-run", false, "print migrations without applying them")
		version       = fs.Int("version", 0, "migration version (migrate-get)")
		up            = fs.Bool("up", false, "print the up SQL (migrate-get)")
		down          = fs.Bool("down", false, "print the down SQL (migrate-get)")
		verbose       = fs.Bool("verbose", false, "log each applied migration")
	)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	level := slog.LevelWarn
	if *verbose {
		level = slog.LevelInfo
	}
	cfg := &patsmigrate.Config{
		Schema: *schema,
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})),
	}

	if cmd == "migrate-get" {
		return migrateGet(cfg, *version, *up, *down)
	}

	if *databaseURL == "" {
		return errors.New("--database-url or $DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	m, err := patsmigrate.New(pgxdriver.New(pool), cfg)
	if err != nil {
		return err
	}

	switch cmd {
	case "migrate-up", "migrate-down":
		dir := patsmigrate.DirectionUp
		if cmd == "migrate-down" {
			dir = patsmigrate.DirectionDown
		}
		res, err := m.Migrate(ctx, dir, &patsmigrate.MigrateOpts{
			DryRun:        *dryRun,
			MaxSteps:      *maxSteps,
			TargetVersion: *targetVersion,
		})
		if err != nil {
			return err
		}
		if len(res.Versions) == 0 {
			fmt.Println("no migrations to apply")
		}
		for _, v := range res.Versions {
			if *dryRun {
				fmt.Printf("-- %s %03d [%s] (dry run)\n%s\n", res.Direction, v.Version, v.Name, v.SQL)
				continue
			}
			fmt.Printf("applied %s %03d [%s] in %s\n", res.Direction, v.Version, v.Name, v.Duration)
		}
		return nil

	case "migrate-list":
		existing, err := m.ExistingVersions(ctx)
		if err != nil {
			return err
		}
		for _, mig := range m.AllVersions() {
			mark := " "
			if slices.ContainsFunc(existing, func(x patsmigrate.Migration) bool { return x.Version == mig.Version }) {
				mark = "*"
			}
			fmt.Printf("%s %03d %s\n", mark, mig.Version, mig.Name)
		}
		return nil

	case "validate":
		res, err := m.Validate(ctx, &patsmigrate.ValidateOpts{TargetVersion: *targetVersion})
		if err != nil {
			return err
		}
		if !res.OK {
			for _, msg := range res.Messages {
				fmt.Println(msg)
			}
			return errors.New("validation failed")
		}
		fmt.Println("ok")
		return nil
	}
	return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
}

func migrateGet(cfg *patsmigrate.Config, version int, up, down bool) error {
	if up == down {
		return errors.New("migrate-get needs exactly one of --up or --down")
	}
	m, err := patsmigrate.New(nil, cfg)
	if err != nil {
		return err
	}
	mig, err := m.GetVersion(version)
	if err != nil {
		return err
	}
	sql := mig.SQLUp
	if down {
		sql = mig.SQLDown
	}
	fmt.Print(m.Render(sql))
	return nil
}
