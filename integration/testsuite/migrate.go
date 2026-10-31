package testsuite

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dmitrydorofeev/pats"
	"github.com/dmitrydorofeev/pats/patsmigrate"
)

func versions(res *patsmigrate.MigrateResult) []int {
	out := make([]int, len(res.Versions))
	for i, v := range res.Versions {
		out[i] = v.Version
	}
	return out
}

func allVersions(m *patsmigrate.Migrator) []int {
	var out []int
	for _, v := range m.AllVersions() {
		out = append(out, v.Version)
	}
	return out
}

func (e *env) appliedVersions(m *patsmigrate.Migrator) []int {
	e.t.Helper()
	existing, err := m.ExistingVersions(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []int
	for _, v := range existing {
		out = append(out, v.Version)
	}
	return out
}

func (e *env) tableExists(name string) bool {
	e.t.Helper()
	var ok bool
	if err := e.admin.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", e.schema+"."+name).Scan(&ok); err != nil {
		e.t.Fatal(err)
	}
	return ok
}

func testMigrateUpDown(t *testing.T, f Factory) {
	e := newBareEnv(t, f)
	m := e.migrator(nil)
	all := allVersions(m)

	res, err := m.Migrate(ctx, patsmigrate.DirectionUp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := versions(res); !slices.Equal(got, all) {
		t.Fatalf("up applied %v, want %v", got, all)
	}
	if !e.tableExists("messages") {
		t.Fatal("messages table missing after up")
	}
	res, err = m.Migrate(ctx, patsmigrate.DirectionUp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Versions) != 0 {
		t.Fatalf("second up applied %v", versions(res))
	}

	res, err = m.Migrate(ctx, patsmigrate.DirectionDown, &patsmigrate.MigrateOpts{TargetVersion: -1})
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Clone(all)
	slices.Reverse(want)
	if got := versions(res); !slices.Equal(got, want) {
		t.Fatalf("down to -1 applied %v, want %v", got, want)
	}
	if e.tableExists("messages") || e.tableExists("pats_migration") {
		t.Fatal("tables left after full down migration")
	}
	res, err = m.Migrate(ctx, patsmigrate.DirectionDown, nil)
	if err != nil || len(res.Versions) != 0 {
		t.Fatalf("down on an empty schema = %v, %v", res, err)
	}

	if _, err := m.Migrate(ctx, patsmigrate.DirectionUp, nil); err != nil {
		t.Fatalf("up after full down: %v", err)
	}
	c := e.connect()
	ch, h := chanHandler(1)
	if _, err := c.Subscribe(ctx, "after.remigrate", h); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, "after.remigrate", nil); err != nil {
		t.Fatal(err)
	}
	recv(t, ch, 5*time.Second)
}

// A current schema must not be touched again: a migrator that finds nothing
// to do takes no locks on pats tables, so it doesn't block on (or block)
// traffic.
func testMigrateNoLockWhenCurrent(t *testing.T, f Factory) {
	e := newEnv(t, f)
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "LOCK "+e.schema+".messages IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := e.migrator(nil).Migrate(ctx, patsmigrate.DirectionUp, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Migrate on a current schema blocked on a lock held on messages")
	}
}

func testMigrateConcurrent(t *testing.T, f Factory) {
	e := newBareEnv(t, f)
	const n = 5
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		applied []int
		errs    []error
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := e.migrator(nil).Migrate(ctx, patsmigrate.DirectionUp, nil)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			applied = append(applied, versions(res)...)
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent migrate errors: %v", errs)
	}
	m := e.migrator(nil)
	all := allVersions(m)
	slices.Sort(applied)
	if !slices.Equal(applied, all) {
		t.Fatalf("versions applied across migrators = %v, want each of %v once", applied, all)
	}
	if got := e.appliedVersions(m); !slices.Equal(got, all) {
		t.Fatalf("recorded versions = %v, want %v", got, all)
	}
}

// Step selection (MaxSteps, TargetVersion) is unit-tested in patsmigrate;
// this covers the options against a real database.
func testMigrateOpts(t *testing.T, f Factory) {
	e := newBareEnv(t, f)
	m := e.migrator(nil)
	all := allVersions(m)

	vr, err := m.Validate(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vr.OK || len(vr.Messages) != 1 || vr.Messages[0] != fmt.Sprintf("Unapplied migrations: %v", all) {
		t.Fatalf("Validate on an empty schema = %+v", vr)
	}

	res, err := m.Migrate(ctx, patsmigrate.DirectionUp, &patsmigrate.MigrateOpts{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := versions(res); !slices.Equal(got, all) || e.tableExists("pats_migration") {
		t.Fatalf("dry run: reported %v, table exists = %v", got, e.tableExists("pats_migration"))
	}
	if !strings.Contains(res.Versions[0].SQL, "CREATE TABLE "+e.schema+".messages") {
		t.Fatal("dry run SQL is not rendered for the schema")
	}

	if res, err := m.Migrate(ctx, patsmigrate.DirectionUp, &patsmigrate.MigrateOpts{MaxSteps: -1}); err != nil || len(res.Versions) != 0 {
		t.Fatalf("MaxSteps -1 = %v, %v", res, err)
	}

	last := all[len(all)-1]
	res, err = m.Migrate(ctx, patsmigrate.DirectionUp, &patsmigrate.MigrateOpts{TargetVersion: last})
	if err != nil {
		t.Fatal(err)
	}
	if got := versions(res); !slices.Equal(got, all) {
		t.Fatalf("TargetVersion %d applied %v", last, got)
	}
	if got := e.appliedVersions(m); !slices.Equal(got, all) {
		t.Fatalf("recorded versions = %v, want %v", got, all)
	}
	if vr, err := m.Validate(ctx, nil); err != nil || !vr.OK {
		t.Fatalf("Validate after full up = %+v, %v", vr, err)
	}
}

func testConnectDoesNotMigrate(t *testing.T, f Factory) {
	e := newBareEnv(t, f)
	c, err := pats.Connect(ctx, e.h.Driver, pats.WithSchema(e.schema), pats.WithoutJanitor())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if e.tableExists("pats_migration") || e.tableExists("messages") {
		t.Fatal("Connect created pats tables")
	}
	if err := c.Publish(ctx, "no.schema", nil); err == nil {
		t.Fatal("Publish succeeded without a schema")
	}
}
