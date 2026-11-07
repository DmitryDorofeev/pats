package patsmigrate

import (
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

func TestEmbeddedMigrations(t *testing.T) {
	m, err := New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	all := m.AllVersions()
	if len(all) != 2 || all[0].Version != 1 || all[0].Name != "initial schema" || all[1].Version != 2 || all[1].Name != "fanout in go" {
		t.Fatalf("migrations = %+v", all)
	}
	if !strings.Contains(m.Render(all[0].SQLUp), "CREATE TABLE pats.messages") {
		t.Fatal("rendered SQL does not use the default schema")
	}
	if _, err := m.GetVersion(99); err == nil {
		t.Fatal("GetVersion(99) succeeded")
	}
}

func TestNewRejectsBadSchema(t *testing.T) {
	if _, err := New(nil, &Config{Schema: "Bad-Name"}); err == nil {
		t.Fatal("New accepted an invalid schema")
	}
}

func TestMigrationsFromFS(t *testing.T) {
	fsys := fstest.MapFS{
		"migration/main/001_first.up.sql":        {Data: []byte("up1")},
		"migration/main/001_first.down.sql":      {Data: []byte("down1")},
		"migration/main/002_second_one.up.sql":   {Data: []byte("up2")},
		"migration/main/002_second_one.down.sql": {Data: []byte("down2")},
		"migration/other/001_x.up.sql":           {Data: []byte("ignored")},
	}
	got, err := migrationsFromFS(fsys, "main")
	if err != nil {
		t.Fatal(err)
	}
	want := []Migration{
		{Name: "first", Version: 1, SQLUp: "up1", SQLDown: "down1"},
		{Name: "second one", Version: 2, SQLUp: "up2", SQLDown: "down2"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("migration %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	bad := fstest.MapFS{"migration/main/001_first.sql": {Data: []byte("x")}}
	if _, err := migrationsFromFS(bad, "main"); err == nil {
		t.Fatal("accepted a file without .up/.down")
	}
	if _, err := migrationsFromFS(fsys, "nope"); err == nil {
		t.Fatal("accepted a missing line")
	}
}

func TestValidateAndInit(t *testing.T) {
	ok := func(v int) Migration { return Migration{Version: v, SQLUp: "u", SQLDown: "d"} }
	cases := map[string][]Migration{
		"missing down": {{Version: 1, SQLUp: "u"}},
		"missing up":   {{Version: 1, SQLDown: "d"}},
		"zero version": {ok(0)},
		"duplicate":    {ok(1), ok(1)},
		"descending":   {ok(1), ok(2), ok(1)},
		"gap":          {ok(1), ok(3)},
	}
	for name, ms := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("did not panic")
				}
			}()
			validateAndInit(ms)
		})
	}
	if got := validateAndInit([]Migration{ok(1), ok(2)}); len(got) != 2 {
		t.Fatalf("got %d migrations", len(got))
	}
}

func TestSelectSteps(t *testing.T) {
	m := &Migrator{migrations: map[int]Migration{}}
	var all []Migration
	for v := 1; v <= 4; v++ {
		mig := Migration{Version: v}
		m.migrations[v] = mig
		all = append(all, mig)
	}
	reversed := slices.Clone(all)
	slices.Reverse(reversed)

	cases := []struct {
		name    string
		dir     Direction
		opts    MigrateOpts
		targets []Migration // pending (up) or applied (down), in direction order
		want    []int
		wantErr bool
	}{
		{name: "up all", dir: DirectionUp, targets: all, want: []int{1, 2, 3, 4}},
		{name: "up max steps", dir: DirectionUp, opts: MigrateOpts{MaxSteps: 2}, targets: all, want: []int{1, 2}},
		{name: "up none", dir: DirectionUp, opts: MigrateOpts{MaxSteps: -1}, targets: all, want: nil},
		{name: "up to target", dir: DirectionUp, opts: MigrateOpts{TargetVersion: 3}, targets: all, want: []int{1, 2, 3}},
		{name: "up to applied target", dir: DirectionUp, opts: MigrateOpts{TargetVersion: 2}, targets: all[2:], want: nil},
		{name: "up unknown target", dir: DirectionUp, opts: MigrateOpts{TargetVersion: 9}, targets: all, wantErr: true},
		{name: "down one by default", dir: DirectionDown, targets: reversed, want: []int{4}},
		{name: "down max steps", dir: DirectionDown, opts: MigrateOpts{MaxSteps: 3}, targets: reversed, want: []int{4, 3, 2}},
		{name: "down to target", dir: DirectionDown, opts: MigrateOpts{TargetVersion: 2}, targets: reversed, want: []int{4, 3}},
		{name: "down everything", dir: DirectionDown, opts: MigrateOpts{TargetVersion: -1}, targets: reversed, want: []int{4, 3, 2, 1}},
		{name: "down to unapplied target", dir: DirectionDown, opts: MigrateOpts{TargetVersion: 4}, targets: reversed[1:], wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := m.selectSteps(tc.dir, &tc.opts, slices.Clone(tc.targets))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("no error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var versions []int
			for _, x := range got {
				versions = append(versions, x.Version)
			}
			if !slices.Equal(versions, tc.want) {
				t.Fatalf("got %v, want %v", versions, tc.want)
			}
		})
	}
}
