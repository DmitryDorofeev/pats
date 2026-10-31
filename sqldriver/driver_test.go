package sqldriver_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"

	"github.com/dmitrydorofeev/pats"
	"github.com/dmitrydorofeev/pats/internal/testsuite"
	"github.com/dmitrydorofeev/pats/sqldriver"
	"github.com/dmitrydorofeev/pats/sqldriver/pqlisten"
)

func harness(t *testing.T, driverName, dsn string, opts ...sqldriver.Option) testsuite.Harness {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return testsuite.Harness{
		Driver: sqldriver.New(db, opts...),
		Begin: func(ctx context.Context) (pats.Querier, func() error, func() error, error) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return nil, nil, nil, err
			}
			return sqldriver.Tx(tx), tx.Commit, tx.Rollback, nil
		},
	}
}

func TestSuitePgxStdlib(t *testing.T) {
	testsuite.Run(t, func(t *testing.T, dsn string) testsuite.Harness {
		return harness(t, "pgx", dsn)
	})
}

func TestSuiteLibPQ(t *testing.T) {
	testsuite.Run(t, func(t *testing.T, dsn string) testsuite.Harness {
		return harness(t, "postgres", dsn, sqldriver.WithListener(pqlisten.Factory(dsn)))
	})
}

func TestLibPQWithoutListenerFails(t *testing.T) {
	dsn := testsuite.DSN(t)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = pats.Connect(context.Background(), sqldriver.New(db), pats.WithSchema("t_nolisten"))
	if err == nil {
		t.Fatal("expected an error explaining that LISTEN needs the pgx stdlib driver")
	}
	_, _ = db.Exec("DROP SCHEMA IF EXISTS t_nolisten CASCADE")
}
