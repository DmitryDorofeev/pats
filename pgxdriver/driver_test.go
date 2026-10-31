package pgxdriver_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dmitrydorofeev/pats"
	"github.com/dmitrydorofeev/pats/internal/testsuite"
	"github.com/dmitrydorofeev/pats/pgxdriver"
)

func TestSuite(t *testing.T) {
	testsuite.Run(t, func(t *testing.T, dsn string) testsuite.Harness {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return testsuite.Harness{
			Driver: pgxdriver.New(pool),
			Begin: func(ctx context.Context) (pats.Querier, func() error, func() error, error) {
				tx, err := pool.Begin(ctx)
				if err != nil {
					return nil, nil, nil, err
				}
				return pgxdriver.Tx(tx),
					func() error { return tx.Commit(ctx) },
					func() error { return tx.Rollback(ctx) }, nil
			},
		}
	})
}
