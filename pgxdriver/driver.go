// Package pgxdriver adapts a pgx v5 connection pool for pats.
package pgxdriver

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dmitrydorofeev/pats"
)

// PgxQuerier is satisfied by *pgxpool.Pool, *pgxpool.Conn, *pgx.Conn and pgx.Tx.
type PgxQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Driver implements pats.Driver on a *pgxpool.Pool.
type Driver struct {
	querier
	pool *pgxpool.Pool
}

// New returns a pats driver for pool. The listener uses one extra
// connection taken from the pool for the life of the pats.Conn.
func New(pool *pgxpool.Pool) *Driver {
	return &Driver{querier: querier{pool}, pool: pool}
}

// Tx adapts a pgx transaction (or any PgxQuerier) for pats.Conn.PublishTx.
func Tx(q PgxQuerier) pats.Querier { return querier{q} }

type querier struct{ q PgxQuerier }

func (q querier) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := q.q.Exec(ctx, sql, args...)
	return err
}

func (q querier) Query(ctx context.Context, sql string, args ...any) (pats.Rows, error) {
	rows, err := q.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgxRows{rows}, nil
}

type pgxRows struct{ pgx.Rows }

func (r pgxRows) Close() error {
	r.Rows.Close()
	return r.Rows.Err()
}

// Listen takes a connection out of the pool (it is not returned) and runs
// LISTEN on it.
func (d *Driver) Listen(ctx context.Context, channel string) (pats.Listener, error) {
	pc, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn := pc.Hijack()
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		_ = conn.Close(context.Background())
		return nil, err
	}
	return &Listener{conn: conn}, nil
}

// Listener waits for notifications on a dedicated *pgx.Conn. It can also be
// used directly with a connection that already executed LISTEN.
type Listener struct{ conn *pgx.Conn }

// NewListener wraps conn, which must already be listening. Close closes conn.
func NewListener(conn *pgx.Conn) *Listener { return &Listener{conn: conn} }

func (l *Listener) Wait(ctx context.Context) (string, error) {
	n, err := l.conn.WaitForNotification(ctx)
	if err != nil {
		return "", err
	}
	return n.Payload, nil
}

func (l *Listener) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return l.conn.Close(ctx)
}
