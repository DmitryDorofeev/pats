// Package sqldriver adapts a database/sql pool for pats.
//
// Queries work with any PostgreSQL database/sql driver. LISTEN needs
// driver-specific support: with the pgx stdlib driver ("pgx") it works out of
// the box; with lib/pq, pass WithListener(pqlisten.Factory(dsn)).
package sqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/dmitrydorofeev/pats"
)

// SQLQuerier is satisfied by *sql.DB, *sql.Conn and *sql.Tx.
type SQLQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ListenerFactory opens a listener on channel.
type ListenerFactory func(ctx context.Context, channel string) (pats.Listener, error)

// Option configures the driver.
type Option func(*Driver)

// WithListener replaces the default LISTEN implementation, for example with
// pqlisten.Factory when using lib/pq.
func WithListener(f ListenerFactory) Option { return func(d *Driver) { d.listen = f } }

// Driver implements pats.Driver on a *sql.DB.
type Driver struct {
	querier
	db     *sql.DB
	listen ListenerFactory
}

// New returns a pats driver for db.
func New(db *sql.DB, opts ...Option) *Driver {
	d := &Driver{querier: querier{db}, db: db}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Tx adapts a *sql.Tx (or any SQLQuerier) for pats.Conn.PublishTx.
func Tx(q SQLQuerier) pats.Querier { return querier{q} }

type querier struct{ q SQLQuerier }

func (q querier) Exec(ctx context.Context, query string, args ...any) error {
	_, err := q.q.ExecContext(ctx, query, args...)
	return err
}

func (q querier) Query(ctx context.Context, query string, args ...any) (pats.Rows, error) {
	return q.q.QueryContext(ctx, query, args...)
}

// Listen opens a listener. Without WithListener it requires the pgx stdlib
// driver and keeps one connection out of db's pool.
func (d *Driver) Listen(ctx context.Context, channel string) (pats.Listener, error) {
	if d.listen != nil {
		return d.listen(ctx, channel)
	}
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	l := &pgxStdlibListener{conn: conn}
	err = conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("sqldriver: LISTEN needs the pgx stdlib driver (got %T); use WithListener for other drivers", dc)
		}
		_, err := sc.Conn().Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize())
		return err
	})
	if err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}

type pgxStdlibListener struct{ conn *sql.Conn }

func (l *pgxStdlibListener) Wait(ctx context.Context) (string, error) {
	var payload string
	err := l.conn.Raw(func(dc any) error {
		n, err := dc.(*stdlib.Conn).Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		payload = n.Payload
		return nil
	})
	return payload, err
}

// Close discards the connection rather than returning it to the pool, so a
// still-listening session never gets reused for ordinary queries.
func (l *pgxStdlibListener) Close() error {
	err := l.conn.Raw(func(any) error { return driver.ErrBadConn })
	if err != nil && !errors.Is(err, driver.ErrBadConn) {
		_ = l.conn.Close()
		return err
	}
	return l.conn.Close()
}
