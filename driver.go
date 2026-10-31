package pats

import "context"

// Rows is the minimal result-set interface pats needs from a driver.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Querier executes SQL. Placeholders use PostgreSQL's $N syntax.
//
// Driver packages provide adapters so that a caller's transaction can be
// passed to Conn.PublishTx (see pgxdriver.Tx and sqldriver.Tx).
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) error
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// Driver is the database backend used by a Conn.
type Driver interface {
	Querier
	// Listen opens a dedicated connection that has executed LISTEN on channel.
	Listen(ctx context.Context, channel string) (Listener, error)
}

// Listener delivers notification payloads from a single LISTEN connection.
type Listener interface {
	// Wait blocks until a notification arrives and returns its payload.
	// Any error is treated as fatal for this listener: it is closed and a new
	// one is opened.
	Wait(ctx context.Context) (string, error)
	Close() error
}
