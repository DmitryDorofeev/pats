// Package pqlisten provides a LISTEN implementation based on lib/pq, for use
// with sqldriver when database/sql runs on lib/pq.
package pqlisten

import (
	"context"
	"errors"
	"time"

	"github.com/lib/pq"

	"github.com/dmitrydorofeev/pats"
	"github.com/dmitrydorofeev/pats/sqldriver"
)

// Factory returns a sqldriver.ListenerFactory that opens a dedicated lib/pq
// listener connection to dsn.
func Factory(dsn string) sqldriver.ListenerFactory {
	return func(ctx context.Context, channel string) (pats.Listener, error) {
		l := pq.NewListener(dsn, 100*time.Millisecond, 10*time.Second, nil)
		if err := l.Listen(channel); err != nil {
			_ = l.Close()
			return nil, err
		}
		return &listener{l: l}, nil
	}
}

type listener struct{ l *pq.Listener }

var errReconnected = errors.New("pqlisten: connection was re-established")

func (l *listener) Wait(ctx context.Context) (string, error) {
	select {
	case n, ok := <-l.l.Notify:
		if !ok {
			return "", errors.New("pqlisten: listener closed")
		}
		if n == nil {
			// lib/pq reconnected on its own; notifications may have been
			// lost, so report an error to make pats re-sync.
			return "", errReconnected
		}
		return n.Extra, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (l *listener) Close() error { return l.l.Close() }
