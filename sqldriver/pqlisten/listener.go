// Package pqlisten provides a LISTEN implementation based on lib/pq, for use
// with sqldriver when database/sql runs on lib/pq.
package pqlisten

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/dmitrydorofeev/pats"
	"github.com/dmitrydorofeev/pats/sqldriver"
)

// Option configures Factory.
type Option func(*options)

type options struct {
	logger                     *slog.Logger
	minReconnect, maxReconnect time.Duration
}

// WithLogger sets the logger for connection events after the listener is
// established (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// WithReconnect sets lib/pq's reconnect backoff (default 100ms to 10s).
func WithReconnect(min, max time.Duration) Option {
	return func(o *options) { o.minReconnect, o.maxReconnect = min, max }
}

// Factory returns a sqldriver.ListenerFactory that opens a dedicated lib/pq
// listener connection to dsn. The factory waits for the first connection
// until ctx ends, and fails if that connection attempt fails; after that,
// lib/pq reconnects on its own.
func Factory(dsn string, opts ...Option) sqldriver.ListenerFactory {
	o := options{logger: slog.Default(), minReconnect: 100 * time.Millisecond, maxReconnect: 10 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	return func(ctx context.Context, channel string) (pats.Listener, error) {
		var (
			once      sync.Once
			firstErr  = make(chan error, 1)
			connected = make(chan struct{})
		)
		cb := func(ev pq.ListenerEventType, err error) {
			switch ev {
			case pq.ListenerEventConnected:
				once.Do(func() { close(connected) })
			case pq.ListenerEventConnectionAttemptFailed:
				select {
				case <-connected:
					o.logger.Warn("pqlisten: reconnect failed", "error", err)
				default:
					select {
					case firstErr <- err:
					default:
					}
				}
			case pq.ListenerEventDisconnected:
				o.logger.Warn("pqlisten: connection lost", "error", err)
			case pq.ListenerEventReconnected:
				o.logger.Info("pqlisten: reconnected")
			}
		}
		l := pq.NewListener(dsn, o.minReconnect, o.maxReconnect, cb)

		// Listen blocks until a connection is up, so run it aside and give
		// up on ctx or on the first failed attempt.
		done := make(chan error, 1)
		go func() { done <- l.Listen(channel) }()
		select {
		case err := <-done:
			if err != nil {
				_ = l.Close()
				return nil, err
			}
			return &listener{l: l}, nil
		case err := <-firstErr:
			_ = l.Close()
			<-done
			return nil, fmt.Errorf("pqlisten: connect: %w", err)
		case <-ctx.Done():
			_ = l.Close()
			<-done
			return nil, ctx.Err()
		}
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
