package pats

import (
	"log/slog"
	"time"
)

// Option configures a Conn.
type Option func(*connOptions)

type connOptions struct {
	schema            string
	logger            *slog.Logger
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	consumerTTL       time.Duration
	janitorInterval   time.Duration
	janitor           bool
	retentionMaxAge   time.Duration
	deadLetterMaxAge  time.Duration
	reconnectMin      time.Duration
	reconnectMax      time.Duration
}

func defaultConnOptions() connOptions {
	return connOptions{
		schema:            "pats",
		logger:            slog.Default(),
		pollInterval:      5 * time.Second,
		heartbeatInterval: 10 * time.Second,
		consumerTTL:       60 * time.Second,
		janitorInterval:   30 * time.Second,
		janitor:           true,
		reconnectMin:      100 * time.Millisecond,
		reconnectMax:      10 * time.Second,
	}
}

// WithSchema sets the PostgreSQL schema holding pats's tables (default
// "pats"). The schema name is also used as the NOTIFY channel, so separate
// schemas are fully isolated from each other. Install the schema with
// patsmigrate.Config{Schema: name}.
func WithSchema(name string) Option { return func(o *connOptions) { o.schema = name } }

// WithLogger sets the logger used for background errors.
func WithLogger(l *slog.Logger) Option { return func(o *connOptions) { o.logger = l } }

// WithPollInterval sets the default safety-net poll interval for
// subscriptions. Polling also picks up messages whose ack lease expired.
func WithPollInterval(d time.Duration) Option { return func(o *connOptions) { o.pollInterval = d } }

// WithHeartbeat sets how often ephemeral consumers are kept alive and how long
// an ephemeral consumer may go without a heartbeat before it is removed.
func WithHeartbeat(interval, ttl time.Duration) Option {
	return func(o *connOptions) { o.heartbeatInterval, o.consumerTTL = interval, ttl }
}

// WithJanitorInterval sets how often cleanup runs. Only one Conn per schema
// does the work at a time.
func WithJanitorInterval(d time.Duration) Option {
	return func(o *connOptions) { o.janitorInterval = d }
}

// WithoutJanitor disables background cleanup on this Conn.
func WithoutJanitor() Option { return func(o *connOptions) { o.janitor = false } }

// WithRetention drops messages older than maxAge even if some consumer has
// not processed them yet. Zero (the default) keeps them until processed.
func WithRetention(maxAge time.Duration) Option {
	return func(o *connOptions) { o.retentionMaxAge = maxAge }
}

// WithDeadLetterRetention drops dead letters older than maxAge. Zero (the
// default) keeps them forever.
func WithDeadLetterRetention(maxAge time.Duration) Option {
	return func(o *connOptions) { o.deadLetterMaxAge = maxAge }
}

// SubOption configures a Subscription.
type SubOption func(*subOptions)

type subOptions struct {
	durable      string
	isDurable    bool
	maxInFlight  int
	ackWait      time.Duration
	maxDeliver   int
	manualAck    bool
	pollInterval time.Duration
}

// Durable makes the consumer persistent: it is not removed when subscribers
// go away, and messages published while nobody is subscribed are kept. For
// Subscribe, name identifies the consumer. For QueueSubscribe the queue name
// is the consumer name, and name must be empty or equal to it.
func Durable(name string) SubOption {
	return func(o *subOptions) { o.durable, o.isDurable = name, true }
}

// MaxInFlight limits how many messages the subscription processes
// concurrently (default 1).
func MaxInFlight(n int) SubOption { return func(o *subOptions) { o.maxInFlight = n } }

// AckWait is the lease duration: a message that is not acked within it is
// delivered again (default 30s).
func AckWait(d time.Duration) SubOption { return func(o *subOptions) { o.ackWait = d } }

// MaxDeliver moves a message to the dead-letter table once it would be
// delivered more than n times. Zero (the default) means unlimited.
func MaxDeliver(n int) SubOption { return func(o *subOptions) { o.maxDeliver = n } }

// ManualAck turns off auto-ack. The handler must call Ack, Nak or Term, or
// the message is redelivered after AckWait.
func ManualAck() SubOption { return func(o *subOptions) { o.manualAck = true } }

// SubPollInterval overrides the Conn's poll interval for one subscription.
func SubPollInterval(d time.Duration) SubOption { return func(o *subOptions) { o.pollInterval = d } }
