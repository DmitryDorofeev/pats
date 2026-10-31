// Package pats is a NATS-style publish/subscribe library backed by
// PostgreSQL.
//
// Messages are stored in tables. LISTEN/NOTIFY is used only to wake
// subscribers, which then claim work with SELECT ... FOR UPDATE SKIP LOCKED.
// Delivery is at-least-once with acknowledgements.
package pats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrBadSubject     = errors.New("pats: invalid subject")
	ErrClosed         = errors.New("pats: connection closed")
	ErrFilterMismatch = errors.New("pats: consumer already exists with a different subject filter")
	ErrNoReply        = errors.New("pats: message has no reply subject")
	ErrNotBound       = errors.New("pats: message was not received from a subscription")
	ErrAlreadyAcked   = errors.New("pats: message already acknowledged")
	ErrBadOption      = errors.New("pats: invalid option")
)

// Conn is a connection to the pats message system. It is safe for
// concurrent use.
type Conn struct {
	drv    Driver
	opts   connOptions
	schema string

	q sqlText

	ctx    context.Context
	cancel context.CancelFunc
	bg     sync.WaitGroup

	mu        sync.Mutex
	closed    bool
	subs      map[*Subscription]struct{}
	byConsume map[int64][]*Subscription
}

// Connect creates a Conn on top of drv, installing the schema unless
// WithoutMigrate is given. The returned Conn does not own drv's underlying
// pool; close that yourself after Conn.Close.
func Connect(ctx context.Context, drv Driver, opts ...Option) (*Conn, error) {
	o := defaultConnOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if err := validateSchema(o.schema); err != nil {
		return nil, err
	}
	if o.migrate {
		if err := Migrate(ctx, drv, o.schema); err != nil {
			return nil, err
		}
	}
	// Open the first listener synchronously so misconfiguration surfaces here.
	l, err := drv.Listen(ctx, o.schema)
	if err != nil {
		return nil, fmt.Errorf("pats: listen: %w", err)
	}

	cctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		drv:       drv,
		opts:      o,
		schema:    o.schema,
		q:         newSQLText(o.schema),
		ctx:       cctx,
		cancel:    cancel,
		subs:      map[*Subscription]struct{}{},
		byConsume: map[int64][]*Subscription{},
	}
	c.bg.Add(2)
	go c.listenLoop(l)
	go c.heartbeatLoop()
	if o.janitor {
		c.bg.Add(1)
		go c.janitorLoop()
	}
	return c, nil
}

// Publish sends data to subject.
func (c *Conn) Publish(ctx context.Context, subject string, data []byte) error {
	return c.PublishMsg(ctx, &Msg{Subject: subject, Data: data})
}

// PublishMsg sends m. Only Subject, Data and Reply are used.
func (c *Conn) PublishMsg(ctx context.Context, m *Msg) error {
	if c.isClosed() {
		return ErrClosed
	}
	return c.publish(ctx, c.drv, m)
}

// PublishTx publishes m using q, typically a transaction adapted with
// pgxdriver.Tx or sqldriver.Tx. The message becomes visible to subscribers
// only if that transaction commits.
func (c *Conn) PublishTx(ctx context.Context, q Querier, m *Msg) error {
	return c.publish(ctx, q, m)
}

func (c *Conn) publish(ctx context.Context, q Querier, m *Msg) error {
	if err := ValidateSubject(m.Subject); err != nil {
		return err
	}
	if m.Reply != "" {
		if err := ValidateSubject(m.Reply); err != nil {
			return fmt.Errorf("reply: %w", err)
		}
	}
	return q.Exec(ctx, c.q.publish, m.Subject, m.Data, m.Reply)
}

// Request publishes data to subject with a unique reply subject and waits for
// the first response. Use a context deadline as the timeout.
func (c *Conn) Request(ctx context.Context, subject string, data []byte) (*Msg, error) {
	inbox := NewInbox()
	ch := make(chan *Msg, 1)
	sub, err := c.Subscribe(ctx, inbox, func(m *Msg) {
		select {
		case ch <- m:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	if err := c.PublishMsg(ctx, &Msg{Subject: subject, Data: data, Reply: inbox}); err != nil {
		return nil, err
	}
	select {
	case m := <-ch:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NewInbox returns a unique subject suitable for replies.
func NewInbox() string { return "_INBOX." + randomID() }

// DeleteConsumer removes a consumer, typically a durable one, together with
// all of its pending deliveries.
func (c *Conn) DeleteConsumer(ctx context.Context, name string) error {
	return c.drv.Exec(ctx, c.q.deleteConsumerByName, name)
}

// Subscribe delivers every message matching subject to cb. Each subscription
// gets its own copy of every message (fan-out). Wildcards '*' and '>' are
// supported.
func (c *Conn) Subscribe(ctx context.Context, subject string, cb MsgHandler, opts ...SubOption) (*Subscription, error) {
	return c.subscribe(ctx, subject, "", cb, opts)
}

// QueueSubscribe delivers each message matching subject to exactly one member
// of the queue group named queue, across all processes.
func (c *Conn) QueueSubscribe(ctx context.Context, subject, queue string, cb MsgHandler, opts ...SubOption) (*Subscription, error) {
	if queue == "" {
		return nil, fmt.Errorf("%w: empty queue name", ErrBadOption)
	}
	return c.subscribe(ctx, subject, queue, cb, opts)
}

func (c *Conn) subscribe(ctx context.Context, subject, queue string, cb MsgHandler, opts []SubOption) (*Subscription, error) {
	if cb == nil {
		return nil, fmt.Errorf("%w: nil handler", ErrBadOption)
	}
	if err := ValidatePattern(subject); err != nil {
		return nil, err
	}
	o := subOptions{maxInFlight: 1, ackWait: 30 * time.Second, pollInterval: c.opts.pollInterval}
	for _, opt := range opts {
		opt(&o)
	}
	if o.maxInFlight < 1 || o.ackWait <= 0 || o.maxDeliver < 0 || o.pollInterval <= 0 {
		return nil, fmt.Errorf("%w: MaxInFlight, AckWait and poll interval must be positive", ErrBadOption)
	}

	var name string
	shared := true
	switch {
	case queue != "":
		if o.isDurable && o.durable != "" && o.durable != queue {
			return nil, fmt.Errorf("%w: Durable name %q differs from queue %q", ErrBadOption, o.durable, queue)
		}
		name = queue
	case o.isDurable:
		if o.durable == "" {
			return nil, fmt.Errorf("%w: Durable needs a name", ErrBadOption)
		}
		name = o.durable
	default:
		name = "_eph." + randomID()
		shared = false
	}
	if strings.HasPrefix(name, "_eph.") && shared {
		return nil, fmt.Errorf("%w: consumer names starting with _eph. are reserved", ErrBadOption)
	}

	if c.isClosed() {
		return nil, ErrClosed
	}
	id, err := c.upsertConsumer(ctx, name, subject, o.isDurable)
	if err != nil {
		return nil, err
	}

	s := newSubscription(c, id, name, subject, queue, !shared, cb, o)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		if !shared {
			_ = c.drv.Exec(ctx, c.q.deleteConsumer, id)
		}
		return nil, ErrClosed
	}
	c.subs[s] = struct{}{}
	c.byConsume[id] = append(c.byConsume[id], s)
	c.mu.Unlock()

	s.start()
	return s, nil
}

func (c *Conn) upsertConsumer(ctx context.Context, name, filter string, durable bool) (int64, error) {
	rows, err := c.drv.Query(ctx, c.q.upsertConsumer, name, filter, durable)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, errors.New("pats: upsert consumer returned no rows")
	}
	var (
		id        int64
		curFilter string
	)
	if err := rows.Scan(&id, &curFilter); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if curFilter != filter {
		return 0, fmt.Errorf("%w: %q has filter %q, not %q", ErrFilterMismatch, name, curFilter, filter)
	}
	return id, nil
}

func (c *Conn) removeSub(s *Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, s)
	id := s.consumerID.Load()
	list := c.byConsume[id]
	for i, x := range list {
		if x == s {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(c.byConsume, id)
	} else {
		c.byConsume[id] = list
	}
}

// wake signals every local subscription attached to consumer id.
func (c *Conn) wake(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.byConsume[id] {
		s.signal()
	}
}

func (c *Conn) wakeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for s := range c.subs {
		s.signal()
	}
}

func (c *Conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Drain stops all subscriptions from fetching, waits for in-flight handlers
// to finish (or ctx to end), and closes the Conn.
func (c *Conn) Drain(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.closed = true
	subs := make([]*Subscription, 0, len(c.subs))
	for s := range c.subs {
		subs = append(subs, s)
	}
	c.mu.Unlock()

	var firstErr error
	for _, s := range subs {
		if err := s.Drain(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.cancel()
	c.bg.Wait()
	return firstErr
}

// Close stops all subscriptions, waits for in-flight handlers to return, and
// stops background work. Unacked messages are redelivered after AckWait.
func (c *Conn) Close() error {
	err := c.Drain(context.Background())
	if errors.Is(err, ErrClosed) {
		return nil
	}
	return err
}

func (c *Conn) logError(msg string, err error, args ...any) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	c.opts.logger.Error("pats: "+msg, append([]any{"error", err}, args...)...)
}

func randomID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func joinIDs(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	return strings.Join(parts, ",")
}
