package pats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Subscription is a running subscriber attached to a consumer.
type Subscription struct {
	conn       *Conn
	consumerID atomic.Int64 // replaced if the consumer is re-registered
	name       string
	subject    string
	queue      string
	ephemeral  bool // owned by this subscription alone; deleted on Unsubscribe
	cb         MsgHandler
	opts       subOptions

	wakeCh chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	loop   sync.WaitGroup // the fetch loop
	work   sync.WaitGroup // in-flight handlers

	mu       sync.Mutex
	inflight int
	more     bool // last fetch filled the batch, so more may be waiting
	stopped  bool
}

func newSubscription(c *Conn, id int64, name, subject, queue string, ephemeral bool, cb MsgHandler, o subOptions) *Subscription {
	ctx, cancel := context.WithCancel(c.ctx)
	s := &Subscription{
		conn:      c,
		name:      name,
		subject:   subject,
		queue:     queue,
		ephemeral: ephemeral,
		cb:        cb,
		opts:      o,
		wakeCh:    make(chan struct{}, 1),
		ctx:       ctx,
		cancel:    cancel,
	}
	s.consumerID.Store(id)
	return s
}

// Subject returns the subscription's subject pattern.
func (s *Subscription) Subject() string { return s.subject }

// Queue returns the queue group name, or "" for a fan-out subscription.
func (s *Subscription) Queue() string { return s.queue }

// Consumer returns the name of the underlying consumer.
func (s *Subscription) Consumer() string { return s.name }

func (s *Subscription) start() {
	s.loop.Add(1)
	go s.run()
	s.signal()
}

// signal wakes the fetch loop without blocking; wake-ups coalesce.
func (s *Subscription) signal() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (s *Subscription) signalAfter(d time.Duration) {
	if d <= 0 {
		s.signal()
		return
	}
	time.AfterFunc(d, s.signal)
}

func (s *Subscription) run() {
	defer s.loop.Done()
	poll := time.NewTicker(s.opts.pollInterval)
	defer poll.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wakeCh:
		case <-poll.C:
		}
		s.fetchAll()
	}
}

// fetchAll claims and dispatches messages until the consumer has nothing
// visible left or every in-flight slot is busy.
func (s *Subscription) fetchAll() {
	for s.ctx.Err() == nil {
		s.mu.Lock()
		free := s.opts.maxInFlight - s.inflight
		s.mu.Unlock()
		if free <= 0 {
			// A finishing handler signals the loop if more work may be waiting.
			s.mu.Lock()
			s.more = true
			s.mu.Unlock()
			return
		}
		msgs, err := s.fetch(free)
		if err != nil {
			s.conn.logError("fetch", err, "consumer", s.name)
			return
		}
		s.mu.Lock()
		s.more = len(msgs) == free
		s.inflight += len(msgs)
		s.mu.Unlock()
		for _, m := range msgs {
			s.work.Add(1)
			go s.handle(m)
		}
		if len(msgs) < free {
			return
		}
	}
}

func (s *Subscription) fetch(n int) ([]*Msg, error) {
	cid := s.consumerID.Load()
	rows, err := s.conn.drv.Query(s.ctx, s.conn.q.fetch, cid, n, s.opts.ackWait.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Msg
	for rows.Next() {
		m := &Msg{sub: s, consumerID: cid}
		if err := rows.Scan(&m.ID, &m.Delivery, &m.Subject, &m.Data, &m.Reply); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Subscription) handle(m *Msg) {
	defer func() {
		s.mu.Lock()
		s.inflight--
		more := s.more
		s.mu.Unlock()
		if more {
			s.signal()
		}
		s.work.Done()
	}()

	if s.opts.maxDeliver > 0 && m.Delivery > s.opts.maxDeliver {
		reason := fmt.Sprintf("max deliveries (%d) exceeded", s.opts.maxDeliver)
		s.conn.logError("term", m.Term(reason), "consumer", s.name, "message", m.ID)
		return
	}

	panicked := true
	func() {
		defer func() {
			if r := recover(); r != nil {
				s.conn.logError("handler panic", fmt.Errorf("%v", r), "consumer", s.name, "message", m.ID)
			}
		}()
		s.cb(m)
		panicked = false
	}()

	if m.state.Load() {
		return
	}
	switch {
	case panicked:
		s.conn.logError("nak", m.Nak(s.panicDelay(m.Delivery)), "consumer", s.name, "message", m.ID)
	case !s.opts.manualAck:
		s.conn.logError("ack", m.Ack(), "consumer", s.name, "message", m.ID)
	}
}

// panicDelay is how long a message waits before redelivery after its handler
// panicked: 100ms, doubling with each delivery, capped at AckWait. Without
// it a poison message would be redelivered in a tight loop.
func (s *Subscription) panicDelay(delivery int) time.Duration {
	d := 100 * time.Millisecond << min(max(delivery-1, 0), 20)
	return min(d, s.opts.ackWait)
}

// Unsubscribe stops delivery. Handlers already running finish in the
// background; their messages can still be acked. A plain (non-queue,
// non-durable) subscription's consumer and pending deliveries are removed.
func (s *Subscription) Unsubscribe() error {
	if !s.stop() {
		return nil
	}
	return s.cleanup()
}

// Drain stops fetching new messages and waits for in-flight handlers to
// finish (or ctx to end) before unsubscribing.
func (s *Subscription) Drain(ctx context.Context) error {
	if !s.stop() {
		return nil
	}
	done := make(chan struct{})
	go func() {
		s.work.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if cerr := s.cleanup(); err == nil {
		err = cerr
	}
	return err
}

// stop halts the fetch loop. It reports whether this call did the stopping.
func (s *Subscription) stop() bool {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false
	}
	s.stopped = true
	s.mu.Unlock()
	s.cancel()
	s.loop.Wait()
	s.conn.removeSub(s)
	return true
}

func (s *Subscription) cleanup() error {
	if !s.ephemeral {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	return s.conn.drv.Exec(ctx, s.conn.q.deleteConsumer, s.consumerID.Load())
}
