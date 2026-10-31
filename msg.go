package pats

import (
	"context"
	"sync/atomic"
	"time"
)

// MsgHandler processes messages delivered to a subscription.
type MsgHandler func(m *Msg)

// Msg is a published or received message.
type Msg struct {
	Subject string
	Data    []byte
	Reply   string

	// ID is the message's database id. It is set on received messages.
	ID int64
	// Delivery is 1 on first delivery and increments on each redelivery.
	Delivery int

	sub        *Subscription
	consumerID int64
	state      atomic.Bool // set once Ack/Nak/Term has been called
}

// opTimeout bounds acknowledgement statements, which run outside any caller
// context.
const opTimeout = 30 * time.Second

func (m *Msg) opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), opTimeout)
}

func (m *Msg) settle() error {
	if m.sub == nil {
		return ErrNotBound
	}
	if !m.state.CompareAndSwap(false, true) {
		return ErrAlreadyAcked
	}
	return nil
}

// Ack marks the message as processed for this consumer.
func (m *Msg) Ack() error {
	if err := m.settle(); err != nil {
		return err
	}
	ctx, cancel := m.opCtx()
	defer cancel()
	return m.sub.conn.drv.Exec(ctx, m.sub.conn.q.ack, m.consumerID, m.ID)
}

// Nak asks for redelivery after delay (zero means as soon as possible).
func (m *Msg) Nak(delay time.Duration) error {
	if err := m.settle(); err != nil {
		return err
	}
	ctx, cancel := m.opCtx()
	defer cancel()
	err := m.sub.conn.drv.Exec(ctx, m.sub.conn.q.nak, m.consumerID, m.ID, m.Delivery, delay.Milliseconds())
	if err == nil {
		m.sub.signalAfter(delay)
	}
	return err
}

// Term stops redelivery and moves the message to the dead-letter table with
// the given reason.
func (m *Msg) Term(reason string) error {
	if err := m.settle(); err != nil {
		return err
	}
	ctx, cancel := m.opCtx()
	defer cancel()
	return m.sub.conn.drv.Exec(ctx, m.sub.conn.q.term, m.consumerID, m.ID, m.sub.name, reason)
}

// InProgress extends the ack lease by the subscription's AckWait.
func (m *Msg) InProgress() error {
	if m.sub == nil {
		return ErrNotBound
	}
	if m.state.Load() {
		return ErrAlreadyAcked
	}
	ctx, cancel := m.opCtx()
	defer cancel()
	return m.sub.conn.drv.Exec(ctx, m.sub.conn.q.inProgress, m.consumerID, m.ID, m.Delivery, m.sub.opts.ackWait.Milliseconds())
}

// Respond publishes data to the message's reply subject.
func (m *Msg) Respond(ctx context.Context, data []byte) error {
	if m.Reply == "" {
		return ErrNoReply
	}
	if m.sub == nil {
		return ErrNotBound
	}
	return m.sub.conn.Publish(ctx, m.Reply, data)
}
