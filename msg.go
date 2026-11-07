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

// opTimeout bounds acknowledgement statements issued by the methods that
// take no context, and by pats itself.
const opTimeout = 30 * time.Second

func opCtx() (context.Context, context.CancelFunc) {
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

// Ack marks the message as processed for this consumer. It is AckCtx with a
// 30s timeout.
func (m *Msg) Ack() error {
	ctx, cancel := opCtx()
	defer cancel()
	return m.AckCtx(ctx)
}

// AckCtx marks the message as processed for this consumer. A late ack, after
// the lease expired and the message was redelivered, still removes it.
func (m *Msg) AckCtx(ctx context.Context) error {
	if err := m.settle(); err != nil {
		return err
	}
	ok, err := queryBool(ctx, m.sub.conn.drv, m.sub.conn.q.ack, m.consumerID, m.ID)
	if ok {
		m.sub.conn.release(ctx, m.ID)
	}
	return err
}

// Nak asks for redelivery after delay (zero means as soon as possible). It is
// NakCtx with a 30s timeout.
func (m *Msg) Nak(delay time.Duration) error {
	ctx, cancel := opCtx()
	defer cancel()
	return m.NakCtx(ctx, delay)
}

// NakCtx asks for redelivery after delay (zero means as soon as possible).
// It returns ErrLeaseLost if the lease already expired.
func (m *Msg) NakCtx(ctx context.Context, delay time.Duration) error {
	if err := m.settle(); err != nil {
		return err
	}
	ok, err := queryBool(ctx, m.sub.conn.drv, m.sub.conn.q.nak, m.consumerID, m.ID, m.Delivery, delay.Milliseconds())
	if err != nil {
		return err
	}
	if !ok {
		return ErrLeaseLost
	}
	m.sub.signalAfter(delay)
	return nil
}

// Term stops redelivery and moves the message to the dead-letter table with
// the given reason. It is TermCtx with a 30s timeout.
func (m *Msg) Term(reason string) error {
	ctx, cancel := opCtx()
	defer cancel()
	return m.TermCtx(ctx, reason)
}

// TermCtx stops redelivery and moves the message to the dead-letter table
// with the given reason. It returns ErrLeaseLost if the lease already
// expired; the message is then left to whoever holds it now.
func (m *Msg) TermCtx(ctx context.Context, reason string) error {
	if err := m.settle(); err != nil {
		return err
	}
	ok, err := queryBool(ctx, m.sub.conn.drv, m.sub.conn.q.term, m.consumerID, m.ID, m.Delivery, m.sub.name, reason)
	if err != nil {
		return err
	}
	if !ok {
		return ErrLeaseLost
	}
	m.sub.conn.release(ctx, m.ID)
	return nil
}

// InProgress extends the ack lease by the subscription's AckWait. It is
// InProgressCtx with a 30s timeout.
func (m *Msg) InProgress() error {
	ctx, cancel := opCtx()
	defer cancel()
	return m.InProgressCtx(ctx)
}

// InProgressCtx extends the ack lease by the subscription's AckWait. It
// returns ErrLeaseLost if the lease already expired.
func (m *Msg) InProgressCtx(ctx context.Context) error {
	if m.sub == nil {
		return ErrNotBound
	}
	if m.state.Load() {
		return ErrAlreadyAcked
	}
	ok, err := queryBool(ctx, m.sub.conn.drv, m.sub.conn.q.inProgress, m.consumerID, m.ID, m.Delivery, m.sub.opts.ackWait.Milliseconds())
	if err != nil {
		return err
	}
	if !ok {
		return ErrLeaseLost
	}
	return nil
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
