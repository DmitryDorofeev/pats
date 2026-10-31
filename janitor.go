package pats

import (
	"context"
	"slices"
	"time"
)

const (
	janitorBatch       = 5000
	janitorReapBatches = 10
)

// heartbeatLoop keeps this Conn's non-durable consumers alive and notices
// consumers that disappeared. Consumers that
// stop heartbeating (crashed processes, abandoned queue groups) are removed
// by the janitor after the consumer TTL.
func (c *Conn) heartbeatLoop() {
	defer c.bg.Done()
	t := time.NewTicker(c.opts.heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(c.ctx, max(c.opts.consumerTTL/2, time.Second))
		c.logError("heartbeat", c.heartbeat(ctx))
		cancel()
	}
}

func (c *Conn) heartbeat(ctx context.Context) error {
	c.mu.Lock()
	ids := make([]int64, 0, len(c.byConsume))
	for id, subs := range c.byConsume {
		if len(subs) > 0 {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()
	if len(ids) == 0 {
		return nil
	}

	rows, err := c.drv.Query(ctx, c.q.heartbeat, joinIDs(ids))
	if err != nil {
		return err
	}
	alive := make(map[int64]bool, len(ids))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		alive[id] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, id := range ids {
		if !alive[id] {
			if err := c.consumerGone(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

// consumerGone handles a consumer row that disappeared while local
// subscriptions still use it.
//
// A non-durable consumer can be reaped while its subscribers are alive, e.g.
// if heartbeats failed for longer than the TTL. It is re-registered so the
// subscriptions keep working; messages published in the gap are lost, as for
// any ephemeral subscription. Durable consumers are never reaped, so a missing
// one was deleted on purpose (DeleteConsumer): its subscriptions are stopped
// rather than undoing the delete.
func (c *Conn) consumerGone(ctx context.Context, id int64) error {
	c.mu.Lock()
	subs := slices.Clone(c.byConsume[id])
	c.mu.Unlock()
	if len(subs) == 0 {
		return nil // unsubscribed in the meantime
	}
	s := subs[0]

	if s.opts.isDurable {
		c.opts.logger.Error("pats: durable consumer was deleted while subscribed; stopping its subscriptions", "consumer", s.name)
		for _, x := range subs {
			x.stop()
		}
		return nil
	}

	newID, err := c.upsertConsumer(ctx, s.name, s.subject, false)
	if err != nil {
		return err
	}
	c.mu.Lock()
	subs = c.byConsume[id]
	if len(subs) == 0 {
		c.mu.Unlock()
		// Every subscription went away while we re-registered. Don't leave
		// behind a consumer that nobody reads; a shared queue consumer may
		// have members elsewhere, and is reaped by TTL if not.
		if s.ephemeral {
			return c.drv.Exec(ctx, c.q.deleteConsumer, newID)
		}
		return nil
	}
	delete(c.byConsume, id)
	c.byConsume[newID] = append(c.byConsume[newID], subs...)
	for _, x := range subs {
		x.consumerID.Store(newID)
		x.signal()
	}
	c.mu.Unlock()
	c.opts.logger.Warn("pats: consumer was reaped while in use; re-registered", "consumer", s.name)
	return nil
}

func (c *Conn) janitorLoop() {
	defer c.bg.Done()
	t := time.NewTicker(c.opts.janitorInterval)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		c.runJanitor(c.ctx)
	}
}

// runJanitor performs one cleanup pass.
func (c *Conn) runJanitor(ctx context.Context) {
	_, err := c.deleteCount(ctx, c.q.reapConsumers, c.opts.consumerTTL.Milliseconds())
	c.logError("reap consumers", err)

	if c.opts.retentionMaxAge > 0 {
		c.deleteBatches(ctx, "retention", c.q.retention, c.opts.retentionMaxAge.Milliseconds(), janitorBatch)
	}
	c.reapOrphans(ctx)

	if c.opts.deadLetterMaxAge > 0 {
		_, err := c.deleteCount(ctx, c.q.deadLetterRetention, c.opts.deadLetterMaxAge.Milliseconds())
		c.logError("dead letter retention", err)
	}
}

// reapOrphans deletes messages that no consumer holds a delivery for, which
// happens when a consumer is deleted or reaped. Each run scans at most
// janitorReapBatches batches, continuing where the previous run stopped and
// wrapping around at the end of the table, so the cost per run stays bounded
// however large the backlog of pending messages is.
func (c *Conn) reapOrphans(ctx context.Context) {
	for range janitorReapBatches {
		rows, err := c.drv.Query(ctx, c.q.reapOrphanMessages, c.reapCursor, janitorBatch)
		if err != nil {
			c.logError("reap messages", err)
			return
		}
		var last int64
		if rows.Next() {
			err = rows.Scan(&last)
		}
		if cerr := rows.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			c.logError("reap messages", err)
			return
		}
		c.reapCursor = last
		if last == 0 {
			return
		}
	}
}

// deleteBatches repeats a batched DELETE (whose last argument is the batch
// size) until it removes fewer rows than a full batch.
func (c *Conn) deleteBatches(ctx context.Context, what, stmt string, args ...any) {
	for ctx.Err() == nil {
		n, err := c.deleteCount(ctx, stmt, args...)
		if err != nil {
			c.logError(what, err)
			return
		}
		if n < janitorBatch {
			return
		}
	}
}

func (c *Conn) deleteCount(ctx context.Context, stmt string, args ...any) (int64, error) {
	rows, err := c.drv.Query(ctx, "WITH del AS ("+stmt+" RETURNING 1) SELECT count(*) FROM del", args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int64
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
	}
	return n, rows.Err()
}
