package pats

import (
	"context"
	"time"
)

const janitorBatch = 5000

// heartbeatLoop keeps this Conn's non-durable consumers alive. Consumers that
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
	owners := make(map[int64]*Subscription, len(c.byConsume))
	ids := make([]int64, 0, len(c.byConsume))
	for id, subs := range c.byConsume {
		if len(subs) > 0 && !subs[0].opts.isDurable {
			owners[id] = subs[0]
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

	// A consumer can be reaped while its subscribers are alive, e.g. if
	// heartbeats failed for longer than the TTL. Re-register it so the
	// subscriptions keep working; messages published in the gap are lost,
	// as for any ephemeral subscription.
	for id, s := range owners {
		if alive[id] {
			continue
		}
		newID, err := c.upsertConsumer(ctx, s.name, s.subject, false)
		if err != nil {
			return err
		}
		c.opts.logger.Warn("pats: consumer was reaped while in use; re-registered", "consumer", s.name)
		c.mu.Lock()
		subs := c.byConsume[id]
		delete(c.byConsume, id)
		c.byConsume[newID] = append(c.byConsume[newID], subs...)
		for _, x := range subs {
			x.consumerID.Store(newID)
			x.signal()
		}
		c.mu.Unlock()
	}
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
	c.deleteBatches(ctx, "reap messages", c.q.reapOrphanMessages, janitorBatch)

	if c.opts.deadLetterMaxAge > 0 {
		_, err := c.deleteCount(ctx, c.q.deadLetterRetention, c.opts.deadLetterMaxAge.Milliseconds())
		c.logError("dead letter retention", err)
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
