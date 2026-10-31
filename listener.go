package pats

import (
	"strconv"
	"time"
)

// listenLoop routes notifications to subscriptions. Payloads are consumer
// ids. When the listener fails, it reconnects with backoff and wakes every
// subscription, since notifications may have been missed in between.
func (c *Conn) listenLoop(l Listener) {
	defer c.bg.Done()
	backoff := c.opts.reconnectMin
	for {
		if l == nil {
			var err error
			l, err = c.drv.Listen(c.ctx, c.schema)
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				c.logError("listen", err, "retry_in", backoff)
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, c.opts.reconnectMax)
				continue
			}
			backoff = c.opts.reconnectMin
			c.wakeAll()
		}

		payload, err := l.Wait(c.ctx)
		if err != nil {
			_ = l.Close()
			l = nil
			if c.ctx.Err() != nil {
				return
			}
			c.logError("listener lost, reconnecting", err)
			continue
		}
		id, err := strconv.ParseInt(payload, 10, 64)
		if err != nil {
			continue
		}
		c.wake(id)
	}
}
