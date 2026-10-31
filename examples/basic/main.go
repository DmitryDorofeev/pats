// Command basic demonstrates pats with pgx.
//
//	DATABASE_URL=postgres://user:pass@localhost:5432/db go run ./examples/basic
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dmitrydorofeev/pats"
	"github.com/dmitrydorofeev/pats/pgxdriver"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	nc, err := pats.Connect(ctx, pgxdriver.New(pool))
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Fan-out: every subscription sees every matching message.
	_, err = nc.Subscribe(ctx, "orders.*", func(m *pats.Msg) {
		fmt.Printf("[audit] %s: %s\n", m.Subject, m.Data)
	})
	if err != nil {
		log.Fatal(err)
	}

	// Queue group: each message goes to one worker, across all processes.
	for i := 1; i <= 3; i++ {
		_, err = nc.QueueSubscribe(ctx, "orders.>", "fulfilment", func(m *pats.Msg) {
			fmt.Printf("[worker %d] %s: %s (delivery %d)\n", i, m.Subject, m.Data, m.Delivery)
		}, pats.MaxInFlight(4), pats.MaxDeliver(5))
		if err != nil {
			log.Fatal(err)
		}
	}

	// Request/reply.
	_, err = nc.QueueSubscribe(ctx, "time.now", "clock", func(m *pats.Msg) {
		_ = m.Respond(ctx, []byte(time.Now().Format(time.RFC3339)))
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		if err := nc.Publish(ctx, "orders.created", []byte(fmt.Sprintf("order-%d", i))); err != nil {
			log.Fatal(err)
		}
	}

	// Transactional publish: the message is sent only if the tx commits.
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := nc.PublishTx(ctx, pgxdriver.Tx(tx), &pats.Msg{Subject: "orders.paid", Data: []byte("order-1")}); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	reply, err := nc.Request(rctx, "time.now", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("[request] server time:", string(reply.Data))

	time.Sleep(time.Second)
}
