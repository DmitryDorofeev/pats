# pats

NATS-style publish/subscribe for Go, backed only by PostgreSQL. The name is short for **P**ostgres + N**ATS**.

> pats is inspired by the [nats.go](https://github.com/nats-io/nats.go) API. It is not affiliated with the NATS project and does not speak the NATS protocol.

- `Publish`, `Subscribe` (fan-out), `QueueSubscribe` (competing consumers), `Request`/`Respond`
- NATS subject wildcards: `orders.*`, `orders.>`
- At-least-once delivery with `Ack` / `Nak(delay)` / `Term` / `InProgress`, a configurable lease (`AckWait`), `MaxDeliver` and a dead-letter table
- Durable consumers that keep messages while nobody is subscribed
- Transactional publish (outbox): the message is delivered only if your transaction commits
- Works with **pgx** (`pgxpool`) and **database/sql** (pgx stdlib or lib/pq)

## How it works

Messages are rows. Every subscription is backed by a *consumer* row. An `AFTER INSERT` trigger on `messages` creates one `deliveries` row per matching consumer and calls `pg_notify(<schema>, <consumer id>)`.

Each `Conn` keeps one connection in `LISTEN`. A notification only *wakes* the matching subscriptions. They then claim work with `SELECT … FOR UPDATE SKIP LOCKED`, which sets a lease (`visible_at`). Ack deletes the delivery. If a message isn't acked before its lease runs out, it is delivered again.

A slow poll (default 5s) is a safety net that also picks up expired leases. After a lost listener connection, every subscription re-syncs.

| API | Consumer |
|---|---|
| `Subscribe(ctx, subj, cb)` | private, removed on `Unsubscribe` |
| `QueueSubscribe(ctx, subj, "q", cb)` | shared by all members of `q`, in every process |
| `…, pats.Durable("name")` | persistent; survives restarts |

Non-durable consumers are kept alive by heartbeats. The janitor removes them once they have no heartbeat for the TTL (default 60s). The janitor runs on one `Conn` per schema at a time, chosen by an advisory lock. It also deletes fully processed messages and applies the optional retention settings.

## Usage

```go
pool, _ := pgxpool.New(ctx, dsn)
nc, err := pats.Connect(ctx, pgxdriver.New(pool)) // installs schema "pats"
defer nc.Close()

nc.Subscribe(ctx, "orders.*", func(m *pats.Msg) {
    // returning without calling Ack/Nak/Term auto-acks; a panic naks
})

nc.QueueSubscribe(ctx, "orders.>", "workers", handle,
    pats.MaxInFlight(8), pats.AckWait(time.Minute), pats.MaxDeliver(5))

nc.Publish(ctx, "orders.created", data)

tx, _ := pool.Begin(ctx)
nc.PublishTx(ctx, pgxdriver.Tx(tx), &pats.Msg{Subject: "orders.paid", Data: data})
tx.Commit(ctx)

reply, err := nc.Request(ctxWithTimeout, "svc.echo", data) // responder: m.Respond(ctx, data)
```

database/sql:

```go
db, _ := sql.Open("pgx", dsn)                       // github.com/jackc/pgx/v5/stdlib
nc, _ := pats.Connect(ctx, sqldriver.New(db))

db, _ := sql.Open("postgres", dsn)                  // github.com/lib/pq
nc, _ := pats.Connect(ctx, sqldriver.New(db, sqldriver.WithListener(pqlisten.Factory(dsn))))

nc.PublishTx(ctx, sqldriver.Tx(sqlTx), msg)
```

See [examples/basic](examples/basic/main.go) for a runnable program.

## Options

Conn options:

| Option | Default | Purpose |
|---|---|---|
| `WithSchema(name)` | `pats` | Schema for the tables. Also used as the NOTIFY channel. |
| `WithoutMigrate()` | | Don't install the schema. Use `SchemaSQL(schema)` with your own migration tool instead. |
| `WithPollInterval(d)` | 5s | Safety-net polling interval. |
| `WithHeartbeat(interval, ttl)` | 10s, 60s | How often ephemeral consumers send heartbeats, and how long they live without one. |
| `WithJanitorInterval(d)` / `WithoutJanitor()` | 30s | How often cleanup runs, or turn it off. |
| `WithRetention(maxAge)` | off | Drop messages older than `maxAge` even if they haven't been processed. |
| `WithDeadLetterRetention(maxAge)` | off | Prune old dead letters. |
| `WithLogger(*slog.Logger)` | `slog.Default()` | Logger for background errors. |

Subscription options:

| Option | Default |
|---|---|
| `Durable(name)` | off |
| `MaxInFlight(n)` | 1 |
| `AckWait(d)` | 30s |
| `MaxDeliver(n)` | 0 (unlimited) |
| `ManualAck()` | off (auto-ack) |
| `SubPollInterval(d)` | the Conn's poll interval |

## Semantics and caveats

- **At-least-once.** Handlers must be idempotent. A message can be delivered again after a lease expires, a crash, or a `Nak`.
- **Ordering.** Each consumer claims messages in id order. With `MaxInFlight > 1` or several queue members they are processed concurrently, so there is no strict ordering.
- **Ephemeral subscriptions** only see messages published after the subscription was created. That holds for plain `Subscribe` and for non-durable queue groups. A queue group keeps receiving messages for up to the TTL after its last member leaves.
- **Every `Conn` receives every notification on the schema's channel** and filters them locally. For very high publish rates, use separate schemas to partition traffic.
- **Message cleanup.** Acking usually deletes a processed message right away. When several consumers ack the same message at the same moment, the janitor deletes it later instead.

## Testing

```sh
go test -race ./...                          # starts postgres:17 via testcontainers (Docker)
PATS_TEST_DSN=postgres://... go test ./... # or use an existing database
```

The behaviour suite in `internal/testsuite` runs against pgx, database/sql with pgx stdlib, and database/sql with lib/pq. Every test runs in its own schema.
