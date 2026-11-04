# pats

<p align="center"><img src="pats.gif" alt="An animated cartoon puppy being patted on the head" width="320"></p>

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

Each `Conn` keeps one connection in `LISTEN`. A notification only *wakes* the matching subscriptions. They then claim work with `SELECT … FOR UPDATE SKIP LOCKED`, which sets a lease (`visible_at`). Ack deletes the delivery, and the last ack of a message deletes the message. If a message isn't acked before its lease runs out, it is delivered again. A message that matches no consumer when it is published is not stored.

A slow poll (default 5s) is a safety net that also picks up expired leases. After a lost listener connection, every subscription re-syncs.

| API | Consumer |
|---|---|
| `Subscribe(ctx, subj, cb)` | private, removed on `Unsubscribe` |
| `QueueSubscribe(ctx, subj, "q", cb)` | shared by all members of `q`, in every process |
| `…, pats.Durable("name")` | persistent; survives restarts |

Non-durable consumers are kept alive by heartbeats. The janitor removes them once they have no heartbeat for the TTL (default 60s). The janitor runs on one `Conn` per schema at a time, chosen by an advisory lock. It also deletes messages left behind when a consumer is removed (scanning a bounded batch per run) and applies the optional retention settings.

## Usage

```go
pool, _ := pgxpool.New(ctx, dsn)
drv := pgxdriver.New(pool)

// Install or upgrade the schema first; Connect never migrates.
migrator, _ := patsmigrate.New(drv, nil) // schema "pats"
migrator.Migrate(ctx, patsmigrate.DirectionUp, nil)

nc, err := pats.Connect(ctx, drv)
defer nc.Close()

nc.Subscribe(ctx, "orders.*", func(m *pats.Msg) {
    // returning without calling Ack/Nak/Term auto-acks; a panic naks with backoff
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
nc, _ := pats.Connect(ctx, sqldriver.New(db, sqldriver.WithListener(
    pqlisten.Factory(dsn, pqlisten.WithLogger(logger)))))

nc.PublishTx(ctx, sqldriver.Tx(sqlTx), msg)
```

See [examples/basic](examples/basic/main.go) for a runnable program.

## Migrations

The schema is versioned and managed by package `patsmigrate`, modelled on River's `rivermigrate`. `pats.Connect` does not create or change tables: run migrations at deploy time, or from your application before connecting.

Migrations are numbered `NNN_name.up.sql` / `NNN_name.down.sql` pairs embedded in the binary. Applied versions are recorded in `<schema>.pats_migration`. Each version runs in its own transaction together with its version row. When the schema is already current, `Migrate` only reads `pats_migration` and takes no locks on pats tables. Migrators running at the same time from several processes are serialised by an advisory lock, and each version is applied once.

```go
m, err := patsmigrate.New(drv, &patsmigrate.Config{Schema: "pats"}) // any pats.Querier
res, err := m.Migrate(ctx, patsmigrate.DirectionUp, nil)             // all pending versions
res, err = m.Migrate(ctx, patsmigrate.DirectionDown, nil)            // one step down (the default)
res, err = m.Migrate(ctx, patsmigrate.DirectionDown,
    &patsmigrate.MigrateOpts{TargetVersion: -1})                     // remove everything
vr, err := m.Validate(ctx, nil)                                      // vr.OK is false while versions are pending
```

pats is unreleased, so the whole schema is currently a single migration, `001_initial_schema`. Later schema changes will be added as `002_…` and so on.

`MigrateOpts` also has `MaxSteps`, `TargetVersion` and `DryRun`. If you use another migration tool (Goose, Atlas, …), copy the SQL from `m.AllVersions()` and pass it through `m.Render(sql)` to fill in the schema name.

The CLI does the same:

```sh
go run github.com/dmitrydorofeev/pats/cmd/pats migrate-up   --database-url "$DATABASE_URL"
go run github.com/dmitrydorofeev/pats/cmd/pats migrate-list  # * marks applied versions
go run github.com/dmitrydorofeev/pats/cmd/pats validate      # non-zero exit if versions are pending
go run github.com/dmitrydorofeev/pats/cmd/pats migrate-down --target-version -1
go run github.com/dmitrydorofeev/pats/cmd/pats migrate-get --version 1 --up --schema pats
```

## Options

Conn options:

| Option | Default | Purpose |
|---|---|---|
| `WithSchema(name)` | `pats` | Schema for the tables. Also used as the NOTIFY channel. Must match `patsmigrate.Config.Schema`. |
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

## Acknowledgements

Every `Msg` method has a variant that takes a context (`AckCtx`, `NakCtx`, `TermCtx`, `InProgressCtx`). The plain forms use a 30s timeout.

`Nak`, `Term` and `InProgress` only act on the delivery you hold. If its lease (`AckWait`) has already expired and the message was handed out again, they return `ErrLeaseLost` and change nothing. A late `Term` therefore can't dead-letter a message that another worker is processing. A late `Ack` still removes the message, because it was processed.

If a handler panics, the message is naked with a backoff of 100ms that doubles with each delivery, capped at `AckWait`. Set `MaxDeliver` to dead-letter poison messages eventually.

## Semantics and caveats

- **PgBouncer.** Transaction pooling is not supported: `pats` relies on a dedicated PostgreSQL session for `LISTEN` notifications. Use session pooling or connect directly to PostgreSQL.
- **At-least-once.** Handlers must be idempotent. A message can be delivered again after a lease expires, a crash, or a `Nak`.
- **Ordering.** Each consumer claims messages in id order. With `MaxInFlight > 1` or several queue members they are processed concurrently, so there is no strict ordering.
- **Ephemeral subscriptions** only see messages published after the subscription was created. That holds for plain `Subscribe` and for non-durable queue groups. A queue group keeps receiving messages for up to the TTL after its last member leaves.
- **Every `Conn` receives every notification on the schema's channel** and filters them locally. For very high publish rates, use separate schemas to partition traffic.
- **Consumer names are shared between queues and durables.** `QueueSubscribe(…, "x", …)` and `Subscribe(…, Durable("x"))` attach to the same consumer `x`, so they compete for its messages. Use distinct names.
- **`DeleteConsumer`** stops this `Conn`'s subscriptions on the consumer. Subscriptions in other processes notice at their next heartbeat: durable ones stop, and non-durable ones re-create the consumer.
- **`Request`** uses one inbox subscription per `Conn`, created on first use. Each request then costs a single publish.

## Testing

```sh
go test -race ./...                                        # unit tests, no database
cd integration && go test -race ./...                      # starts postgres:17 via testcontainers (Docker)
cd integration && PATS_TEST_DSN=postgres://... go test ./... # or use an existing database
```

The behaviour suite in `integration/testsuite` runs against pgx, database/sql with pgx stdlib, and database/sql with lib/pq. Every test runs in its own schema. It lives in a separate module so that testcontainers isn't a dependency of the library.

## License

MIT. See [LICENSE](LICENSE).
