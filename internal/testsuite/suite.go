// Package testsuite holds behavioural tests that every pats driver must
// pass. Driver packages call Run with a Factory.
package testsuite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dmitrydorofeev/pats"
)

// Harness is what a driver package provides to the suite.
type Harness struct {
	Driver pats.Driver
	// Begin starts a transaction and returns it adapted for PublishTx.
	Begin func(ctx context.Context) (q pats.Querier, commit, rollback func() error, err error)
}

// Factory builds a Harness for dsn. Resources should be released with
// t.Cleanup.
type Factory func(t *testing.T, dsn string) Harness

var (
	dsnOnce sync.Once
	dsnVal  string
	dsnErr  error
)

// DSN returns the test database: $PATS_TEST_DSN if set, otherwise a
// PostgreSQL container started once per test binary.
func DSN(t testing.TB) string {
	t.Helper()
	dsnOnce.Do(func() {
		if v := os.Getenv("PATS_TEST_DSN"); v != "" {
			dsnVal = v
			return
		}
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("pats"),
			tcpostgres.WithUsername("pats"),
			tcpostgres.WithPassword("pats"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			dsnErr = err
			return
		}
		dsnVal, dsnErr = c.ConnectionString(ctx, "sslmode=disable")
	})
	if dsnErr != nil {
		t.Skipf("no test database (set PATS_TEST_DSN or run Docker): %v", dsnErr)
	}
	return dsnVal
}

type env struct {
	t      *testing.T
	h      Harness
	schema string
	admin  *pgxpool.Pool
}

func newEnv(t *testing.T, f Factory) *env {
	t.Helper()
	dsn := DSN(t)
	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var b [6]byte
	_, _ = rand.Read(b[:])
	e := &env{t: t, h: f(t, dsn), schema: "t_" + hex.EncodeToString(b[:]), admin: admin}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+e.schema+" CASCADE")
	})
	return e
}

// connect opens a Conn on the test schema. By default polling is effectively
// off, so tests exercise the LISTEN/NOTIFY wake-up path.
func (e *env) connect(opts ...pats.Option) *pats.Conn {
	e.t.Helper()
	all := append([]pats.Option{
		pats.WithSchema(e.schema),
		pats.WithPollInterval(time.Hour),
	}, opts...)
	c, err := pats.Connect(context.Background(), e.h.Driver, all...)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = c.Close() })
	return c
}

func (e *env) count(query string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.admin.QueryRow(context.Background(), fmt.Sprintf(query, e.schema), args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out: "+msg, args...)
}

func recv(t *testing.T, ch <-chan *pats.Msg, timeout time.Duration) *pats.Msg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(timeout):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func noRecv(t *testing.T, ch <-chan *pats.Msg, d time.Duration) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected message on %s: %q", m.Subject, m.Data)
	case <-time.After(d):
	}
}

func chanHandler(n int) (chan *pats.Msg, pats.MsgHandler) {
	ch := make(chan *pats.Msg, n)
	return ch, func(m *pats.Msg) { ch <- m }
}

var ctx = context.Background()

// Run executes the whole suite against the driver built by f.
func Run(t *testing.T, f Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"SubjectMatchesSQL", testSubjectMatchesSQL},
		{"FanOut", testFanOut},
		{"QueueGroupExactlyOnce", testQueueGroup},
		{"NakRedelivery", testNak},
		{"AckWaitRedelivery", testAckWait},
		{"MaxDeliverDeadLetter", testMaxDeliver},
		{"TermAndPanic", testTermAndPanic},
		{"TransactionalPublish", testTxPublish},
		{"RequestReply", testRequestReply},
		{"DurableOffline", testDurable},
		{"FilterMismatch", testFilterMismatch},
		{"UnsubscribeRemovesConsumer", testUnsubscribe},
		{"ListenerReconnect", testListenerReconnect},
		{"Janitor", testJanitor},
		{"HeartbeatKeepsConsumerAlive", testHeartbeat},
		{"ReapedConsumerReregistered", testReregister},
		{"Drain", testDrain},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, f)
		})
	}
}

func testSubjectMatchesSQL(t *testing.T, f Factory) {
	e := newEnv(t, f)
	e.connect()
	cases := []struct {
		p, s string
		want bool
	}{
		{"a", "a", true}, {"a", "b", false}, {"a.b", "a", false}, {"a", "a.b", false},
		{"a.*", "a.b", true}, {"a.*", "a", false}, {"a.*", "a.b.c", false}, {"*", "a", true},
		{"*", "a.b", false}, {"a.>", "a.b", true}, {"a.>", "a.b.c", true}, {"a.>", "a", false},
		{">", "a", true}, {">", "a.b.c", true}, {"a.*.>", "a.b.c", true}, {"a.*.>", "a.b", false},
		{"a.*.c", "a.x.c", true}, {"a.*.c", "a.x.d", false},
	}
	for _, c := range cases {
		var got bool
		err := e.admin.QueryRow(ctx, "SELECT "+e.schema+".subject_matches($1, $2)", c.p, c.s).Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want || got != pats.SubjectMatches(c.p, c.s) {
			t.Errorf("subject_matches(%q, %q) = %v, want %v", c.p, c.s, got, c.want)
		}
	}
}

func testFanOut(t *testing.T, f Factory) {
	e := newEnv(t, f)
	// Concurrent acks from several consumers can leave a message for the
	// janitor, so run it often.
	c := e.connect(pats.WithJanitorInterval(100 * time.Millisecond))
	var chans []chan *pats.Msg
	for i := 0; i < 3; i++ {
		ch, h := chanHandler(10)
		if _, err := c.Subscribe(ctx, "a.*", h); err != nil {
			t.Fatal(err)
		}
		chans = append(chans, ch)
	}
	other, h := chanHandler(10)
	if _, err := c.Subscribe(ctx, "b.>", h); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := c.Publish(ctx, "a.x", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, ch := range chans {
		for i := 0; i < 3; i++ {
			m := recv(t, ch, 5*time.Second)
			if m.Subject != "a.x" || string(m.Data) != fmt.Sprint(i) || m.Delivery != 1 {
				t.Fatalf("got %+v, want a.x/%d", m, i)
			}
		}
	}
	noRecv(t, other, 200*time.Millisecond)
	// All deliveries acked, so the messages are removed.
	eventually(t, 5*time.Second, func() bool { return e.count("SELECT count(*) FROM %s.messages") == 0 },
		"messages not cleaned up after ack")
}

func testQueueGroup(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c1, c2 := e.connect(), e.connect()
	const total = 500
	var (
		mu    sync.Mutex
		seen  = map[string]int{}
		count atomic.Int64
	)
	h := func(m *pats.Msg) {
		mu.Lock()
		seen[string(m.Data)]++
		mu.Unlock()
		count.Add(1)
	}
	for i := 0; i < 5; i++ {
		c := c1
		if i%2 == 1 {
			c = c2
		}
		if _, err := c.QueueSubscribe(ctx, "jobs.>", "workers", h, pats.MaxInFlight(4)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < total; i++ {
		if err := c1.Publish(ctx, "jobs.run", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 20*time.Second, func() bool { return count.Load() >= total }, "processed %d/%d", count.Load(), total)
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total || count.Load() != total {
		t.Fatalf("processed %d messages, %d distinct; want %d each", count.Load(), len(seen), total)
	}
	for k, v := range seen {
		if v != 1 {
			t.Fatalf("message %s processed %d times", k, v)
		}
	}
}

func testNak(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	ch := make(chan *pats.Msg, 4)
	_, err := c.Subscribe(ctx, "nak", func(m *pats.Msg) {
		if m.Delivery == 1 {
			if err := m.Nak(300 * time.Millisecond); err != nil {
				t.Error(err)
			}
		}
		ch <- m
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := c.Publish(ctx, "nak", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, ch, 5*time.Second); m.Delivery != 1 {
		t.Fatalf("first delivery = %d", m.Delivery)
	}
	m := recv(t, ch, 5*time.Second)
	if m.Delivery != 2 {
		t.Fatalf("second delivery = %d", m.Delivery)
	}
	if el := time.Since(start); el < 250*time.Millisecond {
		t.Fatalf("redelivered after %v, before Nak delay", el)
	}
}

func testAckWait(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	ch, h := chanHandler(4)
	_, err := c.Subscribe(ctx, "slow", h, pats.ManualAck(), pats.AckWait(300*time.Millisecond),
		pats.SubPollInterval(100*time.Millisecond), pats.MaxInFlight(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, "slow", nil); err != nil {
		t.Fatal(err)
	}
	recv(t, ch, 5*time.Second) // never acked
	m := recv(t, ch, 5*time.Second)
	if m.Delivery != 2 {
		t.Fatalf("delivery = %d, want 2", m.Delivery)
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := m.Ack(); !errors.Is(err, pats.ErrAlreadyAcked) {
		t.Fatalf("second Ack = %v", err)
	}
	noRecv(t, ch, 600*time.Millisecond)
}

func testMaxDeliver(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	var calls atomic.Int64
	_, err := c.QueueSubscribe(ctx, "md", "q", func(m *pats.Msg) {
		calls.Add(1)
		_ = m.Nak(0)
	}, pats.MaxDeliver(3))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, "md", []byte("poison")); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		return e.count("SELECT count(*) FROM %s.dead_letters WHERE consumer_name = 'q' AND attempts = 4") == 1
	}, "message not dead-lettered")
	if n := calls.Load(); n != 3 {
		t.Fatalf("handler called %d times, want 3", n)
	}
	if n := e.count("SELECT count(*) FROM %s.deliveries"); n != 0 {
		t.Fatalf("%d deliveries left", n)
	}
}

func testTermAndPanic(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	var panics atomic.Int64
	done := make(chan struct{})
	_, err := c.Subscribe(ctx, "tp.*", func(m *pats.Msg) {
		switch m.Subject {
		case "tp.term":
			_ = m.Term("bad payload")
		case "tp.panic":
			if m.Delivery == 1 {
				panics.Add(1)
				panic("boom")
			}
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Publish(ctx, "tp.term", nil)
	_ = c.Publish(ctx, "tp.panic", nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("panicked message was not redelivered")
	}
	eventually(t, 5*time.Second, func() bool {
		return e.count("SELECT count(*) FROM %s.dead_letters WHERE reason = 'bad payload'") == 1
	}, "termed message not dead-lettered")
}

func testTxPublish(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	ch, h := chanHandler(4)
	if _, err := c.Subscribe(ctx, "tx", h); err != nil {
		t.Fatal(err)
	}

	q, _, rollback, err := e.h.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PublishTx(ctx, q, &pats.Msg{Subject: "tx", Data: []byte("rolled back")}); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}

	q, commit, _, err := e.h.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PublishTx(ctx, q, &pats.Msg{Subject: "tx", Data: []byte("committed")}); err != nil {
		t.Fatal(err)
	}
	noRecv(t, ch, 200*time.Millisecond) // not visible before commit
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, ch, 5*time.Second); string(m.Data) != "committed" {
		t.Fatalf("got %q", m.Data)
	}
	noRecv(t, ch, 200*time.Millisecond)
}

func testRequestReply(t *testing.T, f Factory) {
	e := newEnv(t, f)
	server, client := e.connect(), e.connect()
	_, err := server.QueueSubscribe(ctx, "svc.echo", "echo", func(m *pats.Msg) {
		if err := m.Respond(ctx, append([]byte("echo: "), m.Data...)); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reply, err := client.Request(rctx, "svc.echo", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if string(reply.Data) != "echo: hi" {
		t.Fatalf("reply = %q", reply.Data)
	}

	rctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := client.Request(rctx, "svc.nobody", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Request without responder = %v", err)
	}
	// Inbox consumers are removed after each request.
	if n := e.count("SELECT count(*) FROM %s.consumers WHERE filter LIKE '_INBOX.%%'"); n != 0 {
		t.Fatalf("%d inbox consumers left", n)
	}
}

func testDurable(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c1 := e.connect()
	ch, h := chanHandler(10)
	if _, err := c1.Subscribe(ctx, "d.*", h, pats.Durable("audit")); err != nil {
		t.Fatal(err)
	}
	_ = c1.Publish(ctx, "d.1", []byte("1"))
	recv(t, ch, 5*time.Second)
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	pub := e.connect()
	for i := 2; i <= 4; i++ {
		_ = pub.Publish(ctx, "d.x", []byte(fmt.Sprint(i)))
	}
	c2 := e.connect()
	if _, err := c2.Subscribe(ctx, "d.*", h, pats.Durable("audit")); err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 4; i++ {
		if m := recv(t, ch, 5*time.Second); string(m.Data) != fmt.Sprint(i) {
			t.Fatalf("got %q, want %d", m.Data, i)
		}
	}
	if err := c2.DeleteConsumer(ctx, "audit"); err != nil {
		t.Fatal(err)
	}
}

func testFilterMismatch(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	if _, err := c.QueueSubscribe(ctx, "a.*", "g", func(*pats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.QueueSubscribe(ctx, "b.*", "g", func(*pats.Msg) {}); !errors.Is(err, pats.ErrFilterMismatch) {
		t.Fatalf("err = %v, want ErrFilterMismatch", err)
	}
	if _, err := c.Subscribe(ctx, "a.>.b", func(*pats.Msg) {}); !errors.Is(err, pats.ErrBadSubject) {
		t.Fatalf("err = %v, want ErrBadSubject", err)
	}
	if err := c.Publish(ctx, "a.*", nil); !errors.Is(err, pats.ErrBadSubject) {
		t.Fatalf("publish wildcard err = %v", err)
	}
}

func testUnsubscribe(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	ch, h := chanHandler(4)
	sub, err := c.Subscribe(ctx, "u", h)
	if err != nil {
		t.Fatal(err)
	}
	if n := e.count("SELECT count(*) FROM %s.consumers"); n != 1 {
		t.Fatalf("consumers = %d", n)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if n := e.count("SELECT count(*) FROM %s.consumers"); n != 0 {
		t.Fatalf("consumers after unsubscribe = %d", n)
	}
	_ = c.Publish(ctx, "u", nil)
	noRecv(t, ch, 200*time.Millisecond)
}

func (e *env) listenerPID() int {
	var pid int
	err := e.admin.QueryRow(ctx,
		"SELECT coalesce(max(pid), 0) FROM pg_stat_activity WHERE query ILIKE 'LISTEN %' AND query LIKE '%' || $1 || '%'",
		e.schema).Scan(&pid)
	if err != nil {
		e.t.Fatal(err)
	}
	return pid
}

func testListenerReconnect(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	ch, h := chanHandler(4)
	if _, err := c.Subscribe(ctx, "r", h); err != nil {
		t.Fatal(err)
	}
	pid := e.listenerPID()
	if pid == 0 {
		t.Fatal("listener backend not found")
	}
	if _, err := e.admin.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	// A message published while the listener is down is caught up on
	// reconnect.
	_ = c.Publish(ctx, "r", []byte("during"))
	recv(t, ch, 15*time.Second)

	eventually(t, 15*time.Second, func() bool {
		p := e.listenerPID()
		return p != 0 && p != pid
	}, "listener did not reconnect")
	_ = c.Publish(ctx, "r", []byte("after"))
	if m := recv(t, ch, 5*time.Second); string(m.Data) != "after" {
		t.Fatalf("got %q", m.Data)
	}
}

func testJanitor(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect(pats.WithJanitorInterval(100*time.Millisecond), pats.WithHeartbeat(50*time.Millisecond, 500*time.Millisecond))
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.consumers (name, filter, last_seen) VALUES ('stale', 'j.*', now() - interval '1 hour')", e.schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.consumers (name, filter, durable, last_seen) VALUES ('keep', 'nothing', true, now() - interval '1 hour')", e.schema)); err != nil {
		t.Fatal(err)
	}
	_ = c.Publish(ctx, "j.1", nil)  // delivery for 'stale'
	_ = c.Publish(ctx, "none", nil) // matches nobody
	eventually(t, 5*time.Second, func() bool {
		return e.count("SELECT count(*) FROM %s.consumers WHERE name = 'stale'") == 0 &&
			e.count("SELECT count(*) FROM %s.messages") == 0
	}, "janitor did not clean up")
	if n := e.count("SELECT count(*) FROM %s.consumers WHERE name = 'keep'"); n != 1 {
		t.Fatal("durable consumer was reaped")
	}
}

func testHeartbeat(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect(pats.WithJanitorInterval(100*time.Millisecond), pats.WithHeartbeat(50*time.Millisecond, 400*time.Millisecond))
	ch, h := chanHandler(4)
	if _, err := c.QueueSubscribe(ctx, "hb", "hbq", h); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	_ = c.Publish(ctx, "hb", nil)
	recv(t, ch, 5*time.Second)
}

func testDrain(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect()
	release := make(chan struct{})
	var finished atomic.Bool
	if _, err := c.QueueSubscribe(ctx, "dr", "drq", func(m *pats.Msg) {
		<-release
		finished.Store(true)
	}); err != nil {
		t.Fatal(err)
	}
	_ = c.Publish(ctx, "dr", nil)
	eventually(t, 5*time.Second, func() bool {
		return e.count("SELECT count(*) FROM %s.deliveries WHERE attempts = 1") == 1
	}, "message not claimed")
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(release)
	}()
	if err := c.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("Drain returned before the handler finished")
	}
	if n := e.count("SELECT count(*) FROM %s.deliveries"); n != 0 {
		t.Fatalf("message not acked during drain: %d deliveries", n)
	}
	if err := c.Publish(ctx, "dr", nil); !errors.Is(err, pats.ErrClosed) {
		t.Fatalf("publish after drain = %v", err)
	}
}

func testReregister(t *testing.T, f Factory) {
	e := newEnv(t, f)
	c := e.connect(pats.WithoutJanitor(), pats.WithHeartbeat(50*time.Millisecond, time.Minute))
	ch, h := chanHandler(4)
	if _, err := c.Subscribe(ctx, "rr", h); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, fmt.Sprintf("DELETE FROM %s.consumers", e.schema)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		return e.count("SELECT count(*) FROM %s.consumers") == 1
	}, "consumer not re-registered")
	_ = c.Publish(ctx, "rr", []byte("back"))
	if m := recv(t, ch, 5*time.Second); string(m.Data) != "back" {
		t.Fatalf("got %q", m.Data)
	}
}
