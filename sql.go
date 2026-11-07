package pats

import (
	"fmt"
	"strings"
)

// sqlText holds every runtime statement, rendered for one schema. Arguments
// are limited to strings, []byte, int64, int and bool so that pgx, pgx/stdlib
// and lib/pq all encode them the same way. Durations are passed as
// milliseconds.
type sqlText struct {
	publish              string
	upsertConsumer       string
	deleteConsumer       string
	deleteConsumerByName string
	fetch                string
	ack                  string
	release              string
	nak                  string
	inProgress           string
	term                 string
	heartbeat            string
	reapConsumers        string
	reapOrphanMessages   string
	retention            string
	deadLetterRetention  string
}

func newSQLText(schema string) sqlText {
	r := strings.NewReplacer("S.", schema+".", "{{lock}}", fmt.Sprintf("%d, hashtext('janitor.%s')", AdvisoryLockClass, schema))
	return sqlText{
		// Fans the message out to every matching consumer in one statement,
		// so it is atomic without a transaction of its own. A message that no
		// consumer wants is not stored. NOTIFY is only delivered on commit, so
		// publishing inside a caller's transaction is atomic with the
		// caller's other writes. $1 subject, $2 data, $3 reply, $4 channel.
		publish: r.Replace(`
WITH s AS (
    SELECT string_to_array($1::text, '.') AS t
), matched AS (
    SELECT c.id FROM S.consumers c
    WHERE NOT c.wild AND c.filter = $1::text
    UNION ALL
    SELECT c.id FROM S.consumers c, s
    WHERE c.wild
      AND CASE WHEN c.tail THEN cardinality(s.t) > cardinality(c.tokens)
               ELSE cardinality(s.t) = cardinality(c.tokens) END
      AND NOT EXISTS (
          SELECT 1 FROM unnest(c.tokens) WITH ORDINALITY AS p(tok, i)
          WHERE p.tok <> '*' AND p.tok <> s.t[p.i])
), m AS (
    INSERT INTO S.messages (subject, data, reply_to)
    SELECT $1::text, $2::bytea, NULLIF($3::text, '')
    WHERE EXISTS (SELECT 1 FROM matched)
    RETURNING id
), d AS (
    INSERT INTO S.deliveries (consumer_id, message_id)
    SELECT matched.id, m.id FROM matched CROSS JOIN m
    RETURNING consumer_id
)
SELECT pg_notify($4::text, d.consumer_id::text) FROM d`),

		// $1 name, $2 filter, $3 durable, then patternColumns(filter):
		// $4 wild, $5 dot-joined tokens, $6 tail.
		upsertConsumer: r.Replace(`
INSERT INTO S.consumers (name, filter, durable, wild, tokens, tail)
VALUES ($1, $2, $3, $4, string_to_array($5::text, '.'), $6)
ON CONFLICT (name) DO UPDATE
SET last_seen = now(), durable = S.consumers.durable OR EXCLUDED.durable
RETURNING id, filter`),

		deleteConsumer:       r.Replace(`DELETE FROM S.consumers WHERE id = $1`),
		deleteConsumerByName: r.Replace(`DELETE FROM S.consumers WHERE name = $1`),

		// $1 consumer id, $2 batch size, $3 lease in ms.
		fetch: r.Replace(`
WITH c AS (
    SELECT message_id FROM S.deliveries
    WHERE consumer_id = $1 AND visible_at <= now()
    ORDER BY message_id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
), u AS (
    UPDATE S.deliveries d
    SET attempts = d.attempts + 1, visible_at = now() + $3::bigint * interval '1 millisecond'
    FROM c
    WHERE d.consumer_id = $1 AND d.message_id = c.message_id
    RETURNING d.message_id, d.attempts
)
SELECT u.message_id, u.attempts, m.subject, coalesce(m.data, ''::bytea), coalesce(m.reply_to, '')
FROM u JOIN S.messages m ON m.id = u.message_id
ORDER BY u.message_id`),

		// Ack and term remove a delivery; release then deletes the message
		// once no consumer holds it (see Conn.release).
		ack: r.Replace(`
DELETE FROM S.deliveries WHERE consumer_id = $1 AND message_id = $2
RETURNING true`),

		release: r.Replace(`
DELETE FROM S.messages
WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM S.deliveries WHERE message_id = $1)`),

		// $1 consumer, $2 message, $3 attempts (lease owner check), $4 delay ms.
		nak: r.Replace(`
UPDATE S.deliveries SET visible_at = now() + $4::bigint * interval '1 millisecond'
WHERE consumer_id = $1 AND message_id = $2 AND attempts = $3
RETURNING true`),

		inProgress: r.Replace(`
UPDATE S.deliveries SET visible_at = now() + $4::bigint * interval '1 millisecond'
WHERE consumer_id = $1 AND message_id = $2 AND attempts = $3
RETURNING true`),

		// $1 consumer id, $2 message id, $3 attempts, $4 consumer name, $5 reason.
		// Attempts must match the delivery's, so that a handler whose lease
		// expired cannot dead-letter a message handed to someone else.
		term: r.Replace(`
WITH d AS (
    DELETE FROM S.deliveries
    WHERE consumer_id = $1 AND message_id = $2 AND attempts = $3
    RETURNING message_id, attempts
)
INSERT INTO S.dead_letters (consumer_name, message_id, subject, data, reply_to, attempts, reason)
SELECT $4, m.id, m.subject, m.data, m.reply_to, d.attempts, $5
FROM d JOIN S.messages m ON m.id = d.message_id
RETURNING true`),

		heartbeat: r.Replace(`
UPDATE S.consumers SET last_seen = now()
WHERE id = ANY(string_to_array($1, ',')::bigint[])
RETURNING id`),

		// Janitor statements take a transaction-scoped advisory lock so that
		// only one Conn per schema does the work at a time.
		reapConsumers: r.Replace(`
DELETE FROM S.consumers
WHERE (SELECT pg_try_advisory_xact_lock({{lock}}))
  AND NOT durable AND last_seen < now() - $1::bigint * interval '1 millisecond'`),

		// Messages are normally deleted by the last ack or term. Deleting a
		// consumer leaves messages that only it held; this finds them by
		// walking the table in id order, one bounded batch at a time.
		// $1 start after this id, $2 batch size. Returns the last id
		// scanned, or 0 at the end of the table.
		reapOrphanMessages: r.Replace(`
WITH batch AS (
    SELECT id FROM S.messages WHERE id > $1 ORDER BY id LIMIT $2
), del AS (
    DELETE FROM S.messages m
    USING batch b
    WHERE m.id = b.id
      AND (SELECT pg_try_advisory_xact_lock({{lock}}))
      AND NOT EXISTS (SELECT 1 FROM S.deliveries d WHERE d.message_id = m.id)
    RETURNING 1
)
SELECT coalesce(max(id), 0) FROM batch`),

		retention: r.Replace(`
DELETE FROM S.messages
WHERE (SELECT pg_try_advisory_xact_lock({{lock}}))
  AND id IN (
    SELECT id FROM S.messages
    WHERE created_at < now() - $1::bigint * interval '1 millisecond'
    ORDER BY id
    LIMIT $2
  )`),

		deadLetterRetention: r.Replace(`
DELETE FROM S.dead_letters
WHERE (SELECT pg_try_advisory_xact_lock({{lock}}))
  AND died_at < now() - $1::bigint * interval '1 millisecond'`),
	}
}
