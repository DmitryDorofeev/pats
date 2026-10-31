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
		publish: r.Replace(`
INSERT INTO S.messages (subject, data, reply_to) VALUES ($1, $2, NULLIF($3, ''))`),

		upsertConsumer: r.Replace(`
INSERT INTO S.consumers (name, filter, durable) VALUES ($1, $2, $3)
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

		// The ack and term functions also delete the message once no
		// consumer holds it any more (see the migration).
		ack: r.Replace(`SELECT S.ack($1, $2)`),

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
		term: r.Replace(`SELECT S.term($1, $2, $3, $4, $5)`),

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
