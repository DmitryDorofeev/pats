-- Fan-out, subject matching and message release move from the trigger and
-- plpgsql functions into the Go client. Consumers store the matching columns
-- that the client computes from filter (see patternColumns): wild, the
-- pattern tokens without a trailing '>', and whether it ends in '>'.

ALTER TABLE {{schema}}.consumers
    ADD COLUMN wild boolean,
    ADD COLUMN tokens text[],
    ADD COLUMN tail boolean;

UPDATE {{schema}}.consumers SET
    wild = is_wild,
    tokens = string_to_array(regexp_replace(filter, '(^|\.)>$', ''), '.'),
    tail = filter ~ '(^|\.)>$';

ALTER TABLE {{schema}}.consumers
    ALTER COLUMN wild SET NOT NULL,
    ALTER COLUMN tokens SET NOT NULL,
    ALTER COLUMN tail SET NOT NULL;

-- Dropping is_wild also drops consumers_exact_filter_idx.
ALTER TABLE {{schema}}.consumers DROP COLUMN is_wild;
-- Lets publish find exact-subject consumers (most of them, including every
-- request inbox) without evaluating the wildcard match.
CREATE INDEX consumers_exact_filter_idx ON {{schema}}.consumers (filter) WHERE NOT wild;

DROP TRIGGER messages_fanout ON {{schema}}.messages;
DROP FUNCTION {{schema}}.term(bigint, bigint, int, text, text);
DROP FUNCTION {{schema}}.ack(bigint, bigint);
DROP FUNCTION {{schema}}.release_message(bigint);
DROP FUNCTION {{schema}}.on_message_insert();
DROP FUNCTION {{schema}}.subject_matches(text, text);
