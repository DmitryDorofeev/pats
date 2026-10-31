-- pats schema v1. Every statement is idempotent; the whole file runs as a
-- single implicit transaction under an advisory lock (see migrate.go).
-- {{schema}} is replaced with the schema name and {{channel}} with the
-- LISTEN/NOTIFY channel name.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.schema_migrations (
    version    int PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {{schema}}.messages (
    id         bigserial PRIMARY KEY,
    subject    text NOT NULL,
    data       bytea,
    reply_to   text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS messages_created_at_idx ON {{schema}}.messages (created_at);

-- A consumer is the durable state behind a subscription: a fan-out
-- subscriber, a queue group, or a named durable.
CREATE TABLE IF NOT EXISTS {{schema}}.consumers (
    id         bigserial PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    filter     text NOT NULL,
    durable    boolean NOT NULL DEFAULT false,
    last_seen  timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- One row per (consumer, message) that still has to be processed.
CREATE TABLE IF NOT EXISTS {{schema}}.deliveries (
    consumer_id bigint NOT NULL REFERENCES {{schema}}.consumers (id) ON DELETE CASCADE,
    message_id  bigint NOT NULL REFERENCES {{schema}}.messages (id) ON DELETE CASCADE,
    attempts    int NOT NULL DEFAULT 0,
    visible_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_id, message_id)
);
CREATE INDEX IF NOT EXISTS deliveries_claim_idx ON {{schema}}.deliveries (consumer_id, visible_at, message_id);
CREATE INDEX IF NOT EXISTS deliveries_message_idx ON {{schema}}.deliveries (message_id);

CREATE TABLE IF NOT EXISTS {{schema}}.dead_letters (
    id            bigserial PRIMARY KEY,
    consumer_name text NOT NULL,
    message_id    bigint NOT NULL,
    subject       text NOT NULL,
    data          bytea,
    reply_to      text,
    attempts      int NOT NULL,
    reason        text NOT NULL,
    died_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dead_letters_died_at_idx ON {{schema}}.dead_letters (died_at);

-- NATS-style matching: '*' matches one token, a trailing '>' matches one or
-- more tokens.
CREATE OR REPLACE FUNCTION {{schema}}.subject_matches(pattern text, subject text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    p text[];
    s text[];
    np int;
    ns int;
BEGIN
    IF strpos(pattern, '*') = 0 AND strpos(pattern, '>') = 0 THEN
        RETURN pattern = subject;
    END IF;
    p := string_to_array(pattern, '.');
    s := string_to_array(subject, '.');
    np := coalesce(array_length(p, 1), 0);
    ns := coalesce(array_length(s, 1), 0);
    FOR i IN 1..np LOOP
        IF p[i] = '>' THEN
            RETURN ns >= i;
        END IF;
        IF i > ns THEN
            RETURN false;
        END IF;
        IF p[i] <> '*' AND p[i] <> s[i] THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN np = ns;
END
$$;

-- Fan a new message out to every matching consumer and wake its subscribers.
-- NOTIFY is only delivered on commit, so publishing inside a caller's
-- transaction is atomic with the caller's other writes.
CREATE OR REPLACE FUNCTION {{schema}}.on_message_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cid bigint;
BEGIN
    FOR cid IN
        INSERT INTO {{schema}}.deliveries (consumer_id, message_id)
        SELECT c.id, NEW.id
        FROM {{schema}}.consumers c
        WHERE {{schema}}.subject_matches(c.filter, NEW.subject)
        RETURNING consumer_id
    LOOP
        PERFORM pg_notify('{{channel}}', cid::text);
    END LOOP;
    RETURN NULL;
END
$$;

DROP TRIGGER IF EXISTS messages_fanout ON {{schema}}.messages;
CREATE TRIGGER messages_fanout
    AFTER INSERT ON {{schema}}.messages
    FOR EACH ROW EXECUTE FUNCTION {{schema}}.on_message_insert();

INSERT INTO {{schema}}.schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING;
