-- {{schema}} is replaced with the schema name and {{channel}} with the
-- LISTEN/NOTIFY channel name.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE {{schema}}.pats_migration (
    id         bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    version    bigint NOT NULL,
    CONSTRAINT version CHECK (version >= 1)
);
CREATE UNIQUE INDEX ON {{schema}}.pats_migration USING btree (version);

CREATE TABLE {{schema}}.messages (
    id         bigserial PRIMARY KEY,
    subject    text NOT NULL,
    data       bytea,
    reply_to   text,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX messages_created_at_idx ON {{schema}}.messages (created_at);

-- A consumer is the durable state behind a subscription: a fan-out
-- subscriber, a queue group, or a named durable.
CREATE TABLE {{schema}}.consumers (
    id         bigserial PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    filter     text NOT NULL,
    -- Subjects are validated so that '*' and '>' only occur as whole tokens.
    is_wild    boolean NOT NULL GENERATED ALWAYS AS (strpos(filter, '*') > 0 OR strpos(filter, '>') > 0) STORED,
    durable    boolean NOT NULL DEFAULT false,
    last_seen  timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);
-- Lets fan-out find exact-subject consumers (most of them, including every
-- request inbox) without evaluating subject_matches.
CREATE INDEX consumers_exact_filter_idx ON {{schema}}.consumers (filter) WHERE NOT is_wild;

-- One row per (consumer, message) that still has to be processed.
CREATE TABLE {{schema}}.deliveries (
    consumer_id bigint NOT NULL REFERENCES {{schema}}.consumers (id) ON DELETE CASCADE,
    message_id  bigint NOT NULL REFERENCES {{schema}}.messages (id) ON DELETE CASCADE,
    attempts    int NOT NULL DEFAULT 0,
    visible_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_id, message_id)
);
CREATE INDEX deliveries_claim_idx ON {{schema}}.deliveries (consumer_id, visible_at, message_id);
CREATE INDEX deliveries_message_idx ON {{schema}}.deliveries (message_id);

CREATE TABLE {{schema}}.dead_letters (
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
CREATE INDEX dead_letters_died_at_idx ON {{schema}}.dead_letters (died_at);

-- NATS-style matching: '*' matches one token, a trailing '>' matches one or
-- more tokens.
CREATE FUNCTION {{schema}}.subject_matches(pattern text, subject text)
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
-- transaction is atomic with the caller's other writes. A message that no
-- consumer wants is deleted straight away.
CREATE FUNCTION {{schema}}.on_message_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cid bigint;
    matched boolean := false;
BEGIN
    FOR cid IN
        INSERT INTO {{schema}}.deliveries (consumer_id, message_id)
        SELECT c.id, NEW.id
        FROM {{schema}}.consumers c
        WHERE NOT c.is_wild AND c.filter = NEW.subject
        UNION ALL
        SELECT c.id, NEW.id
        FROM {{schema}}.consumers c
        WHERE c.is_wild AND {{schema}}.subject_matches(c.filter, NEW.subject)
        RETURNING consumer_id
    LOOP
        matched := true;
        PERFORM pg_notify('{{channel}}', cid::text);
    END LOOP;
    IF NOT matched THEN
        DELETE FROM {{schema}}.messages WHERE id = NEW.id;
    END IF;
    RETURN NULL;
END
$$;

CREATE TRIGGER messages_fanout
    AFTER INSERT ON {{schema}}.messages
    FOR EACH ROW EXECUTE FUNCTION {{schema}}.on_message_insert();

-- Deletes a message once no consumer holds a delivery for it. The row lock
-- serialises consumers releasing the same message, and each statement in a
-- plpgsql function takes a fresh snapshot, so whichever consumer gets the lock
-- last sees every other delivery gone. (A single DELETE ... WHERE NOT EXISTS
-- would let two concurrent acks each see the other's row and both keep it.)
CREATE FUNCTION {{schema}}.release_message(p_message bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM {{schema}}.messages WHERE id = p_message FOR UPDATE;
    IF NOT EXISTS (SELECT 1 FROM {{schema}}.deliveries WHERE message_id = p_message) THEN
        DELETE FROM {{schema}}.messages WHERE id = p_message;
    END IF;
END
$$;

-- Removes a consumer's delivery and, if it was the last one, the message.
-- Returns false if the delivery no longer exists.
CREATE FUNCTION {{schema}}.ack(p_consumer bigint, p_message bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM {{schema}}.deliveries WHERE consumer_id = p_consumer AND message_id = p_message;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    PERFORM {{schema}}.release_message(p_message);
    RETURN true;
END
$$;

-- Moves a delivery to dead_letters. p_attempts must match the delivery's
-- attempts, so that a handler whose lease expired cannot dead-letter a
-- message that has since been handed to someone else. Returns false if the
-- delivery is gone or leased again.
CREATE FUNCTION {{schema}}.term(p_consumer bigint, p_message bigint, p_attempts int, p_consumer_name text, p_reason text)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    WITH d AS (
        DELETE FROM {{schema}}.deliveries
        WHERE consumer_id = p_consumer AND message_id = p_message AND attempts = p_attempts
        RETURNING message_id, attempts
    )
    INSERT INTO {{schema}}.dead_letters (consumer_name, message_id, subject, data, reply_to, attempts, reason)
    SELECT p_consumer_name, m.id, m.subject, m.data, m.reply_to, d.attempts, p_reason
    FROM d JOIN {{schema}}.messages m ON m.id = d.message_id;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    PERFORM {{schema}}.release_message(p_message);
    RETURN true;
END
$$;
