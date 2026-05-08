-- Three-layer idempotency surface (architecture L622-630). Slice 1 only lands
-- the SCHEMA — the matching code in `src/idempotency/{request,event,correlation}.rs`
-- is identity-stubbed (// STUB(epic-5.2)) until Story 5.2 un-stubs it. Tables
-- exist now so 5.2 can drop in queries without a co-landed migration.

-- Layer 1 (request-level): hash the request, return the cached response on
-- replay. UUID key is the client-supplied idempotency key (BFF surface).
CREATE TABLE idempotency_keys (
    key             UUID         PRIMARY KEY,
    request_hash    BYTEA        NOT NULL,
    response_body   JSONB,
    recorded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Layer 2 (event-level): consumer-side dedup of outbox events by
-- (gateway_id, sequence). The gateway writes its events with sequence from
-- outbox_events.sequence; consumers (Symphony) record processed pairs here.
CREATE TABLE processed_events (
    gateway_id      TEXT         NOT NULL,
    sequence        BIGINT       NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (gateway_id, sequence)
);
