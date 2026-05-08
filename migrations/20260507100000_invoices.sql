-- Invoice aggregate: projection table + event-sourcing event table.
-- Reference shape: bria/src/payout/repo.rs (bria_payouts + bria_payout_events
-- pair). bria predates es-entity; the gateway drives event-sourcing through
-- the es-entity 0.9.5 derive macros instead, but the two-table layout is the
-- same.

CREATE TABLE invoices (
    id              UUID         PRIMARY KEY,
    payment_hash    BYTEA        NOT NULL UNIQUE,
    wallet_id       UUID         NOT NULL,
    amount_msat     BIGINT       NOT NULL,
    expiry_at       TIMESTAMPTZ  NOT NULL,
    state           TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_invoices_wallet_id ON invoices(wallet_id);

-- Columns required by es-entity 0.9.5's `EsRepo` derive:
--   `event_type` is populated by reading the `"type"` field out of the
--   serialized event JSON (es_entity_macros/src/repo/persist_events_fn.rs:148).
--   `context` is referenced unconditionally by the SELECT in the generated
--   load query (es_entity_macros/src/query/mod.rs:62), even when the
--   `event_context` flag is off — `CASE WHEN $2 THEN e.context ELSE NULL`.
-- Same shape as blink-card's `authorization_events`.
CREATE TABLE invoice_events (
    id           UUID         NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    sequence     INT          NOT NULL,
    event_type   VARCHAR      NOT NULL,
    event        JSONB        NOT NULL,
    context      JSONB,
    recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, sequence)
);
