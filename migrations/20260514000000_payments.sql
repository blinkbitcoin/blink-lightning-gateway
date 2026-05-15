-- Payment aggregate: projection table + event-sourcing event table.
-- 
-- Per ADR #1 (aggregate-per-table): payments and invoices are distinct
-- aggregates with their own tables. An outbound payment may target ANY
-- BOLT11 invoice (including invoices not in this gateway's `invoices`
-- table), so no foreign key to `invoices`.

CREATE TABLE payments (
    id              UUID         PRIMARY KEY,
    payment_hash    BYTEA        NOT NULL UNIQUE,
    wallet_id       UUID         NOT NULL,
    amount_msat     BIGINT       NOT NULL,
    max_fee_msat    BIGINT       NOT NULL,
    state           TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_wallet_id ON payments(wallet_id);

-- Columns required by es-entity 0.9.5's `EsRepo` derive
CREATE TABLE payment_events (
    id           UUID         NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    sequence     INT          NOT NULL,
    event_type   VARCHAR      NOT NULL,
    event        JSONB        NOT NULL,
    context      JSONB,
    recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, sequence)
);
