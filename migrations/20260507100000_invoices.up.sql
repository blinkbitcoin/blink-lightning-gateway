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

CREATE TABLE invoice_events (
    id           UUID         NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    sequence     INT          NOT NULL,
    event        JSONB        NOT NULL,
    recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, sequence)
);
