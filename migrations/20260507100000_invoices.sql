-- Invoice aggregate: projection table + event-sourcing event table.

CREATE TABLE invoices (
    id              UUID         PRIMARY KEY,
    payment_hash    BYTEA        NOT NULL UNIQUE,
    wallet_id       UUID         NOT NULL,
    amount_msat     BIGINT,
    expiry_at       TIMESTAMPTZ  NOT NULL,
    state           TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_invoices_wallet_id ON invoices(wallet_id);

-- Columns required by es-entity 0.9.5's `EsRepo` derive.
CREATE TABLE invoice_events (
    id           UUID         NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    sequence     INT          NOT NULL,
    event_type   VARCHAR      NOT NULL,
    event        JSONB        NOT NULL,
    context      JSONB,
    recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, sequence)
);
