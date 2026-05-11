-- pg_notify event outbox for the gateway. Modeled on
-- blink-card/migrations/004_card_events_outbox.sql + 007_outbox_webhook_id_and_sat_amount.sql,
-- with two deliberate divergences:
--
-- 1. Channel name is `gateway_events` (not `card_events`) so the listener loop
--    is rail-agnostic — future LN/USD/etc. domain events ride the same channel.
-- 2. NO `webhook_id` column. blink-card uses webhook_id for defense-in-depth
--    dedup on its inbound webhook ingress (Visa pushes events at us). LN has no
--    webhook ingress: invoices and payments are driven by RPC + LND
--    subscription, not external pushes. The equivalent dedup surfaces in the
--    LN gateway are: (a) `external_id` natural-key uniqueness on `invoices`
--    for client-supplied stable IDs (per ADR-0001, wired in Story 5.2), and
--    (b) Symphony's own `processed_events` table for consumer-side event dedup
--    (Symphony's DB, not ours — per ADR-0002). Adding `webhook_id` here would
--    mean every insert writes a value that is never read; the comment is here
--    so a future maintainer reviewing the schema understands why the
--    divergence is deliberate.

CREATE TABLE outbox_events (
    sequence            BIGSERIAL    PRIMARY KEY,
    correlation_id      VARCHAR(255) NOT NULL,
    domain_event_type   VARCHAR(64)  NOT NULL,   -- LN-specific (e.g., lightning_invoice_settled)
    event_type          VARCHAR(64)  NOT NULL,   -- standardized 8-event vocabulary (architecture L1042-1052)
    reference_id        VARCHAR(255) NOT NULL,
    amount_sat          BIGINT       NOT NULL DEFAULT 0,
    timestamp           TIMESTAMPTZ  NOT NULL,
    gateway_metadata    JSONB        NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_events_correlation ON outbox_events(correlation_id);
CREATE INDEX idx_outbox_events_reference   ON outbox_events(reference_id);

-- Indexes on domain_event_type and event_type omitted intentionally — same
-- rationale as blink-card/migrations/004_*.sql:23-26 (low-cardinality enum
-- columns where the b-tree write overhead outweighs read benefit).

CREATE OR REPLACE FUNCTION notify_gateway_event() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('gateway_events', NEW.sequence::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_events_notify
    AFTER INSERT ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION notify_gateway_event();
