# Migrations

`sqlx::migrate!()` reads this directory at compile time and applies the `*.up.sql`
files against the target database in **lexicographic order** by filename. The
filename's leading `<YYYYMMDDHHMMSS>` prefix IS the version sqlx tracks; do not
reuse a prefix.

## Naming convention

Workspace-wide: **`<YYYYMMDDHHMMSS>_<name>.{up,down}.sql`**. Generate via:

```sh
date +%Y%m%d%H%M%S
# or
cargo sqlx migrate add <name>
```

Never use `001_*.sql`-style sequential prefixes — that's the blink-card legacy
pattern; bria, symphony, and cala (and this gateway) all use timestamps.

See `CLAUDE.md` § Conventions for the full convention statement.

## What lives here (Slice 1)

| File | Purpose |
|---|---|
| `20260507100000_invoices.up.sql` | `invoices` projection + `invoice_events` event-source table (the EsEntity pair) |
| `20260507100100_outbox_events.up.sql` | `outbox_events` table + `pg_notify('gateway_events', ...)` trigger |
| `20260507100200_idempotency_stubs.up.sql` | Schema-only `idempotency_keys` + `processed_events` (queries land in Story 5.2) |

Every `up.sql` has a paired `down.sql` that respects FK ordering.

## What does NOT live here

- **No sqlxmq migrations** — sqlxmq stays unused in Slice 1 (the dep is pinned
  in `Cargo.toml` from Story 1.1 but no module imports it). Its 5 vendored SQL
  files plus PG17 + concurrent-poll vendor patches land alongside the first
  story that actually uses a sqlxmq job (likely Story 4.3 chaos tests). See
  `_bmad-output/implementation-artifacts/deferred-work.md` for the open
  upstream issues (sqlxmq#56, sqlxmq#52).

- **No application migrations from sibling repos** — re-derive shapes (see the
  blink-card outbox file references in story 1.4 AC2), do not copy verbatim.
