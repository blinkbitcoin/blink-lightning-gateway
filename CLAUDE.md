# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

`blink-lightning-gateway` is a native Rust Lightning payment gateway for the Blink platform. Currently in dev/prototyp phase, no production traffic. All architecture / epic / story decisions live under `_bmad-output/`.

## Common commands

```sh
# Reproducible toolchain (preferred). Drops you in a shell with cargo, rover, tilt, typos, protoc.
nix develop

make check-code        # cargo fmt --check + cargo clippy --all-features -D warnings + typos

# Build / test
make build             # cargo build --release
make test              # cargo test
cargo test <name>      # single test
make integration-test  # SQLX_OFFLINE=true cargo test --test integration

# Security
make audit-code        # cargo audit
```

`SQLX_OFFLINE=true` is the build pattern — `.sqlx/` query cache is committed (NOT gitignored). 

## Architecture 

### Greenfield

Built from `cargo new`, NOT a fork of `blink-card`. Reference implementations are studied and re-derived, never copied wholesale:

| Reference (sibling repo) | What to extract | What to leave behind |
|---|---|---|
| `blink/core/api/src/domain/payments/` (TS) | LN state-machine semantics, HTLC/MPP/intraledger logic | TS idioms, MongoDB shapes, medici accounting |
| `bria/` | DDD bounded contexts, repo patterns — **primary structural reference** | UTXO/on-chain code |
| `blink-card/` | pg_notify outbox (LISTEN/backfill, batch 1000), `correlation_id` as a column on the outbox table (no separate idempotency module — see ADR-0002), Symphony gRPC contract | Card-specific code, file-tree shape |
| `es-entity` | Event-sourcing primitives, repo derive macros | — |
| `symphony/` | `GatewayEventSource` consumer-side trait; **also where new LN Cala templates land (ADR #2)** | Symphony internals |

### Module layout (bria-style; flat per-bounded-context)

```
src/
├── invoice/  payment/  htlc/   # aggregates: entity.rs + repo.rs + event.rs + error.rs
├── primitives/                  # value objects: PaymentHash, MilliSatoshi, Pubkey, BoltInvoice...
├── outbox/                      # pg_notify EventPublisher (correlation_id is a column here; no separate idempotency module — see ADR-0002)
├── lnd/  symphony/  api/  app/  # adapters + inbound surfaces + application services
```


## Conventions to follow

- **Errors:** `thiserror` for typed errors in entity/repo/domain. `anyhow::Error` only at the application-service boundary. Never `panic!` / `unwrap()` in production paths. gRPC `Status` mapping centralized in `src/api/error.rs`.
- **Logging:** `tracing` only (never `log`, never `println!`). Structured fields, not formatted strings: `tracing::info!(payment_hash = %hash, "settled")` — NOT `tracing::info!("settled {}", hash)`. Required fields on domain logs: `payment_hash`, `wallet_id`, `correlation_id`.
- **Naming:** Aggregates singular, no `Ln` prefix (`Invoice`, not `LnInvoice`). gRPC service `LightningPaymentGatewayService`. Tables snake_case plural.
- **Migration filenames: `<YYYYMMDDHHMMSS>_<name>.sql`.** Workspace-wide convention (sqlx `migrate!` reads the prefix as the version). bria, symphony, cala all use this. Generate via `date +%Y%m%d%H%M%S` or `cargo sqlx migrate add`.
- **File naming:** modern Rust (`<modname>.rs` + `<modname>/<sub>.rs`), NOT `mod.rs`.
- **Tests:** Pure logic (entity validation, value-object round-trips, mockall-mocked services with no I/O) lives inline as `#[cfg(test)] mod tests` in the source file. **Anything that boots Postgres, opens a TCP connection, or starts a tonic server lives under `tests/` — never inline in `src/`.** Matches workspace practice in `blink-card`, `bria`, `cala`, and `es-entity`. Integration tests run in parallel — each test owns its own testcontainers Postgres (see `tests/integration/common.rs::TestDatabase::new` for the retry-on-Docker-contention pattern lifted from `blink-card/tests/common/mod.rs`); no `#[serial]` markers needed. `rstest` for parameterized cases. Integration tests are a separate compilation unit and can't see the lib's `mockall::automock`-generated mocks (gated on lib `cfg(test)`); hand-write a tiny stub impl of the trait — see `tests/invoice_create_producer_flow.rs::CannedLnd` for the pattern.

## BMAD planning artifacts (canonical context)

This repo uses BMAD workflows; planning docs are the source of truth for scope decisions:

- `_bmad-output/planning-artifacts/architecture.md` — ADRs, module layout, patterns (1000+ lines; read relevant sections)
- `_bmad-output/planning-artifacts/epics.md` — full epic 1-6 breakdown
- `_bmad-output/planning-artifacts/prd.md` — PRD
- `_bmad-output/implementation-artifacts/sprint-status.yaml` — current story status (epic-1 in-progress, Story 1.1 in review)
- `_bmad-output/implementation-artifacts/<story>.md` — per-story specs with ACs, dev notes, references into other repos
- `_bmad-output/decisions/` — filed ADRs (ADR-0001 onward); ADR template in `_bmad-output/templates/`

When implementing a story, read the matching story file first — it carries citations into sibling repos with file:line references that are load-bearing.

## Workspace context

All sibling repos (`blink/`, `bria/`, `blink-card/`, `symphony/`, `es-entity/`, etc.) are one level up (../) and checked out alongside this one and are **canonical truth** for code-level claims — when planning docs reference them, prefer reading the actual file at the cited path. Workspace-level `CLAUDE.md` describes the broader Blink architecture.
