# Blink LN Gateway - Build and Development

.PHONY: build test clean check-code audit-code subgraph-generate subgraph-check supergraph-update \
        clean-db start-db reset-db migrate sqlx-prepare integration-test e2e-up e2e-down e2e-test

build:
	@echo "Building blink-ln-gateway..."
	cargo build --release

test:
	@echo "Running tests..."
	cargo test

clean:
	@echo "Cleaning build artifacts..."
	cargo clean

# Tighter than blink-card: -D warnings is mandatory per architecture.md
# Enforcement Guidelines and Story 1.6 AC. typos is wired into the gate
# (blink-card runs typos via Nix shell separately).
check-code:
	SQLX_OFFLINE=true cargo fmt --check --all
	SQLX_OFFLINE=true cargo clippy --all-features -- -D warnings
	typos

audit-code:
	SQLX_OFFLINE=true cargo audit

# TODO(story 1.6): wire cargo deny check into CI gates
# TODO(story 1.7): supergraph-update target wires `rover supergraph compose` against snapshot
# TODO(story 1.8): migrate / sqlx-prepare get real once first migration lands

subgraph-generate:
	@echo "TODO(epic-2): generate GraphQL schema once write_sdl bin exists"

subgraph-check:
	@echo "TODO(epic-2): diff committed schema against generated"

supergraph-update:
	@echo "TODO(story 1.7): rover supergraph compose against snapshot"

clean-db:
	@echo "TODO(story 1.8): docker compose down -v once db is wired"

start-db:
	@echo "TODO(story 1.8): docker compose up -d postgres once db is wired"

reset-db: clean-db start-db

migrate:
	@echo "TODO(story 1.8): cargo sqlx migrate run once first migration lands"

sqlx-prepare:
	@echo "TODO(story 1.8): cargo sqlx prepare once queries land"

integration-test:
	@echo "Running integration tests"
	SQLX_OFFLINE=true cargo test --test integration

# E2E test environment targets (orchestrated by Tilt) — wire in Epic 2+
e2e-up:
	tilt up

e2e-down:
	tilt down

e2e-test:
	@echo "TODO(epic-2): wire e2e/run-tests.sh once stack exists"
