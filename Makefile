# Blink LN Gateway - Build and Development

.PHONY: build test clean check-code audit-code subgraph-generate subgraph-check supergraph-update \
        clean-db start-db reset-db migrate sqlx-prepare integration-test e2e-up e2e-down e2e-test

# DATABASE_URL targets the dev/docker-compose.yml Postgres. All recipes that
# touch sqlx use it; production deployments override at runtime.
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/blink_ln_gateway
export DATABASE_URL

build:
	@echo "Building blink-ln-gateway..."
	SQLX_OFFLINE=true cargo build --release

test:
	@echo "Running tests..."
	SQLX_OFFLINE=true cargo test

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

subgraph-generate:
	@echo "TODO(epic-2): generate GraphQL schema once write_sdl bin exists"

subgraph-check:
	@echo "TODO(epic-2): diff committed schema against generated"

supergraph-update:
	@echo "TODO(story 1.7): rover supergraph compose against snapshot"

# Local-dev Postgres lifecycle. The integration test suite uses
# testcontainers (separate ephemeral container); these targets are for the
# DEV Postgres that backs `cargo sqlx prepare` + manual exploration.
clean-db:
	docker compose -f dev/docker-compose.yml down -v

start-db:
	docker compose -f dev/docker-compose.yml up -d postgres
	@printf "Waiting for postgres to be healthy"
	@for i in $$(seq 1 30); do \
		if docker compose -f dev/docker-compose.yml exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then \
			echo " ready."; exit 0; \
		fi; \
		printf "."; sleep 1; \
	done; \
	echo " timed out."; exit 1

reset-db: clean-db start-db migrate

migrate:
	cargo sqlx migrate run

sqlx-prepare:
	cargo sqlx prepare --workspace -- --tests

integration-test:
	@echo "Running integration tests"
	SQLX_OFFLINE=true cargo test --tests

# E2E test environment targets (orchestrated by Tilt) — wire in Story 1.6
e2e-up:
	tilt up

e2e-down:
	tilt down

e2e-test:
	@echo "TODO(story 1.6): wire e2e/run-tests.sh once stack exists"
