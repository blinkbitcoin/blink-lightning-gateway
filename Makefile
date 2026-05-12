# Blink LN Gateway - Build and Development

.PHONY: build test clean check-code audit-code subgraph-generate subgraph-check supergraph-update \
        clean-db start-db reset-db migrate sqlx-prepare integration-test e2e-up e2e-down e2e-test

# DATABASE_URL targets the dev/docker-compose.yml Postgres. All recipes that
# touch sqlx use it; production deployments override at runtime.
DATABASE_URL ?= postgres://postgres:postgres@localhost:5435/blink_lightning_gateway
export DATABASE_URL

build:
	@echo "Building blink-lightning-gateway..."
	SQLX_OFFLINE=true cargo build --release

test:
	@echo "Running unit tests (lib + bins)..."
	SQLX_OFFLINE=true cargo test --lib --bins

clean:
	@echo "Cleaning build artifacts..."
	cargo clean

check-code:
	SQLX_OFFLINE=true cargo fmt --check --all
	SQLX_OFFLINE=true cargo clippy --all-features -- -D warnings
	typos

audit-code:
	SQLX_OFFLINE=true cargo audit

subgraph-generate:
	@echo "Generating GraphQL schema..."
	SQLX_OFFLINE=true cargo run --quiet --bin write_sdl > subgraph/schema.graphql

subgraph-check:
	@echo "Checking schema consistency..."
	@SQLX_OFFLINE=true cargo run --quiet --bin write_sdl > /tmp/gateway-current-sdl.txt
	@if diff -q subgraph/schema.graphql /tmp/gateway-current-sdl.txt > /dev/null; then \
		echo "Schema is up to date."; \
	else \
		echo "Schema differs from the committed snapshot."; \
		echo "Run 'make subgraph-generate' to regenerate subgraph/schema.graphql, then commit."; \
		diff subgraph/schema.graphql /tmp/gateway-current-sdl.txt; \
		rm -f /tmp/gateway-current-sdl.txt; \
		exit 1; \
	fi
	@rm -f /tmp/gateway-current-sdl.txt

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
	@echo "Running integration tests..."
	SQLX_OFFLINE=true cargo test --test integration

e2e-up:
	tilt up

e2e-down:
	tilt down

e2e-test:
	@echo "TODO(story 5.3): wire dev/run-tests.sh once stack exists"
