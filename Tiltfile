# blink-lightning-gateway local dev stack
#
# Composed files:
#   1. `vendor/blink-quickstart/docker-compose.yml` — upstream Blink dev
#      stack (galoy, oathkeeper, kratos, api-keys, notifications,
#      bitcoind, lnd1, bria, fulcrum, stablesats, svix, otel-agent).
#   2. `dev/docker-compose.e2e.yml` — symphony + symphony-pg overlay
#      + apollo-router volume override.
#   3. `dev/docker-compose.yml` — gateway-pg.
#
# The gateway binary itself runs on the host via `cargo run` so it can
# rebuild on changes (Tilt watches `src/`, `Cargo.toml`, `ln-gateway.yml`,
# `migrations/`).

# Detect CI mode (`tilt ci` passes "ci" as first arg).
is_ci = sys.argv[1] == "ci"

# Load environment variables from .env.local if present (Tilt extension).
load('ext://dotenv', 'dotenv')
if os.path.exists('.env.local'):
    dotenv('.env.local')

# Full stack: blink-quickstart + gateway overlay + gateway-pg.
docker_compose([
    'vendor/blink-quickstart/docker-compose.yml',
    'dev/docker-compose.e2e.yml',
    'dev/docker-compose.yml',
])

# Gateway's own Postgres (defined in dev/docker-compose.yml).
dc_resource('postgres', labels=['blink-lightning-gateway'],
  links=[
      link("postgres://postgres:postgres@localhost:5435/blink_lightning_gateway", "database"),
  ])

# Set DEBUG=1 to enable debug-profile builds (slower link, faster compile).
debug_mode = os.getenv('DEBUG', '').lower() in ['1', 'true', 'yes']
cargo_flag = '' if debug_mode else ' --release'

# Gateway binary as a local resource (host-side). `serve_cmd` re-sources
# `.env.local` just-in-time so any env vars provisioned by earlier Tilt
# resources land in the process environment.
gateway_serve_cmd = """\
set -a
[ -f ./.env.local ] && . ./.env.local
set +a
SQLX_OFFLINE=true cargo run{flag} --bin blink-lightning-gateway
""".format(flag=cargo_flag)

# Kill any orphaned gateway from a previous session. Tilt's `local_resource`
# child gets reparented to launchd if Tilt itself dies (force-quit, crash,
# terminal closed), and on slow shutdowns Tilt may give up before our
# SIGTERM handler completes its grace-window drain. Either way, the next
# `tilt up` finds ports 6691 + 8080 occupied. The `|| true` keeps `cmd`
# happy when there's nothing to kill.
#
# SQLX_OFFLINE=true: build against the committed `.sqlx/` query cache, same
# as every Makefile target. Without it, a DATABASE_URL inherited from the
# `tilt up` shell flips sqlx to online verification against gateway-pg,
# which has no tables until the binary runs its migrations at serve time.
gateway_build_cmd = (
    'pkill -9 -f "target/(release|debug)/blink-lightning-gateway" || true; ' +
    'SQLX_OFFLINE=true cargo build' + cargo_flag
)

local_resource(
  name='blink-lightning-gateway',
  labels=['blink-lightning-gateway'],
  cmd=gateway_build_cmd,
  serve_cmd=gateway_serve_cmd,
  serve_env={
    "PG_CON": "postgres://postgres:postgres@localhost:5435/blink_lightning_gateway",
    "BLINK_LIGHTNING_GATEWAY_CONFIG": "ln-gateway.yml",
    "RUST_LOG": "debug" if debug_mode else "info",
    "RUST_BACKTRACE": "1" if debug_mode else "0",
  },
  resource_deps=[
    "postgres",
    "otel-agent",
  ],
  deps=[
    "src",
    "Cargo.toml",
    "ln-gateway.yml",
    "migrations",
  ],
  readiness_probe=probe(http_get=http_get_action(port=8080, path='/health/ready')),
)

# Quickstart service groupings (mirrors `blink-card/Tiltfile:86-115`).
galoy_services = [
    "apollo-router", "galoy", "trigger", "redis",
    "mongodb", "mongodb-migrate",
    "price", "price-history", "price-history-migrate", "price-history-pg",
    "svix", "svix-pg",
    "stablesats",
    "api-keys", "api-keys-pg",
]
auth_services = [
    "oathkeeper", "kratos", "kratos-pg",
    "hydra", "hydra-pg", "hydra-migrate",
]
bitcoin_services = [
    "bitcoind", "bitcoind-signer",
    "lnd1", "lnd-outside-1",
    "bria", "bria-pg",
    "fulcrum",
]

for service in galoy_services:
    dc_resource(service, labels=["galoy"])
for service in auth_services:
    dc_resource(service, labels=["auth"])
for service in bitcoin_services:
    dc_resource(service, labels=["bitcoin"])

# Supergraph generation. Appends the gateway's subgraph to the
# vendored supergraph-config.yaml and runs `rover supergraph compose`.
local_resource(
  name='supergraph-generate',
  labels=['blink-lightning-gateway'],
  cmd='dev/generate-supergraph.sh',
  deps=[
    'subgraph/schema.graphql',
    'vendor/blink-quickstart/dev/config/apollo-federation/supergraph-config.yaml',
  ],
)

dc_resource('apollo-router', labels=['galoy'], resource_deps=['supergraph-generate'])
dc_resource('otel-agent', labels=["otel"])
dc_resource('quickstart-test', labels=['quickstart'], auto_init=False)

# Symphony resources (defined in dev/docker-compose.e2e.yml).
dc_resource('symphony', labels=['symphony'])
dc_resource('symphony-migrate', labels=['symphony'])
dc_resource('symphony-pg', labels=['symphony'])

# In CI mode, run e2e tests as a local_resource so `tilt ci` waits for
# them. The `make e2e-test` target is a TODO comment today; a later
# Epic 5 story (5.3 — CI hardening) replaces it with a real run script.
if is_ci:
    local_resource(
      name='e2e-tests',
      labels=['e2e'],
      cmd='make e2e-test',
      resource_deps=[
        'blink-lightning-gateway',
        'symphony',
        'galoy',
        'oathkeeper',
        'bitcoind',
        'lnd1',
        'bria',
        'stablesats',
      ],
    )
