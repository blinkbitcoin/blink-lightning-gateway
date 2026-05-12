#!/bin/bash
set -euo pipefail

# Generates the Apollo Federation supergraph for the local dev stack.
# Takes the vendored blink-quickstart supergraph-config.yaml, appends the
# gateway's subgraph entry (if not already present), resolves all schema
# file paths to absolute, and runs `rover supergraph compose`.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENDORED_CONFIG="$REPO_ROOT/vendor/blink-quickstart/dev/config/apollo-federation/supergraph-config.yaml"
VENDORED_DIR="$(dirname "$VENDORED_CONFIG")"
OUTPUT_DIR="$REPO_ROOT/dev/config/apollo-federation"

mkdir -p "$OUTPUT_DIR"

# apollo-router reads router.yaml at /repo/dev/router.yaml on startup;
# our overlay replaces the vendored mount, so copy the file across.
cp "$VENDORED_DIR/router.yaml" "$OUTPUT_DIR/router.yaml"

# Resolve relative schema paths to absolute by prepending VENDORED_DIR.
# `rover` resolves `file:` paths relative to the config file's location;
# prepending the vendored dir lets the filesystem walk the `../` chains.
sed 's|file: \${env\.\([^:]*\):-\(\.\./[^}]*\)}|file: ${env.\1:-'"$VENDORED_DIR"'/\2}|g' \
  "$VENDORED_CONFIG" > "$OUTPUT_DIR/supergraph-config.yaml"

# Append the gateway subgraph behind an env-var flag (default OFF).
#
# The gateway's `lnInvoiceCreate` mutation currently conflicts with the
# same mutation in the `public` subgraph from blink-core: the
# `LnInvoicePayload.errors` field is `[GraphqlError!]!` in the gateway
# and `[Error!]!` in `public`, which fails `rover supergraph compose`
# with a federation type-incompatibility error. Resolving this is
# out-of-scope for Story 2.1 — it lands in a later story that aligns
# the gateway's schema with the public schema (Story 5.2 wallet-ownership
# work is the natural place since it's already touching cross-subgraph
# concerns; Story 5.3 wires the federation composition CI gate).
#
# Until then, apollo-router composes the quickstart-only supergraph
# (public + api_keys + notifications) and the gateway's GraphQL surface
# remains reachable directly on port 6691. Set
# `INCLUDE_GATEWAY_SUBGRAPH=1` to force the append (currently fails the
# compose; useful while iterating on the schema fix).
if [[ "${INCLUDE_GATEWAY_SUBGRAPH:-0}" == "1" ]]; then
  if ! grep -q '^\s*lightning_gateway:' "$OUTPUT_DIR/supergraph-config.yaml"; then
    cat >> "$OUTPUT_DIR/supergraph-config.yaml" << EOF
  lightning_gateway:
    routing_url: http://host.docker.internal:6691/graphql
    schema:
      file: \${env.LIGHTNING_GATEWAY_SCHEMA:-$REPO_ROOT/subgraph/schema.graphql}
EOF
  fi
fi

echo "--- Composing supergraph"
cd "$OUTPUT_DIR"

# Vendor-relative paths use blink-core-schemas; export the env-var
# overrides the config already supports so the absolute paths win.
export PUBLIC_SCHEMA="${PUBLIC_SCHEMA:-$REPO_ROOT/vendor/blink-core-schemas/core/api/src/graphql/public/schema.graphql}"
export API_KEYS_SCHEMA="${API_KEYS_SCHEMA:-$REPO_ROOT/vendor/blink-core-schemas/core/api-keys/subgraph/schema.graphql}"
export NOTIFICATIONS_SCHEMA="${NOTIFICATIONS_SCHEMA:-$REPO_ROOT/vendor/blink-core-schemas/core/notifications/subgraph/schema.graphql}"

rover supergraph compose \
  --config supergraph-config.yaml \
  --output supergraph.graphql \
  --elv2-license=accept

echo "✅ Supergraph generated at dev/config/apollo-federation/supergraph.graphql"
