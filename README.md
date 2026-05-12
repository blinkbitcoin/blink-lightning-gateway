# blink-lightning-gateway

Native Rust Lightning payment gateway for the Blink platform.

## Setup

`nix develop` for a reproducible toolchain (Rust pinned via `rust-toolchain.toml` plus `rover`, `tilt`, `typos`, `protoc`, `vendir`), or use system Rust matching `rust-toolchain.toml` and install `typos` separately.

```sh
make check-code   # cargo fmt + cargo clippy -D warnings + typos
```

## Local development

The dev stack is orchestrated by Tilt. The root-level `Tiltfile` composes
the upstream `blink-quickstart` services, a gateway-side Symphony overlay
(`dev/docker-compose.e2e.yml`), and the gateway's own Postgres
(`dev/docker-compose.yml`). The `blink-lightning-gateway` binary itself
runs on the host (via `cargo run`) so it rebuilds on source changes.

```sh
cp .env.example .env.local   # then edit any overrides
make e2e-up                  # tilt up
make e2e-down                # tilt down
```

The gateway exposes three ports by default (see `ln-gateway.yml` for
overrides):

| Service              | Port | Probe                                                              |
|----------------------|------|--------------------------------------------------------------------|
| GraphQL subgraph     | 6691 | `curl http://localhost:6691/graphql` (POST)                        |
| gRPC + tonic-health  | 6690 | `grpcurl -plaintext localhost:6690 grpc.health.v1.Health/Check`    |
| HTTP health          | 8080 | `curl http://localhost:8080/health/ready`                          |

To run the gateway outside Tilt (e.g. against an already-running stack):

```sh
cargo run --bin blink-lightning-gateway
```

The binary reads `PG_CON` from the environment and `ln-gateway.yml` from
the repo root by default.
