# blink-ln-gateway

Native Rust Lightning payment gateway for the Blink platform. C2-Discovery prototype scope: dev/staging only, no production traffic.

Architecture, ADRs, and epic breakdown live in the workspace under `../_bmad-output/planning-artifacts/`.

## Setup

`nix develop` for a reproducible toolchain (Rust pinned via `rust-toolchain.toml` plus `rover`, `tilt`, `typos`, `protoc`), or use system Rust matching `rust-toolchain.toml` and install `typos` separately.

```sh
make check-code   # cargo fmt + cargo clippy -D warnings + typos
```
