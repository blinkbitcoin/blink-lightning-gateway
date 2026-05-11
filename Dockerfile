FROM clux/muslrust:1.89.0-stable AS build

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy everything (dockerignore will exclude target/ etc)
COPY . .

# Set environment variables
ENV PROTOC_INCLUDE=/usr/include
ENV SQLX_OFFLINE=true

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --locked --release && \
    find target -name "blink-lightning-gateway" -type f -executable && \
    cp $(find target -name "blink-lightning-gateway" -type f -executable | head -1) /tmp/blink-lightning-gateway
FROM ubuntu:24.04
COPY --from=build /tmp/blink-lightning-gateway /usr/local/bin/blink-lightning-gateway
USER 1000
CMD ["blink-lightning-gateway"]
