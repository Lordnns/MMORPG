# syntax=docker/dockerfile:1.7

FROM rust:1.90-bookworm AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*

COPY Code/ ./Code/
COPY External_Dependencies/ ./External_Dependencies/

WORKDIR /build/Code
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/Code/target \
    cargo build --release -p gatekeeper && \
    cp target/release/gatekeeper /usr/local/bin/

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libfontconfig1-dev \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/gatekeeper /usr/local/bin/

EXPOSE 3000/tcp
ENTRYPOINT ["/usr/local/bin/gatekeeper"]