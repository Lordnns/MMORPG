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
    cargo build --release -p orchestrator && \
    cp target/release/orchestrator /usr/local/bin/

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
 && curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz \
    | tar -xzC /tmp \
 && mv /tmp/docker/docker /usr/local/bin/docker \
 && chmod +x /usr/local/bin/docker \
 && rm -rf /tmp/docker /var/lib/apt/lists/* \
 && apt-get purge -y curl \
 && apt-get autoremove -y
COPY --from=builder /usr/local/bin/orchestrator /usr/local/bin/
EXPOSE 9000/udp
ENTRYPOINT ["/usr/local/bin/orchestrator"]