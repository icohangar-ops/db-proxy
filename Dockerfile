# ---- Build stage ----
FROM rust:1.95-bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

# ---- Runtime stage ----
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates wget && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -r proxy && useradd -r -g proxy -d /app proxy

COPY --from=builder /build/target/release/db-proxy /app/db-proxy

USER proxy

ENV PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- "http://127.0.0.1:${PORT}/health" >/dev/null || exit 1

CMD ["/app/db-proxy"]
