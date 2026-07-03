# syntax=docker/dockerfile:1

# ---- builder: glibc toolchain (NOT musl) ----
FROM rust:1-bookworm AS builder

WORKDIR /app

# Pre-create target dir so cargo can write to it after we copy as root.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/keep-sse /keep-sse

# ---- runtime: distroless (glibc, not musl) ----
FROM gcr.io/distroless/cc-debian12:nonroot

# CA bundle so https upstreams work if ever configured; copied from builder.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_DIR=/etc/ssl/certs

COPY --from=builder /keep-sse /usr/local/bin/keep-sse

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/keep-sse"]
CMD ["--listen", "0.0.0.0:8080"]
