# syntax=docker/dockerfile:1

# ---- builder: glibc toolchain (NOT musl) ----
FROM rust:1-bookworm AS builder

WORKDIR /app

# PPROF=true 构建带内嵌 CPU 采样器的 profiling 镜像；否则走默认 release。
ARG PPROF=false

# Pre-create target dir so cargo can write to it after we copy as root.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    if [ "$PPROF" = "true" ]; then \
        cargo build --profile pprof --locked --features pprof && \
        cp target/pprof/keep-sse /keep-sse; \
    else \
        cargo build --release --locked && \
        cp target/release/keep-sse /keep-sse; \
    fi

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
