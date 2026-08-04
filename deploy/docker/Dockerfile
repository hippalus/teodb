# syntax=docker/dockerfile:1
#
# Multi-stage build for TeoDB
#
# Build:
#   docker build -f deploy/docker/Dockerfile -t teodb:latest .
#
# Run (standalone):
#   docker run -p 8080:8080 -p 8815:8815 teodb:latest --config /app/config/standalone.toml
#
# Run (production-like homogeneous cluster — use docker-compose.production.yaml):
#   Data node:     teodb --config /app/config/data-node.toml
#   Control plane: teodb --config /app/config/control-plane.toml
#
# Build layout:
#   - The frontend is built once in its own Node stage and copied into the Rust
#     build as a prebuilt `frontend/dist` (TEODB_SKIP_UI_BUILD=1 tells build.rs
#     not to invoke npm again).
#   - cargo-chef compiles the (large) dependency graph in a cached layer that is
#     only invalidated when Cargo.toml/Cargo.lock change, so editing TeoDB source
#     does not trigger a full rebuild of arrow/datafusion/ballista.

# --- Frontend build stage ---
FROM node:slim AS frontend-builder
WORKDIR /app/frontend

COPY frontend/package.json frontend/package-lock.json ./
# node:slim tracks the latest Node release; npm ci still honours package-lock.json
# so the JS dependency set stays reproducible while the toolchain stays current.
RUN npm ci

COPY frontend/ ./
RUN npm run build

# --- cargo-chef base (shared toolchain + build deps) ---
FROM rust:1.97.1-slim AS chef
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler libprotobuf-dev libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked

# --- Plan the dependency graph (recipe is just Cargo.{toml,lock} digests) ---
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --- Build ---
FROM chef AS builder
ARG CARGO_BUILD_JOBS=2
ARG CARGO_PROFILE_RELEASE_LTO=thin
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=4

ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS} \
    CARGO_PROFILE_RELEASE_LTO=${CARGO_PROFILE_RELEASE_LTO} \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=${CARGO_PROFILE_RELEASE_CODEGEN_UNITS} \
    PROTOC_INCLUDE=/usr/include \
    TEODB_SKIP_UI_BUILD=1

# Compile dependencies first using the same profile/env as the final build so
# the artifacts are reused. This layer is cached until the recipe changes.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Now build the binary against the real source. Dependency artifacts above are
# reused from the layer cache; only TeoDB crates recompile on a source change.
COPY . .
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist
RUN cargo build --release --locked --bin teodb

# --- Runtime ---
FROM debian:stable-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl libssl3 && \
    rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user. Named volumes (see the compose files) inherit the
# ownership of /app/data from the image on first initialisation, so the WAL,
# cache, and spill directories stay writable without running as root.
RUN useradd --system --create-home --uid 10001 teodb
WORKDIR /app

COPY --from=builder /app/target/release/teodb /app/teodb
RUN mkdir -p /app/data/wal /app/data/cache /app/data/spill && \
    chown -R teodb:teodb /app
USER teodb

# REST API + Flight SQL + Ballista scheduler/executor
EXPOSE 8080 8815 50050 50051 50052

# No HEALTHCHECK here on purpose: the REST liveness/readiness probes (/live,
# /ready on the REST port) only exist for the data-node/standalone roles, not the
# control-plane role. Define per-role probes in compose/Helm instead.

ENTRYPOINT ["/app/teodb"]
