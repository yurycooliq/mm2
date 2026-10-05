# syntax=docker/dockerfile:1
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS build
WORKDIR /build
ARG TARGETARCH
ENV CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY config ./config
COPY fixtures ./fixtures
RUN --mount=type=cache,id=mo-money-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=mm2-target-1.98.1-${TARGETARCH},target=/build/target,sharing=locked \
    cargo test --workspace --locked -- --test-threads=2 && cargo build --release --locked --workspace \
    && cp target/release/mm2-engine /build/mm2-engine \
    && cp target/release/mm2-emulator /build/mm2-emulator

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS runtime
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* && mkdir /data && chown 10001:10001 /data
COPY --from=build /build/mm2-engine /build/mm2-emulator /usr/local/bin/
COPY config/demo.toml /app/config.toml
USER 10001:10001
WORKDIR /app
EXPOSE 8088
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["mm2-engine","healthcheck","--port","8088"]
ENTRYPOINT ["mm2-engine"]
CMD ["--config","/app/config.toml"]
