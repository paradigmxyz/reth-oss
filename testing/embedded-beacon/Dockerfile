# syntax=docker/dockerfile:1.7
ARG RUST_IMAGE=rust:1.95-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
FROM ${RUST_IMAGE} AS build-environment
RUN apt-get update && apt-get install -y --no-install-recommends clang lld libclang-dev cmake pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /source

FROM build-environment AS builder
ARG SOURCE_REV=0000000000000000000000000000000000000000
ARG SOURCE_DIRTY=true
ARG BUILD_JOBS=1
ENV VERGEN_GIT_SHA=${SOURCE_REV} VERGEN_GIT_DESCRIBE=${SOURCE_REV} VERGEN_GIT_DIRTY=${SOURCE_DIRTY}
ENV CARGO_BUILD_JOBS=${BUILD_JOBS} CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 RUST_MIN_STACK=8388608
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang RUSTFLAGS="-C link-arg=-fuse-ld=lld"
COPY . .
# Cached artifacts can be newer than preserved files from a fresh COPY snapshot.
# Refresh embedded Rust inputs only inside this isolated build context.
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    find crates/embedded-beacon bin/reth-embedded-beacon testing/embedded-beacon/spec-tests -type f -name '*.rs' -exec touch {} + && \
    cargo build --locked -p reth-embedded-beacon-launcher --features embedded-beacon && \
    mkdir -p /out && cp target/debug/reth-embedded-beacon /out/ && \
    /out/reth-embedded-beacon node --beacon.status > /out/readiness.json

FROM builder AS component-tests
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    cargo test --locked -p 'reth-embedded-beacon-*' && \
    cargo test --locked -p reth-embedded-beacon-execution-proxy --features beacon-types --lib && \
    cargo test --locked -p reth-embedded-beacon-launcher --features embedded-beacon --bin reth-embedded-beacon reth::tests

FROM builder AS peer-transport
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    cargo test --locked -p reth-embedded-beacon-network --features transport --lib
FROM builder AS peer-discovery
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    cargo test --locked -p reth-embedded-beacon-network --features discovery --lib

FROM builder AS shared-pool
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    cargo test --locked -p reth-embedded-beacon-shared-data --features reth --lib
FROM builder AS engine-parity
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    cargo test --locked -p reth-embedded-beacon-execution-proxy --features integration-tests --test real_engine_parity -- --test-threads=1
FROM builder AS node-integration
ENV RUST_MIN_STACK=8388608
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    rustup component add clippy && \
    cargo test --locked -p reth-embedded-beacon-node --features gossip-validation,peer-runtime,execution-verification,checkpoint-http,beacon-api --lib -- --test-threads=1 && \
    cargo clippy --locked -p reth-embedded-beacon-node --features gossip-validation,peer-runtime,execution-verification,checkpoint-http,beacon-api --all-targets --no-deps -- -D warnings
FROM builder AS launcher-profile
ENV RUST_MIN_STACK=8388608
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    rustup component add clippy && \
    cargo test --locked -p reth-embedded-beacon-shared-data --features reth --lib -- --test-threads=1 && \
    cargo test --locked -p reth-embedded-beacon-launcher --no-default-features --test logging && \
    cargo test --locked -p reth-embedded-beacon-launcher --features embedded-beacon --bin reth-embedded-beacon -- --test-threads=1 && \
    cargo clippy --locked -p reth-embedded-beacon-launcher --features embedded-beacon --all-targets --no-deps -- -D warnings && \
    /out/reth-embedded-beacon node --embedded-beacon --minimal --beacon.profile > /out/minimal-profile.json && \
    /out/reth-embedded-beacon node --embedded-beacon --beacon.profile > /out/standard-profile.json && \
    cat /out/minimal-profile.json

FROM builder AS execution-fixtures-build
RUN --mount=type=cache,id=embedded-beacon-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=embedded-beacon-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=embedded-beacon-target-rust195,target=/source/target,sharing=locked \
    cargo test --locked --profile hivetests -p ef-tests --lib && \
    cargo test --locked --profile hivetests -p ef-tests --test tests --features ef-tests --no-run --message-format=json > /out/execution-build.jsonl && \
    python3 -c 'import json, shutil; entries=[json.loads(line) for line in open("/out/execution-build.jsonl")]; binaries=[e["executable"] for e in entries if e.get("reason")=="compiler-artifact" and "/testing/ef-tests#" in e.get("package_id", "") and e.get("target", {}).get("name")=="tests" and e.get("executable")]; assert len(binaries)==1, binaries; shutil.copy2(binaries[0], "/out/execution-fixtures")'
FROM ${RUNTIME_IMAGE} AS execution-fixtures
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=execution-fixtures-build /out/execution-fixtures /usr/local/bin/execution-fixtures
ENV RETH_EF_TEST_THREADS=2 RETH_EEST_FIXTURES=/fixtures/blockchain_tests
ENTRYPOINT ["/usr/local/bin/execution-fixtures"]
CMD ["--exact", "eest_fixtures", "--nocapture", "--test-threads=1"]

FROM ${RUNTIME_IMAGE} AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --uid 10001 --create-home beacon && mkdir -p /data && chown beacon:beacon /data
COPY --from=builder /out/reth-embedded-beacon /usr/local/bin/reth-embedded-beacon
RUN ln -s reth-embedded-beacon /usr/local/bin/reth
COPY --from=builder /out/readiness.json /usr/local/share/crates/embedded-beacon/readiness.json
COPY LICENSE-APACHE LICENSE-MIT /usr/local/share/embedded-beacon/
USER 10001:10001
WORKDIR /data
EXPOSE 5052 8545 8551 30303/tcp 30303/udp 9000/tcp 9000/udp 9001/udp
ENTRYPOINT ["/usr/local/bin/reth-embedded-beacon"]
CMD ["node"]
