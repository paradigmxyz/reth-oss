#!/usr/bin/env bash
# Checks the additive feature on the existing Linux toolchain.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."
mkdir -p testing/embedded-beacon/artifacts
export CARGO_TARGET_DIR="$PWD/testing/embedded-beacon/artifacts/target-linux"
case "${1:-feature}" in
  feature)
    cargo check --locked -p reth-embedded-beacon-launcher --features embedded-beacon > testing/embedded-beacon/artifacts/launcher-linux-build.log 2>&1
    result=$?
    tail -n 40 testing/embedded-beacon/artifacts/launcher-linux-build.log
    exit "$result"
    ;;
  toml)
    dprint check "crates/embedded-beacon/**/Cargo.toml" "bin/reth-embedded-beacon/Cargo.toml" "testing/embedded-beacon/spec-tests/Cargo.toml" "Cargo.toml" > testing/embedded-beacon/artifacts/toml-format-check.log 2>&1
    result=$?
    tail -n 40 testing/embedded-beacon/artifacts/toml-format-check.log
    exit "$result"
    ;;
  *)
    printf 'Usage: %s [feature|toml]\n' "$0" >&2
    exit 2
    ;;
esac
