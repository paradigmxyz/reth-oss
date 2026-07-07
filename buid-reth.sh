#!/usr/bin/env bash

set -euo pipefail

PLATFORM="${PLATFORM:-linux/amd64}"
RETH_IMAGE="${RETH_IMAGE:-reth-eip8268:latest}"

if [[ ! -f "Dockerfile" ]]; then
  echo "Run this script from the reth-oss directory (Dockerfile not found)." >&2
  exit 1
fi

# Remove the "jit" feature if present
sed -i '/^[[:space:]]*"jit",$/d' bin/reth/Cargo.toml

echo "Building $RETH_IMAGE"
docker build --platform "$PLATFORM" -t "$RETH_IMAGE" .

echo
docker image inspect "$RETH_IMAGE" --format '{{index .RepoTags 0}} {{.Os}}/{{.Architecture}}'