#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."
target="${1:-engine-parity}"
case "$target" in component-tests|peer-transport|peer-discovery|shared-pool|engine-parity|node-integration|launcher-profile) ;; *) echo "unknown Docker check target: $target" >&2; exit 2 ;; esac
mkdir -p testing/embedded-beacon/artifacts
revision="$(git rev-parse HEAD)"
dirty=true
if git diff --quiet HEAD && test -z "$(git ls-files --others --exclude-standard)"; then dirty=false; fi
docker build --progress=plain --output type=cacheonly --target "$target" -f testing/embedded-beacon/Dockerfile \
  --build-arg SOURCE_REV="$revision" --build-arg SOURCE_DIRTY="$dirty" \
  --build-arg BUILD_JOBS="${BEACON_BUILD_JOBS:-1}" . \
  2>&1 | tee "testing/embedded-beacon/artifacts/docker-${target}.log"
