#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."
mkdir -p testing/embedded-beacon/artifacts
image="${BEACON_IMAGE:-embedded-beacon:test}"
revision="$(git rev-parse HEAD)"
dirty=true
if git diff --quiet HEAD && test -z "$(git ls-files --others --exclude-standard)"; then dirty=false; fi
docker build --progress=plain --target runtime -f testing/embedded-beacon/Dockerfile \
  --build-arg SOURCE_REV="$revision" --build-arg SOURCE_DIRTY="$dirty" \
  --build-arg BUILD_JOBS="${BEACON_BUILD_JOBS:-1}" -t "$image" . \
  2>&1 | tee testing/embedded-beacon/artifacts/docker-build.log
docker run --rm "$image" node --beacon.status > testing/embedded-beacon/artifacts/docker-readiness.json
docker image inspect "$image" --format '{{.Id}}' > testing/embedded-beacon/artifacts/docker-image-id.txt
