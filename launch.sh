#!/usr/bin/env bash
# Launch the optional beacon follower from the repository root.
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
exec bash "$repo_root/testing/embedded-beacon/scripts/launch-node.sh" "$@"
