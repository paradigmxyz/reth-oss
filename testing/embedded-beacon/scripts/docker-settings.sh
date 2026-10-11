#!/usr/bin/env bash
# Shared Docker paths for embedded-beacon tooling; caller environment/context stays authoritative.

beacon_docker_settings() {
    local helper_directory selected_profile="${BEACON_PROFILE:-release}"
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --profile)
                [[ "$#" -ge 2 && -n "$2" ]] || {
                    printf -- '--profile requires release, maxperf or debug.\n' >&2
                    return 2
                }
                selected_profile="$2"
                shift 2
                ;;
            --profile=*) selected_profile="${1#--profile=}"; shift ;;
            *) printf 'Unknown launcher argument: %s\n' "$1" >&2; return 2 ;;
        esac
    done
    case "$selected_profile" in release|maxperf|debug) ;;
        *) printf 'Build profile must be release, maxperf or debug.\n' >&2; return 2 ;;
    esac
    export BEACON_PROFILE="$selected_profile"
    helper_directory="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
    repo="$(cd -- "$helper_directory/../../.." && pwd -P)"
    [[ -r "$repo/Cargo.toml" && -r "$repo/testing/embedded-beacon/Dockerfile" ]] || {
        printf 'Embedded beacon repository files are missing.\n' >&2
        return 2
    }
    build_image="${BEACON_BUILD_IMAGE:-embedded-beacon:build-environment}"
    image="${BEACON_RUN_IMAGE:-$build_image}"
    target="$(realpath -m -- "${BEACON_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/reth-embedded-beacon/target-hoodi}")"
    cache="$(realpath -m -- "${CARGO_HOME:-$HOME/.cargo}")"
    data="$(realpath -m -- "${BEACON_DATA_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/reth-embedded-beacon/hoodi}")"
    profile="$selected_profile"
    build_jobs="${BEACON_BUILD_JOBS:-8}"
    container_user="$(id -u):$(id -g)"
    metrics_addr="${BEACON_METRICS_ADDR:-127.0.0.1:9001}"

    [[ "$build_jobs" =~ ^[1-9][0-9]*$ ]] || {
        printf 'BEACON_BUILD_JOBS must be positive.\n' >&2
        return 2
    }
}

beacon_ensure_build_image() {
    if docker image inspect "$build_image" >/dev/null 2>&1; then
        return
    fi
    case "$build_image" in sha256:*|*@sha256:*)
        printf 'Selected build image is missing locally: %s\n' "$build_image" >&2
        return 2
        ;;
    esac
    docker build --progress=plain --target build-environment \
        -f "$repo/testing/embedded-beacon/Dockerfile" -t "$build_image" "$repo"
}

# The caller opens descriptor 9 on the shared target lock before entering this helper.
# A waiting launcher follows the owner's Cargo output without starting a second compiler.
beacon_acquire_build_lock() {
    local log_directory="$1" own_log="$2" owner_log='' follower='' status=0 candidate
    if ! flock -n 9; then
        printf '[beacon] Waiting for the active build; its output follows below.\n'
        if [[ -r "$target/.hoodi-build-log" ]]; then
            IFS= read -r owner_log < "$target/.hoodi-build-log" || true
            [[ -f "$owner_log" && "$owner_log" != "$own_log" ]] || owner_log=''
        fi
        if [[ -z "$owner_log" ]]; then
            while IFS= read -r candidate; do
                if [[ "$candidate" != "$own_log" ]] &&
                    grep -qE '^(Validating current optional integration|Started:)' "$candidate"; then
                    owner_log="$candidate"
                    break
                fi
            done < <(find "$log_directory" -maxdepth 1 -type f \
                \( -name 'validate-dev-*.log' -o -name 'build-*.log' \) \
                -size +0c -printf '%T@ %p\n' | sort -nr | cut -d ' ' -f 2-)
        fi
        if [[ -n "$owner_log" ]]; then
            printf '[beacon] Active build log: %s\n' "$owner_log"
            tail --pid="$$" -n 40 -f -- "$owner_log" 9>&- &
            follower=$!
        else
            printf '[beacon] The build owner has not published its log yet.\n'
        fi
        flock 9 || status=$?
        if [[ -n "$follower" ]]; then
            kill "$follower" 2>/dev/null || true
            wait "$follower" 2>/dev/null || true
        fi
        [[ "$status" == 0 ]] || return "$status"
        printf '[beacon] Build lock acquired; continuing with this launcher.\n'
    fi
    printf '%s\n' "$own_log" > "$target/.hoodi-build-log"
}
