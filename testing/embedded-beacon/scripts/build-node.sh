#!/usr/bin/env bash
# Build the optional beacon binary with streamed Cargo output and reusable verification.
# Source docker-settings.sh and call beacon_docker_settings before using this module.

beacon_prepare_binary() {
    binary="$target/$profile/reth-embedded-beacon"
    (
        set -o pipefail
        local build_directory="$target/.beacon-builds/$profile"
        local legacy_directory="$repo/testing/embedded-beacon/devnet/work/hoodi-live"
        local run_id log source_proof result cargo_profile status
        run_id="$(date -u +%Y%m%dT%H%M%SZ)-$BASHPID"
        log="$build_directory/build-$run_id.log"
        source_proof="$build_directory/sources.sha256"
        result="$build_directory/result"
        mkdir -p -- "$target" "$cache" "$build_directory" || return
        exec 9>"$target/.hoodi-build.lock" || return
        beacon_acquire_build_lock "$legacy_directory" "$log" || return
        cd -- "$repo" || return

        # A build already in progress may have produced the requested executable.
        if beacon_binary_is_current "$source_proof" "$result" "$build_directory"; then
            printf '[beacon] Using verified %s binary: %s\n' "$profile" "$binary"
            return
        fi
        if beacon_reuse_legacy_binary "$legacy_directory"; then
            printf '[beacon] Using verified %s binary: %s\n' "$profile" "$binary"
            return
        fi

        beacon_ensure_build_image || return
        cargo_profile="$profile"
        [[ "$profile" != debug ]] || cargo_profile=dev
        beacon_source_manifest > "$source_proof" || return
        printf 'build_profile=%s\ncompleted=false\n' "$profile" > "$result" || return
        printf '\033[1;36m[beacon] Building %s; Cargo output follows.\033[0m\n' "$profile"
        printf '[beacon] Build log: %s\n' "$log"
        if {
            printf 'Started: %s\nProfile: %s\n' "$(date -u +%FT%TZ)" "$profile"
            docker run --rm --init --name "reth-embedded-beacon-build-$run_id" \
                --user "$container_user" \
                --mount "type=bind,src=$repo,dst=/workspace,readonly" \
                --mount "type=bind,src=$target,dst=/target" \
                --mount "type=bind,src=$cache,dst=/cache" \
                --workdir /workspace \
                --env CARGO_HOME=/cache --env CARGO_TARGET_DIR=/target \
                --env "CARGO_BUILD_JOBS=$build_jobs" --env CARGO_TERM_COLOR=always \
                --env "BEACON_PROFILE=$profile" --env "BEACON_CARGO_PROFILE=$cargo_profile" \
                --env CARGO_PROFILE_DEV_DEBUG=0 --env CARGO_PROFILE_DEV_INCREMENTAL=false \
                --env RUST_MIN_STACK=8388608 \
                --env CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang \
                --env 'RUSTFLAGS=-C link-arg=-fuse-ld=lld' \
                "$build_image" bash -c '
                    set -euo pipefail
                    # Read metadata from the checkout even if the image embeds a revision.
                    unset VERGEN_GIT_SHA VERGEN_GIT_DESCRIBE VERGEN_GIT_DIRTY
                    rustc --version
                    if [[ "$BEACON_PROFILE" == debug ]]; then
                        cargo --config profile.dev.package.sha2.opt-level=3 build \
                            --profile "$BEACON_CARGO_PROFILE" --locked \
                            -p reth-embedded-beacon-launcher --features embedded-beacon
                    else
                        cargo build --profile "$BEACON_CARGO_PROFILE" --locked \
                            -p reth-embedded-beacon-launcher --features embedded-beacon
                    fi
                    "/target/$BEACON_PROFILE/reth-embedded-beacon" --version
                    "/target/$BEACON_PROFILE/reth-embedded-beacon" node --beacon.status \
                        > "/target/.beacon-builds/$BEACON_PROFILE/readiness.json"
                ' 9>&-
        } 2>&1 | tee -a -- "$log" 9>&-; then
            status=0
        else
            status=$?
        fi
        if [[ "$status" != 0 ]]; then
            printf 'build_profile=%s\nexit_code=%s\ncompleted=false\n' "$profile" "$status" > "$result"
            return "$status"
        fi
        if ! beacon_manifest_matches "$source_proof" "$build_directory"; then
            printf '[beacon] Sources changed during compilation; rerun to build the current code.\n' >&2
            return 2
        fi
        [[ -x "$binary" ]] || return 2
        {
            printf 'build_profile=%s\nbuild_image=%s\nfinished_utc=%s\nexit_code=0\ncompleted=true\n' \
                "$profile" "$build_image" "$(date -u +%FT%TZ)"
            sha256sum -- "$binary"
        } > "$result" || return
        printf '\033[1;32m[beacon] Verified %s build completed.\033[0m\n' "$profile"
    )
}

# Include filenames as well as hashes so added and removed sources invalidate the proof.
beacon_source_manifest() {
    local root_file
    for root_file in Cargo.toml Cargo.lock rust-toolchain.toml rust-toolchain \
        testing/embedded-beacon/scripts/build-node.sh; do
        [[ ! -f "$root_file" ]] || sha256sum -- "$root_file" || return
    done
    find .cargo bin crates testing \
        \( -name target -o -name work -o -name downloads -o -name fixtures -o -name .git \) -prune -o \
        -type f \( -name '*.rs' -o -name Cargo.toml -o -name '*.c' -o -name '*.h' \
        -o -path '.cargo/*.toml' -o -path 'bin/reth-embedded-beacon/src/reth/presentation/polar-bear.txt' \) \
        -print0 | sort -z | xargs -0 -r sha256sum
}

beacon_manifest_matches() {
    local proof="$1" directory="$2" candidate status=0
    [[ -r "$proof" ]] || return 1
    candidate="$(mktemp "$directory/check.XXXXXX")" || return
    beacon_source_manifest > "$candidate" && cmp -s -- "$proof" "$candidate" || status=$?
    rm -f -- "$candidate"
    return "$status"
}

beacon_binary_result_matches() {
    local result="$1" record
    [[ -x "$binary" && -r "$result" ]] || return 1
    grep -Fxq -- 'completed=true' "$result" || return 1
    grep -Fxq -- 'exit_code=0' "$result" || return 1
    grep -Fxq -- "build_profile=$profile" "$result" || return 1
    record="$(sha256sum -- "$binary")" || return
    grep -Fxq -- "$record" "$result"
}

beacon_binary_is_current() {
    local proof="$1" result="$2" directory="$3"
    beacon_binary_result_matches "$result" || return 1
    grep -Fxq -- "build_image=$build_image" "$result" || return 1
    beacon_manifest_matches "$proof" "$directory"
}

# Older local proofs remain usable, including a build the launcher just waited for.
beacon_reuse_legacy_binary() {
    local directory="$1" proof pattern="build-$profile-*.sources.sha256"
    [[ -d "$directory" && -x "$binary" ]] || return 1
    [[ "$profile" != debug ]] || pattern='validate-dev-*.sources.sha256'
    local -a proof_patterns=( -name "$pattern" )
    [[ "$profile" != release ]] || proof_patterns+=( -o -name 'build-20*.sources.sha256' )
    while IFS= read -r proof; do
        if beacon_binary_result_matches "${proof%.sources.sha256}.result" &&
            sha256sum --check --status "$proof"; then
            return 0
        fi
    done < <(find "$directory" -maxdepth 1 -type f \
        \( "${proof_patterns[@]}" \) \
        -print | sort -r)
    return 1
}
