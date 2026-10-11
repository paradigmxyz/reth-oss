#!/usr/bin/env bash
# Build and run one Hoodi node with the optional embedded consensus follower.
set -euo pipefail
umask 077
scripts_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo="$(cd -- "$scripts_dir/../../.." && pwd -P)"

usage() {
    cat <<'HELP'
Usage: bash launch.sh --hoodi [options]

  --hoodi                Sync Hoodi with --minimal --embedded-beacon.
  --profile PROFILE      maxperf (default), release or debug.
  --data-dir PATH        Reuse this node data directory.
  --advertise-ip IPV4    Public beacon discovery address; detected if omitted.
  --config FILE          Load optional local shell settings.
  --no-grafana           Run metrics without starting the monitoring stack.
  --verbose              Enable beacon debug logging.
  --fcr                  Enable fast confirmation and its dedicated debug diagnostics.
  --state-cache-mib MIB   Decoded beacon cache (default: 512 MiB; range: 1-2048).
  --cache-mib MIB         Shared byte cache (default: 512 MiB; range: 1-4096).
  -h, --help             Show this help.

Builds when needed, streams build and node logs, and runs until Ctrl+C.
Requires Linux/WSL, Docker, curl, jq and the standard Unix tools.
HELP
}

network=''
selected_profile="${BEACON_PROFILE:-maxperf}"
selected_data=''
selected_ip=''
selected_state_cache=''
selected_shared_cache=''
settings_file="${BEACON_SETTINGS_FILE:-$repo/launch.local.sh}"
settings_explicit=false
observability="${BEACON_OBSERVABILITY:-true}"
log_filter=info
fcr=false
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --hoodi) network=hoodi; shift ;;
        --profile|--data-dir|--advertise-ip|--config|--state-cache-mib|--cache-mib)
            [[ "$#" -ge 2 && -n "$2" ]] || { printf '%s requires a value.\n' "$1" >&2; exit 2; }
            case "$1" in
                --profile) selected_profile="$2" ;;
                --data-dir) selected_data="$2" ;;
                --state-cache-mib) selected_state_cache="$2" ;;
                --cache-mib) selected_shared_cache="$2" ;;
                --advertise-ip) selected_ip="$2" ;;
                --config) settings_file="$2"; settings_explicit=true ;;
            esac
            shift 2
            ;;
        --profile=*) selected_profile="${1#--profile=}"; shift ;;
        --state-cache-mib=*) selected_state_cache="${1#--state-cache-mib=}"; shift ;;
        --cache-mib=*) selected_shared_cache="${1#--cache-mib=}"; shift ;;
        --no-grafana) observability=false; shift ;;
        --verbose) log_filter=info,beacon=debug; shift ;;
        --fcr) fcr=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
done
[[ "$network" == hoodi ]] || { usage >&2; exit 2; }
fcr_args=()
if [[ "$fcr" == true ]]; then
    fcr_args=(--fcr)
    log_filter+=,beacon::fcr=debug
fi
if [[ -f "$settings_file" ]]; then
    source "$settings_file"
elif [[ "$settings_explicit" == true || -n "${BEACON_SETTINGS_FILE:-}" ]]; then
    printf 'Local settings file does not exist.\n' >&2
    exit 2
fi
shared_cache_mib="${selected_shared_cache:-${BEACON_CACHE_MIB:-512}}"
[[ "$shared_cache_mib" =~ ^[1-9][0-9]{0,3}$ ]] && ((shared_cache_mib <= 4096)) || {
    printf 'Shared byte cache must be between 1 and 4096 MiB.\n' >&2
    exit 2
}
state_cache_mib="${selected_state_cache:-${BEACON_STATE_CACHE_MIB:-512}}"
[[ "$state_cache_mib" =~ ^[1-9][0-9]{0,3}$ ]] && ((state_cache_mib <= 2048)) || {
    printf 'Decoded state cache must be between 1 and 2048 MiB.\n' >&2
    exit 2
}
[[ -z "$selected_data" ]] || export BEACON_DATA_DIR="$selected_data"
[[ -z "$selected_ip" ]] || export BEACON_ADVERTISE_IP="$selected_ip"
source "$scripts_dir/docker-settings.sh"
beacon_docker_settings --profile "$selected_profile"
[[ "$metrics_addr" =~ ^127\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3}):([1-9][0-9]{0,4})$ ]] || {
    printf 'BEACON_METRICS_ADDR must be a loopback IPv4 address and port.\n' >&2
    exit 2
}
for part in "${BASH_REMATCH[@]:1:3}"; do
    [[ "$part" == 0 || "$part" != 0* ]] && ((10#$part <= 255)) || exit 2
done
(( ${metrics_addr##*:} <= 65535 )) || exit 2
source "$scripts_dir/build-node.sh"
source "$scripts_dir/network-inputs.sh"
cd -- "$repo"
for dependency in docker curl jq sha256sum flock realpath tee; do
    command -v "$dependency" >/dev/null || { printf 'Required tool is missing: %s\n' "$dependency" >&2; exit 2; }
done
docker_endpoint="${DOCKER_HOST:-}"
[[ -n "$docker_endpoint" ]] || docker_endpoint="$(docker context inspect --format '{{.Endpoints.docker.Host}}')"
[[ "$docker_endpoint" == unix://* ]] || { printf 'Use a local Linux Docker socket.\n' >&2; exit 2; }
[[ "$(docker info --format '{{.OSType}}')" == linux ]] || {
    printf 'Use a local Linux Docker daemon, including Docker in WSL.\n' >&2
    exit 2
}
case "$observability" in true|false) ;; *) printf 'BEACON_OBSERVABILITY must be true or false.\n' >&2; exit 2 ;; esac
if [[ -d "$data" && ! -f "$data/.embedded-hoodi-test" && -n "$(find "$data" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    printf 'The nonempty data directory has no embedded Hoodi ownership marker.\n' >&2
    exit 2
fi
mkdir -p -- "$data"
[[ -f "$data/.embedded-hoodi-test" ]] || printf 'Owned embedded Hoodi node data.\n' > "$data/.embedded-hoodi-test"
exec 8>"$data/.live-run.lock"
flock -n 8 || { printf 'A launcher already owns this Hoodi data directory.\n' >&2; exit 2; }
launch_work="$repo/testing/embedded-beacon/devnet/work/hoodi-live"
mkdir -p -- "$launch_work"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
container="reth-embedded-beacon-hoodi-live-$run_id"
log="$launch_work/node-$run_id.log"
exec > >(tee -a "$log") 2>&1
printf '\033[1;36m[beacon] Hoodi | %s | minimal profile\033[0m\n' "$profile"
printf '[beacon] Data directory: %s\n' "$data"
beacon_prepare_binary
beacon_prepare_hoodi_inputs

# Keep the same verified HTTPS hostname inside Docker while avoiding unreliable WSL DNS.
checkpoint_host="${checkpoint_url#https://}"
checkpoint_host="${checkpoint_host%%/*}"
checkpoint_addresses="$(beacon_resolve_ipv4 "$checkpoint_host")" || {
    printf '[beacon] Cannot resolve the checkpoint provider for the node container.\n' >&2
    exit 2
}
checkpoint_dns_args=()
while IFS= read -r address; do
    checkpoint_dns_args+=(--add-host "$checkpoint_host:$address")
done <<< "$checkpoint_addresses"

public_ipv4() {
    local address="$1" first second third fourth
    [[ "$address" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || return 1
    IFS=. read -r first second third fourth <<< "$address"
    local octet
    for octet in "$first" "$second" "$third" "$fourth"; do
        [[ "$octet" == 0 || "$octet" != 0* ]] && (( 10#$octet <= 255 )) || return 1
    done
    (( first > 0 && first < 224 )) || return 1
    case "$first.$second" in 10.*|127.*|169.254|192.168) return 1 ;; esac
    (( first != 172 || second < 16 || second > 31 )) || return 1
    (( first != 100 || second < 64 || second > 127 )) || return 1
    (( first != 198 || (second != 18 && second != 19) )) || return 1
    case "$first.$second.$third" in 192.0.0|192.0.2|198.51.100|203.0.113) return 1 ;; esac
}
advertised_ip="${BEACON_ADVERTISE_IP:-}"
if [[ -z "$advertised_ip" && -f "$launch_work/runtime.json" ]]; then
    advertised_ip="$(jq -er '.advertised_ipv4 // empty' "$launch_work/runtime.json" 2>/dev/null || true)"
fi
if [[ -z "$advertised_ip" ]]; then
    printf '[beacon] Detecting the public discovery address.\n'
    address_resolve="$(beacon_https_resolve_args https://api.ipify.org)" || {
        printf '[beacon] Cannot resolve the public discovery address service.\n' >&2
        exit 2
    }
    mapfile -t address_resolve_args <<< "$address_resolve"
    advertised_ip="$(curl --ipv4 "${address_resolve_args[@]}" --fail --silent --show-error --proto '=https' --max-redirs 0 --connect-timeout 5 --max-time 15 --max-filesize 64 https://api.ipify.org)"
fi
public_ipv4 "$advertised_ip" || { printf 'A valid public IPv4 discovery address is required.\n' >&2; exit 2; }
if [[ "$observability" == true ]]; then
    export BEACON_METRICS_ADDR="$metrics_addr"
    bash "$repo/testing/embedded-beacon/devnet/observability/run.sh" up
fi
docker image inspect "$image" >/dev/null
printf '%s\n' "$container" > "$launch_work/active-container.txt"
printf '[beacon] Starting authenticated checkpoint sync: %s\n' "$trusted_checkpoint"
printf '[beacon] RPC: http://127.0.0.1:8545 | Beacon API: http://127.0.0.1:5052\n'
printf '[beacon] Metrics: http://%s/ | Press Ctrl+C to stop.\n' "$metrics_addr"
exec docker run --rm --init --name "$container" \
    --label reth.embedded-beacon.test=hoodi-live \
    --network host "${checkpoint_dns_args[@]}" --user "$container_user" --read-only --cap-drop ALL \
    --security-opt no-new-privileges --tmpfs /tmp:rw,nosuid,nodev,size=256m \
    --mount "type=bind,src=$repo,dst=/workspace,readonly" \
    --mount "type=bind,src=$target,dst=/target,readonly" \
    --mount "type=bind,src=$data,dst=/data" \
    --workdir /workspace --env HOME=/data --env RUST_BACKTRACE=1 \
    "$image" "/target/$profile/reth-embedded-beacon" \
    node --chain hoodi --minimal --embedded-beacon --beacon.experimental-sync "${fcr_args[@]}" \
    --beacon.state-cache-mib "$state_cache_mib" --beacon.cache-mib "$shared_cache_mib" \
    --datadir /data --port 30303 --discovery.v5.port 30304 --nat "extip:$advertised_ip" \
    --disable-auth-server --ipcdisable \
    --http --http.addr 127.0.0.1 --http.port 8545 --http.api eth,net,web3 \
    --beacon.sync-mode checkpoint --beacon.checkpoint-url "$checkpoint_url" \
    --beacon.trusted-checkpoint "$trusted_checkpoint" \
    --beacon.listen-ip "$advertised_ip" --beacon.bind-ip 0.0.0.0 \
    --beacon.bootnodes-file "/workspace/${bootnodes#"$repo/"}" \
    --beacon.http --beacon.http-addr 127.0.0.1 --beacon.http-port 5052 \
    --metrics "$metrics_addr" --log.stdout.filter "$log_filter" --color always
