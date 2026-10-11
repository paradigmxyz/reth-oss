#!/usr/bin/env bash
# Prepare private monitoring without replacing mounted files or touching node data.
set -euo pipefail

owner=io.reth.embedded-beacon.observability

fail() {
    printf '%s\n' "$*" >&2
    exit 1
}

require() {
    command -v "$1" >/dev/null || fail "Monitoring requires $1."
}

port_number() {
    local variable=$1 value=$2
    [[ "$value" =~ ^[1-9][0-9]{0,4}$ ]] && ((value <= 65535)) ||
        fail "$variable must be a port between 1 and 65535 without leading zeros."
    printf '%s' "$value"
}

inspect_resource() {
    local kind=$1 name=$2 output
    if output=$(docker "$kind" inspect "$name" 2>&1); then
        jq -ce '.[0]' <<< "$output" || fail "Invalid Docker inspection for $name."
    else
        case "${output,,}" in
            *'no such container'*|*'no such volume'*) printf 'null' ;;
            *) fail "${output:-Cannot inspect Docker resource $name.}" ;;
        esac
    fi
}

check_resources() {
    local service number name container volume listener ids container_id
    for service in prometheus grafana; do
        if [[ "$service" == prometheus ]]; then number=$prometheus_port; else number=$grafana_port; fi
        name="$project-$service"
        container=$(inspect_resource container "$name")
        if [[ "$container" != null ]] &&
            ! jq -e --arg owner "$owner" --arg project "$project" \
                '.Config.Labels[$owner] == $project' <<< "$container" >/dev/null; then
            fail "Container $name belongs to another setup; choose a different project name."
        fi
        volume=$(inspect_resource volume "$name-data")
        if [[ "$volume" != null ]] &&
            ! jq -e --arg owner "$owner" --arg project "$project" \
                '.Labels[$owner] == $project' <<< "$volume" >/dev/null; then
            fail "Volume $name-data belongs to another setup; choose a different project name."
        fi
        if [[ "$action" == up ]]; then
            listener=$(ss -H -ltn "sport = :$number") || fail 'Cannot inspect local listener ports.'
            if [[ -n "$listener" ]] &&
                ! jq -e --arg owner "$owner" --arg number "$number" \
                    '.State.Running == true and .HostConfig.NetworkMode == "host" and
                     .Config.Labels[($owner + ".port")] == $number' <<< "$container" >/dev/null; then
                fail "Local port $number is busy; set a different monitoring port."
            fi
        fi
    done
    # Compose commands can affect every container carrying this project label.
    ids=$(docker ps -aq --filter "label=com.docker.compose.project=$project") ||
        fail 'Cannot inspect Docker Compose project ownership.'
    while IFS= read -r container_id; do
        [[ -n "$container_id" ]] || continue
        container=$(inspect_resource container "$container_id")
        jq -e --arg owner "$owner" --arg project "$project" \
            '.Config.Labels[$owner] == $project' <<< "$container" >/dev/null ||
            fail "Docker Compose project $project contains an unrelated container."
    done <<< "$ids"
}

check_ready() {
    local deadline=$SECONDS service number path status failures
    [[ "$action" != ready ]] || deadline=$((SECONDS + 60))
    while true; do
        failures=()
        for service in prometheus grafana; do
            if [[ "$service" == prometheus ]]; then
                number=$prometheus_port
                path=/-/ready
            else
                number=$grafana_port
                path=/api/health
            fi
            status=$(curl --noproxy '*' --silent --connect-timeout 1 --max-time 2 \
                --output /dev/null --write-out '%{http_code}' \
                "http://127.0.0.1:$number$path") || status=unavailable
            [[ "$status" == 200 ]] || failures+=("$service")
        done
        if (( ${#failures[@]} == 0 )); then
            printf 'Prometheus and Grafana are ready.\n'
            return
        fi
        ((SECONDS < deadline)) ||
            fail "Monitoring is not ready: ${failures[*]}. Inspect docker compose logs."
        sleep 1
    done
}

main() {
    [[ $# == 2 ]] || fail 'Usage: prepare.sh STACK_DIRECTORY {up|stop|status|config|ready}'
    local stack=$1 endpoint raw_target host raw_port part image variable config credentials expected dependency file password
    action=$2
    case "$action" in up|stop|status|config|ready) ;; *) fail 'Invalid monitoring action.' ;; esac
    project=${BEACON_OBSERVABILITY_PROJECT:-reth-embedded-beacon-observability}
    [[ "$project" =~ ^[a-z0-9][a-z0-9_-]{0,47}$ ]] ||
        fail 'BEACON_OBSERVABILITY_PROJECT must contain 1-48 lowercase Docker name characters.'
    prometheus_port=$(port_number BEACON_PROMETHEUS_PORT "${BEACON_PROMETHEUS_PORT:-9090}")
    grafana_port=$(port_number BEACON_GRAFANA_PORT "${BEACON_GRAFANA_PORT:-3000}")
    require curl
    if [[ "$action" == ready || "$action" == status ]]; then
        check_ready
        return
    fi
    for dependency in docker jq openssl flock; do require "$dependency"; done
    [[ "$action" != up ]] || require ss
    endpoint=${DOCKER_HOST:-}
    [[ -n "$endpoint" ]] || endpoint=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
    [[ "$endpoint" == unix://* ]] || fail 'This stack requires a local Linux Docker daemon with host networking.'
    [[ "$(docker info --format '{{.OSType}}')" == linux ]] || fail 'This stack requires Linux Docker containers.'
    raw_target=${BEACON_METRICS_ADDR:-127.0.0.1:9001}
    host=${raw_target%:*}
    raw_port=${raw_target##*:}
    [[ "$host" != localhost ]] || host=127.0.0.1
    [[ "$host" =~ ^127\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$ ]] ||
        fail 'BEACON_METRICS_ADDR must be a loopback IPv4 address and port.'
    for part in "${BASH_REMATCH[@]:1}"; do
        [[ "$part" == 0 || "$part" != 0* ]] && ((10#$part <= 255)) || fail 'BEACON_METRICS_ADDR must be a loopback IPv4 address and port.'
    done
    raw_port=$(port_number BEACON_METRICS_ADDR "$raw_port")
    [[ "$raw_port" != "$prometheus_port" && "$raw_port" != "$grafana_port" &&
       "$prometheus_port" != "$grafana_port" ]] || fail 'Metrics, Prometheus and Grafana ports must be distinct.'
    for variable in BEACON_PROMETHEUS_IMAGE BEACON_GRAFANA_IMAGE; do
        image=${!variable:-}
        if [[ -n "$image" && ( "$image" != *:* || "$image" == *:latest ) ]]; then
            fail "$variable must specify a pinned tag or digest."
        fi
    done
    check_resources
    work="$stack/work/$project"
    [[ ! -L "$stack/work" && ! -L "$work" ]] || fail 'Monitoring work directories must not be symbolic links.'
    mkdir -p -- "$work"
    [[ ! -L "$work/.prepare.lock" ]] || fail 'Monitoring preparation lock must not be a symbolic link.'
    exec 9> "$work/.prepare.lock"
    flock 9
    config="$work/prometheus.yml"
    credentials="$work/admin.env"
    for file in "$config" "$credentials"; do
        [[ ! -L "$file" && ( ! -e "$file" || -f "$file" ) ]] ||
            fail 'Monitoring configuration and credentials must be regular files, not symbolic links.'
    done
    expected=$(printf 'global:\n  scrape_interval: 5s\n  evaluation_interval: 5s\nscrape_configs:\n  - job_name: embedded-beacon\n    metrics_path: /\n    scrape_timeout: 4s\n    static_configs:\n      - targets: [\x27%s:%s\x27]\n' "$host" "$raw_port")
    # Overwrite in place only when content differs, preserving a live bind mount's inode.
    if [[ ! -f "$config" ]] || [[ "$(cat -- "$config")" != "$expected" ]]; then
        printf '%s\n' "$expected" > "$config"
    fi
    # The unprivileged Prometheus container must read its bind-mounted configuration.
    chmod 644 -- "$config"
    if [[ ! -e "$credentials" ]]; then
        password=$(openssl rand -hex 32) || fail 'Cannot generate monitoring administrator credentials.'
        [[ "$password" =~ ^[0-9a-f]{64}$ ]] || fail 'Invalid monitoring credential generator output.'
        (umask 077; set -C; printf 'GF_SECURITY_ADMIN_PASSWORD=%s\n' "$password" > "$credentials")
    fi
    chmod 600 -- "$credentials"
}

main "$@"
