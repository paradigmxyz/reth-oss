#!/usr/bin/env bash
# Use the caller's Docker context and environment; never change a running node.
set -euo pipefail

stack_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
action=${1:-up}
case "$action" in
  up|stop|status|config) ;;
  *) printf 'Usage: bash %s {up|stop|status|config}\n' "$0" >&2; exit 2 ;;
esac
command -v docker >/dev/null
command -v bash >/dev/null
docker compose version >/dev/null

export BEACON_OBSERVABILITY_PROJECT=${BEACON_OBSERVABILITY_PROJECT:-reth-embedded-beacon-observability}
export BEACON_METRICS_ADDR=${BEACON_METRICS_ADDR:-127.0.0.1:9001}
export BEACON_PROMETHEUS_PORT=${BEACON_PROMETHEUS_PORT:-9090}
export BEACON_GRAFANA_PORT=${BEACON_GRAFANA_PORT:-3000}
bash "$stack_dir/prepare.sh" "$stack_dir" "$action"
export BEACON_OBSERVABILITY_WORK="$stack_dir/work/$BEACON_OBSERVABILITY_PROJECT"
compose=(docker compose --project-name "$BEACON_OBSERVABILITY_PROJECT"
  --env-file "$BEACON_OBSERVABILITY_WORK/admin.env" --file "$stack_dir/compose.yml")
# Quiet validation avoids displaying the generated administrator credential.
"${compose[@]}" config --quiet

case "$action" in
  config)
    printf 'Configuration valid; metrics target %s. No services started.\n' "$BEACON_METRICS_ADDR"
    ;;
  up)
    for service in prometheus grafana; do
      docker volume create --label "io.reth.embedded-beacon.observability=$BEACON_OBSERVABILITY_PROJECT" \
        "$BEACON_OBSERVABILITY_PROJECT-$service-data" >/dev/null
    done
    "${compose[@]}" up --detach
    bash "$stack_dir/prepare.sh" "$stack_dir" ready
    printf 'Dashboard: http://127.0.0.1:%s/d/embedded-beacon/embedded-beacon\n' "$BEACON_GRAFANA_PORT"
    printf 'Prometheus: http://127.0.0.1:%s/targets\n' "$BEACON_PROMETHEUS_PORT"
    ;;
  stop)
    "${compose[@]}" stop
    printf 'Monitoring stopped; named volumes and the node are preserved.\n'
    ;;
  status)
    "${compose[@]}" ps
    bash "$stack_dir/prepare.sh" "$stack_dir" status
    ;;
esac
