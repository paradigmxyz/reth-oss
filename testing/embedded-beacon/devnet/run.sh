#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."
profile="${1:-gloas-transition}"
case "$profile" in historical|gloas-transition|gloas-genesis) ;; *) echo "unknown profile: $profile" >&2; exit 2 ;; esac
image="${BEACON_IMAGE:-embedded-beacon:test}"
kurtosis="${KURTOSIS_BIN:-$PWD/testing/embedded-beacon/devnet/work/tools/kurtosis}"
enclave="${BEACON_ENCLAVE:-embedded-beacon-${profile}-$(date +%s)-$$}"
case "$enclave" in embedded-beacon-*) ;; *) echo 'enclave must use embedded-beacon- prefix' >&2; exit 2 ;; esac
if [[ ! "$enclave" =~ ^[a-z0-9-]+$ ]]; then echo 'invalid enclave name' >&2; exit 2; fi
out="$PWD/testing/embedded-beacon/artifacts/devnet/$profile/$enclave"
schedule="testing/embedded-beacon/devnet/package/schedules/$profile.json"
mkdir -p "$out"
python3 - "$schedule" "$out/service-plan.json" <<'PY'
import json
import re
import sys
from pathlib import Path

schedule = json.loads(Path(sys.argv[1]).read_text())
roles = {"reference_el", "reference_beacon", "sut_el", "sut_beacon"}
if (schedule.get("slots_per_epoch") != 32 or schedule.get("seconds_per_slot") != 6
        or schedule.get("slot_duration_ms") != 6000 or schedule.get("logical_node_count") != 2
        or schedule.get("consensus_mode") != "independent_embedded"
        or schedule.get("sut_consensus_mode") != "embedded_only"):
    raise SystemExit("required topology is two logical nodes, embedded-only SUT, 32 slots per epoch and 6-second slots")
services = schedule.get("services")
logs = schedule.get("log_services")
identifier = re.compile(r"[a-z0-9][a-z0-9-]{0,127}")
if not isinstance(services, dict) or set(services) != roles:
    raise SystemExit("schedule must define the four endpoint roles")

for value in services.values():
    if (not isinstance(value, dict) or set(value) != {"name", "port"}
            or any(not isinstance(item, str) or not identifier.fullmatch(item)
                   for item in value.values())):
        raise SystemExit("invalid service endpoint metadata")
if (services["sut_el"]["name"] != services["sut_beacon"]["name"]
        or len({services["reference_el"]["name"], services["reference_beacon"]["name"], services["sut_el"]["name"]}) != 3):
    raise SystemExit("execution and beacon APIs must share one embedded service beside the reference pair")
if (not isinstance(logs, list) or not 1 <= len(logs) <= 32
        or any(not isinstance(item, str) or not identifier.fullmatch(item) for item in logs)
        or len(set(logs)) != len(logs)
        or not {item["name"] for item in services.values()} <= set(logs)):
    raise SystemExit("invalid log service metadata")
Path(sys.argv[2]).write_text(json.dumps({"services": services, "log_services": logs}, indent=2) + "\n")
PY
log_service_lines="$(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))["log_services"]))' "$out/service-plan.json")"
mapfile -t log_services <<< "$log_service_lines"
if "$kurtosis" enclave inspect "$enclave" > /dev/null 2>&1; then
  echo "refusing to reuse existing enclave: $enclave" >&2
  exit 2
fi
created=false
cleanup() {
  result=$?
  trap - EXIT
  if [ "$created" = true ]; then
    "$kurtosis" enclave inspect --full-uuids "$enclave" > "$out/enclave-inspect.log" 2>&1 || true
    for service in "${log_services[@]}"; do
      "$kurtosis" service logs --all "$enclave" "$service" > "$out/$service.log" 2>&1 || true
    done
    enclave_uuid="$(awk '$1 == "UUID:" {print $2}' "$out/enclave-inspect.log")"
    if [[ "$enclave_uuid" =~ ^[a-f0-9]{32}$ ]]; then
      docker ps -a --filter "label=com.kurtosistech.enclave-id=$enclave_uuid" > "$out/container-status.log" 2>&1 || true
    fi
    if [ "${BEACON_KEEP_ENCLAVE:-0}" != 1 ]; then
      "$kurtosis" enclave rm -f "$enclave" > "$out/cleanup.log" 2>&1 || true
    fi
  fi
  echo "Devnet artifacts: $out"
  exit "$result"
}
trap cleanup EXIT
docker image inspect "$image" --format '{{.Id}}' > "$out/sut-image-id.txt"
docker run --rm "$image" node --beacon.status > "$out/readiness.json"
ready="$(python3 - "$out/readiness.json" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1]))
fields = ("full_state_transition", "full_fork_choice", "peer_runtime", "checkpoint_bootstrap", "verified_state_recovery", "full_spec_conformance")
if status.get("target_fork") != "gloas" or any(type(status.get(key)) is not bool for key in fields):
    raise SystemExit("compiled readiness has an invalid schema")
print("true" if all(status[key] for key in fields) else "false")
PY
)"
probe=()
if [ "$ready" = false ]; then
  docker run --rm -i --entrypoint sh "$image" -s < testing/embedded-beacon/devnet/probe-launch.sh > "$out/embedded-launch-probe.json" 2> "$out/embedded-launch-probe.log"
  docker run --rm -i --entrypoint sh "$image" -s -- mainnet < testing/embedded-beacon/devnet/probe-launch.sh > "$out/mainnet-launch-probe.json" 2> "$out/mainnet-launch-probe.log"
  probe+=(--embedded-launch-probe "$out/embedded-launch-probe.json")
fi
python3 testing/embedded-beacon/devnet/checks/verify.py --preflight-only \
  --readiness-file "$out/readiness.json" --schedule "$schedule" \
  --output-dir "$out" "${probe[@]}"
python3 testing/embedded-beacon/devnet/package/prepare_package.py
args="$(python3 -c 'import json,sys; print(json.dumps({"profile":sys.argv[1],"sut_image":sys.argv[2]}))' "$profile" "$image")"
created=true
"$kurtosis" run --enclave "$enclave" testing/embedded-beacon/devnet/package "$args" 2>&1 | tee "$out/kurtosis-run.log"
endpoint() {
  local address
  address="$("$kurtosis" port print "$enclave" "$1" "$2")" || return $?
  if [[ -z "$address" || "$address" == *$'\n'* || "$address" == *$'\r'* ]]; then
    echo 'invalid endpoint returned by Kurtosis' >&2
    return 2
  fi
  case "$address" in http://*|https://*) printf '%s' "$address" ;; *) printf 'http://%s' "$address" ;; esac
}
endpoint_role() {
  local metadata service port
  metadata="$(python3 -c 'import json,sys; s=json.load(open(sys.argv[1]))["services"][sys.argv[2]]; print(s["name"],s["port"])' "$out/service-plan.json" "$1")" || return $?
  read -r service port <<< "$metadata"
  endpoint "$service" "$port"
}
reference_el="$(endpoint_role reference_el)"
reference_beacon="$(endpoint_role reference_beacon)"
sut_el="$(endpoint_role sut_el)"
sut_beacon="$(endpoint_role sut_beacon)"
timeout="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["timeout_seconds"])' "$schedule")"
# Readiness metadata cannot establish a deployment. Derive expected identities from the
# actual local images and inspect the enclave's live containers in the verifier.
"$kurtosis" enclave inspect --full-uuids "$enclave" > "$out/deployment-inspect.log"
enclave_uuid="$(awk '$1 == "UUID:" {print $2}' "$out/deployment-inspect.log")"
if [[ ! "$enclave_uuid" =~ ^[a-f0-9]{32}$ ]]; then echo 'invalid deployed enclave UUID' >&2; exit 2; fi
python3 - "$out/service-plan.json" "$enclave_uuid" "$image" "$out/independent-runtime-provenance.json" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

services = json.loads(Path(sys.argv[1]).read_text())["services"]
pins = json.loads(Path("testing/embedded-beacon/devnet/package/pins.json").read_text())
def image_id(reference):
    return subprocess.check_output(["docker", "image", "inspect", reference, "--format", "{{.Id}}"], text=True).strip()
value = {
    "schema_version": 1,
    "enclave_uuid": sys.argv[2],
    "roles": {"reference_execution": services["reference_el"]["name"], "reference_beacon": services["reference_beacon"]["name"], "sut": services["sut_el"]["name"]},
    "allowed_services": [services["reference_el"]["name"], services["reference_beacon"]["name"], services["sut_el"]["name"], "vc-1-reth-lighthouse", "spamoor", "validator-key-generation-cl-validator-keystore"],
    "ports": {"reference_execution": "8545/tcp", "reference_beacon": "4000/tcp", "sut_execution": "8545/tcp", "sut_beacon": "5052/tcp"},
    "images": {"reference_execution": image_id(pins["images"]["reference_el"]["image"]), "reference_beacon": image_id(pins["images"]["beacon_and_validator"]["image"]), "sut": image_id(sys.argv[3])},
}
Path(sys.argv[4]).write_text(json.dumps(value, indent=2) + "\n")
PY
python3 testing/embedded-beacon/devnet/checks/verify.py \
  --sut-el-url "$sut_el" --reference-el-url "$reference_el" \
  --sut-beacon-url "$sut_beacon" --reference-beacon-url "$reference_beacon" \
  --readiness-file "$out/readiness.json" --independent-runtime-provenance "$out/independent-runtime-provenance.json" \
  --schedule "$schedule" \
  --output-dir "$out" --timeout-seconds "$timeout"
