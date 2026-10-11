#!/bin/sh
# Executed inside the built image; the isolated probe must reject before DB creation.
set -u
network="${1:-hoodi}"
case "$network" in hoodi|mainnet) ;; *) echo 'unsupported probe network' >&2; exit 2 ;; esac
probe_dir=/tmp/embedded-beacon-readiness-probe
if [ -e "$probe_dir" ]; then
  echo 'probe path already exists' >&2
  exit 2
fi
reth-embedded-beacon node --chain "$network" --datadir "$probe_dir" --embedded-beacon \
  --beacon.listen-ip 1.1.1.1 --beacon.bootnodes-file /tmp/absent-seeds.txt \
  --beacon.sync-mode genesis --beacon.genesis /tmp/absent-genesis.ssz \
  --beacon.archive /tmp/absent-archive > /tmp/embedded-probe.stdout 2> /tmp/embedded-probe.stderr
result=$?
initialized=false
[ ! -e "$probe_dir" ] || initialized=true
readiness_error=false
grep -Fq 'live beacon sync unavailable:' /tmp/embedded-probe.stderr && readiness_error=true
cat /tmp/embedded-probe.stderr >&2
printf '{"exit_code":%s,"datadir_initialized":%s,"readiness_error":%s}\n' "$result" "$initialized" "$readiness_error"
[ "$result" -ne 0 ] && [ "$initialized" = false ] && [ "$readiness_error" = true ]
