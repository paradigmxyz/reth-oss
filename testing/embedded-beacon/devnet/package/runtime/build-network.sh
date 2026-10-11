#!/usr/bin/env sh
set -eu
# The generator created these originals once for both logical participants.
test -s /network-configs/genesis.ssz
test -s /network-configs/genesis.json
test -s /network-configs/genesis_validators_root.txt
test -s /network-configs/deposit_contract_block_hash.txt
test -s /inputs/bootnodes.txt
mkdir /embedded-network
jq --arg time "$(cat /inputs/genesis-time.txt)" \
   --arg validators "$(cat /network-configs/genesis_validators_root.txt)" \
   --arg execution "$(cat /network-configs/deposit_contract_block_hash.txt)" \
   --slurpfile genesis /network-configs/genesis.json '
  def root_bytes:
    gsub("[[:space:]]"; "") | ascii_downcase |
    if test("^0x[0-9a-f]{64}$") then .[2:] else error("invalid generated root") end |
    explode | map(if . >= 48 and . <= 57 then . - 48 else . - 87 end) as $digits |
    [range(0;64;2) | $digits[.] * 16 + $digits[. + 1]] |
    if any(. != 0) then . else error("zero generated root") end;
  ($validators | root_bytes) as $vr | ($execution | root_bytes) as $eh |
  ($time | gsub("[[:space:]]"; "") | tonumber) as $gt |
  if $gt <= 0 or $gt != ($gt | floor) or $genesis[0].config.chainId != .consensus.chain_id
  then error("generated execution/consensus identity mismatch") else . end |
  .execution_genesis_hash = $eh | .terminal_block_hash = $eh |
  .consensus.genesis_validators_root = $vr | .consensus.genesis_time = $gt
' /inputs/embedded-template.json > /embedded-network/embedded-beacon.json
# ENR syntax is an input bound only. The node checks the original signature and network.
awk 'length($0) > 4096 || $0 !~ /^enr:[A-Za-z0-9_-]+$/ {exit 1} END {if (NR != 1) exit 1}' /inputs/bootnodes.txt
cp /inputs/bootnodes.txt /embedded-network/bootnodes.txt
chmod 0444 /embedded-network/embedded-beacon.json /embedded-network/bootnodes.txt
