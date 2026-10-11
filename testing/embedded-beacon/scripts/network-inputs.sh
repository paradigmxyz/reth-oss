#!/usr/bin/env bash
# Select public Hoodi checkpoint metadata; the node authenticates all original SSZ.
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/host-dns.sh"

beacon_fetch_input() {
    local url="$1" destination="$2" limit="$3" accept="${4:-application/json}"
    local partial="${destination}.partial" headers="${destination}.headers" status resolve
    local -a resolve_args=()
    resolve="$(beacon_https_resolve_args "$url")" || {
        printf '[beacon] Cannot resolve the network metadata host: %s\n' "$url" >&2
        return 2
    }
    mapfile -t resolve_args <<< "$resolve"
    if ! (set -o pipefail; curl --ipv4 "${resolve_args[@]}" --proto '=https' --max-redirs 0 --fail --silent --show-error \
        --connect-timeout 10 --max-time 30 --max-filesize "$limit" \
        --header "Accept: $accept" --dump-header "$headers" "$url" |
        head -c "$((limit + 1))" > "$partial"); then
        printf '[beacon] Network metadata download failed: %s\n' "$url" >&2
        rm -f -- "$partial" "$headers"
        return 2
    fi
    status="$(awk '/^HTTP\// { status = $2 } END { print status }' "$headers")"
    if [[ "$status" != 200 || "$(wc -c < "$partial")" -gt "$limit" ]]; then
        printf '[beacon] Network metadata response was redirected, rejected or oversized.\n' >&2
        rm -f -- "$partial" "$headers"
        return 2
    fi
    rm -f -- "$headers"
    mv -- "$partial" "$destination"
}

beacon_prepare_hoodi_inputs() {
    local catalog="$repo/testing/embedded-beacon/devnet/public/network-sources.json"
    local inputs registry revision config_digest root epoch
    local filename expected_root actual_root file_hash source_base identity spec_keys dependency
    for dependency in curl jq sha256sum awk head wc tr mktemp date; do
        command -v "$dependency" >/dev/null || {
            printf '[beacon] Required launcher dependency is missing: %s\n' "$dependency" >&2
            return 2
        }
    done
    jq -e '.schema_version == 1 and (.networks.hoodi |
        .chain_id == 560048 and .genesis_time == 1742213400 and
        .genesis_validators_root == "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f" and
        .genesis_fork_version == "0x10000910" and
        .checkpoint_url == "https://checkpoint-sync.hoodi.ethpandaops.io" and
        .checkpoint_access == "public" and .registry.repository == "eth-clients/hoodi" and
        .registry.directory == "metadata" and
        (.reviewed_config.source_commit | test("^[0-9a-f]{40}$")) and
        (.reviewed_config.sha256 | test("^[0-9a-f]{64}$")))' "$catalog" >/dev/null || {
        printf '[beacon] The checked Hoodi source catalog is invalid.\n' >&2
        return 2
    }
    checkpoint_url="$(jq -r '.networks.hoodi.checkpoint_url' "$catalog")"
    revision="$(jq -r '.networks.hoodi.reviewed_config.source_commit' "$catalog")"
    config_digest="$(jq -r '.networks.hoodi.reviewed_config.sha256' "$catalog")"
    source_base="https://raw.githubusercontent.com/eth-clients/hoodi/$revision/metadata"
    inputs="$(mktemp -d -- "$launch_work/network-inputs.XXXXXX")" || return
    registry="$inputs/registry"
    mkdir -- "$registry" || return
    printf '[beacon] Downloading reviewed Hoodi metadata and current finalized checkpoint.\n'
    for filename in config.yaml genesis_validators_root.txt bootstrap_nodes.yaml; do
        beacon_fetch_input "$source_base/$filename" "$registry/$filename" 262144 text/plain || return
    done
    file_hash="$(sha256sum "$registry/config.yaml")"
    [[ "${file_hash%% *}" == "$config_digest" ]] || {
        printf '[beacon] Registry configuration differs from the reviewed Hoodi config.\n' >&2
        return 2
    }
    expected_root="$(jq -r '.networks.hoodi.genesis_validators_root' "$catalog")"
    actual_root="$(tr -d '\r\n' < "$registry/genesis_validators_root.txt")"
    [[ "$actual_root" == "$expected_root" ]] || {
        printf '[beacon] Registry genesis validators root differs from Hoodi.\n' >&2
        return 2
    }
    # Preserve original signed records. Text extraction never substitutes for signature checks.
    bootnodes="$registry/bootnodes.txt"
    awk '
        {
            sub(/#.*/, "")
            sub(/^[[:space:]]+/, "")
            sub(/[[:space:]]+$/, "")
            if ($0 == "") next
            if ($0 !~ /^- enr:[A-Za-z0-9_-]+$/ || length($0) > 4098) exit 1
            record = substr($0, 3)
            if (seen[record]++ || ++count > 128) exit 1
            print record
        }
        END { if (count == 0) exit 1 }
    ' "$registry/bootstrap_nodes.yaml" > "$bootnodes" || {
        printf '[beacon] Signed discovery inventory is malformed, duplicated or oversized.\n' >&2
        return 2
    }
    beacon_fetch_input "$checkpoint_url/eth/v1/beacon/genesis" "$inputs/genesis.json" 1048576 || return
    beacon_fetch_input "$checkpoint_url/eth/v1/config/spec" "$inputs/spec.json" 1048576 || return
    beacon_fetch_input "$checkpoint_url/eth/v1/beacon/states/head/finality_checkpoints" \
        "$inputs/finality.json" 1048576 || return
    identity="$(jq -c '.networks.hoodi |
        {genesis_time: (.genesis_time | tostring), genesis_validators_root, genesis_fork_version}' "$catalog")"
    jq -e --argjson identity "$identity" '.data == $identity' "$inputs/genesis.json" >/dev/null || {
        printf '[beacon] Provider genesis identity differs from Hoodi.\n' >&2
        return 2
    }
    spec_keys='PRESET_BASE CONFIG_NAME DEPOSIT_CHAIN_ID DEPOSIT_NETWORK_ID GENESIS_FORK_VERSION
        ALTAIR_FORK_VERSION ALTAIR_FORK_EPOCH BELLATRIX_FORK_VERSION BELLATRIX_FORK_EPOCH
        CAPELLA_FORK_VERSION CAPELLA_FORK_EPOCH DENEB_FORK_VERSION DENEB_FORK_EPOCH
        ELECTRA_FORK_VERSION ELECTRA_FORK_EPOCH FULU_FORK_VERSION FULU_FORK_EPOCH
        GLOAS_FORK_VERSION GLOAS_FORK_EPOCH SECONDS_PER_SLOT SLOT_DURATION_MS MAX_PAYLOAD_SIZE'
    awk -v keys="$spec_keys" '
        BEGIN { split(keys, names, /[[:space:]]+/); for (i in names) wanted[names[i]] = 1 }
        /^[A-Z0-9_]+:/ {
            separator = index($0, ":")
            key = substr($0, 1, separator - 1)
            if (!(key in wanted)) next
            value = substr($0, separator + 1)
            sub(/#.*/, "", value)
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            print key "\t" value
        }
    ' "$registry/config.yaml" |
        jq -Rn '[inputs | split("\t") | {key: .[0], value: .[1]}] | from_entries' > "$registry/spec.json"
    jq -e --slurpfile reviewed "$registry/spec.json" '
        .data as $spec |
        all(["PRESET_BASE", "CONFIG_NAME", "DEPOSIT_CHAIN_ID", "SECONDS_PER_SLOT"][]; $spec[.] != null) and
        all($reviewed[0] | to_entries[];
            . as $expected |
            ($spec[$expected.key] == null) or
            (($spec[$expected.key] | tostring | ascii_downcase) == ($expected.value | ascii_downcase)))
    ' "$inputs/spec.json" >/dev/null || {
        printf '[beacon] Provider consensus configuration differs from the reviewed Hoodi schedule.\n' >&2
        return 2
    }
    # Validate decimal strings lexically; jq arithmetic cannot preserve every u64 value.
    jq -e '
        .data.finalized as $checkpoint |
        ($checkpoint.epoch | type == "string" and test("^[1-9][0-9]*$") and
            (length < 20 or (length == 20 and . <= "18446744073709551615"))) and
        ($checkpoint.root | type == "string" and test("^0x[0-9a-fA-F]{64}$") and
            ascii_downcase != "0x0000000000000000000000000000000000000000000000000000000000000000")
    ' "$inputs/finality.json" >/dev/null || {
        printf '[beacon] Provider finalized checkpoint epoch or root is malformed.\n' >&2
        return 2
    }
    epoch="$(jq -r '.data.finalized.epoch' "$inputs/finality.json")"
    root="$(jq -r '.data.finalized.root | ascii_downcase' "$inputs/finality.json")"
    trusted_checkpoint="$epoch:$root"
    (
        cd -- "$inputs"
        sha256sum registry/config.yaml registry/genesis_validators_root.txt \
            registry/bootstrap_nodes.yaml registry/bootnodes.txt genesis.json spec.json finality.json
    ) > "$inputs/sources.sha256"
    jq -n --arg revision "$revision" --arg checkpoint_url "$checkpoint_url" \
        --arg epoch "$epoch" --arg root "$root" --arg trusted "$trusted_checkpoint" \
        --arg fetched_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson identity "$identity" '
        {schema_version: 1, network: "hoodi", provider: "ethPandaOps", fetched_at: $fetched_at,
         identity: $identity, checkpoint_url: $checkpoint_url,
         registry: {repository: "eth-clients/hoodi", source_commit: $revision,
                    source_base_url: ("https://raw.githubusercontent.com/eth-clients/hoodi/" + $revision + "/metadata"),
                    bootnodes: "registry/bootnodes.txt",
                    metadata_checksums: "sources.sha256", checkpoint_trust: "none"},
         checkpoint: {epoch: $epoch, block_root: $root, trusted_checkpoint: $trusted},
         checkpoint_status: "provider_selected_finalized",
         trust_basis: "The operator authorized this public provider for Hoodi checkpoint synchronization.",
         independent_finality_proof: false, ssz_authentication: "required_by_node",
         original_enr_signature_verification: "required_by_node"}' > "$inputs/inputs.json"
    printf '[beacon] Selected checkpoint epoch %s; full SSZ authentication runs in the node.\n' "$epoch"
}
