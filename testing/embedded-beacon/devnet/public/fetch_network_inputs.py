#!/usr/bin/env python3
"""Save bounded public network inputs with exact source/checkpoint provenance.

Checkpoint selection requires an explicit trust basis. The provider-selected finalized
root is experimental trust, not independent finality proof. The Rust node authenticates
downloaded SSZ and original signed discovery records before importing them.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib import request

from hoodi_metadata import extract_seeds, fetch as fetch_hoodi, verify as verify_hoodi

HERE = Path(__file__).resolve().parent
DEFAULT_OUTPUT = HERE.parent / "work/network-inputs"
MAX_BYTES = 1024 * 1024


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("network input source redirected unexpectedly")


def get(url, max_bytes=MAX_BYTES, accept="application/json", include_version=False):
    opener = request.build_opener(NoRedirect())
    req = request.Request(url, headers={"User-Agent": "reth-embedded-beacon-network-inputs", "Accept": accept})
    with opener.open(req, timeout=30) as response:
        raw = response.read(max_bytes + 1)
        version = response.headers.get("Eth-Consensus-Version")
    if len(raw) > max_bytes:
        raise ValueError("network metadata response exceeds bound")
    return (raw, version) if include_version else raw


def save(directory, name, url, raw):
    (directory / name).write_bytes(raw)
    return {"url": url, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def json_bytes(value):
    return (json.dumps(value, indent=2) + "\n").encode()


def root(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value) or int(value, 16) == 0:
        raise ValueError("checkpoint root is malformed or zero")
    return value.lower()


def positive_decimal(value):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*", value) or not 0 < int(value) < 2**64:
        raise ValueError("checkpoint epoch must be a positive u64 decimal")
    return int(value)


def checked_checkpoint(network, genesis, finality, block, version):
    identity = genesis["data"]
    expected = {key: str(network[key]) for key in ("genesis_time", "genesis_validators_root", "genesis_fork_version")}
    if identity != expected:
        raise ValueError("provider genesis differs from the selected network")
    finalized = finality["data"]["finalized"]
    epoch, block_root = positive_decimal(finalized["epoch"]), root(finalized["root"])
    if version not in ("phase0", "altair", "bellatrix", "capella", "deneb", "electra", "fulu", "gloas"):
        raise ValueError("provider block has no supported consensus version")
    if len(block) < 184 or int.from_bytes(block[:4], "little") != 100:
        raise ValueError("provider signed beacon block prefix is malformed")
    # Every supported SignedBeaconBlock starts with an offset, signature and this
    # fixed BeaconBlock prefix. Full SSZ decoding/root verification is done by Rust.
    slot = int.from_bytes(block[100:108], "little")
    if not 0 <= slot <= epoch * 32:
        raise ValueError("checkpoint block is later than its selected epoch boundary")
    state_root = root("0x" + block[148:180].hex())
    return {"epoch": epoch, "block_root": block_root, "block_slot": slot,
            "boundary_slot": epoch * 32, "state_root": state_root,
            "consensus_version": version, "ssz_authentication": "required_by_node",
            "trusted_checkpoint": f"{epoch}:{block_root}"}


def registry_metadata(name, network, directory):
    registry = network["registry"]
    repository = registry["repository"]
    metadata = directory / "registry"
    if name == "hoodi":
        proof = fetch_hoodi(metadata)
        checked = verify_hoodi(metadata)
        return {"repository": repository, "source_commit": proof["source_commit"],
                "files": proof["files"], "bootnodes": "registry/bootnodes.txt",
                "config_scalars": checked["config"], "schedules": checked["schedules"],
                "gas_limit_schedule_advisory": True}
    metadata.mkdir()
    ref = json.loads(get(f"https://api.github.com/repos/{repository}/git/ref/heads/{registry['branch']}"))
    revision = ref["object"]["sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("registry source revision is malformed")
    files = {}
    for filename in registry["files"]:
        url = f"https://raw.githubusercontent.com/{repository}/{revision}/{registry['directory']}/{filename}"
        files[filename] = save(metadata, filename, url, get(url))
    config = (metadata / "config.yaml").read_text()
    match = re.search(r"^DEPOSIT_CHAIN_ID:\s*([0-9]+)\s*$", config, re.MULTILINE)
    if not match or int(match[1]) != network["chain_id"]:
        raise ValueError("registry configuration differs from selected chain identity")
    if name == "sepolia":
        seeds = extract_seeds((metadata / "bootstrap_nodes.yaml").read_bytes())
    else:
        seeds = (metadata / "bootstrap_nodes.txt").read_bytes()
        lines = [line.partition("#")[0].strip() for line in seeds.decode().splitlines()]
        lines = [line for line in lines if line]
        if not 1 <= len(lines) <= 128 or len(set(lines)) != len(lines) or any(
                not re.fullmatch(r"enr:[A-Za-z0-9_-]{1,4092}", line) for line in lines):
            raise ValueError("registry signed bootstrap inventory is malformed")
        seeds = ("\n".join(lines) + "\n").encode()
    (metadata / "bootnodes.txt").write_bytes(seeds)
    return {"repository": repository, "source_commit": revision, "files": files,
            "bootnodes": "registry/bootnodes.txt", "bootnodes_sha256": hashlib.sha256(seeds).hexdigest()}


def fetch_network(name, network, output, trust_basis):
    directory = output / name
    directory.mkdir(parents=True, exist_ok=False)
    registry = registry_metadata(name, network, directory)
    descriptor = {"schema_version": 1, "network": name, "provider": "ethPandaOps",
                  "fetched_at": datetime.now(timezone.utc).isoformat(), "identity": {
                      key: network[key] for key in ("chain_id", "genesis_time", "genesis_validators_root", "genesis_fork_version")},
                  "checkpoint_url": network["checkpoint_url"], "registry": registry,
                  "original_enr_signature_verification": "required_by_node", "files": {}}
    revision = registry["source_commit"]
    metadata = network["registry"]
    descriptor["genesis_ssz_source"] = (
        f"https://raw.githubusercontent.com/{metadata['repository']}/{revision}/{metadata['directory']}/{metadata['genesis_file']}")
    descriptor["genesis_ssz_downloaded"] = False
    if network["checkpoint_access"] != "public":
        descriptor.update(checkpoint=None, checkpoint_status="provider_api_key_required",
                          trust_basis=None)
    else:
        base = network["checkpoint_url"]
        documents = {}
        for filename, path in (("genesis.json", "/eth/v1/beacon/genesis"),
                               ("spec.json", "/eth/v1/config/spec"),
                               ("finality.json", "/eth/v1/beacon/states/head/finality_checkpoints")):
            url = base + path
            raw = get(url)
            documents[filename] = json.loads(raw)
            descriptor["files"][filename] = save(directory, filename, url, raw)
        finalized_root = root(documents["finality.json"]["data"]["finalized"]["root"])
        url = base + "/eth/v2/beacon/blocks/" + finalized_root
        raw, version = get(url, max_bytes=10 * 1024 * 1024, accept="application/octet-stream",
                           include_version=True)
        descriptor["files"]["block.ssz"] = save(directory, "block.ssz", url, raw)
        descriptor["block_ssz_consensus_version_header"] = version
        if version is None:
            block_json = get(url)
            document = json.loads(block_json)
            message = document["data"]["message"]
            if (message["slot"] != str(int.from_bytes(raw[100:108], "little")) or
                    message["state_root"].lower() != "0x" + raw[148:180].hex()):
                raise ValueError("provider block JSON metadata differs from original SSZ prefix")
            descriptor["files"]["block.json"] = save(directory, "block.json", url, block_json)
            version = document.get("version")
        checkpoint = checked_checkpoint(network, documents["genesis.json"],
                                        documents["finality.json"], raw, version)
        descriptor.update(checkpoint=checkpoint, checkpoint_status="provider_selected_finalized",
                          trust_basis=trust_basis, independent_finality_proof=False)
        descriptor["checkpoint_ssz_sources"] = {
            "block": base + "/eth/v2/beacon/blocks/" + checkpoint["block_root"],
            "post_block_state": base + "/eth/v2/debug/beacon/states/" + checkpoint["state_root"],
            "epoch_boundary_state": base + "/eth/v2/debug/beacon/states/" + str(checkpoint["boundary_slot"])}
    (directory / "inputs.json").write_bytes(json_bytes(descriptor))
    return descriptor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", action="append", choices=("mainnet", "hoodi", "sepolia"),
                        help="Network to prepare; repeat the flag, or omit it for all three")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--trust-provider-finalized", required=True, metavar="BASIS",
                        help="Explicit reason to trust this provider checkpoint for this experiment")
    args = parser.parse_args()
    if not args.trust_provider_finalized.strip() or len(args.trust_provider_finalized) > 2048:
        parser.error("checkpoint trust basis must be nonempty and at most 2048 characters")
    catalog = json.loads((HERE / "network-sources.json").read_bytes())
    args.output.mkdir(parents=True, exist_ok=False)
    descriptors = {}
    for name in args.network or ("hoodi", "mainnet", "sepolia"):
        descriptors[name] = fetch_network(name, catalog["networks"][name], args.output,
                                          args.trust_provider_finalized)
        checkpoint = descriptors[name]["checkpoint"]
        print(f"{name}: " + (checkpoint["trusted_checkpoint"] if checkpoint else "provider API key required"), flush=True)
    (args.output / "inputs.json").write_bytes(json_bytes(
        {"schema_version": 1, "networks": descriptors}))
    print(str(args.output.resolve() / "inputs.json"), flush=True)


if __name__ == "__main__":
    main()
