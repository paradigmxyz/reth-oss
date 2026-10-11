#!/usr/bin/env python3
"""Bounded official Hoodi configuration and original signed discovery records.

These inputs bind network identity; they never select a trusted checkpoint. Original ENR
signatures/fork identity are checked by the actual node, not by this text extractor.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
from urllib import request

CHAIN_ID = 560048
GENESIS_TIME = 1742213400
GENESIS_EXECUTION_HASH = "0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b"
GENESIS_VALIDATORS_ROOT = "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f"
MAX_METADATA_BYTES = 256 * 1024
REPOSITORY = "eth-clients/hoodi"
FILE_NAMES = ("config.yaml", "genesis_validators_root.txt", "bootstrap_nodes.yaml")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def extract_seeds(raw):
    if len(raw) > MAX_METADATA_BYTES:
        raise ValueError("official discovery metadata exceeds bound")
    seeds = []
    for line in raw.decode("utf-8").splitlines():
        # YAML comments are outside the original unquoted base64url record.
        stripped = line.partition("#")[0].strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not re.fullmatch(r"- enr:[A-Za-z0-9_-]{1,4092}", stripped):
            raise ValueError("unexpected discovery metadata entry")
        seeds.append(stripped[2:])
    if not 1 <= len(seeds) <= 128 or len(set(seeds)) != len(seeds):
        raise ValueError("official discovery inventory is empty, duplicated or oversized")
    return ("\n".join(seeds) + "\n").encode()


def config_scalars(raw):
    if len(raw) > MAX_METADATA_BYTES:
        raise ValueError("official consensus metadata exceeds bound")
    values = {}
    for line in raw.decode("utf-8").splitlines():
        match = re.fullmatch(r"([A-Z0-9_]+):\s+([^#]+?)(?:\s+#.*)?", line)
        if match:
            key, value = match.groups()
            if key in values:
                raise ValueError("duplicate official consensus parameter")
            values[key] = value.strip()
    expected = {
        "PRESET_BASE": "mainnet", "CONFIG_NAME": "hoodi", "DEPOSIT_CHAIN_ID": str(CHAIN_ID),
        "DEPOSIT_NETWORK_ID": str(CHAIN_ID), "MIN_GENESIS_TIME": "1742212800",
        "GENESIS_DELAY": "600", "SECONDS_PER_SLOT": "12", "SLOT_DURATION_MS": "12000",
        "GENESIS_FORK_VERSION": "0x10000910", "ALTAIR_FORK_VERSION": "0x20000910",
        "ALTAIR_FORK_EPOCH": "0", "BELLATRIX_FORK_VERSION": "0x30000910",
        "BELLATRIX_FORK_EPOCH": "0", "CAPELLA_FORK_VERSION": "0x40000910",
        "CAPELLA_FORK_EPOCH": "0", "DENEB_FORK_VERSION": "0x50000910",
        "DENEB_FORK_EPOCH": "0", "ELECTRA_FORK_VERSION": "0x60000910",
        "ELECTRA_FORK_EPOCH": "2048", "FULU_FORK_VERSION": "0x70000910",
        "FULU_FORK_EPOCH": "50688", "GLOAS_FORK_VERSION": "0x80000910",
        "GLOAS_FORK_EPOCH": "132352", "MAX_PAYLOAD_SIZE": "10485760",
    }
    if any(values.get(key) != value for key, value in expected.items()):
        raise ValueError("official metadata differs from Hoodi identity/mainnet consensus preset")
    config_schedules(raw)
    return values


def config_schedules(raw):
    """Extract only the reviewed registry schedules, preserving their declared order."""
    lines = raw.decode("utf-8").splitlines()
    schedules = {}
    expected = {
        "BLOB_SCHEDULE": ("MAX_BLOBS_PER_BLOCK", [
            {"EPOCH": 52480, "MAX_BLOBS_PER_BLOCK": 15},
            {"EPOCH": 54016, "MAX_BLOBS_PER_BLOCK": 21}]),
        "GAS_LIMIT_SCHEDULE": ("GAS_LIMIT", [{"EPOCH": 132352, "GAS_LIMIT": 200000000}]),
    }
    for name, (field, reviewed) in expected.items():
        starts = [index for index, line in enumerate(lines) if line == name + ":"]
        if len(starts) != 1:
            raise ValueError("official network schedule is absent or duplicated")
        rows = []
        for line in lines[starts[0] + 1:]:
            if not line.strip() or line.startswith("#"):
                continue
            if not line.startswith("  "):
                break
            epoch = re.fullmatch(r"  - EPOCH: (0|[1-9][0-9]*)", line)
            value = re.fullmatch(r"    " + field + r": (0|[1-9][0-9]*)", line)
            if epoch:
                rows.append({"EPOCH": int(epoch[1])})
            elif value and rows and field not in rows[-1]:
                rows[-1][field] = int(value[1])
            else:
                raise ValueError("official network schedule entry is malformed")
        if rows != reviewed:
            raise ValueError("official network schedule differs from the reviewed Hoodi configuration")
        schedules[name] = rows
    return schedules


def verify(directory):
    directory = Path(directory)
    raw_proof = (directory / "provenance.json").read_bytes()
    if len(raw_proof) > MAX_METADATA_BYTES:
        raise ValueError("metadata provenance exceeds bound")
    proof = json.loads(raw_proof)
    if (proof.get("schema_version") != 1 or proof.get("repository") != REPOSITORY or
            not re.fullmatch(r"[0-9a-f]{40}", proof.get("source_commit", "")) or
            proof.get("checkpoint_trust") != "none"):
        raise ValueError("metadata does not identify the exact official source revision")
    if set(proof.get("files", {})) != set(FILE_NAMES):
        raise ValueError("metadata provenance inventory differs")
    originals = {}
    for name in FILE_NAMES:
        path = directory / name
        if not path.is_file() or path.stat().st_size > MAX_METADATA_BYTES:
            raise ValueError("original metadata is missing or oversized")
        raw = path.read_bytes()
        record = proof["files"][name]
        url = f"https://raw.githubusercontent.com/{REPOSITORY}/{proof['source_commit']}/metadata/{name}"
        if record != {"url": url, "sha256": digest(raw), "bytes": len(raw)}:
            raise ValueError("original official metadata checksum/source differs")
        originals[name] = raw
    if originals["genesis_validators_root.txt"].decode().strip().lower() != GENESIS_VALIDATORS_ROOT:
        raise ValueError("official registry identity differs from Hoodi")
    config = config_scalars(originals["config.yaml"])
    seeds = extract_seeds(originals["bootstrap_nodes.yaml"])
    supplied = directory / "bootnodes.txt"
    if not supplied.is_file() or supplied.stat().st_size > MAX_METADATA_BYTES or supplied.read_bytes() != seeds:
        raise ValueError("seed file is not the unchanged official signed record inventory")
    if proof.get("bootnodes_sha256") != digest(seeds):
        raise ValueError("derived seed inventory checksum differs")
    return {"proof": proof, "config": config,
            "schedules": config_schedules(originals["config.yaml"]), "bootnodes": supplied.resolve()}


def fetch(directory):
    directory = Path(directory)
    # Never replace an existing operator artifact or reuse an incomplete download directory.
    directory.mkdir(parents=True, exist_ok=False)
    def get(url):
        req = request.Request(url, headers={"User-Agent": "embedded-beacon-hoodi-source-audit"})
        with request.urlopen(req, timeout=20) as response:
            raw = response.read(MAX_METADATA_BYTES + 1)
        if len(raw) > MAX_METADATA_BYTES:
            raise ValueError("official response exceeds metadata bound")
        return raw
    ref = json.loads(get(f"https://api.github.com/repos/{REPOSITORY}/git/ref/heads/main"))
    commit = ref["object"]["sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("invalid official source revision")
    files = {}
    for name in FILE_NAMES:
        url = f"https://raw.githubusercontent.com/{REPOSITORY}/{commit}/metadata/{name}"
        raw = get(url)
        (directory / name).write_bytes(raw)
        files[name] = {"url": url, "sha256": digest(raw), "bytes": len(raw)}
    seeds = extract_seeds((directory / "bootstrap_nodes.yaml").read_bytes())
    (directory / "bootnodes.txt").write_bytes(seeds)
    proof = {"schema_version": 1, "repository": REPOSITORY, "source_commit": commit,
             "files": files, "bootnodes_sha256": digest(seeds), "checkpoint_trust": "none",
             "enr_signature_verification": "actual_node_required", "deployment": "not_run"}
    (directory / "provenance.json").write_bytes((json.dumps(proof, indent=2) + "\n").encode())
    verify(directory)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--fetch", action="store_true", help="Read only official metadata into a new directory")
    args = parser.parse_args()
    result = fetch(args.directory) if args.fetch else verify(args.directory)["proof"]
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
