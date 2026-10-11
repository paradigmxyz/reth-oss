#!/usr/bin/env python3
"""Own one minimal Hoodi follower and stop after five validated canonical additions.

No process starts until the complete original fixture gate and compiled readiness pass.
Trust is an explicit operator checkpoint; downloaded provider heads never choose it.
This bounded observation is not full public-network or mainnet synchronization acceptance.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
TESTING_ROOT = HERE.parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE.parent / "checks"))
from contracts import CheckError, Endpoint, HttpTransport, NotReady, decimal, hex_bytes, obj, quantity, schedule
from execution import block_at
from verify import READINESS_FIELDS
from hoodi_metadata import CHAIN_ID, GENESIS_EXECUTION_HASH, GENESIS_TIME, GENESIS_VALIDATORS_ROOT, digest, verify as verify_metadata

BLOCK_LIMIT = 5
MAX_CHAIN_WALK = 128
MAX_SMALL_JSON = 1024 * 1024


def small_json(path):
    path = Path(path)
    if not path.is_file() or path.stat().st_size > MAX_SMALL_JSON:
        raise CheckError("small input document is missing or oversized")
    return json.loads(path.read_bytes())


def full_conformance(args):
    """Invoke the existing strict original-source gate; no expected-failure substitutions."""
    path = TESTING_ROOT / "scripts/check-complete-consensus-fixture-results.py"
    spec = importlib.util.spec_from_file_location("complete_fixture_gate", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    pins = module.read_json(TESTING_ROOT / "spec-tests/full-corpus-pins.json")
    if pins.get("required_case_count") != 73992:
        raise CheckError("complete fixture inventory requires explicit review after count changes")
    exit_file = Path(args.fixture_exit_file)
    if exit_file.stat().st_size > 32:
        raise CheckError("runner exit artifact exceeds bound")
    exit_text = exit_file.read_text().strip()
    if exit_text != "0":
        raise CheckError("complete fixture runner did not terminate successfully")
    return module.gate(
        pins, module.read_json(Path(args.fixture_report)),
        module.read_json(Path(args.primary_provenance)), module.read_json(Path(args.compliance_provenance)),
        Path(args.primary_manifest), Path(args.compliance_manifest), 0,
    )


def checkpoint(value):
    match = re.fullmatch(r"(0|[1-9][0-9]*):(0x[0-9a-fA-F]{64})", value)
    if not match or not 0 < int(match[1]) < 2**64 or int(match[2], 16) == 0:
        raise CheckError("explicit trusted checkpoint must be positive EPOCH:0xROOT")
    return {"epoch": int(match[1]), "root": match[2].lower(), "authority": "explicit_operator_selection"}


def checked_inputs(args):
    if sys.platform != "linux":
        raise CheckError("owned listener attribution and graceful smoke execution require Linux")
    trusted = checkpoint(args.trusted_checkpoint)
    if not args.trust_basis.strip() or len(args.trust_basis) > 2048:
        raise CheckError("record an explicit checkpoint trust basis; provider head alone is not trust")
    source = urlsplit(args.checkpoint_url)
    if (source.scheme != "https" or not source.hostname or source.username or source.password or
            source.query or source.fragment):
        raise CheckError("checkpoint artifact source must be an explicit credential-free HTTPS URL")
    if not ipaddress.ip_address(args.advertised_ip).is_global:
        raise CheckError("public networking requires a literal globally routable advertised IP")
    if not 1 <= args.timeout_seconds <= 7200:
        raise CheckError("smoke deadline must be between one second and two hours")
    ports = [args.rpc_port, args.beacon_port, args.peer_port, args.quic_port,
             args.execution_port, args.execution_port + 1]
    if len(set(ports)) != len(ports) or any(not 1 <= value <= 65535 for value in ports):
        raise CheckError("owned service ports must be distinct and nonzero")
    metadata = verify_metadata(args.metadata_dir)
    selected = Path(args.bootnodes_file).resolve()
    if not selected.is_file() or selected.stat().st_size > 256 * 1024:
        raise CheckError("explicit signed seed selection is missing or oversized")
    lines = selected.read_text().splitlines()
    originals = set(metadata["bootnodes"].read_text().splitlines())
    if not 1 <= len(lines) <= 128 or len(set(lines)) != len(lines) or not set(lines) <= originals:
        raise CheckError("seed selection must preserve exact original published signed records")
    metadata["selected_bootnodes"] = selected
    return trusted, metadata


def binary_hash(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            value.update(chunk)
    return value.hexdigest()


def compiled_profile(binary):
    result = subprocess.run([str(binary), "node", "--chain", "hoodi", "--minimal", "--embedded-beacon",
                             "--beacon.profile"], capture_output=True, check=True, timeout=30)
    if len(result.stdout) > MAX_SMALL_JSON:
        raise CheckError("compiled profile exceeds bound")
    document = obj(json.loads(result.stdout), "compiled profile")
    profile, status = obj(document.get("profile"), "resource profile"), obj(document.get("readiness"), "readiness")
    if (profile.get("minimal") is not True or profile.get("retain_all_unfinalized") is not True or
            profile.get("separate_el_cl_pins") is not True or status.get("target_fork") != "gloas" or
            any(status.get(key) is not True for key in READINESS_FIELDS)):
        raise CheckError("compiled minimal profile or full authenticated readiness is incomplete")
    return document


def argv(args, binary, directory, seed_file):
    bind_ip = "0.0.0.0" if ipaddress.ip_address(args.advertised_ip).version == 4 else "::"
    return [str(binary), "node", "--chain", "hoodi", "--minimal", "--embedded-beacon",
            "--datadir", str(directory / "node-data"), "--beacon.sync-mode", "checkpoint",
            "--beacon.checkpoint-url", args.checkpoint_url,
            "--beacon.trusted-checkpoint", args.trusted_checkpoint,
            "--beacon.listen-ip", args.advertised_ip, "--beacon.bind-ip", bind_ip,
            "--beacon.bootnodes-file", str(seed_file), "--beacon.port", str(args.peer_port),
            "--beacon.quic-port", str(args.quic_port),
            "--beacon.http", "--beacon.http-addr", "127.0.0.1", "--beacon.http-port", str(args.beacon_port),
            "--disable-auth-server", "--ipcdisable",
            "--http", "--http.addr", "127.0.0.1", "--http.port", str(args.rpc_port), "--http.api", "eth,net,web3",
            "--port", str(args.execution_port), "--discovery.port", str(args.execution_port),
            "--discovery.v5.port", str(args.execution_port + 1)]


def free_tcp_ports(ports):
    reservations = []
    try:
        for port in ports:
            bound = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            reservations.append(bound)
            bound.bind(("127.0.0.1", port))
    finally:
        for bound in reservations:
            bound.close()


def owned_listeners(pid, ports):
    """Attribute both observed APIs to the exact child PID, never an unrelated local node."""
    descriptors = list((Path("/proc") / str(pid) / "fd").iterdir())
    if len(descriptors) > 65536:
        raise CheckError("owned descriptor inventory exceeds bound")
    sockets = set()
    for descriptor in descriptors:
        try:
            match = re.fullmatch(r"socket:\[(\d+)\]", os.readlink(descriptor))
        except FileNotFoundError:
            continue
        if match:
            sockets.add(match[1])
    found = {}
    for line in Path("/proc/net/tcp").read_text().splitlines()[1:]:
        fields = line.split()
        address, port = fields[1].split(":")
        value = int(port, 16)
        if fields[3] == "0A" and value in ports and address == "0100007F":
            if value in found:
                raise CheckError("API listener ownership is ambiguous")
            found[value] = fields[9]
    if set(found) != set(ports) or not set(found.values()) <= sockets:
        raise NotReady("owned child has not opened both checked loopback API sockets")
    return {str(port): inode for port, inode in found.items()}


def network_identity(beacon, execution, metadata):
    config = schedule(beacon)
    if (config["slots_per_epoch"] != 32 or config["slot_duration_ms"] != 12000 or
            config["genesis"]["genesis_time"] != GENESIS_TIME or
            config["genesis"]["genesis_validators_root"] != GENESIS_VALIDATORS_ROOT or
            config["genesis"]["genesis_fork_version"] != "0x10000910"):
        raise CheckError("observed node is not the original Hoodi mainnet-preset network")
    expected = metadata["config"]
    for fork in config["forks"]:
        name = fork["name"].upper()
        if name == "PHASE0":
            continue
        if name + "_FORK_EPOCH" not in expected:
            if fork["epoch"] != 2**64 - 1:
                raise CheckError("compiled public fork is active without current official configuration")
        elif fork["epoch"] != int(expected[name + "_FORK_EPOCH"]) or fork["version"] != expected[name + "_FORK_VERSION"].lower():
            raise CheckError("compiled fork schedule differs from original official metadata")
    if quantity(execution.rpc("eth_chainId", []), "execution chain id") != CHAIN_ID:
        raise CheckError("execution API has another chain id")
    genesis = block_at(execution, 0)
    if genesis["hash"].lower() != GENESIS_EXECUTION_HASH:
        raise CheckError("execution API has another canonical genesis hash")
    return {"consensus": config, "execution_chain_id": CHAIN_ID, "execution_genesis_hash": GENESIS_EXECUTION_HASH,
            "consensus_preset": "mainnet", "minimal_is_resource_profile": True}


def header(beacon, identity):
    response = obj(beacon.beacon(f"/eth/v1/beacon/headers/{identity}"), "beacon header")
    data = obj(response.get("data"), "beacon header data")
    if response.get("execution_optimistic") is not False or data.get("canonical") is not True:
        raise NotReady("beacon block has no actual non-optimistic canonical authority")
    message = obj(obj(data.get("header"), "signed header").get("message"), "header message")
    root = hex_bytes(data.get("root"), 32, "beacon root")
    if identity.startswith("0x") and root != identity.lower():
        raise CheckError("retained header returned another original root")
    return {"root": root, "slot": decimal(message.get("slot"), "header slot"), "message": message}


def checked_block(beacon, execution, current):
    response = obj(beacon.beacon(f"/eth/v2/beacon/blocks/{current['root']}"), "signed block response")
    if response.get("execution_optimistic") is not False:
        raise NotReady("actual imported signed block is still execution optimistic")
    if response.get("version") not in ("deneb", "electra", "fulu"):
        raise CheckError("current public smoke requires the original supported Hoodi payload schema")
    signed = obj(response.get("data"), "signed block")
    hex_bytes(signed.get("signature"), 96, "original signed block signature")
    message = obj(signed.get("message"), "block message")
    for key in ("slot", "proposer_index", "parent_root", "state_root"):
        if message.get(key) != current["message"].get(key):
            raise CheckError("complete signed block and original header commitments differ")
    payload = obj(obj(message.get("body"), "signed body").get("execution_payload"), "accepted payload")
    height = decimal(payload.get("block_number"), "beacon execution height")
    block = block_at(execution, height)
    for cl, el in (("block_hash", "hash"), ("parent_hash", "parentHash"),
                   ("state_root", "stateRoot"), ("receipts_root", "receiptsRoot")):
        if hex_bytes(payload.get(cl), 32, cl) != block[el].lower():
            raise CheckError("beacon payload differs from canonical Reth execution " + el)
    for cl, el in (("gas_limit", "gasLimit"), ("gas_used", "gasUsed"), ("timestamp", "timestamp"),
                   ("base_fee_per_gas", "baseFeePerGas"), ("blob_gas_used", "blobGasUsed"), ("excess_blob_gas", "excessBlobGas")):
        if decimal(payload.get(cl), cl, 2**256 - 1) != quantity(block.get(el), el):
            raise CheckError("beacon payload differs from canonical Reth " + el)
    if block["hash"].lower() == "0x" + "00" * 32:
        raise NotReady("execution payload is absent")
    state_root = hex_bytes(message.get("state_root"), 32, "poststate commitment")
    actual = obj(obj(beacon.beacon(f"/eth/v1/beacon/states/{state_root}/root"), "retained state").get("data"), "state root data")
    if hex_bytes(actual.get("root"), 32, "retained state commitment") != state_root:
        raise CheckError("retained poststate commitment differs from actual signed block")
    return {"beacon_slot": current["slot"], "beacon_root": current["root"],
            "beacon_parent_root": hex_bytes(message.get("parent_root"), 32, "beacon parent"),
            "beacon_state_root": state_root, "fork": response["version"],
            "execution_height": height, "execution_hash": block["hash"].lower(),
            "execution_parent_hash": block["parentHash"].lower(),
            "gas_limit": quantity(block["gasLimit"], "gas limit"), "gas_used": quantity(block["gasUsed"], "gas used"),
            "execution_optimistic": False, "canonical": True}


def additions(beacon, execution, previous, head):
    path, seen, current = [], set(), head
    for _ in range(MAX_CHAIN_WALK):
        if current["root"] == previous["beacon_root"]:
            path.reverse()
            parent = previous
            for record in path:
                if (record["beacon_parent_root"] != parent["beacon_root"] or
                        record["beacon_slot"] <= parent["beacon_slot"] or
                        record["execution_height"] != parent["execution_height"] + 1 or
                        record["execution_parent_hash"] != parent["execution_hash"]):
                    raise CheckError("new canonical signed blocks do not preserve exact beacon/execution ancestry")
                parent = record
            return path
        if current["root"] in seen or current["slot"] <= previous["beacon_slot"]:
            raise CheckError("canonical head reorged across the smoke anchor or repeated ancestry")
        seen.add(current["root"])
        record = checked_block(beacon, execution, current)
        path.append(record)
        current = header(beacon, record["beacon_parent_root"])
    raise CheckError("canonical ancestry walk exceeds bounded observation work")


def latest_head(execution):
    block = obj(execution.rpc("eth_getBlockByNumber", ["latest", False]), "canonical execution head")
    height = quantity(block.get("number"), "execution head height")
    original = block_at(execution, height)
    if original["hash"].lower() != hex_bytes(block.get("hash"), 32, "head hash"):
        raise NotReady("execution head changed during original canonical observation")
    return {"height": height, "hash": original["hash"].lower(), "gas_limit": quantity(original["gasLimit"], "head gas limit"),
            "gas_used": quantity(original["gasUsed"], "head gas used")}


def stop_owned(child):
    """Signal only the Popen-owned PID. Preserve forced cleanup as a failed graceful result."""
    if child.poll() is not None:
        return {"graceful": False, "reason": "child_exited_before_owned_stop", "exit_code": child.returncode}
    child.send_signal(signal.SIGINT)
    try:
        code = child.wait(timeout=45)
        return {"graceful": code == 0, "signal": "SIGINT", "exit_code": code}
    except subprocess.TimeoutExpired:
        child.terminate()
    try:
        code = child.wait(timeout=10)
        return {"graceful": False, "signal": "SIGTERM_after_timeout", "exit_code": code}
    except subprocess.TimeoutExpired:
        child.kill()
        return {"graceful": False, "signal": "SIGKILL_after_timeout", "exit_code": child.wait(timeout=10)}


def observe(child, args, metadata, report):
    deadline = time.monotonic() + args.timeout_seconds
    transport = HttpTransport(deadline, request_timeout=3)
    beacon = Endpoint(f"http://127.0.0.1:{args.beacon_port}", "owned-beacon", transport)
    execution = Endpoint(f"http://127.0.0.1:{args.rpc_port}", "owned-reth", transport)
    baseline, records, last_condition = None, [], "awaiting authenticated bootstrap and actual execution reconciliation"
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise CheckError("owned follower exited before five actual new canonical blocks")
        try:
            report["owned_listeners"] = owned_listeners(child.pid, [args.rpc_port, args.beacon_port])
            if "network" not in report:
                report["network"] = network_identity(beacon, execution, metadata)
            syncing = obj(obj(beacon.beacon("/eth/v1/node/syncing"), "sync response").get("data"), "owner sync status")
            if any(type(syncing.get(key)) is not bool for key in ("is_syncing", "is_optimistic", "el_offline")):
                raise CheckError("actual owner synchronization status lacks explicit flags")
            if syncing["is_optimistic"] or syncing["el_offline"]:
                raise NotReady("owner has no actual validated execution association")
            head = header(beacon, "head")
            latest = latest_head(execution)
            if baseline is None:
                candidate_baseline = checked_block(beacon, execution, head)
                if latest["height"] != candidate_baseline["execution_height"] or latest["hash"] != candidate_baseline["execution_hash"]:
                    raise NotReady("beacon and canonical execution head are changing during baseline capture")
                baseline = candidate_baseline
                report["baseline"] = baseline
                report["baseline_execution_head"] = latest
            else:
                path = additions(beacon, execution, records[-1] if records else baseline, head)
                if path:
                    head_record = path[-1]
                    if latest["height"] != head_record["execution_height"] or latest["hash"] != head_record["execution_hash"]:
                        raise NotReady("actual Beacon and Reth canonical heads are not the same accepted payload")
                    records.extend(path[:BLOCK_LIMIT - len(records)])
                # Each previously counted root must still be canonical, including across polls.
                for record in records:
                    current = header(beacon, record["beacon_root"])
                    if current["slot"] != record["beacon_slot"]:
                        raise CheckError("a counted original canonical root changed coordinates")
                report.update({"new_validated_canonical_blocks": records, "last_execution_head": latest,
                               "last_owner_sync_status": syncing})
                if len(records) == BLOCK_LIMIT and latest["height"] > report["baseline_execution_head"]["height"]:
                    report["bounded_advancement_verified"] = True
                    return
        except NotReady as error:
            last_condition = str(error)
            report["waiting_for"] = last_condition
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    raise CheckError("bounded smoke deadline expired: " + last_condition)


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "metadata-dir", "bootnodes-file", "fixture-report", "fixture-exit-file",
                 "primary-manifest", "primary-provenance", "compliance-manifest", "compliance-provenance"):
        result.add_argument("--" + name, required=True, type=Path)
    result.add_argument("--trusted-checkpoint", required=True)
    result.add_argument("--trust-basis", required=True)
    result.add_argument("--checkpoint-url", required=True)
    result.add_argument("--advertised-ip", required=True)
    result.add_argument("--timeout-seconds", type=int, default=3600)
    for name, default in (("rpc-port", 18545), ("beacon-port", 15052), ("peer-port", 19000),
                          ("quic-port", 19001), ("execution-port", 19303)):
        result.add_argument("--" + name, type=int, default=default)
    result.add_argument("--artifact-root", type=Path, default=TESTING_ROOT / "artifacts/hoodi-smoke")
    return result


def main():
    args = parser().parse_args()
    report = {"schema_version": 1, "network_name": "hoodi", "required_new_blocks": BLOCK_LIMIT,
              "status": "blocked", "bounded_advancement_verified": False,
              "public_network_sync_acceptance": False, "mainnet_sync_acceptance": False,
              "independent_ssz_or_bls_reverification": False,
              "stop_policy": "observer sends owned SIGINT after five validated additions; concurrent imports can occur before shutdown"}
    child, directory, executable_hash = None, None, None
    try:
        trusted, metadata = checked_inputs(args)
        gate = full_conformance(args)
        binary = args.binary.resolve(strict=True)
        executable_hash = binary_hash(binary)
        profile = compiled_profile(binary)
        free_tcp_ports([args.rpc_port, args.beacon_port, args.peer_port, args.execution_port])
        directory = args.artifact_root.resolve() / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + os.urandom(6).hex())
        directory.mkdir(parents=True, exist_ok=False)
        # Freeze the original checked seed inventory consumed by this particular process.
        seed = directory / "bootnodes.txt"
        shutil.copyfile(metadata["selected_bootnodes"], seed)
        command = argv(args, binary, directory, seed)
        fixture_inputs = {name: {"path": str(Path(getattr(args, name)).resolve()), "sha256": binary_hash(getattr(args, name))}
                          for name in ("fixture_report", "fixture_exit_file", "primary_manifest", "primary_provenance",
                                       "compliance_manifest", "compliance_provenance")}
        report.update({"status": "running", "fixture_artifact_identities": fixture_inputs, "trusted_checkpoint": trusted, "trust_basis": args.trust_basis,
                       "checkpoint_source": args.checkpoint_url, "compiled_profile": profile,
                       "complete_fixture_gate": gate, "binary": str(binary), "binary_sha256": executable_hash,
                       "seed_source": metadata["proof"], "seed_sha256": digest(seed.read_bytes()),
                       "process_argv": command, "started_utc": datetime.now(timezone.utc).isoformat()})
        (directory / "launch.json").write_text(json.dumps(report, indent=2) + "\n")
        with (directory / "node.log").open("wb") as log:
            child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                     start_new_session=True, env={**os.environ, "RUST_BACKTRACE": "1"})
            report["owned_pid"] = child.pid
            observe(child, args, metadata, report)
            report["owned_shutdown"] = stop_owned(child)
            child = None
        if not report["owned_shutdown"]["graceful"]:
            raise CheckError("owned follower did not complete graceful shutdown")
        if binary_hash(binary) != executable_hash:
            raise CheckError("actual executable identity changed during observation")
        report["status"] = "passed"
        report.pop("waiting_for", None)
    except KeyboardInterrupt:
        report["status"] = "interrupted"
        report["error"] = "operator interrupted the owned bounded smoke observation"
    except (CheckError, ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        report["status"] = "failed" if directory is not None else "blocked"
        report["error"] = str(error)
    finally:
        if child is not None:
            report["owned_shutdown"] = stop_owned(child)
        report["finished_utc"] = datetime.now(timezone.utc).isoformat()
        if directory is not None:
            (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
            print("Preserved smoke artifacts: " + str(directory))
        else:
            print(json.dumps(report, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
