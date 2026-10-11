#!/usr/bin/env python3
"""Read-only public-chain sync, canonical advancement and restart acceptance."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import time

from beacon import finalized_header, fixed_beacon
from contracts import CheckError, Endpoint, HttpTransport, NotReady, active_fork, decimal, normalized, hex_bytes, obj, quantity, schedule
from execution import compare_execution, execution_identity
from provenance import deployment_provenance
from verify import readiness_gate, write_report

CHAIN_IDS = {"mainnet": 1, "hoodi": 560048}
MAX_REPORT_BYTES = 64 * 1024 * 1024


def load_restart(path, digest):
    """Bind one completed prior observation; live endpoints must reverify every saved anchor."""
    if path is None:
        if digest is not None:
            raise CheckError("restart checksum requires a prior report")
        return None
    if digest is None:
        raise CheckError("restart observation requires the exact prior report checksum")
    raw = Path(path).read_bytes()
    if len(raw) > MAX_REPORT_BYTES or hashlib.sha256(raw).hexdigest() != hex_bytes(digest, 32, "restart report checksum")[2:]:
        raise CheckError("restart report size or checksum differs")
    try:
        previous = obj(json.loads(raw), "restart report")
    except (ValueError, UnicodeDecodeError):
        raise CheckError("restart report is malformed") from None
    if previous.get("schema_version") != 1 or previous.get("status") != "passed" or previous.get("public_network_sync_acceptance") is not True:
        raise CheckError("restart requires a completed public observation")
    return previous


def caught_up(beacon, execution, config, max_lag, now):
    """Require actual sync flags and a recent canonical head, independently of peer count."""
    syncing = obj(obj(beacon.beacon("/eth/v1/node/syncing"), "node sync response").get("data"), "node sync data")
    for key in ("is_syncing", "is_optimistic", "el_offline"):
        if type(syncing.get(key)) is not bool:
            raise CheckError("node sync flags must be explicit booleans")
        if syncing[key]:
            raise NotReady("embedded consensus or execution is not fully verified and caught up")
    head_slot = decimal(syncing.get("head_slot"), "sync head slot")
    if decimal(syncing.get("sync_distance"), "sync distance") > max_lag:
        raise NotReady("embedded sync distance exceeds the allowed live lag")
    response = obj(beacon.beacon("/eth/v1/beacon/headers/head"), "canonical head response")
    data = obj(response.get("data"), "head header data")
    if response.get("execution_optimistic") is not False or data.get("canonical") is not True:
        raise NotReady("embedded head is not canonical and execution verified")
    message = obj(obj(data.get("header"), "signed head header").get("message"), "head header")
    slot = decimal(message.get("slot"), "head slot")
    if slot != head_slot:
        raise NotReady("head changed while collecting sync observation")
    root = hex_bytes(data.get("root"), 32, "head root")
    genesis = config["genesis"]["genesis_time"]
    if now < genesis:
        raise CheckError("public network genesis is in the future")
    wall_slot = int((now - genesis) * 1000) // config["slot_duration_ms"]
    if slot > wall_slot + 1:
        raise CheckError("embedded beacon head is ahead of the checked clock")
    if wall_slot - slot > max_lag:
        raise NotReady("embedded beacon head is stale")
    peers = obj(obj(beacon.beacon("/eth/v1/node/peer_count"), "peer count response").get("data"), "peer count data")
    connected = decimal(peers.get("connected"), "connected peer count")
    if connected == 0:
        raise NotReady("embedded node has no connected consensus peers")
    latest = obj(execution.rpc("eth_getBlockByNumber", ["latest", False]), "latest execution block")
    height = quantity(latest.get("number"), "latest execution height")
    timestamp = quantity(latest.get("timestamp"), "latest execution timestamp")
    if timestamp > now + config["slot_duration_ms"] / 1000:
        raise CheckError("embedded execution head is ahead of the checked clock")
    if now - timestamp > (max_lag + 1) * config["slot_duration_ms"] / 1000:
        raise NotReady("embedded execution head is stale")
    return {"beacon_slot": slot, "beacon_root": root, "fork": active_fork(config, slot),
            "execution_height": height, "execution_hash": hex_bytes(latest.get("hash"), 32, "latest execution hash"),
            "connected_consensus_peers": connected, "wall_slot": wall_slot}


def verify_restart(previous, network, config, sut_beacon, reference_beacon, sut_el, reference_el):
    if previous is None:
        return None
    if previous.get("execution_network") != network or previous.get("network") != config:
        raise CheckError("restarted node changed its original public network identity")
    anchor = obj(previous.get("finalized_anchor"), "prior finalized anchor")
    slot = anchor.get("beacon_slot")
    height = anchor.get("execution_height")
    if type(slot) is not int or type(height) is not int or slot < 0 or height < 0:
        raise CheckError("prior finalized anchor has invalid coordinates")
    root = hex_bytes(anchor.get("beacon_root"), 32, "prior finalized beacon root")
    execution_hash = hex_bytes(anchor.get("execution_hash"), 32, "prior finalized execution hash")
    for node in (sut_beacon, reference_beacon):
        observed = fixed_beacon(node, slot)
        if observed is None:
            raise NotReady("restarted node cannot yet retrieve the previous finalized beacon anchor")
        if observed[0].root != root:
            raise CheckError("previous finalized beacon anchor changed after restart")
    compared = compare_execution(sut_el, reference_el, height)
    if compared["hash"] != execution_hash:
        raise CheckError("previous finalized execution anchor changed after restart")
    return {"previous_beacon_slot": slot, "previous_beacon_root": root,
            "previous_execution_height": height, "previous_execution_hash": execution_hash,
            "retained_canonical_anchors_reverified": True}


def observe(args, report):
    provenance = deployment_provenance(args.independent_runtime_provenance, args)
    report["deployment_provenance"] = provenance
    previous = load_restart(args.resume_report, args.resume_report_sha256)
    deadline = time.monotonic() + args.timeout_seconds
    transport = HttpTransport(deadline)
    sut_beacon = Endpoint(args.sut_beacon_url, "sut-beacon", transport)
    reference_beacon = Endpoint(args.reference_beacon_url, "reference-beacon", transport)
    sut_el = Endpoint(args.sut_el_url, "sut-execution", transport)
    reference_el = Endpoint(args.reference_el_url, "reference-execution", transport)
    config, network, initial, restart = None, None, None, None
    last_condition = "authenticating public network and live node"
    while time.monotonic() < deadline:
        try:
            if config is None:
                candidate = schedule(sut_beacon)
                if candidate != schedule(reference_beacon):
                    raise CheckError("public beacon endpoints disagree on original genesis or schedule")
                identity = execution_identity(sut_el, reference_el)
                if identity["chain_id"] != CHAIN_IDS[args.network]:
                    raise CheckError("execution chain id differs from requested public network")
                if identity["genesis_execution_hash"] != args.expected_execution_genesis_hash:
                    raise CheckError("execution genesis differs from the independently supplied public identity")
                if candidate["genesis"]["genesis_validators_root"] != args.expected_genesis_validators_root:
                    raise CheckError("consensus genesis differs from the independently supplied public identity")
                config, network = candidate, identity
                report.update({"network": config, "execution_network": network})
            live = caught_up(sut_beacon, sut_el, config, args.max_head_lag_slots, time.time())
            if args.expected_live_fork is not None and live["fork"] != args.expected_live_fork:
                raise CheckError("live consensus fork differs from requested acceptance fork")
            if previous is not None and restart is None:
                restart = verify_restart(previous, network, config, sut_beacon, reference_beacon, sut_el, reference_el)
                report["restart"] = restart
            a, b = finalized_header(sut_beacon), finalized_header(reference_beacon)
            slot = min(a.slot, b.slot)
            left, right = fixed_beacon(sut_beacon, slot), fixed_beacon(reference_beacon, slot)
            if left is None or right is None:
                raise NotReady("common finalized beacon block is unavailable")
            if left[0].root != right[0].root or left[3] != right[3] or normalized(left[2]) != normalized(right[2]):
                raise CheckError("common public finalized beacon block, state or execution payload differs")
            if left[2] is None:
                raise CheckError("public synchronization requires a real accepted execution payload")
            height = decimal(left[2]["block_number"], "finalized beacon execution height")
            comparison = compare_execution(sut_el, reference_el, height, left[2])
            anchor = {"beacon_slot": slot, "beacon_root": left[0].root,
                      "execution_height": height, "execution_hash": comparison["hash"]}
            report.update({"live_head": live, "finalized_anchor": anchor,
                           "finalized_execution": comparison, "independent_ssz_root_computation": False})
            if initial is None:
                initial = {**anchor, "live_beacon_slot": live["beacon_slot"], "live_execution_height": live["execution_height"]}
                report["initial_observation"] = initial
            advanced = slot > initial["beacon_slot"] and height >= initial["execution_height"] + args.min_finalized_blocks
            advanced = advanced and live["beacon_slot"] > initial["live_beacon_slot"] and live["execution_height"] > initial["live_execution_height"]
            if previous is not None:
                advanced = advanced and slot > previous["finalized_anchor"]["beacon_slot"] and height > previous["finalized_anchor"]["execution_height"]
            if advanced:
                if deployment_provenance(args.independent_runtime_provenance, args) != provenance:
                    raise CheckError("live independent deployment changed during public observation")
                report.pop("waiting_for", None)
                report.update({"status": "passed", "public_network_sync_acceptance": True,
                               "restart_resume_acceptance": previous is not None,
                               "scope": "observed verified canonical progression; complete historical genesis sync is not asserted"})
                return
            last_condition = "awaiting verified beacon, execution and finalized advancement"
        except NotReady as error:
            last_condition = str(error)
        report.update({"status": "waiting", "waiting_for": last_condition})
        write_report(args.output_dir, report)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(args.poll_interval, remaining))
    raise CheckError("public observation deadline expired: " + last_condition)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", choices=CHAIN_IDS, required=True)
    for role in ("sut", "reference"):
        for layer in ("beacon", "el"):
            parser.add_argument(f"--{role}-{layer}-url", required=True)
    parser.add_argument("--expected-execution-genesis-hash", required=True)
    parser.add_argument("--expected-genesis-validators-root", required=True)
    parser.add_argument("--expected-live-fork", choices=("bellatrix", "capella", "deneb", "electra", "fulu", "gloas"))
    parser.add_argument("--readiness-file", required=True)
    parser.add_argument("--independent-runtime-provenance", required=True)
    parser.add_argument("--resume-report")
    parser.add_argument("--resume-report-sha256")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=2400)
    parser.add_argument("--poll-interval", type=float, default=6)
    parser.add_argument("--max-head-lag-slots", type=int, default=4)
    parser.add_argument("--min-finalized-blocks", type=int, default=2)
    args = parser.parse_args(argv)
    report = {"schema_version": 1, "status": "starting", "requested_network": args.network,
              "public_network_sync_acceptance": False, "restart_resume_acceptance": False}
    try:
        if any(not math.isfinite(value) or value <= 0 for value in (args.timeout_seconds, args.poll_interval)) or args.max_head_lag_slots < 1 or args.min_finalized_blocks < 1:
            raise CheckError("public observation limits must be finite and positive")
        args.expected_execution_genesis_hash = hex_bytes(args.expected_execution_genesis_hash, 32, "expected execution genesis hash")
        args.expected_genesis_validators_root = hex_bytes(args.expected_genesis_validators_root, 32, "expected genesis validators root")
        report["embedded_readiness"] = readiness_gate(args.readiness_file)
        if not report["embedded_readiness"]["ready"]:
            report.update({"status": "blocked", "error": "independent embedded beacon readiness is false"})
            write_report(args.output_dir, report)
            return 2
        observe(args, report)
        write_report(args.output_dir, report)
        return 0
    except (CheckError, OSError, ValueError, KeyError, TypeError) as error:
        report.update({"status": "failed", "public_network_sync_acceptance": False,
                       "restart_resume_acceptance": False,
                       "error": str(error) if isinstance(error, CheckError) else "public observation artifact could not be processed"})
        write_report(args.output_dir, report)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
