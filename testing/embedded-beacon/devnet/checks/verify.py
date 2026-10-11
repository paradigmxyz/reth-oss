#!/usr/bin/env python3
"""Condition-based devnet verification with separate embedded-runtime readiness evidence."""

import argparse
import json
import math
from pathlib import Path
import tempfile
import time
from beacon import finalized_header, fixed_beacon
from contracts import CheckError, NotReady, Endpoint, HttpTransport, FAR_FUTURE_EPOCH, FORKS, active_fork, array, decimal, normalized, obj, required_forks, schedule
from execution import compare_execution, execution_identity, finalized_el
from provenance import deployment_provenance

READINESS_FIELDS = ("full_state_transition", "full_fork_choice", "peer_runtime", "checkpoint_bootstrap", "verified_state_recovery", "full_spec_conformance")


def readiness_gate(path, probe_path=None):
    try:
        status = obj(json.loads(Path(path).read_text(encoding="utf-8-sig")), "embedded readiness")
    except (OSError, ValueError):
        raise CheckError("embedded readiness artifact is missing or malformed") from None
    if status.get("target_fork") != "gloas" or any(type(status.get(key)) is not bool for key in READINESS_FIELDS):
        raise CheckError("embedded readiness artifact lacks required Gloas capabilities")
    ready, probe = all(status[key] for key in READINESS_FIELDS), None
    if probe_path is not None:
        try:
            probe = obj(json.loads(Path(probe_path).read_text(encoding="utf-8-sig")), "embedded launch probe")
        except (OSError, ValueError):
            raise CheckError("embedded launch probe artifact is missing or malformed") from None
        if type(probe.get("exit_code")) is not int or type(probe.get("datadir_initialized")) is not bool or type(probe.get("readiness_error")) is not bool:
            raise CheckError("embedded launch probe lacks exit code, datadir, or readiness error evidence")
        if not ready and not probe["readiness_error"]:
            raise CheckError("embedded launch probe failed for a reason other than readiness")
        if not ready and (probe["exit_code"] == 0 or probe["datadir_initialized"]):
            raise CheckError("incomplete embedded runtime failed to stop before database initialization")
    return {"capabilities": status, "ready": ready, "fail_closed_probe_verified": probe is not None and not ready, "embedded_runtime_acceptance": False}


def write_report(directory, report):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=directory, prefix="report-", suffix=".tmp", delete=False) as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
        temporary = Path(output.name)
    temporary.replace(directory / "verification.json")


def check_planned(config, path, names):
    planned = obj(json.loads(Path(path).read_text(encoding="utf-8-sig")), "planned fork schedule")
    if planned.get("slots_per_epoch") != config["slots_per_epoch"] or planned.get("slot_duration_ms") != config["slot_duration_ms"]:
        raise CheckError("deployed consensus clock differs from planned schedule")
    if planned.get("genesis_fork") != active_fork(config, 0):
        raise CheckError("deployed genesis fork differs from planned schedule")
    if planned.get("expected_forks") != names:
        raise CheckError("required fork coverage differs from planned schedule")
    planned_forks = array(planned.get("forks"), "planned forks")
    if len(planned_forks) != len(config["forks"]) or {obj(entry, "planned fork").get("name") for entry in planned_forks} != {entry["name"] for entry in config["forks"]}:
        raise CheckError("planned schedule must describe every deployed fork exactly once")
    for entry in planned_forks:
        entry = obj(entry, "planned fork")
        actual = next((item for item in config["forks"] if item["name"] == entry.get("name")), None)
        if actual is None or type(entry.get("epoch")) is not int or actual["epoch"] != entry["epoch"] or actual["version"] != entry.get("version"):
            raise CheckError("deployed fork configuration differs from planned schedule")

    return planned


def merge_contract(config, planned, network, consensus_only):
    """Require observable canonical terminal-block settings for historical execution."""
    if planned is None or "merge_transition" not in planned:
        return None
    metadata = obj(planned["merge_transition"], "planned merge transition")
    if set(metadata) != {"terminal_block", "activation_epoch"} or metadata.get("terminal_block") != "execution_genesis":
        raise CheckError("historical merge plan must identify the execution genesis terminal block")
    epoch = metadata.get("activation_epoch")
    bellatrix = next((fork for fork in config["forks"] if fork["name"] == "bellatrix"), None)
    if type(epoch) is not int or epoch <= 0 or epoch == FAR_FUTURE_EPOCH or bellatrix is None or epoch != bellatrix["epoch"] or "bellatrix" not in planned["expected_forks"]:
        raise CheckError("historical merge activation must match the required Bellatrix boundary")
    if consensus_only or network is None:
        raise CheckError("historical merge requires execution endpoints")
    terminal = config.get("terminal_block")
    if terminal is None:
        raise CheckError("historical merge requires advertised terminal block configuration")
    if terminal["hash"] != network["genesis_execution_hash"]:
        raise CheckError("configured terminal block differs from shared execution genesis")
    if terminal["activation_epoch"] != epoch:
        raise CheckError("configured terminal block activation differs from planned Bellatrix epoch")
    return {"terminal_block": "execution_genesis", "terminal_block_hash": terminal["hash"], "activation_epoch": epoch, "first_accepted_execution": None}


def observe_fork(entry, config, limit, sut_beacon, reference_beacon, sut_el, reference_el, comparisons, consensus_only, merge=None):
    name = entry["name"]
    activation_slot = entry["epoch"] * config["slots_per_epoch"]
    start = max(1, activation_slot)
    next_boundary = min((item["epoch"] * config["slots_per_epoch"] for item in config["forks"] if item["epoch"] > entry["epoch"]), default=FAR_FUTURE_EPOCH)
    stop = min(activation_slot + config["slots_per_epoch"] - 1, next_boundary - 1, limit)
    for slot in range(start, stop + 1):
        try:
            left, right = fixed_beacon(sut_beacon, slot), fixed_beacon(reference_beacon, slot)
        except NotReady:
            continue
        if left is None and right is None:
            continue
        if left is None or right is None or left[0].root != right[0].root:
            raise CheckError(f"finalized beacon boundary blocks differ for {name}")
        if left[1]["version"] != name or right[1]["version"] != name:
            raise CheckError(f"scheduled fork {name} did not activate at its boundary")
        if left[3] != right[3]:
            raise CheckError(f"fork-boundary consensus state roots differ for {name}")
        if normalized(left[2]) != normalized(right[2]):
            raise CheckError(f"fork-boundary execution payloads differ for {name}")
        transition = None
        if merge is not None and name == "bellatrix":
            if left[2] is None or decimal(left[2]["block_number"], "first execution height") != 1 or left[2]["parent_hash"].lower() != merge["terminal_block_hash"]:
                raise CheckError("Bellatrix boundary must contain the first accepted execution block with the canonical terminal parent")
            transition = {"height": 1, "hash": left[2]["block_hash"].lower(), "parent_hash": left[2]["parent_hash"].lower(), "beacon_slot": slot, "beacon_root": left[0].root}
        if left[2] is not None:
            if consensus_only:
                raise CheckError("consensus-only historical case entered an execution fork")
            height = decimal(left[2]["block_number"], "boundary execution height")
            comparisons[height] = compare_execution(sut_el, reference_el, height, left[2])
        before = None
        if entry["epoch"] > 0:
            for prior_slot in range(start - 1, max(-1, start - config["slots_per_epoch"] - 1), -1):
                try:
                    a, b = fixed_beacon(sut_beacon, prior_slot), fixed_beacon(reference_beacon, prior_slot)
                except NotReady:
                    continue
                if a is None and b is None:
                    continue
                if a is None or b is None or a[0].root != b[0].root:
                    raise CheckError("pre-fork finalized beacon blocks differ")
                version = active_fork(config, prior_slot)
                if a[1]["version"] != version or b[1]["version"] != version:
                    raise CheckError("pre-fork block has wrong consensus version")
                if a[3] != b[3] or normalized(a[2]) != normalized(b[2]):
                    raise CheckError("pre-fork consensus states or payloads differ")
                before = {"slot": prior_slot, "version": version, "root": a[0].root, "state_root": a[3], "state_lookup": {"sut": a[4], "reference": b[4]}}
                break
            if before is None:
                raise NotReady(f"no finalized pre-boundary block observed for {name}")
        if transition is not None:
            merge["first_accepted_execution"] = transition
        return {"fork": name, "activation_epoch": entry["epoch"], "first_observed_slot": slot, "beacon_root": left[0].root, "beacon_state_root": left[3], "state_lookup": {"sut": left[4], "reference": right[4]}, "independent_ssz_root_computation": False, "before_boundary": before, "execution_fields_checked": left[2] is not None}
    return None


def verify(args, report):
    provenance = deployment_provenance(args.independent_runtime_provenance, args)
    report["deployment_provenance"] = provenance
    deadline = time.monotonic() + args.timeout_seconds
    transport = HttpTransport(deadline)
    sut_beacon, reference_beacon = Endpoint(args.sut_beacon_url, "sut-beacon", transport), Endpoint(args.reference_beacon_url, "reference-beacon", transport)
    sut_el = None if args.consensus_only else Endpoint(args.sut_el_url, "sut-execution", transport)
    reference_el = None if args.consensus_only else Endpoint(args.reference_el_url, "reference-execution", transport)
    names = args.expected_forks.split(",")
    if not names or any(not name for name in names) or len(set(names)) != len(names):
        raise CheckError("expected fork list must be nonempty and unique")
    config, expected, initial, merge = None, None, None, None
    coverage, comparisons = {}, {}
    last_condition = "initializing endpoints"
    while time.monotonic() < deadline:
        try:
            if config is None:
                candidate = schedule(sut_beacon)
                if candidate != schedule(reference_beacon):
                    raise CheckError("beacon endpoints disagree on genesis or fork configuration")
                expected = required_forks(candidate, names)
                planned = check_planned(candidate, args.schedule_file, names) if args.schedule_file else None
                if not args.consensus_only:
                    report["execution_network"] = execution_identity(sut_el, reference_el)
                merge = merge_contract(candidate, planned, report.get("execution_network"), args.consensus_only)
                if merge is not None:
                    report["merge_transition"] = merge
                config = candidate
                report["network"] = config
            a, b = finalized_header(sut_beacon), finalized_header(reference_beacon)
            if initial is None:
                initial = {"sut_beacon_slot": a.slot, "reference_beacon_slot": b.slot, "common_execution_height": None}
                report["initial_finality"] = initial
            limit = min(a.slot, b.slot)
            for entry in expected:
                if entry["name"] not in coverage:
                    observation = observe_fork(entry, config, limit, sut_beacon, reference_beacon, sut_el, reference_el, comparisons, args.consensus_only, merge)
                    if observation is not None:
                        coverage[entry["name"]] = observation
            report["fork_coverage"] = list(coverage.values())
            receipts, execution_advanced = 0, args.consensus_only
            if not args.consensus_only:
                common = min(finalized_el(sut_el), finalized_el(reference_el))
                if initial["common_execution_height"] is None:
                    initial["common_execution_height"] = common
                comparisons[common] = compare_execution(sut_el, reference_el, common)
                receipts = sum(item["transaction_count"] for item in comparisons.values())
                if receipts < args.min_transaction_receipts:
                    for height in range(common - 1, max(-1, common - 65), -1):
                        if height not in comparisons:
                            comparisons[height] = compare_execution(sut_el, reference_el, height)
                            receipts += comparisons[height]["transaction_count"]
                        if receipts >= args.min_transaction_receipts:
                            break
                execution_advanced = common >= initial["common_execution_height"] + args.min_finalized_blocks
                report["finalized_execution_comparisons"] = [comparisons[key] for key in sorted(comparisons)]
                report["finalized_transaction_receipts_checked"] = receipts
            beacon_advanced = a.slot > initial["sut_beacon_slot"] and b.slot > initial["reference_beacon_slot"]
            report["latest_finality"] = {"sut_beacon_slot": a.slot, "reference_beacon_slot": b.slot, "sut_beacon_root": a.root, "reference_beacon_root": b.root}
            merge_verified = merge is None or merge["first_accepted_execution"] is not None
            if len(coverage) == len(expected) and merge_verified and beacon_advanced and execution_advanced and (args.consensus_only or receipts >= args.min_transaction_receipts):
                report.pop("waiting_for", None)
                if deployment_provenance(args.independent_runtime_provenance, args) != provenance:
                    raise CheckError("live independent deployment changed during verification")
                report.update({"embedded_runtime_acceptance": True, "public_network_sync_acceptance": False, "status": "passed"})
                return
            last_condition = f"awaiting forks {[name for name in names if name not in coverage]}, advancing finality, or finalized receipt workload"
        except NotReady as exc:
            last_condition = str(exc)
        report.update({"status": "waiting", "waiting_for": last_condition})
        write_report(args.output_dir, report)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(args.poll_interval, remaining))
    raise CheckError(f"verification deadline expired: {last_condition}")


def parser():
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--sut-el-url")
    command.add_argument("--reference-el-url")
    command.add_argument("--sut-beacon-url")
    command.add_argument("--reference-beacon-url")
    command.add_argument("--expected-forks", help="Comma-separated coverage; defaults to schedule.expected_forks")
    command.add_argument("--schedule-file", "--schedule", dest="schedule_file")
    command.set_defaults(consensus_only=False)
    command.add_argument("--preflight-only", action="store_true")
    command.add_argument("--independent-runtime-provenance", "--provenance-file", dest="independent_runtime_provenance")
    command.add_argument("--readiness-file", required=True)
    command.add_argument("--embedded-launch-probe")
    command.add_argument("--require-embedded-ready", action="store_true", help=argparse.SUPPRESS)
    command.add_argument("--output-dir", required=True)
    command.add_argument("--timeout-seconds", type=float, default=2400)
    command.add_argument("--poll-interval", type=float, default=1)
    command.add_argument("--min-finalized-blocks", type=int, default=2)
    command.add_argument("--min-transaction-receipts", type=int, default=1)
    return command


def main(argv=None):
    args = parser().parse_args(argv)
    report = {"schema_version": 2, "status": "starting", "test_topology": "reference_pair_and_embedded_node", "embedded_runtime_acceptance": False, "public_network_sync_acceptance": False, "required_forks": []}
    try:
        if args.expected_forks is None:
            if args.schedule_file is None:
                raise CheckError("expected forks require --schedule or --expected-forks")
            planned = obj(json.loads(Path(args.schedule_file).read_text(encoding="utf-8-sig")), "planned fork schedule")
            names = array(planned.get("expected_forks"), "planned expected forks")
            if not names or any(not isinstance(name, str) or name not in FORKS for name in names) or len(set(names)) != len(names):
                raise CheckError("planned expected forks must be supported and unique")
            args.expected_forks = ",".join(names)
        report["required_forks"] = args.expected_forks.split(",")
        if not math.isfinite(args.timeout_seconds) or not math.isfinite(args.poll_interval) or args.timeout_seconds <= 0 or args.poll_interval <= 0 or args.min_finalized_blocks < 1 or args.min_transaction_receipts < 0:
            raise CheckError("verification limits must be finite and positive; receipt minimum may be zero")
        report["embedded_readiness"] = readiness_gate(args.readiness_file, args.embedded_launch_probe)
        if not report["embedded_readiness"]["ready"]:
            report.update({"status": "blocked", "error": "independent embedded beacon readiness is false"})
            write_report(args.output_dir, report)
            print(json.dumps({"status": "blocked", "embedded_runtime_acceptance": False}))
            return 2
        if args.schedule_file is None:
            raise CheckError("independent verification requires the complete planned schedule")
        planned = obj(json.loads(Path(args.schedule_file).read_text(encoding="utf-8-sig")), "independent schedule")
        if planned.get("preset") != "mainnet" or planned.get("slots_per_epoch") != 32 or planned.get("slot_duration_ms") != 6000 or planned.get("consensus_mode") != "independent_embedded":
            raise CheckError("independent plan requires Mainnet preset, 32 slots per epoch, 6000ms slots, and embedded consensus mode")
        if args.preflight_only:
            report.update({"status": "ready_not_deployed", "embedded_runtime_acceptance": False})
            write_report(args.output_dir, report)
            print(json.dumps({"status": "ready_not_deployed", "embedded_runtime_acceptance": False}))
            return 0
        if not all((args.sut_el_url, args.reference_el_url, args.sut_beacon_url, args.reference_beacon_url)):
            raise CheckError("independent verification requires all four mapped participant endpoints")
        verify(args, report)
        write_report(args.output_dir, report)
        print(json.dumps({"status": "passed", "embedded_runtime_acceptance": True, "public_network_sync_acceptance": False}))
        return 0
    except (CheckError, OSError, ValueError) as exc:
        report.update({"status": "failed", "error": str(exc) if isinstance(exc, CheckError) else "local verification artifact could not be processed"})
        write_report(args.output_dir, report)
        print(json.dumps({"status": "failed", "error": report["error"], "embedded_runtime_acceptance": False}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
