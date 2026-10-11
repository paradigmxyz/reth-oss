#!/usr/bin/env python3
"""Report subset fixture diagnostics against reviewed, release-bound expected failures.

Accepted diagnostic failures never establish complete consensus conformance.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys


MARKERS = ("spec_only_bls_disabled", "spec_only_execution_mock",
           "spec_only_data_availability_mock", "spec_only_asserted_graph")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field: " + key)
        result[key] = value
    return result


def reject_non_finite(value):
    raise ValueError("non-finite JSON number: " + value)


def read_json(path, limit=64 * 1024 * 1024):
    if path.stat().st_size > limit:
        raise ValueError(f"JSON exceeds bounded input limit: {path.name}")
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_non_finite, object_pairs_hook=unique_object)


def gate(expected, report, provenance, manifest, preset, runner_exit_code):
    if expected.get("format_version") != 1:
        raise ValueError("unsupported expected-failure format")
    if expected.get("expected_outcome") != "unsupported":
        raise ValueError("pending expectations may allow only unsupported outcomes")
    release = expected["spec_release"]
    if report.get("spec_release") != release or provenance.get("spec_release") != release:
        raise ValueError("fixture release differs from the reviewed expectations")
    if provenance.get("profile") != "full":
        raise ValueError("CI must download the complete target-fork profile")
    if report.get("errors"):
        raise ValueError("fixture setup/integrity errors cannot be expected failures")
    if runner_exit_code not in (0, 1):
        raise ValueError(f"fixture runner infrastructure/build failure: {runner_exit_code}")
    if manifest.stat().st_size > 512 * 1024 * 1024:
        raise ValueError("fixture inventory exceeds its bounded input limit")
    digest = hashlib.sha256()
    with manifest.open("rb") as stream:
        while chunk := stream.read(65536):
            digest.update(chunk)
    if digest.hexdigest() != provenance.get("manifest_sha256", "").removeprefix("0x"):
        raise ValueError("fixture inventory digest differs from authenticated provenance")
    identities = set()
    pattern = re.compile(r'\s*"([^"\\]+/manifest\.yaml)":')
    with manifest.open(encoding="utf-8") as stream:
        for line in stream:
            match = pattern.match(line)
            if match:
                identity = match.group(1).removesuffix("/manifest.yaml")
                if identity in identities:
                    raise ValueError("duplicate fixture case identity")
                identities.add(identity)
    if not identities or any(not identity.startswith(preset + "/") for identity in identities):
        raise ValueError("fixture shard is empty or contains another preset")
    required = expected["required_inventory"][preset]
    identity_digest = hashlib.sha256(("\n".join(sorted(identities)) + "\n").encode()).hexdigest()
    if len(identities) != required["cases"] or identity_digest != required["case_ids_sha256"]:
        raise ValueError("required published cases are missing, added or renamed")
    if provenance.get("case_count") != len(identities) or provenance.get("published_case_count") != len(identities):
        raise ValueError("download did not extract every published target case")
    allowed_entries = expected["expected_failures"]
    if not isinstance(allowed_entries, list) or any(not isinstance(value, str) for value in allowed_entries):
        raise ValueError("expected failures must be explicit case identities")
    allowed = set(allowed_entries)
    if len(allowed) != len(allowed_entries):
        raise ValueError("duplicate expected-failure identity")
    cases = report.get("cases", [])
    actual = {}
    markers = {marker: 0 for marker in MARKERS}
    for case in cases:
        identity = case["path"]
        if identity in actual:
            raise ValueError("duplicate result case identity")
        outcome = case["outcome"]
        if outcome not in ("passed", "failed", "unsupported"):
            raise ValueError("unknown fixture outcome")
        seconds = case.get("seconds")
        if type(seconds) not in (float, int) or not math.isfinite(seconds) or seconds < 0:
            raise ValueError("invalid fixture execution duration")
        if not isinstance(case.get("message"), str) or outcome == "passed" and case["message"] != "":
            raise ValueError("invalid fixture outcome message")
        for marker in MARKERS:
            if type(case.get(marker)) is not bool:
                raise ValueError("missing or invalid fixture assumption marker")
            markers[marker] += int(case[marker])
        actual[identity] = outcome
    if set(actual) != identities:
        raise ValueError("runner results omit or add fixture cases")
    failures = {identity for identity, outcome in actual.items() if outcome != "passed"}
    unexpected = sorted(identity for identity in failures
                        if identity not in allowed or actual[identity] != "unsupported")
    if unexpected:
        raise ValueError(f"{len(unexpected)} unexpected fixture failures: " + ", ".join(unexpected[:10]))
    if (runner_exit_code == 0) != (not failures):
        raise ValueError("runner status does not match its reported case outcomes")
    improvements = sorted(identity for identity in identities & allowed if actual[identity] == "passed")
    return {"accepted": True, "spec_release": release, "preset": preset, "cases": len(identities),
            "passed": len(identities) - len(failures), "expected_failures": len(failures),
            "unexpected_failures": 0, "newly_passing_expected_cases": improvements,
            "complete_conformance": False, "runtime_synchronization_verified": False,
            "fixture_assumptions_are_runtime_proof": False, **markers}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--provenance", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--preset", required=True, choices=["general", "mainnet", "minimal"])
    parser.add_argument("--runner-exit-code", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = gate(read_json(args.expected), read_json(args.report), read_json(args.provenance),
                      args.manifest, args.preset, args.runner_exit_code)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(f"{result['cases']} cases: {result['passed']} passed, {result['expected_failures']} expected unsupported, {len(result['newly_passing_expected_cases'])} stale expectations")
    except (ValueError, KeyError, TypeError, AttributeError, OSError) as error:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"accepted": False, "error": str(error)}, indent=2) + "\n", encoding="utf-8")
        print(f"consensus fixture CI gate failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
