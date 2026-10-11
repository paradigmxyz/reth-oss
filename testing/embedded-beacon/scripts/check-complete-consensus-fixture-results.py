#!/usr/bin/env python3
"""Verify one fresh complete consensus report against both original pinned source inventories.

Expected-failure diagnostics cannot satisfy this gate. Fixture assumptions remain explicit;
independent runtime synchronization is a separate milestone.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PINS = REPOSITORY_ROOT / "testing/embedded-beacon/spec-tests/full-corpus-pins.json"
CASE_DOMAIN = b"embedded-beacon-full-case-ids-v1\0"
FILE_DOMAIN = b"embedded-beacon-full-input-files-v1\0"
MARKERS = ("spec_only_bls_disabled", "spec_only_execution_mock",
           "spec_only_data_availability_mock", "spec_only_asserted_graph")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def count(value, description):
    require(type(value) is int and value >= 0, "invalid " + description)
    return value


def checksum(value, description):
    require(isinstance(value, str), "invalid " + description + " checksum")
    digest = value.removeprefix("0x")
    require(re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
            "invalid " + description + " checksum")
    return digest


def reject_non_finite(value):
    raise ValueError("non-finite JSON number: " + value)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON field: " + key)
        result[key] = value
    return result


def read_json(path, limit=64 * 1024 * 1024):
    require(path.stat().st_size <= limit, "JSON exceeds input limit: " + path.name)
    with path.open(encoding="utf-8") as stream:
        return json.load(stream, parse_constant=reject_non_finite, object_pairs_hook=unique_object)


def manifest(path, expected):
    require(path.stat().st_size <= 512 * 1024 * 1024, "manifest exceeds input limit")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(65536):
            digest.update(chunk)
    require(digest.hexdigest() == checksum(expected, "manifest"),
            "manifest differs from independently pinned original bytes")
    return read_json(path, 512 * 1024 * 1024)


def sized(hasher, value):
    hasher.update(len(value).to_bytes(8, "little"))
    hasher.update(value)


def canonical_path(relative):
    require(isinstance(relative, str) and relative != "", "invalid canonical fixture path")
    path = PurePosixPath(relative)
    require(not path.is_absolute() and "\\" not in relative and ":" not in relative and
            all(part not in ("", ".", "..") for part in relative.split("/")) and
            path.as_posix() == relative, "unsafe canonical fixture path: " + relative)
    return relative


def case_ids_digest(identities):
    digest = hashlib.sha256(CASE_DOMAIN)
    for identity in sorted(identities):
        sized(digest, identity.encode("utf-8"))
    return digest.hexdigest()


def source_identity(pins, role, provenance, manifest_path, file_hasher):
    expected = pins["corpora"][role]
    release = pins["spec_release"]
    require(provenance.get("spec_release") == release and provenance.get("profile") == "full" and
            provenance.get("corpus_role") == role, "wrong " + role + " source release, profile or role")
    require(not provenance.get("error"), role + " source preparation failed")
    require(checksum(provenance.get("manifest_sha256"), role + " provenance") ==
            checksum(expected["manifest_sha256"], role + " pinned manifest"),
            role + " provenance does not bind the pinned original manifest")
    archives = provenance.get("archives")
    require(isinstance(archives, list) and len(archives) == len(expected["archives"]),
            role + " archive provenance is incomplete")
    actual_archives = {}
    for archive in archives:
        name = archive.get("name")
        require(name in expected["archives"] and name not in actual_archives,
                role + " archive provenance contains an unexpected or duplicate asset")
        require(count(archive.get("bytes"), "archive bytes") > 0, "empty source archive")
        require(archive.get("digest_source") in
                {"published_release_metadata", "independently_pinned_release_digest"},
                "source archive has no authenticated digest provenance")
        actual_archives[name] = checksum(archive.get("sha256"), "archive")
    require(actual_archives == expected["archives"], role + " archive digest differs from source pin")
    data = manifest(manifest_path, expected["manifest_sha256"])
    require(data.get("spec_release") == release, "manifest release differs from pinned schema")
    files = data.get("files")
    suites = data.get("required_suites")
    require(isinstance(files, dict) and len(files) == expected["file_count"],
            role + " original file count differs from pin")
    require(isinstance(suites, list) and len(suites) == expected["required_suite_count"] and
            len(set(suites)) == len(suites), role + " required suite inventory differs from pin")
    identities = set()
    for relative in sorted(files):
        canonical_path(relative)
        sized(file_hasher, role.encode("utf-8"))
        sized(file_hasher, relative.encode("utf-8"))
        file_hasher.update(bytes.fromhex(checksum(files[relative], "original fixture")))
        if len(relative.split("/")) >= 7 and relative.endswith((".yaml", ".ssz_snappy")):
            identities.add(relative.rsplit("/", 1)[0])
    require(len(identities) == expected["case_count"], role + " original case count differs from pin")
    actual_suites = {"/".join(identity.split("/")[:5]) for identity in identities}
    require(all(canonical_path(suite) in actual_suites for suite in suites),
            role + " required suite is absent or noncanonical")
    require(count(provenance.get("case_count"), "source cases") == len(identities) and
            count(provenance.get("published_case_count"), "published cases") == len(identities) and
            count(provenance.get("extracted_files"), "source files") == len(files),
            role + " preparation did not preserve the complete original inventory")
    return identities, {"role": role, "files": len(files), "cases": len(identities)}


def coverage_summary(cases):
    """Group exact published identities and disclose assumptions without creating new test credit."""
    families = {}
    for case in cases:
        path = canonical_path(case.get("path"))
        parts = path.split("/")
        require(len(parts) >= 6, "fixture result lacks a complete handler identity")
        key = tuple(parts[:5])
        family = families.setdefault(key, {"preset": key[0], "fork": key[1], "runner": key[2],
                                          "handler": key[3], "suite": key[4], "cases": 0,
                                          "passed": 0, "failed": 0, "unsupported": 0,
                                          **{marker: 0 for marker in MARKERS}})
        outcome = case.get("outcome")
        require(outcome in ("passed", "failed", "unsupported"), "unknown fixture outcome")
        family["cases"] += 1
        family[outcome] += 1
        for marker in MARKERS:
            require(type(case.get(marker)) is bool, "missing or invalid fixture assumption marker")
            family[marker] += int(case[marker])
    return [families[key] for key in sorted(families)]


def gate(pins, report, primary_provenance, compliance_provenance,
         primary_manifest, compliance_manifest, runner_exit_code):
    require(type(runner_exit_code) is int and runner_exit_code == 0,
            f"complete runner did not exit successfully: {runner_exit_code}")
    require(pins.get("format_version") == 1 and pins.get("corpus_role_order") == ["primary", "compliance"],
            "unknown complete source pin format or role order")
    require(report.get("spec_release") == pins["spec_release"] and report.get("complete_matrix") is True,
            "report is focused or uses another schema release")
    require(isinstance(report.get("errors"), list) and not report["errors"],
            "complete report contains setup or integrity errors")
    evidence = report.get("full_corpus")
    require(isinstance(evidence, dict), "report has no fresh complete source authentication evidence")
    file_hasher = hashlib.sha256(FILE_DOMAIN)
    identities = set()
    counts = []
    for role, provenance, path in [("primary", primary_provenance, primary_manifest),
                                   ("compliance", compliance_provenance, compliance_manifest)]:
        source_cases, source_counts = source_identity(pins, role, provenance, path, file_hasher)
        require(not identities.intersection(source_cases), "duplicate canonical case across source roles")
        identities.update(source_cases)
        counts.append(source_counts)
    require(sum(source["files"] for source in counts) == pins["required_file_checks"],
            "complete original file count differs from pin")
    input_digest = file_hasher.hexdigest()
    identity_digest = case_ids_digest(identities)
    require(input_digest == checksum(pins["canonical_input_files_sha256"], "pinned full input") and
            identity_digest == checksum(pins["canonical_case_ids_sha256"], "pinned full cases") and
            len(identities) == pins["required_case_count"], "complete original input/case identity differs from pin")
    require(evidence.get("corpora") == counts and
            checksum(evidence.get("input_files_sha256"), "reported full input") == input_digest and
            checksum(evidence.get("case_ids_sha256"), "reported full cases") == identity_digest,
            "reported complete authentication evidence differs from original sources")
    cases = report.get("cases")
    require(isinstance(cases, list) and len(cases) == len(identities), "complete results omit or add cases")
    observed = set()
    markers = {marker: 0 for marker in MARKERS}
    for case in cases:
        path = canonical_path(case.get("path"))
        require(path not in observed, "duplicate complete result case: " + path)
        observed.add(path)
        require(case.get("outcome") == "passed", "complete fixture did not pass: " + path)
        require(isinstance(case.get("message"), str) and case["message"] == "", "passing case has a failure message")
        seconds = case.get("seconds")
        require(type(seconds) in (float, int) and math.isfinite(seconds) and seconds >= 0,
                "invalid case execution duration")
        for marker in MARKERS:
            require(type(case.get(marker)) is bool, "missing or invalid fixture assumption marker")
            markers[marker] += int(case[marker])
    require(observed == identities and case_ids_digest(observed) == identity_digest,
            "complete results substitute original case identities")
    return {"accepted": True, "spec_release": pins["spec_release"], "cases": len(cases),
            "passed": len(cases), "failed": 0, "unsupported": 0, "setup_errors": 0,
            "complete_conformance": True, "runtime_synchronization_verified": False,
            "canonical_case_ids_sha256": identity_digest, "canonical_input_files_sha256": input_digest,
            "corpora": counts, "handler_coverage": coverage_summary(cases),
            "fixture_assumptions_are_runtime_proof": False, **markers}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pins", type=Path, default=DEFAULT_PINS)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--primary-manifest", required=True, type=Path)
    parser.add_argument("--primary-provenance", required=True, type=Path)
    parser.add_argument("--compliance-manifest", required=True, type=Path)
    parser.add_argument("--compliance-provenance", required=True, type=Path)
    parser.add_argument("--runner-exit-code", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    status = 0
    try:
        result = gate(read_json(args.pins), read_json(args.report), read_json(args.primary_provenance),
                      read_json(args.compliance_provenance), args.primary_manifest,
                      args.compliance_manifest, args.runner_exit_code)
    except (ValueError, KeyError, TypeError, AttributeError, OSError) as error:
        result = {"accepted": False, "complete_conformance": False,
                  "runtime_synchronization_verified": False, "error": str(error)}
        print("complete consensus fixture gate failed: " + str(error), file=sys.stderr)
        status = 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if status == 0:
        print(f"{result['cases']} original cases passed; complete fixture conformance established")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
