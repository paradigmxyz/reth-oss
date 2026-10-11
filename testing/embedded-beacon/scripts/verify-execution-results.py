#!/usr/bin/env python3
"""Gate a complete execution fixture report against exact, authenticated expectations."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys

SHA256 = re.compile(r"[0-9a-f]{64}")
OUTCOMES = {"passed", "failed", "skipped"}
MAX_FILES = 100_000


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key: " + key)
        result[key] = value
    return result


def read_json(path, limit):
    if path.stat().st_size > limit:
        raise ValueError("JSON input exceeds size limit: " + path.name)
    record = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    require(isinstance(record, dict), "JSON input must be an object: " + path.name)
    return record


def require(condition, message):
    if not condition:
        raise ValueError(message)


def count(value, name):
    require(type(value) is int and value >= 0, "invalid count: " + name)
    return value


def sha256(value, name):
    require(isinstance(value, str) and SHA256.fullmatch(value), "invalid SHA-256: " + name)
    return value


def relative_path(value):
    require(isinstance(value, str) and bool(value), "invalid fixture path")
    path = PurePosixPath(value)
    require(not path.is_absolute() and ".." not in path.parts and "\\" not in value and ":" not in value,
            "unsafe fixture path")
    require(path.as_posix() == value and path.parts[0] == "blockchain_tests" and path.suffix == ".json",
            "fixture path is not canonical blockchain JSON")
    return value


def evaluate(provenance, selection, report, expectations, fixture_root, runner_status):
    """Check exact inventory and outcome/error fingerprints without hiding new failures."""
    require(all(isinstance(value, dict) for value in (provenance, selection, report, expectations)),
            "gate inputs must be JSON objects")
    require(type(provenance.get("schema_version")) is int and provenance["schema_version"] == 1,
            "unsupported provenance schema")
    release = provenance.get("release")
    require(isinstance(release, str) and release.startswith("tests@"), "invalid execution release")
    archive = sha256(provenance.get("archive_sha256"), "archive")
    require(provenance.get("fixture_format") == "blockchain_test", "wrong fixture format")
    require(provenance.get("complete_execution_conformance") is False,
            "blockchain-only provenance must not claim complete execution conformance")
    require(provenance.get("excluded_formats") == ["state_test", "blockchain_test_engine",
            "blockchain_test_engine_x", "transaction_test"], "unexpected excluded fixture formats")
    records = provenance.get("files")
    require(isinstance(records, list) and 0 < len(records) <= MAX_FILES, "invalid fixture inventory")
    inventory = {}
    for record in records:
        require(isinstance(record, dict), "malformed fixture inventory entry")
        path = relative_path(record.get("path"))
        require(path not in inventory, "duplicate fixture inventory path: " + path)
        count(record.get("bytes"), "fixture bytes")
        inventory[path] = sha256(record.get("sha256"), "fixture")
    require(selection.get("release") == release and selection.get("archive_sha256") == archive,
            "release selection differs from provenance")
    require(count(selection.get("verified_files"), "selection files") == len(inventory),
            "verified fixture count differs from provenance")

    require(type(expectations.get("schema_version")) is int and expectations["schema_version"] == 1,
            "unsupported expectation schema")
    entries = expectations.get("known_failures")
    require(isinstance(entries, list) and len(entries) <= MAX_FILES, "invalid known-failure list")
    known = {}
    inactive = []
    seen = set()
    for entry in entries:
        require(isinstance(entry, dict), "malformed known-failure entry")
        path = relative_path(entry.get("path"))
        expected_release = entry.get("release")
        require(isinstance(expected_release, str) and expected_release.startswith("tests@"),
                "invalid expected release")
        expected_archive = sha256(entry.get("archive_sha256"), "expected archive")
        sha256(entry.get("fixture_sha256"), "expected fixture")
        require(entry.get("outcome") in {"failed", "skipped"}, "invalid expected failure outcome")
        require(isinstance(entry.get("error"), str) and 0 < len(entry["error"]) <= 16_384,
                "expected failure needs an exact error")
        require(isinstance(entry.get("reason"), str) and bool(entry["reason"].strip()),
                "expected failure needs a reason")
        key = (expected_release, expected_archive, path)
        require(key not in seen, "duplicate expected failure: " + path)
        seen.add(key)
        if expected_release == release and expected_archive == archive:
            require(path in inventory, "expected failure missing from bound release: " + path)
            require(entry["fixture_sha256"] == inventory[path], "expected fixture hash mismatch: " + path)
            known[path] = entry
        else:
            inactive.append({"path": path, "release": expected_release, "reason": "different_release_or_archive"})

    require(report.get("suite") == ".", "unexpected execution suite")
    root = fixture_root.resolve()
    directory = report.get("fixture_directory")
    require(isinstance(directory, str) and Path(directory).is_absolute(), "invalid report fixture directory")
    require(Path(directory).resolve() == root / "blockchain_tests", "report used a different fixture directory")
    require(report.get("complete_execution_conformance") is False,
            "blockchain-only report must not claim complete execution conformance")
    cases = report.get("cases")
    require(isinstance(cases, list) and len(cases) <= MAX_FILES, "invalid result cases")
    observed = {}
    totals = Counter()
    for case in cases:
        require(isinstance(case, dict), "malformed result case")
        path_value = case.get("path")
        require(isinstance(path_value, str) and Path(path_value).is_absolute(), "result path must be absolute")
        try:
            path = relative_path(Path(path_value).resolve().relative_to(root).as_posix())
        except ValueError as error:
            raise ValueError("result path escapes the authenticated fixture directory") from error
        require(path not in observed, "duplicate result fixture: " + path)
        outcome = case.get("outcome")
        require(outcome in OUTCOMES, "unrecognized result outcome")
        require(isinstance(case.get("description"), str), "invalid case description")
        require((outcome == "passed" and case.get("error") is None) or
                (outcome != "passed" and isinstance(case.get("error"), str) and bool(case["error"])),
                "outcome and error disagree: " + path)
        observed[path] = case
        totals[outcome] += 1
    require(set(observed) == set(inventory),
            f"incomplete result inventory: missing={len(set(inventory) - set(observed))}, extra={len(set(observed) - set(inventory))}")
    require(count(report.get("fixture_files"), "report files") == len(inventory), "report fixture count mismatch")
    for outcome in OUTCOMES:
        require(count(report.get(outcome + "_files"), outcome) == totals[outcome], "report outcome count mismatch")
    require(totals["passed"] > 0, "execution report contains no passing fixtures")
    require(type(runner_status) is int and runner_status in {0, 101}, "execution runner failed outside the test-result contract")
    require(runner_status == (101 if totals["failed"] else 0), "runner exit status disagrees with results")

    accepted = []
    stale = []
    failures = []
    for path, case in sorted(observed.items()):
        entry = known.get(path)
        if case["outcome"] == "passed":
            if entry is not None:
                stale.append({"path": path, "reason": "now_passes"})
        elif entry is not None and case["outcome"] == entry["outcome"] and case["error"] == entry["error"]:
            accepted.append({"path": path, "outcome": case["outcome"], "error": case["error"]})
        else:
            failures.append({"path": path, "outcome": case["outcome"], "error": case["error"],
                             "reason": "unlisted_failure" if entry is None else "expected_error_or_outcome_changed"})
    return {"schema_version": 1, "status": "failed" if failures else "passed_with_known_failures" if accepted else "passed",
            "release": release, "archive_sha256": archive, "fixture_files": len(inventory),
            "passed_files": totals["passed"], "failed_files": totals["failed"], "skipped_files": totals["skipped"],
            "runner_exit_status": runner_status, "accepted_known_failures": accepted,
            "unexpected_failures": failures, "stale_expectations": stale, "inactive_expectations": inactive,
            "complete_execution_conformance": False, "excluded_formats": provenance["excluded_formats"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provenance", required=True, type=Path)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--expectations", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--runner-status", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args()
    try:
        selection = read_json(args.selection, 64 * 1024)
        provenance = read_json(args.provenance, 32 * 1024**2)
        expected_digest = sha256(selection.get("provenance_sha256"), "selected provenance")
        require(hashlib.sha256(args.provenance.read_bytes()).hexdigest() == expected_digest,
                "provenance changed after fixture download")
        require(args.runner_status.stat().st_size <= 16, "invalid runner status file")
        status = args.runner_status.read_text(encoding="utf-8").strip()
        require(status in {"0", "101"}, "execution runner failed outside the test-result contract")
        summary = evaluate(provenance, selection,
                           read_json(args.results, 128 * 1024**2), read_json(args.expectations, 1024**2),
                           args.fixtures, int(status))
        summary["provenance_sha256"] = expected_digest
    except (OSError, ValueError, KeyError, TypeError) as error:
        summary = {"schema_version": 1, "status": "failed", "gate_error": str(error),
                   "complete_execution_conformance": False}
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 1 if summary["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
