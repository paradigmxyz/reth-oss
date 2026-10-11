#!/usr/bin/env python3
"""Prepare and verify an independent, provenance-bound transport regression suite.

Only ``run`` invokes Cargo. Preparation never edits a production manifest or lock,
never deletes an existing directory, and leaves its uniquely owned temporary copy
available for inspection. Contract tests exercise preparation and gates offline;
only a successful complete Rust run is regression execution evidence.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib

SNAPSHOT = Path("crates/embedded-beacon/gossipsub")
NORMALIZATION = b'\n# Verification-only: restore the original workspace test dependency.\n[dev-dependencies.quickcheck]\nversion = "=1.0.3"\n\n# Keep this disposable copy outside the production workspace.\n[workspace]\n'
PACKAGE = "libp2p-gossipsub"
VERSION = "0.49.5"
REVISION = "d6a16b6a88959d1b25e9807d89959b2f9127724c"
ARCHIVE_SHA256 = "3573f3d8e30bd62cda336df5c7c1041a1caa40a648376b8e1e274d585c0ed25c"
OWNER_MARKER = ".pubsub-test-copy-owner.json"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def plain_path(path):
    """Reject links and Windows reparse points before following any ancestor."""
    path = Path(os.path.abspath(path))
    for entry in reversed((path, *path.parents)):
        if not entry.exists() and not entry.is_symlink():
            continue
        info = entry.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError(f"linked/reparse path is not allowed: {entry}")
    return path


def relative_name(value):
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise ValueError("provenance contains an unsafe file name")
    path = PurePosixPath(value)
    if path.is_absolute() or str(path) != value or any(p in (".", "..") for p in path.parts):
        raise ValueError(f"provenance contains an unsafe file name: {value}")
    return value


def files(directory):
    directory = plain_path(directory)
    result = {}
    for entry in sorted(directory.rglob("*")):
        plain_path(entry)
        if entry.is_file():
            result[entry.relative_to(directory).as_posix()] = sha256(entry)
        elif not entry.is_dir():
            raise ValueError(f"non-regular snapshot entry: {entry}")
    return result


def read_json(path):
    path = plain_path(path)
    if path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError(f"metadata exceeds the bounded input size: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    plain_path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def verified_snapshot(source):
    source = plain_path(source)
    provenance = read_json(source / "SOURCE-PROVENANCE.json")
    for key, wanted in (("package", PACKAGE), ("version", VERSION), ("license", "MIT"),
                        ("source_revision", REVISION), ("registry_archive_sha256", ARCHIVE_SHA256)):
        if provenance.get(key) != wanted:
            raise ValueError(f"snapshot provenance mismatch: {key}")
    expected = {}
    originals = provenance["original_files"]
    for record in originals:
        name = relative_name(record["path"])
        if name in expected:
            raise ValueError(f"duplicate provenance file: {name}")
        expected[name] = record["sha256"]
    original_names = set(expected)
    for record in provenance["patched_files"]:
        name = relative_name(record["path"])
        if name not in original_names:
            raise ValueError(f"patched file is not an original: {name}")
        expected[name] = record["sha256"]
    for category in ("local_added_files", "local_auxiliary_files"):
        for record in provenance[category]:
            name = relative_name(record["path"])
            if name in expected:
                raise ValueError(f"duplicate provenance file: {name}")
            expected[name] = record["sha256"]
    expected["SOURCE-PROVENANCE.json"] = sha256(source / "SOURCE-PROVENANCE.json")
    actual = files(source)
    if actual != expected:
        changed = sorted(name for name in set(actual) | set(expected) if actual.get(name) != expected.get(name))
        raise ValueError(f"snapshot file/hash mismatch: {', '.join(changed)}")
    for record in originals:
        if record["path"] not in {r["path"] for r in provenance["patched_files"]}:
            if (source / record["path"]).stat().st_size != record["bytes"]:
                raise ValueError(f"original file size mismatch: {record['path']}")
    manifest = tomllib.loads((source / "Cargo.toml").read_text(encoding="utf-8"))
    if manifest["package"]["name"] != PACKAGE or manifest["package"]["version"] != VERSION:
        raise ValueError("package manifest does not match the pinned snapshot")
    if "workspace" in manifest or "quickcheck" in manifest.get("dev-dependencies", {}):
        raise ValueError("production snapshot already contains verification normalization")
    vcs = read_json(source / ".cargo_vcs_info.json")
    if vcs["git"]["sha1"] != REVISION or vcs["path_in_vcs"] != provenance["source_path"]:
        raise ValueError("original source revision does not match provenance")
    if "LICENSE-MIT" not in expected or manifest["package"]["license"] != "MIT":
        raise ValueError("snapshot license notice is missing")
    return actual


def production_hashes(repository):
    return {name: sha256(plain_path(repository / name)) for name in ("Cargo.toml", "Cargo.lock")}


def prepare(repository, temp_root, artifacts):
    repository = plain_path(repository)
    source = plain_path(repository / SNAPSHOT)
    temp_root, artifacts = plain_path(temp_root), plain_path(artifacts)
    if temp_root == source or temp_root.is_relative_to(source):
        raise ValueError("temporary copies must be outside the source snapshot")
    if artifacts == repository or artifacts == source or artifacts.is_relative_to(source):
        raise ValueError("evidence directory must be outside the source snapshot and repository root")
    if artifacts.exists() and any(artifacts.iterdir()):
        raise ValueError("use a fresh evidence directory; existing evidence is never overwritten")
    source_inventory = verified_snapshot(source)
    production = production_hashes(repository)
    temp_root.mkdir(parents=True, exist_ok=True)
    artifacts.mkdir(parents=True, exist_ok=True)
    owner = Path(tempfile.mkdtemp(prefix="embedded-pubsub-tests-", dir=temp_root))
    package = owner / "package"
    shutil.copytree(source, package, symlinks=False)
    if files(package) != source_inventory or verified_snapshot(source) != source_inventory:
        raise ValueError("source changed during temporary-copy preparation")
    original_manifest = (package / "Cargo.toml").read_bytes()
    (package / "Cargo.toml").write_bytes(original_manifest + NORMALIZATION)
    if production_hashes(repository) != production:
        raise ValueError("production manifest/lock changed during preparation")
    record = {
        "format_version": 1, "verification_only": True,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "repository": str(repository), "source": str(source), "owner": str(owner),
        "package": str(package), "artifacts": str(artifacts),
        "source_inventory": source_inventory, "production_files": production,
        "normalized_manifest_sha256": sha256(package / "Cargo.toml"),
        "normalization": {"quickcheck": "=1.0.3", "independent_workspace": True},
        "features": ["metrics"], "test_selection": "complete-library",
    }
    write_json(owner / OWNER_MARKER, record)
    write_json(artifacts / "preparation.json", record)
    shutil.copy2(source / "SOURCE-PROVENANCE.json", artifacts / "SOURCE-PROVENANCE.json")
    shutil.copy2(source / "EMBEDDED-SNAPSHOT.md", artifacts / "EMBEDDED-SNAPSHOT.md")
    shutil.copy2(source / "LICENSE-MIT", artifacts / "LICENSE-MIT")
    shutil.copy2(package / "Cargo.toml", artifacts / "verification-Cargo.toml")
    return record


def verify_preparation(record):
    if record.get("format_version") != 1 or record.get("verification_only") is not True:
        raise ValueError("unsupported temporary-copy preparation record")
    repository, source, owner, package, artifacts = (plain_path(record[key]) for key in
                                                    ("repository", "source", "owner", "package", "artifacts"))
    if source != repository / SNAPSHOT or package != owner / "package" or owner.is_relative_to(source):
        raise ValueError("temporary-copy ownership/path mismatch")
    if not owner.name.startswith("embedded-pubsub-tests-") or read_json(owner / OWNER_MARKER) != record:
        raise ValueError("temporary-copy ownership record mismatch")
    if verified_snapshot(source) != record["source_inventory"]:
        raise ValueError("production snapshot changed since preparation")
    if production_hashes(repository) != record["production_files"]:
        raise ValueError("production manifest/lock changed since preparation")
    if record["features"] != ["metrics"] or record["test_selection"] != "complete-library":
        raise ValueError("full metrics library-suite contract changed")
    if record["normalization"] != {"quickcheck": "=1.0.3", "independent_workspace": True}:
        raise ValueError("verification-only dependency normalization changed")
    expected = dict(record["source_inventory"])
    expected["Cargo.toml"] = record["normalized_manifest_sha256"]
    # Cargo may regenerate only the disposable lock and its isolated build cache.
    copied = files(package)
    copied.pop("Cargo.lock", None)
    expected.pop("Cargo.lock", None)
    if copied != expected or (package / "Cargo.toml").read_bytes() != (source / "Cargo.toml").read_bytes() + NORMALIZATION:
        raise ValueError("temporary package source/manifest changed")
    return package, owner, artifacts


def check_inventory(log):
    names, summaries = [], []
    with Path(log).open(encoding="utf-8") as stream:
        for line in stream:
            if match := re.fullmatch(r"([^\s]+): test\n?", line):
                names.append(match.group(1))
            if match := re.fullmatch(r"(\d+) tests?, (\d+) benchmarks?\n?", line):
                summaries.append(tuple(map(int, match.groups())))
    expected = len(names)
    if expected == 0 or len(set(names)) != expected or summaries != [(expected, 0)]:
        raise ValueError("library inventory is incomplete, duplicated or contains benchmarks")
    return sorted(names)


def check_run(log, inventory, exit_code):
    headers, summaries, passed = [], [], []
    with Path(log).open(encoding="utf-8") as stream:
        for line in stream:
            if match := re.fullmatch(r"running (\d+) tests?\n?", line):
                headers.append(int(match.group(1)))
            if match := re.fullmatch(r"test ([^\s]+)(?: - should panic)? \.\.\. ok\n?", line):
                passed.append(match.group(1))
            if match := re.fullmatch(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in .+\n?", line):
                summaries.append((match.group(1), *map(int, match.groups()[1:])))
    expected = len(inventory)
    if exit_code != 0 or headers != [expected] or summaries != [("ok", expected, 0, 0, 0, 0)]:
        raise ValueError("complete library run failed or skipped/filtered tests")
    if len(passed) != expected or sorted(passed) != inventory or len(set(passed)) != expected:
        raise ValueError("terminal test outcomes do not match the complete library inventory")
    return {"tests": expected, "passed": expected, "failed": 0, "ignored": 0, "filtered": 0,
            "test_names_sha256": hashlib.sha256(("\n".join(inventory) + "\n").encode()).hexdigest()}


def command_run(command, directory, log, environment):
    """Use an argument vector and preserve merged output even for Cargo failures."""
    with Path(log).open("wb") as evidence:
        with subprocess.Popen(command, cwd=directory, env=environment, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, shell=False) as child:
            for line in child.stdout:
                evidence.write(line)
                evidence.flush()
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
            return child.wait()


def compiled_library(log, package, owner):
    executables = []
    with Path(log).open(encoding="utf-8") as stream:
        for line in stream:
            try:
                artifact = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(artifact, dict) or artifact.get("reason") != "compiler-artifact":
                continue
            target = artifact.get("target", {})
            if target.get("name") != "libp2p_gossipsub" or target.get("kind") != ["lib"]:
                continue
            if artifact.get("profile", {}).get("test") is not True or not artifact.get("executable"):
                continue
            if "metrics" not in artifact.get("features", []):
                raise ValueError("compiled library does not include metrics")
            if plain_path(target["src_path"]) != package / "src/lib.rs":
                raise ValueError("compiled library does not belong to the verified temporary source")
            executable = plain_path(artifact["executable"])
            if not executable.is_file() or not executable.is_relative_to(owner / "target"):
                raise ValueError("compiled library executable is outside the owned target directory")
            executables.append(executable)
    if len(executables) != 1:
        raise ValueError("compiler output must identify exactly one metrics library test executable")
    return executables[0]


def run(record):
    package, owner, artifacts = verify_preparation(record)
    gate = {"format_version": 1, "accepted": False, "features": ["metrics"],
            "test_selection": "complete-library", "commands": [],
            "source_provenance_sha256": record["source_inventory"]["SOURCE-PROVENANCE.json"]}
    environment = dict(os.environ, CARGO_TERM_COLOR="never", CARGO_TARGET_DIR=str(owner / "target"))
    commands = [(["cargo", "generate-lockfile", "--manifest-path", str(package / "Cargo.toml")], "lock-resolution.log"),
                (["cargo", "test", "--locked", "--manifest-path", str(package / "Cargo.toml"),
                  "--features", "metrics", "--lib", "--no-run", "--message-format=json"], "library-build.log")]

    def execute(command, filename):
        status = command_run(command, package, artifacts / filename, environment)
        gate["commands"].append({"argv": command, "log": filename, "exit_code": status})
        if status:
            raise ValueError(f"command failed ({status}): {filename}")
        return status

    try:
        for command, filename in commands:
            execute(command, filename)
        executable = compiled_library(artifacts / "library-build.log", package, owner)
        gate["test_executable"] = str(executable)
        gate["test_executable_sha256"] = sha256(executable)
        execute([str(executable), "--list"], "library-inventory.log")
        inventory = check_inventory(artifacts / "library-inventory.log")
        write_json(artifacts / "library-inventory.json", inventory)
        gate["library_tests"] = len(inventory)
        status = execute([str(executable), "--test-threads=1"], "library-tests.log")
        gate.update(check_run(artifacts / "library-tests.log", inventory, status))
        if sha256(executable) != gate["test_executable_sha256"]:
            raise ValueError("compiled executable changed between inventory and execution")
        gate["accepted"] = True
    except (OSError, ValueError) as error:
        gate["error"] = str(error)
    finally:
        try:
            verify_preparation(record)
            gate["production_files_unchanged"] = True
            gate["copied_source_unchanged"] = True
        except (OSError, ValueError) as error:
            gate["accepted"] = False
            gate["source_verification_error"] = str(error)
        if (package / "Cargo.lock").exists():
            shutil.copy2(package / "Cargo.lock", artifacts / "verification-Cargo.lock")
        gate["log_sha256"] = {entry["log"]: sha256(artifacts / entry["log"]) for entry in gate["commands"]}
        write_json(artifacts / "ci-gate.json", gate)
    return gate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare", help="verify and copy source without running Cargo")
    preparation.add_argument("--repository", type=Path, required=True)
    preparation.add_argument("--temp-root", type=Path, required=True)
    preparation.add_argument("--artifacts", type=Path, required=True)
    execution = commands.add_parser("run", help="run the complete isolated metrics library suite")
    execution.add_argument("--preparation", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        result = (prepare(arguments.repository, arguments.temp_root, arguments.artifacts)
                  if arguments.command == "prepare" else run(read_json(arguments.preparation)))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 1 if result.get("accepted") is False else 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"scoped pubsub verification rejected: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
