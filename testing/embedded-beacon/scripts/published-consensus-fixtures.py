#!/usr/bin/env python3
"""Resolve one reviewed release, authenticate original archives, and gate fresh conformance.

No expected failures, result-derived pins, filtered static values, or frozen local totals are used.
Unknown schema releases require explicit source review before acquisition or execution.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tarfile


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


download = module("published_archive_download", "download-consensus-fixtures.py")
policy = module("published_complete_policy", "check-complete-consensus-fixture-results.py")
FORKS = ["phase0", "altair", "bellatrix", "capella", "deneb", "electra", "fulu", "gloas"]
ROLES = [("primary", [("mainnet.tar.gz", "mainnet"), ("minimal.tar.gz", "minimal"),
                       ("general.tar.gz", "general")]),
         ("compliance", [("comptests.tar.gz", "minimal")])]
ARCHIVES = {name for _, archives in ROLES for name, _ in archives}
MAX_ARCHIVE_BYTES = 2 * 1024**3
MAX_FILES = 2_000_000
MAX_MEMBER_BYTES = 1024**3
MAX_EXTRACTED_BYTES = 40 * 1024**3
require = policy.require
checksum = policy.checksum


def bytes_json(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = bytes_json(value)
    path.write_bytes(raw)
    return "0x" + hashlib.sha256(raw).hexdigest()


def trusted_json(path, expected, maximum=64 * 1024):
    require(path.stat().st_size <= maximum, "trusted record exceeds resource limit")
    require(download.digest_file(path) == checksum(expected, "independent record"),
            "record differs from separately supplied digest")
    return policy.read_json(path, maximum)


def validate_resolution(resolution):
    require(resolution.get("format_version") == 1 and
            resolution.get("kind") == "authenticated_published_consensus_release",
            "unknown resolved release record schema")
    release = resolution.get("release_metadata")
    require(isinstance(release, dict) and release.get("tag_name") == download.SUPPORTED_RELEASE,
            "resolved release schema has not been explicitly reviewed")
    require(release.get("draft") is False and isinstance(release.get("published_at"), str) and
            release["published_at"] != "", "resolved release is not published")
    require(type(release.get("id")) is int and release["id"] > 0,
            "resolved release has no immutable release identity")
    requested = resolution.get("requested_release")
    require(requested in ("latest", release["tag_name"]), "resolved release differs from the requested source")
    request_url = download.RELEASE_API + ("?per_page=100" if requested == "latest" else "/tags/" + requested)
    require(resolution.get("request_url") == request_url and
            resolution.get("metadata_url") == download.RELEASE_API + "/tags/" + release["tag_name"],
            "resolved release metadata origin differs from the official release API")
    selected = {}
    require(isinstance(release.get("assets"), list), "release has no original asset metadata")
    for asset in release["assets"]:
        name = asset.get("name")
        if name not in ARCHIVES:
            continue
        require(name not in selected, "duplicate published fixture archive")
        require(type(asset.get("id")) is int and asset["id"] > 0,
                "fixture asset has no immutable identity")
        size = asset.get("size")
        require(type(size) is int and 0 < size <= MAX_ARCHIVE_BYTES,
                "fixture archive exceeds resource limit")
        url = asset.get("browser_download_url")
        prefix = "https://github.com/ethereum/consensus-specs/releases/download/" + release["tag_name"] + "/"
        require(isinstance(url, str) and url == prefix + name,
                "fixture asset URL does not bind the resolved original release")
        digest, source = download.asset_digest(asset, release["tag_name"])
        selected[name] = {"name": name, "bytes": size, "sha256": "0x" + digest,
                          "digest_source": source, "asset_id": asset["id"], "url": url}
    require(set(selected) == ARCHIVES, "resolved release omits an original fixture archive")
    require(resolution.get("archives") == [selected[name] for name in sorted(selected)],
            "resolved archive identities differ from original published metadata")
    return release, selected


def resolve(requested):
    release = download.resolve_release(requested)
    # Review the schema before downloads; latest is never silently replaced with an older tag.
    require(release.get("tag_name") == download.SUPPORTED_RELEASE,
            "latest published schema requires explicit review: " + str(release.get("tag_name")))
    resolution = {"format_version": 1, "kind": "authenticated_published_consensus_release",
                  "requested_release": requested,
                  "request_url": download.RELEASE_API + ("?per_page=100" if requested == "latest" else "/tags/" + requested),
                  "metadata_url": download.RELEASE_API + "/tags/" + release["tag_name"],
                  "release_metadata": release, "archives": []}
    selected = {}
    for asset in release.get("assets", []):
        if asset.get("name") in ARCHIVES:
            digest, source = download.asset_digest(asset, release["tag_name"])
            selected[asset["name"]] = {"name": asset["name"], "bytes": asset.get("size"),
                "sha256": "0x" + digest, "digest_source": source, "asset_id": asset.get("id"),
                "url": asset.get("browser_download_url")}
    resolution["archives"] = [selected[name] for name in sorted(selected)]
    validate_resolution(resolution)
    return resolution


def archive_members(archive, preset, limits, files, fixtures=None):
    """Reconstruct all original target-fork members; optionally extract without filtering values."""
    with tarfile.open(archive, "r|gz") as stream:
        for member in stream:
            parts = download.safe_member(member.name)
            if not parts or member.isdir():
                continue
            require(member.isfile(), "archive links and special members are forbidden")
            if parts[0] != preset:
                continue
            original_case = len(parts) >= 7 and parts[1] in FORKS
            configuration = (parts[-1] == "config.yaml" and len(parts) <= 6 and
                             (len(parts) == 2 or parts[1] in FORKS))
            if not (original_case or configuration):
                continue
            relative = "/".join(parts)
            policy.canonical_path(relative)
            require(relative not in files, "duplicate original archive member: " + relative)
            require(0 <= member.size <= MAX_MEMBER_BYTES, "original member exceeds resource limit")
            limits["files"] += 1
            limits["bytes"] += member.size
            require(limits["files"] <= MAX_FILES and limits["bytes"] <= limits["max_bytes"],
                    "original release extraction exceeds resource limit")
            source = stream.extractfile(member)
            require(source is not None, "original member has no bytes")
            hasher = hashlib.sha256()
            actual_size = 0
            output = None
            if fixtures is not None:
                destination = fixtures.joinpath(*parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                output = destination.open("xb")
            try:
                while chunk := source.read(65536):
                    actual_size += len(chunk)
                    require(actual_size <= member.size, "original member exceeds declared size")
                    hasher.update(chunk)
                    if output is not None:
                        output.write(chunk)
            finally:
                source.close()
                if output is not None:
                    output.close()
            require(actual_size == member.size, "original member is truncated")
            files[relative] = "0x" + hasher.hexdigest()


def identities(manifest):
    return {path.rsplit("/", 1)[0] for path in manifest["files"]
            if len(path.split("/")) >= 7 and path.endswith((".yaml", ".ssz_snappy"))}


def original_manifest(archives, selection, max_bytes, fixtures=None):
    files = {}
    limits = {"files": 0, "bytes": 0, "max_bytes": max_bytes}
    for name, preset in selection:
        archive_members(archives[name], preset, limits, files, fixtures)
    manifest = {"spec_release": download.SUPPORTED_RELEASE, "required_suites": [], "files": files}
    cases = identities(manifest)
    require(cases, "original source has no executable cases")
    manifest["required_suites"] = sorted({"/".join(case.split("/")[:5]) for case in cases})
    return manifest


def verify_archives(resolution, work):
    _, expected = validate_resolution(resolution)
    result = {}
    for name, proof in expected.items():
        path = work / name
        require(path.is_file() and not path.is_symlink(), "original archive is absent or a symlink")
        require(path.stat().st_size == proof["bytes"] and
                download.digest_file(path) == checksum(proof["sha256"], "published archive"),
                "original archive bytes differ from authenticated published digest: " + name)
        result[name] = path
    return result


def derive_source(resolution_sha256, manifests, manifest_digests):
    file_hasher = hashlib.sha256(policy.FILE_DOMAIN)
    unique = set()
    corpora = []
    for role, _ in ROLES:
        manifest = manifests[role]
        cases = identities(manifest)
        require(not unique.intersection(cases), "duplicate canonical case across original source roles")
        unique.update(cases)
        for path in sorted(manifest["files"]):
            policy.canonical_path(path)
            policy.sized(file_hasher, role.encode())
            policy.sized(file_hasher, path.encode())
            file_hasher.update(bytes.fromhex(checksum(manifest["files"][path], "original member")))
        corpora.append({"role": role, "manifest_sha256": manifest_digests[role],
                        "files": len(manifest["files"]), "cases": len(cases),
                        "required_suites": len(manifest["required_suites"])})
    present = {(case.split("/")[0], case.split("/")[1]) for case in unique}
    require(all((preset, fork) in present for preset in ("mainnet", "minimal") for fork in FORKS) and
            any(preset == "general" for preset, _ in present), "original release lacks the full fork/preset matrix")
    return {"format_version": 1, "spec_release": download.SUPPORTED_RELEASE,
            "resolution_sha256": "0x" + checksum(resolution_sha256, "resolution"),
            "corpus_role_order": ["primary", "compliance"], "target_forks": FORKS,
            "canonical_case_ids_sha256": "0x" + policy.case_ids_digest(unique),
            "canonical_input_files_sha256": "0x" + file_hasher.hexdigest(),
            "required_case_count": len(unique),
            "required_file_checks": sum(corpus["files"] for corpus in corpora), "corpora": corpora}


def prepare(resolution, resolution_sha256, work, output, max_bytes):
    release, _ = validate_resolution(resolution)
    require(not output.exists() or not any(output.iterdir()), "prepared output must be absent or empty")
    require(0 < max_bytes <= 200 * 1024**3, "invalid extraction byte budget")
    require(not work.resolve().is_relative_to(output.resolve()), "archive cache must remain outside prepared output")
    output.mkdir(parents=True, exist_ok=True)
    assets = {asset["name"]: asset for asset in release["assets"] if asset["name"] in ARCHIVES}
    for name in sorted(ARCHIVES):
        download.download_asset(assets[name], work, release["tag_name"], MAX_ARCHIVE_BYTES)
    archives = verify_archives(resolution, work)
    manifests = {}
    digests = {}
    for role, selection in ROLES:
        manifests[role] = original_manifest(archives, selection, max_bytes, output / role)
        digests[role] = write_json(output / (role + "-manifest.json"), manifests[role])
    source = derive_source(resolution_sha256, manifests, digests)
    digest = write_json(output / "source-record.json", source)
    return {"resolution_sha256": resolution_sha256, "source_record_sha256": digest,
            "primary_manifest_sha256": digests["primary"], "compliance_manifest_sha256": digests["compliance"],
            "spec_release": source["spec_release"], "cases": source["required_case_count"],
            "complete_conformance": False, "runtime_synchronization_verified": False}


def check(resolution, resolution_sha256, source, source_sha256, work,
          primary_manifest, compliance_manifest, wrapper, runner_exit_code, max_bytes):
    require(type(runner_exit_code) is int and runner_exit_code == 0,
            "fresh authenticated release runner did not exit successfully")
    archives = verify_archives(resolution, work)
    original = {}
    digests = {}
    supplied = {"primary": primary_manifest, "compliance": compliance_manifest}
    for role, selection in ROLES:
        original[role] = original_manifest(archives, selection, max_bytes)
        raw = bytes_json(original[role])
        digests[role] = "0x" + hashlib.sha256(raw).hexdigest()
        actual = policy.manifest(supplied[role], digests[role])
        require(actual == original[role], role + " manifest differs from complete original archive inventory")
    expected_source = derive_source(resolution_sha256, original, digests)
    require(source == expected_source, "source record differs from independently reconstructed original archives")
    require(wrapper.get("format_version") == 1 and wrapper.get("source") == expected_source and
            checksum(wrapper.get("source_record_sha256"), "reported source record") == checksum(source_sha256, "source record"),
            "fresh report does not bind the authenticated original source record")
    # Reuse strict all-pass result policy with dynamically authenticated pre-execution source pins.
    pins = {"format_version": 1, "spec_release": source["spec_release"],
            "corpus_role_order": source["corpus_role_order"],
            "required_file_checks": source["required_file_checks"],
            "required_case_count": source["required_case_count"],
            "canonical_input_files_sha256": source["canonical_input_files_sha256"],
            "canonical_case_ids_sha256": source["canonical_case_ids_sha256"], "corpora": {}}
    provenances = {}
    _, proofs = validate_resolution(resolution)
    for corpus, (role, selection) in zip(source["corpora"], ROLES):
        pins["corpora"][role] = {"manifest_sha256": corpus["manifest_sha256"],
            "file_count": corpus["files"], "case_count": corpus["cases"],
            "required_suite_count": corpus["required_suites"],
            "archives": {name: checksum(proofs[name]["sha256"], "published archive") for name, _ in selection}}
        provenances[role] = {"spec_release": source["spec_release"], "profile": "full", "corpus_role": role,
            "manifest_sha256": corpus["manifest_sha256"], "archives": [proofs[name] for name, _ in selection],
            "case_count": corpus["cases"], "published_case_count": corpus["cases"], "extracted_files": corpus["files"]}
    result = policy.gate(pins, wrapper.get("report"), provenances["primary"], provenances["compliance"],
                         primary_manifest, compliance_manifest, runner_exit_code)
    result.update({"authentication_mode": "original_published_release_archives",
                   "resolution_sha256": resolution_sha256, "source_record_sha256": source_sha256,
                   "original_archive_reconstruction_verified": True, "expected_failures_allowed": False})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    resolving = commands.add_parser("resolve", help="resolve latest once; refuse unreviewed schema")
    resolving.add_argument("--release", default="latest")
    resolving.add_argument("--output", type=Path, required=True)
    for name in ("prepare", "check"):
        command = commands.add_parser(name)
        command.add_argument("--resolution", type=Path, required=True)
        command.add_argument("--resolution-sha256", required=True)
        command.add_argument("--work", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--max-extracted-bytes", type=int, default=MAX_EXTRACTED_BYTES)
        if name == "check":
            command.add_argument("--source-record", type=Path, required=True)
            command.add_argument("--source-record-sha256", required=True)
            command.add_argument("--primary-manifest", type=Path, required=True)
            command.add_argument("--compliance-manifest", type=Path, required=True)
            command.add_argument("--report", type=Path, required=True)
            command.add_argument("--runner-exit-code", type=int, required=True)
    args = parser.parse_args()
    try:
        if args.command == "resolve":
            resolution = resolve(args.release)
            digest = write_json(args.output, resolution)
            result = {"spec_release": download.SUPPORTED_RELEASE, "resolution_sha256": digest,
                      "complete_conformance": False}
        else:
            resolution = trusted_json(args.resolution, args.resolution_sha256, 8 * 1024**2)
            if args.command == "prepare":
                result = prepare(resolution, args.resolution_sha256, args.work, args.output, args.max_extracted_bytes)
            else:
                source = trusted_json(args.source_record, args.source_record_sha256)
                result = check(resolution, args.resolution_sha256, source, args.source_record_sha256,
                               args.work, args.primary_manifest, args.compliance_manifest,
                               policy.read_json(args.report), args.runner_exit_code, args.max_extracted_bytes)
                write_json(args.output, result)
    except (ValueError, KeyError, TypeError, AttributeError, OSError, tarfile.TarError) as error:
        result = {"accepted": False, "complete_conformance": False,
                  "runtime_synchronization_verified": False, "error": str(error)}
        if args.command == "check":
            write_json(args.output, result)
        print(json.dumps(result), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
