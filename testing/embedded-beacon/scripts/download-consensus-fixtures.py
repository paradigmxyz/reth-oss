#!/usr/bin/env python3
"""Authenticate a published fixture release and extract an explicit test profile."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tarfile
import urllib.request
import uuid

SUPPORTED_RELEASE = "v1.7.0-beta.3"
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
FULL_CORPUS_PINS = REPOSITORY_ROOT / "testing/embedded-beacon/spec-tests/full-corpus-pins.json"
RELEASE_API = "https://api.github.com/repos/ethereum/consensus-specs/releases"
TARGET_FORKS = {"phase0", "altair", "bellatrix", "capella", "deneb", "electra", "fulu", "gloas"}
STATIC_TYPES = {"BeaconState", "BeaconBlock", "SignedBeaconBlock", "BeaconBlockHeader"}
GLOAS_TYPES = {"ExecutionPayloadBid", "SignedExecutionPayloadBid", "PayloadAttestationData", "PayloadAttestation", "PayloadAttestationMessage", "IndexedPayloadAttestation", "ExecutionPayload", "ExecutionRequests", "ExecutionPayloadEnvelope", "SignedExecutionPayloadEnvelope"}
BLS_HANDLERS = {"verify", "fast_aggregate_verify", "aggregate_verify", "eth_fast_aggregate_verify", "eth_aggregate_pubkeys"}
GENERIC_HANDLERS = {"boolean", "uints", "basic_vector", "basic_progressive_list", "bitvector", "bitlist", "progressive_bitlist"}
ALTAIR_EPOCH_HANDLERS = {"justification_and_finalization", "inactivity_updates", "rewards_and_penalties", "registry_updates", "slashings", "eth1_data_reset", "effective_balance_updates", "slashings_reset", "randao_mixes_reset", "historical_roots_update", "participation_flag_updates", "sync_committee_updates"}
EPOCH_HANDLERS = {"justification_and_finalization", "rewards_and_penalties", "registry_updates", "slashings", "eth1_data_reset", "effective_balance_updates", "slashings_reset", "randao_mixes_reset", "historical_roots_update", "participation_record_updates"}
PINNED_DIGESTS = {
    "mainnet.tar.gz": "a50c3eae743fcf69e17e3510b84e94dcabf96977655788c66bdc43a96590fdf5",
    "minimal.tar.gz": "b26832555fa5a67cc3964bf140ae99cb662f8045d6d205993e3fa27feb39ca6f",
    "general.tar.gz": "c4f395f71f606c28093500e32067990e430cc73a2277be6f3bf5090adcce3f67",
    "comptests.tar.gz": "eaef466f1434dfad72e50c1e52449109f430c920e51181c0c19bff28e2aeb67d",
}


def request_json(url: str):
    request = urllib.request.Request(url, headers={"User-Agent": "embedded-beacon-fixture-check", "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("release metadata exceeds resource limit")
    return json.loads(raw)


def select_latest(releases: list[dict]) -> dict:
    # The default release endpoint excludes prereleases; list and include them.
    published = [release for release in releases if not release.get("draft") and release.get("published_at") and release.get("tag_name")]
    if not published:
        raise ValueError("no published release, including prereleases, is available")
    return max(published, key=lambda release: release["published_at"])


def resolve_release(release: str) -> dict:
    if release == "latest":
        return select_latest(request_json(RELEASE_API + "?per_page=100"))
    if not re.fullmatch(r"v[0-9][A-Za-z0-9.\-]*", release):
        raise ValueError("invalid release tag")
    return request_json(RELEASE_API + "/tags/" + release)


def digest_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def asset_digest(asset: dict, tag: str) -> tuple[str, str]:
    value = asset.get("digest")
    if isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        expected = value[7:]
        if tag == SUPPORTED_RELEASE and asset["name"] in PINNED_DIGESTS and PINNED_DIGESTS[asset["name"]] != expected:
            raise ValueError("published asset digest differs from the independently pinned digest")
        return expected, "published_release_metadata"
    if tag == SUPPORTED_RELEASE and asset["name"] in PINNED_DIGESTS:
        return PINNED_DIGESTS[asset["name"]], "independently_pinned_release_digest"
    raise ValueError("release asset has no authenticated SHA-256 digest")


def download_asset(asset: dict, work: Path, tag: str, maximum: int) -> tuple[Path, dict]:
    name = asset["name"]
    if PurePosixPath(name).name != name or name not in PINNED_DIGESTS:
        raise ValueError("unexpected release asset name")
    size = asset.get("size")
    if not isinstance(size, int) or size <= 0 or size > maximum:
        raise ValueError("archive size exceeds resource limit")
    expected, digest_source = asset_digest(asset, tag)
    url = asset["browser_download_url"]
    if not isinstance(url, str) or not url.startswith("https://"):
        raise ValueError("archive URL must use HTTPS")
    work.mkdir(parents=True, exist_ok=True)
    destination = work / name
    if destination.exists() and (destination.stat().st_size != size or digest_file(destination) != expected):
        # Preserve corrupt evidence without allowing a partial cache to block retries.
        destination.replace(work / (name + ".invalid-" + uuid.uuid4().hex))
    if not destination.exists():
        partial = work / (name + ".partial")
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "embedded-beacon-fixture-check"})
            hasher = hashlib.sha256()
            count = 0
            with urllib.request.urlopen(request, timeout=120) as response, partial.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    count += len(chunk)
                    if count > size:
                        raise ValueError("archive download exceeds published size")
                    hasher.update(chunk)
                    output.write(chunk)
            if count != size or hasher.hexdigest() != expected:
                raise ValueError("archive download size or SHA-256 mismatch")
            os.replace(partial, destination)
        finally:
            partial.unlink(missing_ok=True)
    return destination, {"name": name, "bytes": size, "sha256": expected, "digest_source": digest_source, "url": url}


def safe_member(name: str) -> tuple[str, ...]:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name or ":" in name:
        raise ValueError("unsafe archive member path")
    parts = path.parts
    if not parts or parts[0] != "tests":
        return ()
    return parts[1:]


def supported_case(parts: tuple[str, ...]) -> bool:
    if len(parts) < 6 or parts[0] not in {"mainnet", "minimal", "general"} or parts[1] not in TARGET_FORKS:
        return False
    preset, fork, runner, handler = parts[:4]
    if runner == "ssz_static":
        return preset != "general"
    if runner == "bls":
        return handler in BLS_HANDLERS
    if runner == "shuffling":
        return handler == "core"
    if runner == "ssz_generic":
        return handler in GENERIC_HANDLERS
    if preset == "general":
        return False
    if runner == "rewards":
        return handler in {"basic", "inactivity_scores", "leak", "random"}
    if runner == "networking":
        return handler in {"get_custody_groups", "compute_columns_for_custody_group"}
    if runner == "light_client":
        return handler in {"sync", "update_ranking", "single_merkle_proof"}
    if fork == "phase0":
        return runner == "sanity" and handler in {"slots", "blocks"} or runner == "operations" and handler in {"block_header", "attestation", "attester_slashing", "proposer_slashing", "deposit", "voluntary_exit"} or runner == "epoch_processing" and handler in EPOCH_HANDLERS
    if fork == "altair":
        return runner == "sanity" and handler in {"slots", "blocks"} or runner == "operations" and handler in {"block_header", "attestation", "attester_slashing", "proposer_slashing", "deposit", "voluntary_exit", "sync_aggregate"} or runner == "epoch_processing" and handler in ALTAIR_EPOCH_HANDLERS or runner == "fork" and handler == "fork"
    if fork in TARGET_FORKS - {"phase0", "altair"}:
        return (runner == "fork" and handler == "fork" or
                runner == "sanity" and handler in {"slots", "blocks"} or
                runner == "operations" and handler in {
                    "block_header", "attestation", "attester_slashing", "proposer_slashing", "deposit",
                    "voluntary_exit", "sync_aggregate", "execution_payload", "withdrawals",
                    "bls_to_execution_change", "deposit_request", "withdrawal_request", "consolidation_request",
                    "builder_deposit_request", "builder_exit_request", "execution_payload_bid",
                    "parent_execution_payload", "payload_attestation", "voluntary_exit_churn"} or
                runner == "epoch_processing" and handler in ALTAIR_EPOCH_HANDLERS | {
                    "historical_summaries_update", "pending_deposits", "pending_deposits_churn",
                    "pending_consolidations", "proposer_lookahead", "builder_pending_payments", "ptc_window"})
    return False


def extract_archive(archive: Path, fixtures: Path, profile: str, preset: str, limits: dict, files: dict, cases: dict) -> None:
    with tarfile.open(archive, "r|gz") as stream:
        for member in stream:
            parts = safe_member(member.name)
            if not parts:
                continue
            if member.isdir():
                continue
            if not member.isfile():
                raise ValueError("archive symlinks, hard links and special files are forbidden")
            if parts[0] != preset:
                continue
            is_case = len(parts) >= 7 and parts[1] in TARGET_FORKS
            case = "/".join(parts[:6]) if is_case else None
            supported = is_case and supported_case(parts)
            if case is not None:
                entry = cases.setdefault(case, {"implemented": supported, "extracted": False, "files_in_archive": 0})
                entry["files_in_archive"] += 1
            selected = is_case and (profile == "full" or supported)
            # Preserve every enclosing config, including preset and fork defaults.
            selected = selected or parts[-1] == "config.yaml" and len(parts) <= 6 and (len(parts) == 2 or parts[1] in TARGET_FORKS)
            if not selected:
                continue
            # Static roundtrip mode needs original bytes, expected roots and identity.
            # value.yaml may be much larger than its SSZ; the spec explicitly permits this mode.
            if len(parts) >= 4 and parts[2] == "ssz_static" and parts[-1] not in {"serialized.ssz_snappy", "roots.yaml", "manifest.yaml", "config.yaml"}:
                continue
            if member.size > limits["member_bytes"]:
                raise ValueError("selected fixture member exceeds resource limit: " + member.name)
            limits["files"] += 1
            limits["bytes"] += member.size
            if limits["files"] > limits["max_files"] or limits["bytes"] > limits["max_bytes"]:
                raise ValueError("selected corpus exceeds extraction resource limits")
            relative = "/".join(parts)
            if relative in files:
                raise ValueError("duplicate archive member: " + relative)
            source = stream.extractfile(member)
            if source is None:
                raise ValueError("selected member has no data")
            destination = fixtures.joinpath(*parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            hasher = hashlib.sha256()
            with destination.open("xb") as output:
                while chunk := source.read(65536):
                    hasher.update(chunk)
                    output.write(chunk)
            files[relative] = "0x" + hasher.hexdigest()
            if case is not None:
                cases[case]["extracted"] = True


def write_json(path: Path, value: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def required_archives(profile: str, presets: list[str], corpus_role: str = "combined") -> list[tuple[str, str]]:
    """Complete roles preserve the original primary and compliance inventories separately."""
    if corpus_role == "compliance":
        if profile != "full" or presets != ["minimal"]:
            raise ValueError("compliance role requires full profile and the Minimal preset only")
        return [("comptests.tar.gz", "minimal")]
    if corpus_role == "primary":
        if profile != "full" or set(presets) != {"mainnet", "minimal", "general"}:
            raise ValueError("primary role requires full profile and all three presets")
        return [(preset + ".tar.gz", preset) for preset in presets]
    if corpus_role != "combined":
        raise ValueError("unknown fixture corpus role")
    archives = [(preset + ".tar.gz", preset) for preset in presets]
    if profile == "full" and "minimal" in presets:
        archives.append(("comptests.tar.gz", "minimal"))
    return archives


def validate_full_source(pins: dict, role: str, manifest_digest: str, files: int, cases: int, suites: int) -> None:
    if pins.get("format_version") != 1 or pins.get("spec_release") != SUPPORTED_RELEASE:
        raise ValueError("complete source pins differ from the supported schema release")
    expected = pins["corpora"][role]
    if manifest_digest != expected["manifest_sha256"]:
        raise ValueError("complete " + role + " manifest differs from independently pinned original bytes")
    if (files, cases, suites) != (expected["file_count"], expected["case_count"], expected["required_suite_count"]):
        raise ValueError("complete " + role + " inventory differs from pinned original source counts")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", default="latest", help="latest includes published prereleases")
    parser.add_argument("--profile", choices=["implemented", "full"], default="implemented")
    parser.add_argument("--corpus-role", choices=["combined", "primary", "compliance"], default="combined", help="preserve the two original roles for run-complete; combined remains subset diagnostic")
    parser.add_argument("--presets", nargs="+", choices=["mainnet", "minimal", "general"], default=["mainnet", "minimal", "general"])
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True, help="verified archive cache, outside fixture root")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--max-extracted-bytes", type=int, default=20 * 1024**3)
    args = parser.parse_args()
    if len(set(args.presets)) != len(args.presets):
        parser.error("duplicate presets are forbidden")
    paths = [args.work, args.manifest, args.provenance]
    if any(path.resolve().is_relative_to(args.fixtures.resolve()) for path in paths):
        parser.error("cache, manifest and provenance must remain outside fixture root")
    if args.fixtures.exists() and any(args.fixtures.iterdir()):
        parser.error("fixture output must be absent or empty to avoid stale cases")
    if args.max_extracted_bytes <= 0:
        parser.error("extraction budget must be positive")
    try:
        selection = required_archives(args.profile, args.presets, args.corpus_role)
    except ValueError as error:
        parser.error(str(error))
    release = resolve_release(args.release)
    tag = release["tag_name"]
    # An unknown newer release cannot silently reuse stale schema assumptions.
    if tag != SUPPORTED_RELEASE:
        write_json(args.provenance, {"requested_release": args.release, "resolved_release": tag, "supported_release": SUPPORTED_RELEASE, "complete_conformance": False, "error": "runner schema pin must be updated and validated before this release can be executed"})
        raise ValueError("newest published release differs from validated runner pin: " + tag)
    assets = {asset["name"]: asset for asset in release["assets"]}
    files = {}
    cases = {}
    archives = []
    limits = {"files": 0, "bytes": 0, "member_bytes": 64 * 1024**2, "max_files": 2_000_000, "max_bytes": args.max_extracted_bytes}
    args.fixtures.mkdir(parents=True, exist_ok=True)
    for name, preset in selection:
        asset = assets.get(name)
        if asset is None:
            raise ValueError("required published fixture archive is missing: " + name)
        archive, proof = download_asset(asset, args.work, tag, 2 * 1024**3)
        archives.append(proof)
        extract_archive(archive, args.fixtures, args.profile, preset, limits, files, cases)
    selected = {case: info for case, info in cases.items() if info["extracted"]}
    if not selected:
        raise ValueError("selected profile contains no executable cases")
    manifest = {"spec_release": tag, "required_suites": sorted({"/".join(case.split("/")[:5]) for case in selected}), "files": files}
    digest = write_json(args.manifest, manifest)
    if args.corpus_role != "combined":
        pins = json.loads(FULL_CORPUS_PINS.read_text(encoding="utf-8"))
        try:
            validate_full_source(pins, args.corpus_role, digest, len(files), len(selected), len(manifest["required_suites"]))
        except (ValueError, KeyError, TypeError) as error:
            write_json(args.provenance, {"requested_release": args.release, "spec_release": tag, "profile": args.profile, "corpus_role": args.corpus_role, "manifest_sha256": digest, "complete_conformance": False, "error": str(error)})
            raise
    by_suite = {}
    for case, info in cases.items():
        suite = "/".join(case.split("/")[:5])
        summary = by_suite.setdefault(suite, {"published_cases": 0, "extracted_cases": 0, "unsupported_cases": 0})
        summary["published_cases"] += 1
        summary["extracted_cases"] += int(info["extracted"])
        summary["unsupported_cases"] += int(not info["implemented"])
    provenance = {"kind": "published_release_fixture_profile", "requested_release": args.release, "spec_release": tag, "prerelease": release.get("prerelease", False), "published_at": release.get("published_at"), "release_metadata_url": RELEASE_API, "profile": args.profile, "corpus_role": args.corpus_role, "profile_components": ["structural-all-forks", "processing-all-forks", "rewards", "light-client-verification", "custody", "bls", "generic-ssz", "shuffling"] if args.profile == "implemented" else ["full-target-fork-inventory"], "target_forks": sorted(TARGET_FORKS), "archives": archives, "case_count": len(selected), "published_case_count": len(cases), "unimplemented_case_count": sum(not info["implemented"] for info in cases.values()), "extracted_files": limits["files"], "extracted_bytes": limits["bytes"], "manifest_sha256": digest, "suites": by_suite, "static_assertions": "canonical-byte-roundtrip-and-published-root", "complete_conformance": False}
    write_json(args.provenance, provenance)
    print(json.dumps({"release": tag, "profile": args.profile, "cases": len(selected), "outside_subset_profile_cases": provenance["unimplemented_case_count"], "manifest_sha256": "0x" + digest, "complete_conformance": False}))


if __name__ == "__main__":
    main()
