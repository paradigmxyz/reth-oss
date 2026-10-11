"""Offline adversarial contracts for authenticated published-release conformance.

Synthetic archive contents and outcomes establish policy behavior only, never conformance.
"""

import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("published_consensus", Path(__file__).with_name("published-consensus-fixtures.py"))
published = importlib.util.module_from_spec(spec)
spec.loader.exec_module(published)


class PublishedFixtureContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.work = self.root / "archives"
        self.work.mkdir()
        self.assets = []
        self.pins = {}
        for name, preset in [("mainnet.tar.gz", "mainnet"), ("minimal.tar.gz", "minimal"),
                             ("general.tar.gz", "general"), ("comptests.tar.gz", "minimal")]:
            path = self.work / name
            if name == "comptests.tar.gz":
                forks = ["gloas"]
                suffix = "compliance"
            elif preset == "general":
                forks = ["phase0"]
                suffix = "primary"
            else:
                forks = published.FORKS
                suffix = "primary"
            with tarfile.open(path, "w:gz") as archive:
                for fork in forks:
                    case = f"tests/{preset}/{fork}/ssz_static/BeaconState/suite/{suffix}"
                    for filename, content in [("serialized.ssz_snappy", b"original bytes"),
                                              ("roots.yaml", b"original roots"),
                                              ("value.yaml", b"original static mapping")]:
                        member = tarfile.TarInfo(case + "/" + filename)
                        member.size = len(content)
                        archive.addfile(member, io.BytesIO(content))
                member = tarfile.TarInfo(f"tests/{preset}/config.yaml")
                member.size = 4
                archive.addfile(member, io.BytesIO(b"A: 1"))
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            self.pins[name] = digest
            self.assets.append({"name": name, "id": len(self.assets) + 1, "size": path.stat().st_size,
                "digest": "sha256:" + digest,
                "browser_download_url": "https://github.com/ethereum/consensus-specs/releases/download/" +
                                         published.download.SUPPORTED_RELEASE + "/" + name})
        self.pin_patch = patch.dict(published.download.PINNED_DIGESTS, self.pins)
        self.pin_patch.start()
        self.addCleanup(self.pin_patch.stop)
        self.release = {"tag_name": published.download.SUPPORTED_RELEASE, "id": 123,
                        "draft": False, "prerelease": True, "published_at": "2026-10-01T00:00:00Z",
                        "assets": self.assets}
        with patch.object(published.download, "resolve_release", return_value=self.release):
            self.resolution = published.resolve("latest")
        self.resolution_path = self.root / "resolution.json"
        self.resolution_sha = published.write_json(self.resolution_path, self.resolution)

    def prepared(self):
        directory = self.root / "prepared"
        with patch.object(published.download.urllib.request, "urlopen", side_effect=AssertionError("cache should not fetch")):
            result = published.prepare(self.resolution, self.resolution_sha, self.work, directory, 1024**2)
        source = published.policy.read_json(directory / "source-record.json")
        wrapper = {"format_version": 1, "source_record_sha256": result["source_record_sha256"],
                   "source": source, "report": {"spec_release": source["spec_release"], "complete_matrix": True,
                   "errors": [], "full_corpus": {"case_ids_sha256": source["canonical_case_ids_sha256"],
                   "input_files_sha256": source["canonical_input_files_sha256"],
                   "corpora": [{"role": c["role"], "files": c["files"], "cases": c["cases"]}
                               for c in source["corpora"]]}, "cases": []}}
        for role, _ in published.ROLES:
            manifest = published.policy.read_json(directory / (role + "-manifest.json"))
            for path in sorted(published.identities(manifest)):
                wrapper["report"]["cases"].append({"path": path, "outcome": "passed", "message": "",
                    "seconds": 0.1, **{marker: False for marker in published.policy.MARKERS}})
        return directory, result, source, wrapper

    def check(self, directory, result, source, wrapper, exit_code=0):
        return published.check(self.resolution, self.resolution_sha, source, result["source_record_sha256"],
            self.work, directory / "primary-manifest.json", directory / "compliance-manifest.json",
            wrapper, exit_code, 1024**2)

    def test_resolve_latest_once_preserves_original_metadata_and_refuses_unreviewed_schema(self):
        with patch.object(published.download, "resolve_release", return_value=self.release) as resolve:
            resolution = published.resolve("latest")
        resolve.assert_called_once_with("latest")
        self.assertEqual(resolution["release_metadata"], self.release)
        changed = copy.deepcopy(self.release)
        changed["tag_name"] = "v99.0.0"
        with patch.object(published.download, "resolve_release", return_value=changed), self.assertRaisesRegex(ValueError, "explicit review"):
            published.resolve("latest")

    def test_resolution_refuses_rewritten_asset_identity_url_and_duplicate_metadata(self):
        for mutation in (lambda r: r.update(request_url="https://example.invalid/metadata"),
                         lambda r: r.update(requested_release="v99"),
                         lambda r: r["release_metadata"]["assets"][0].update(id=None),
                         lambda r: r["release_metadata"]["assets"][0].update(browser_download_url="https://example.invalid/file"),
                         lambda r: r["release_metadata"]["assets"].append(r["release_metadata"]["assets"][0]),
                         lambda r: r["archives"][0].update(sha256="0x" + "00" * 32)):
            changed = copy.deepcopy(self.resolution)
            mutation(changed)
            with self.subTest(change=mutation), self.assertRaises(ValueError):
                published.validate_resolution(changed)

    def test_resolution_and_source_records_need_separately_supplied_byte_identity(self):
        self.assertEqual(published.trusted_json(self.resolution_path, self.resolution_sha, 1024**2), self.resolution)
        with self.assertRaisesRegex(ValueError, "separately supplied digest"):
            published.trusted_json(self.resolution_path, "00" * 32, 1024**2)
        self.resolution_path.write_text('{"a":1,"a":2}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON field"):
            published.trusted_json(self.resolution_path, published.download.digest_file(self.resolution_path))

    def test_complete_original_archive_values_preserved_without_frozen_counts(self):
        directory, result, source, wrapper = self.prepared()
        self.assertEqual(source["required_case_count"], 18)
        manifest = published.policy.read_json(directory / "primary-manifest.json")
        self.assertTrue(any(path.endswith("value.yaml") for path in manifest["files"]))
        self.assertFalse(result["complete_conformance"])
        gate = self.check(directory, result, source, wrapper)
        self.assertTrue(gate["accepted"])
        self.assertTrue(gate["original_archive_reconstruction_verified"])
        self.assertFalse(gate["runtime_synchronization_verified"])
        self.assertFalse(gate["expected_failures_allowed"])
        self.assertEqual(sum(item["cases"] for item in gate["handler_coverage"]), 18)

    def test_manifests_rederived_from_original_archive_reject_removed_static_value(self):
        directory, result, source, wrapper = self.prepared()
        path = directory / "primary-manifest.json"
        manifest = published.policy.read_json(path)
        del manifest["files"][next(key for key in manifest["files"] if key.endswith("value.yaml"))]
        published.write_json(path, manifest)
        with self.assertRaisesRegex(ValueError, "original bytes"):
            self.check(directory, result, source, wrapper)

    def test_forged_report_and_source_cannot_hide_original_case_omission(self):
        directory, result, source, wrapper = self.prepared()
        wrapper["report"]["cases"].pop()
        wrapper["source"]["required_case_count"] -= 1
        source["required_case_count"] -= 1
        with self.assertRaisesRegex(ValueError, "reconstructed original archives"):
            self.check(directory, result, source, wrapper)

    def test_duplicate_substituted_unsupported_failed_and_missing_outcomes_all_fail(self):
        directory, result, source, original = self.prepared()
        for mutation in (lambda r: r["report"]["cases"].pop(),
                         lambda r: r["report"]["cases"][0].update(path=r["report"]["cases"][1]["path"]),
                         lambda r: r["report"]["cases"][0].update(path="general/phase0/bls/verify/tests/substitute"),
                         lambda r: r["report"]["cases"][0].update(outcome="unsupported"),
                         lambda r: r["report"]["cases"][0].update(outcome="failed"),
                         lambda r: r["report"].update(errors=["worker failed"])):
            wrapper = copy.deepcopy(original)
            mutation(wrapper)
            with self.subTest(change=mutation), self.assertRaises(ValueError):
                self.check(directory, result, source, wrapper)

    def test_archive_byte_tampering_is_rejected_before_manifest_or_report_trust(self):
        directory, result, source, wrapper = self.prepared()
        path = self.work / "minimal.tar.gz"
        data = bytearray(path.read_bytes())
        data[len(data) // 2] ^= 1
        path.write_bytes(data)
        with self.assertRaisesRegex(ValueError, "published digest"):
            self.check(directory, result, source, wrapper)

    def test_nonzero_runner_exit_cannot_be_overridden_by_passing_report(self):
        directory, result, source, wrapper = self.prepared()
        with self.assertRaisesRegex(ValueError, "did not exit successfully"):
            self.check(directory, result, source, wrapper, 1)

    def test_missing_required_fork_and_duplicate_source_cases_refuse_preexecution_record(self):
        directory, _, _, _ = self.prepared()
        manifests = {role: published.policy.read_json(directory / (role + "-manifest.json")) for role, _ in published.ROLES}
        digests = {role: "00" * 32 for role, _ in published.ROLES}
        altered = copy.deepcopy(manifests)
        altered["primary"]["files"] = {p: v for p, v in altered["primary"]["files"].items() if not p.startswith("mainnet/gloas/")}
        with self.assertRaisesRegex(ValueError, "full fork/preset matrix"):
            published.derive_source(self.resolution_sha, altered, digests)
        altered = copy.deepcopy(manifests)
        primary_case = next(p for p in altered["primary"]["files"] if p.startswith("minimal/gloas/") and p.endswith("roots.yaml"))
        altered["compliance"]["files"][primary_case] = "00" * 32
        with self.assertRaisesRegex(ValueError, "duplicate canonical case"):
            published.derive_source(self.resolution_sha, altered, digests)

    def test_links_duplicate_members_and_resource_overflow_are_rejected(self):
        for kind in ("link", "duplicate", "budget"):
            path = self.root / (kind + ".tar.gz")
            name = "tests/minimal/gloas/ssz_static/BeaconState/suite/case/value.yaml"
            with tarfile.open(path, "w:gz") as archive:
                member = tarfile.TarInfo(name)
                member.size = 3
                if kind == "link":
                    member.type = tarfile.SYMTYPE
                    member.linkname = "../../outside"
                    archive.addfile(member)
                else:
                    archive.addfile(member, io.BytesIO(b"abc"))
                    if kind == "duplicate":
                        archive.addfile(member, io.BytesIO(b"abc"))
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                published.archive_members(path, "minimal", {"files": 0, "bytes": 0, "max_bytes": 1 if kind == "budget" else 10}, {})

    def test_stale_output_directory_is_never_merged_into_original_source(self):
        output = self.root / "stale"
        output.mkdir()
        (output / "old-results.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "absent or empty"):
            published.prepare(self.resolution, self.resolution_sha, self.work, output, 1024**2)


if __name__ == "__main__":
    unittest.main()
