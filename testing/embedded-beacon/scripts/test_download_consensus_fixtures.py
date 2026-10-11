"""Resource and provenance regressions for the fixture downloader."""

import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("consensus_download", Path(__file__).with_name("download-consensus-fixtures.py"))
download = importlib.util.module_from_spec(spec)
spec.loader.exec_module(download)


class DownloadTests(unittest.TestCase):
    def test_full_corpus_pins_resolve_to_the_spec_test_package(self):
        repository = Path(__file__).resolve().parents[3]
        self.assertEqual(download.FULL_CORPUS_PINS, repository / "testing/embedded-beacon/spec-tests/full-corpus-pins.json")
        self.assertTrue(download.FULL_CORPUS_PINS.is_file())
        self.assertTrue((repository / "Cargo.toml").is_file())

    def test_latest_includes_prereleases_and_excludes_drafts(self):
        releases = [{"tag_name": "v1.6", "published_at": "2026-09-01", "draft": False}, {"tag_name": "v1.7-beta", "published_at": "2026-10-01", "prerelease": True}, {"tag_name": "v9", "published_at": "2027-01-01", "draft": True}]
        self.assertEqual(download.select_latest(releases)["tag_name"], "v1.7-beta")

    def test_digest_mismatch_cannot_replace_independent_pin(self):
        asset = {"name": "minimal.tar.gz", "digest": "sha256:" + "00" * 32}
        with self.assertRaisesRegex(ValueError, "differs from the independently pinned digest"):
            download.asset_digest(asset, download.SUPPORTED_RELEASE)
        with self.assertRaisesRegex(ValueError, "no authenticated SHA-256"):
            download.asset_digest({"name": "minimal.tar.gz"}, "v9")
        self.assertTrue(all(len(value) == 64 for value in download.PINNED_DIGESTS.values()))

    def test_archive_traversal_and_special_paths_rejected(self):
        for name in ["/tests/minimal/file", "tests/../escape", "tests/minimal/evil\\file", "tests/C:/escape"]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                download.safe_member(name)
        self.assertEqual(download.safe_member("tests/minimal/config.yaml"), ("minimal", "config.yaml"))

    def test_selected_profile_is_explicit_and_full_inventory_retained(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "cases.tar.gz"
            members = {
                "tests/minimal/phase0/ssz_static/BeaconState/suite/case/roots.yaml": b"root: '0x00'",
                "tests/minimal/phase0/ssz_static/BeaconState/suite/case/value.yaml": b"large mapping",
                "tests/minimal/gloas/fork_choice/on_block/suite/case/steps.yaml": b"[]",
                "tests/minimal/config.yaml": b"CHURN_LIMIT_QUOTIENT: 32",
            }
            with tarfile.open(archive, "w:gz") as output:
                for name, data in members.items():
                    entry = tarfile.TarInfo(name)
                    entry.size = len(data)
                    output.addfile(entry, io.BytesIO(data))
            files, cases = {}, {}
            limits = {"files": 0, "bytes": 0, "member_bytes": 1024, "max_files": 10, "max_bytes": 1024}
            download.extract_archive(archive, root / "fixtures", "implemented", "minimal", limits, files, cases)
            self.assertEqual(len(cases), 2)
            self.assertEqual(sum(info["extracted"] for info in cases.values()), 1)
            self.assertEqual(sum(not info["implemented"] for info in cases.values()), 1)
            self.assertNotIn("minimal/phase0/ssz_static/BeaconState/suite/case/value.yaml", files)
            self.assertIn("minimal/config.yaml", files)

    def test_profile_includes_only_proven_altair_processing_handlers(self):
        for runner, handler in [("fork", "fork"), ("sanity", "blocks"), ("sanity", "slots"), ("operations", "sync_aggregate"), ("operations", "attestation"), ("epoch_processing", "inactivity_updates"), ("epoch_processing", "sync_committee_updates")]:
            self.assertTrue(download.supported_case(("minimal", "altair", runner, handler, "suite", "case")))
        for runner, handler in [("fork_choice", "on_block"), ("epoch_processing", "participation_record_updates")]:
            self.assertFalse(download.supported_case(("minimal", "altair", runner, handler, "suite", "case")))

    def test_cache_download_is_atomic_and_recovers_corrupt_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp)
            data = b"real archive bytes"
            expected = hashlib.sha256(data).hexdigest()
            asset = {"name": "general.tar.gz", "size": len(data), "digest": "sha256:" + expected, "browser_download_url": "https://example.invalid/fixture"}
            (work / asset["name"]).write_bytes(b"corrupt")
            with patch.dict(download.PINNED_DIGESTS, {"general.tar.gz": expected}), patch.object(download.urllib.request, "urlopen", return_value=io.BytesIO(data)):
                path, proof = download.download_asset(asset, work, download.SUPPORTED_RELEASE, 1024)
            self.assertEqual(path.read_bytes(), data)
            self.assertEqual(proof["sha256"], expected)
            self.assertFalse((work / "general.tar.gz.partial").exists())
            self.assertEqual(len(list(work.glob("*.invalid-*"))), 1)

    def test_full_minimal_requires_the_compliance_archive(self):
        self.assertEqual(download.required_archives("full", ["mainnet", "minimal", "general"]), [("mainnet.tar.gz", "mainnet"), ("minimal.tar.gz", "minimal"), ("general.tar.gz", "general"), ("comptests.tar.gz", "minimal")])
        self.assertEqual(download.required_archives("implemented", ["minimal"]), [("minimal.tar.gz", "minimal")])
        self.assertEqual(download.required_archives("full", ["mainnet"]), [("mainnet.tar.gz", "mainnet")])

    def test_complete_roles_keep_the_original_archives_separate(self):
        self.assertEqual(download.required_archives("full", ["mainnet", "minimal", "general"], "primary"),
                         [("mainnet.tar.gz", "mainnet"), ("minimal.tar.gz", "minimal"), ("general.tar.gz", "general")])
        self.assertEqual(download.required_archives("full", ["minimal"], "compliance"),
                         [("comptests.tar.gz", "minimal")])
        for profile, presets, role in [("implemented", ["minimal"], "compliance"),
                                       ("full", ["mainnet", "minimal", "general"], "compliance"),
                                       ("full", ["minimal"], "primary"),
                                       ("full", ["minimal"], "unknown")]:
            with self.subTest(role=role), self.assertRaises(ValueError):
                download.required_archives(profile, presets, role)

    def test_complete_source_counts_and_original_manifest_bytes_are_mandatory(self):
        pins = {"format_version": 1, "spec_release": download.SUPPORTED_RELEASE,
                "corpora": {"primary": {"manifest_sha256": "00" * 32,
                    "file_count": 4, "case_count": 2, "required_suite_count": 1}}}
        download.validate_full_source(pins, "primary", "00" * 32, 4, 2, 1)
        for digest, files, cases, suites in [("11" * 32, 4, 2, 1), ("00" * 32, 3, 2, 1),
                                           ("00" * 32, 4, 1, 1), ("00" * 32, 4, 2, 0)]:
            with self.subTest(digest=digest, files=files), self.assertRaises(ValueError):
                download.validate_full_source(pins, "primary", digest, files, cases, suites)

    def test_failed_download_does_not_install_partial(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp)
            asset = {"name": "general.tar.gz", "size": 3, "digest": "sha256:" + hashlib.sha256(b"abc").hexdigest(), "browser_download_url": "https://example.invalid/fixture"}
            with patch.dict(download.PINNED_DIGESTS, {"general.tar.gz": hashlib.sha256(b"abc").hexdigest()}), patch.object(download.urllib.request, "urlopen", return_value=io.BytesIO(b"bad")):
                with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                    download.download_asset(asset, work, download.SUPPORTED_RELEASE, 1024)
            self.assertFalse((work / "general.tar.gz").exists())
            self.assertFalse((work / "general.tar.gz.partial").exists())


if __name__ == "__main__":
    unittest.main()
