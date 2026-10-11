"""Regression checks for release selection and authenticated execution extraction."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("execution_fixture_download", Path(__file__).with_name("download-execution-fixtures.py"))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExecutionFixtureDownloadTests(unittest.TestCase):
    def make_archive(self, directory, records):
        archive = Path(directory) / "fixtures.tar.gz"
        with tarfile.open(archive, "w:gz") as stream:
            for name, contents, kind in records:
                info = tarfile.TarInfo(name)
                info.type = kind
                info.size = len(contents)
                if kind == tarfile.SYMTYPE:
                    info.linkname = "outside.json"
                stream.addfile(info, io.BytesIO(contents) if kind == tarfile.REGTYPE else None)
        return archive

    def test_latest_selects_newest_tests_channel_including_prereleases(self):
        releases = [
            {"tag_name": "tests@v20.0.2", "published_at": "2026-08-27", "prerelease": False},
            {"tag_name": "tests@v21.0.0", "published_at": "2026-09-23", "prerelease": True},
            {"tag_name": "tests-zkevm@v99", "published_at": "2026-10-01"},
            {"tag_name": "tests@v22", "published_at": "2026-10-02", "draft": True},
        ]
        with patch.object(MODULE, "api_json", return_value=releases):
            self.assertEqual(MODULE.resolve_release("latest")["tag_name"], "tests@v21.0.0")

    def test_missing_tests_channel_is_an_error(self):
        with patch.object(MODULE, "api_json", return_value=[]):
            with self.assertRaisesRegex(ValueError, "no published execution"):
                MODULE.resolve_release("latest")

    def test_missing_published_digest_fails_before_download(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "no published SHA"):
                MODULE.download_asset({"name": "fixtures.tar.gz", "size": 1}, Path(directory))

    def test_corrupt_archive_cache_is_never_reused(self):
        data = b"valid"
        sha = hashlib.sha256(data).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / (sha + ".tar.gz")).write_bytes(b"wrong")
            with self.assertRaisesRegex(ValueError, "fails authentication"):
                MODULE.download_asset({"size": len(data), "digest": "sha256:" + sha}, Path(directory))

    def test_archive_inventory_detects_tampered_and_extra_files(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = self.make_archive(directory, [("fixtures/blockchain_tests/fork/case.json", b"{}", tarfile.REGTYPE), ("fixtures/state_tests/unused.json", b"{}", tarfile.REGTYPE)])
            output = Path(directory) / "output"
            files = MODULE.extract_blockchain(archive, output)
            self.assertEqual(MODULE.verify(output, {"files": files}), 1)
            (output / "blockchain_tests/fork/case.json").write_bytes(b"[]")
            with self.assertRaisesRegex(ValueError, "differ from authenticated"):
                MODULE.verify(output, {"files": files})
            (output / "blockchain_tests/fork/case.json").write_bytes(b"{}")
            (output / "extra.json").write_bytes(b"{}")
            with self.assertRaisesRegex(ValueError, "inventory differs"):
                MODULE.verify(output, {"files": files})

    def test_archive_traversal_links_and_duplicates_fail(self):
        records = [
            [("fixtures/blockchain_tests/../../outside.json", b"{}", tarfile.REGTYPE)],
            [("fixtures/blockchain_tests/link.json", b"", tarfile.SYMTYPE)],
            [("fixtures/blockchain_tests/case.json", b"{}", tarfile.REGTYPE)] * 2,
        ]
        for record in records:
            with self.subTest(record=record), tempfile.TemporaryDirectory() as directory:
                archive = self.make_archive(directory, record)
                with self.assertRaises(ValueError):
                    MODULE.extract_blockchain(archive, Path(directory) / "output")
                self.assertFalse((Path(directory) / "outside.json").exists())

    def test_empty_corpus_and_resource_limit_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = self.make_archive(directory, [("fixtures/state_tests/unused.json", b"{}", tarfile.REGTYPE)])
            with self.assertRaisesRegex(ValueError, "no execution blockchain"):
                MODULE.extract_blockchain(archive, Path(directory) / "empty")
            archive = self.make_archive(directory, [("fixtures/blockchain_tests/large.json", b"{}", tarfile.REGTYPE)])
            with patch.object(MODULE, "MAX_FILE", 1):
                with self.assertRaisesRegex(ValueError, "limit exceeded"):
                    MODULE.extract_blockchain(archive, Path(directory) / "limited")


if __name__ == "__main__":
    unittest.main()
