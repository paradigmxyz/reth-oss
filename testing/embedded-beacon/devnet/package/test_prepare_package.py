"""Cache integrity regressions using bounded in-memory download responses."""

import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import prepare_package


class InterruptedResponse(io.BytesIO):
    def read(self, size=-1):
        if self.tell() > 0:
            raise ConnectionResetError("interrupted test response")
        return super().read(3)


class ArchiveCacheTests(unittest.TestCase):
    def exercise(self, response, expected, expected_error=None, max_bytes=None):
        work_parent = Path(__file__).resolve().parent.parent / "work"
        work_parent.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="archive-cache-test-", dir=work_parent) as temp:
            work = Path(temp)
            self.assertTrue(work.resolve().is_relative_to(work_parent.resolve()))
            target = work / "ethereum-package.tar.gz"
            target.write_bytes(b"old incomplete cache")
            pins = {"ethereum_package": {
                "archive_url": "https://example.invalid/test-only",
                "archive_sha256": hashlib.sha256(expected).hexdigest(),
            }}
            limit = max_bytes or prepare_package.MAX_ARCHIVE_BYTES
            with patch.object(prepare_package.urllib.request, "urlopen", return_value=response), \
                    patch.object(prepare_package, "MAX_ARCHIVE_BYTES", limit):
                if expected_error:
                    with self.assertRaises(expected_error):
                        prepare_package.download_verified_archive(work, pins)
                    self.assertEqual(target.read_bytes(), b"old incomplete cache")
                else:
                    result = prepare_package.download_verified_archive(work, pins)
                    self.assertEqual(result, target)
                    self.assertEqual(target.read_bytes(), expected)
            self.assertEqual(list(work.glob("*.partial")), [])

    def test_valid_archive_replaces_old_incomplete_cache(self):
        self.exercise(io.BytesIO(b"complete verified archive"), b"complete verified archive")

    def test_interrupted_download_never_publishes_partial(self):
        self.exercise(InterruptedResponse(b"incomplete stream"), b"complete stream", ConnectionResetError)

    def test_hash_mismatch_preserves_existing_cache(self):
        self.exercise(io.BytesIO(b"corrupt response"), b"verified response", ValueError)

    def test_download_limit_preserves_existing_cache(self):
        self.exercise(io.BytesIO(b"oversized response"), b"oversized response", ValueError, max_bytes=4)


if __name__ == "__main__":
    unittest.main()
