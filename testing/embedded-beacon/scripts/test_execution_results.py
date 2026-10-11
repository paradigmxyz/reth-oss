"""Contract regressions for complete execution CI reports and exact exception scope."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("execution_result_gate", Path(__file__).with_name("verify-execution-results.py"))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExecutionResultGateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.provenance = {
            "schema_version": 1, "release": "tests@v1", "archive_sha256": "a" * 64,
            "fixture_format": "blockchain_test", "complete_execution_conformance": False,
            "excluded_formats": ["state_test", "blockchain_test_engine", "blockchain_test_engine_x", "transaction_test"],
            "files": [{"path": "blockchain_tests/passed.json", "bytes": 2, "sha256": "b" * 64},
                      {"path": "blockchain_tests/failed.json", "bytes": 3, "sha256": "c" * 64}],
        }
        self.selection = {"release": "tests@v1", "archive_sha256": "a" * 64, "verified_files": 2}
        self.report = {
            "suite": ".", "fixture_directory": str(self.root / "blockchain_tests"),
            "fixture_files": 2, "passed_files": 1, "failed_files": 1, "skipped_files": 0,
            "complete_execution_conformance": False,
            "cases": [
                {"path": str(self.root / "blockchain_tests/passed.json"), "description": "passing file", "outcome": "passed", "error": None},
                {"path": str(self.root / "blockchain_tests/failed.json"), "description": "failing file", "outcome": "failed", "error": "exact consensus mismatch"},
            ],
        }
        self.expectations = {"schema_version": 1, "known_failures": [{
            "release": "tests@v1", "archive_sha256": "a" * 64,
            "path": "blockchain_tests/failed.json", "fixture_sha256": "c" * 64,
            "outcome": "failed", "error": "exact consensus mismatch", "reason": "reviewed recorded mismatch",
        }]}

    def evaluate(self, status=101):
        return MODULE.evaluate(self.provenance, self.selection, self.report, self.expectations, self.root, status)

    def test_known_failure_remains_failed_in_an_accepted_complete_report(self):
        result = self.evaluate()
        self.assertEqual(result["status"], "passed_with_known_failures")
        self.assertEqual((result["passed_files"], result["failed_files"]), (1, 1))
        self.assertEqual(result["accepted_known_failures"][0]["path"], "blockchain_tests/failed.json")
        self.assertFalse(result["complete_execution_conformance"])

    def test_new_error_and_unlisted_failure_cannot_reuse_an_exception(self):
        for alteration in ("error", "expectation"):
            with self.subTest(alteration=alteration):
                report, expectations = copy.deepcopy(self.report), copy.deepcopy(self.expectations)
                if alteration == "error":
                    report["cases"][1]["error"] = "block 1 execution harness failed: database unavailable"
                else:
                    expectations["known_failures"] = []
                result = MODULE.evaluate(self.provenance, self.selection, report, expectations, self.root, 101)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(len(result["unexpected_failures"]), 1)

    def test_different_release_or_archive_does_not_inherit_old_failures(self):
        for field in ("release", "archive_sha256"):
            with self.subTest(field=field):
                provenance, selection = copy.deepcopy(self.provenance), copy.deepcopy(self.selection)
                provenance[field] = "tests@v2" if field == "release" else "d" * 64
                selection[field] = provenance[field]
                result = MODULE.evaluate(provenance, selection, self.report, self.expectations, self.root, 101)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(len(result["inactive_expectations"]), 1)

    def test_changed_fixture_hash_cannot_match_an_old_exception(self):
        self.provenance["files"][1]["sha256"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "expected fixture hash mismatch"):
            self.evaluate()

    def test_passing_improvement_is_reported_as_a_stale_expectation(self):
        self.report["cases"][1].update(outcome="passed", error=None)
        self.report.update(passed_files=2, failed_files=0)
        result = self.evaluate(status=0)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["stale_expectations"], [{"path": "blockchain_tests/failed.json", "reason": "now_passes"}])

    def test_skips_do_not_silently_count_as_passes(self):
        self.report["cases"][1].update(outcome="skipped", error="test was skipped")
        self.report.update(failed_files=0, skipped_files=1)
        self.assertEqual(self.evaluate(status=0)["status"], "failed")

    def test_missing_and_duplicate_results_fail_before_matching_exceptions(self):
        self.report["cases"].pop()
        with self.assertRaisesRegex(ValueError, "incomplete result inventory"):
            self.evaluate()
        self.report["cases"].append(copy.deepcopy(self.report["cases"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate result fixture"):
            self.evaluate()

    def test_counter_lies_and_boolean_counts_are_rejected(self):
        for count in (0, True):
            with self.subTest(count=count):
                self.report["failed_files"] = count
                with self.assertRaises(ValueError):
                    self.evaluate()

    def test_wrong_directory_or_escaped_result_path_is_rejected(self):
        self.report["fixture_directory"] = str(self.root / "substituted")
        with self.assertRaisesRegex(ValueError, "different fixture directory"):
            self.evaluate()
        self.report["fixture_directory"] = str(self.root / "blockchain_tests")
        self.report["cases"][1]["path"] = str(self.root.parent / "outside.json")
        with self.assertRaisesRegex(ValueError, "escapes the authenticated"):
            self.evaluate()

    def test_crash_and_inconsistent_runner_status_cannot_be_allowlisted(self):
        for status in (134, 0):
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.evaluate(status=status)

    def test_duplicate_json_keys_and_oversized_inputs_are_rejected(self):
        path = self.root / "record.json"
        path.write_text('{"failed_files": 1, "failed_files": 0}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            MODULE.read_json(path, 1024)
        with self.assertRaisesRegex(ValueError, "size limit"):
            MODULE.read_json(path, 1)

    def test_claiming_complete_conformance_and_empty_results_are_rejected(self):
        self.report["complete_execution_conformance"] = True
        with self.assertRaisesRegex(ValueError, "must not claim complete"):
            self.evaluate()
        self.report["complete_execution_conformance"] = False
        self.report["cases"] = []
        with self.assertRaisesRegex(ValueError, "incomplete result inventory"):
            self.evaluate()


if __name__ == "__main__":
    unittest.main()
