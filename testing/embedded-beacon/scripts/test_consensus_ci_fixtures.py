"""Contract regressions for the explicit consensus expected-failure CI gate."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("consensus_gate", Path(__file__).with_name("check-consensus-fixture-results.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ConsensusGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.manifest = Path(self.temp.name) / "manifest.json"
        self.identities = ["minimal/gloas/sanity/blocks/suite/a", "minimal/gloas/sanity/blocks/suite/b"]
        self.manifest.write_text(json.dumps({identity + "/manifest.yaml": "00" * 32 for identity in self.identities}, indent=2) + "\n", encoding="utf-8")
        self.expected = {"format_version": 1, "expected_outcome": "unsupported", "spec_release": "v1", "required_inventory": {"minimal": {"cases": 2, "case_ids_sha256": hashlib.sha256(("\n".join(self.identities) + "\n").encode()).hexdigest()}}, "expected_failures": [self.identities[1]]}
        self.provenance = {"spec_release": "v1", "profile": "full", "manifest_sha256": hashlib.sha256(self.manifest.read_bytes()).hexdigest(), "case_count": 2, "published_case_count": 2}
        self.report = {"spec_release": "v1", "errors": [], "complete_matrix": False, "cases": [{"path": identity, "outcome": "passed" if index == 0 else "unsupported", "message": "", "seconds": 0.0, **{marker: False for marker in module.MARKERS}} for index, identity in enumerate(self.identities)]}

    def gate(self, status=1):
        return module.gate(self.expected, self.report, self.provenance, self.manifest, "minimal", status)

    def test_explicit_expected_failure_is_counted(self):
        result = self.gate()
        self.assertEqual((result["passed"], result["expected_failures"]), (1, 1))
        self.assertFalse(result["complete_conformance"])

    def test_new_pass_is_reported_as_stale(self):
        self.report["cases"][1]["outcome"] = "passed"
        self.assertEqual(self.gate(0)["newly_passing_expected_cases"], [self.identities[1]])

    def test_even_an_empty_diagnostic_allowlist_cannot_claim_complete_conformance(self):
        self.expected["expected_failures"] = []
        self.report["cases"][1]["outcome"] = "passed"
        self.report["complete_matrix"] = True
        self.assertFalse(self.gate(0)["complete_conformance"])

    def test_unlisted_failure_rejected(self):
        self.report["cases"][0]["outcome"] = "failed"
        with self.assertRaisesRegex(ValueError, "unexpected fixture failures"):
            self.gate()

    def test_listed_case_cannot_hide_failed_setup_or_changed_assertion(self):
        self.report["cases"][1]["outcome"] = "failed"
        self.report["cases"][1]["message"] = "fixture input exceeds budget"
        with self.assertRaisesRegex(ValueError, "unexpected fixture failures"):
            self.gate()

    def test_blanket_failed_policy_rejected(self):
        self.expected["expected_outcome"] = "failed"
        with self.assertRaisesRegex(ValueError, "only unsupported"):
            self.gate()

    def test_setup_errors_cannot_be_expected(self):
        self.report["errors"] = ["missing fixture input"]
        with self.assertRaisesRegex(ValueError, "setup/integrity"):
            self.gate()

    def test_incomplete_and_duplicate_results_rejected(self):
        for cases in [self.report["cases"][:1], self.report["cases"] + [copy.deepcopy(self.report["cases"][0])]]:
            with self.subTest(cases=cases):
                self.report["cases"] = cases
                with self.assertRaises(ValueError):
                    self.gate()

    def test_release_and_profile_mismatch_rejected(self):
        for key, value in [("spec_release", "v2"), ("profile", "focused")]:
            with self.subTest(key=key):
                old = self.provenance[key]
                self.provenance[key] = value
                with self.assertRaises(ValueError):
                    self.gate()
                self.provenance[key] = old

    def test_inventory_digest_and_counts_rejected(self):
        for key, value in [("manifest_sha256", "ff" * 32), ("case_count", 1), ("published_case_count", 1)]:
            with self.subTest(key=key):
                old = self.provenance[key]
                self.provenance[key] = value
                with self.assertRaises(ValueError):
                    self.gate()
                self.provenance[key] = old

    def test_known_inventory_cannot_silently_change(self):
        self.expected["required_inventory"]["minimal"]["case_ids_sha256"] = "aa" * 32
        with self.assertRaisesRegex(ValueError, "missing, added or renamed"):
            self.gate()

    def test_exit_status_and_infrastructure_failure_rejected(self):
        for status in [0, 2, 101, 137]:
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.gate(status)

    def test_duplicate_expectations_and_unknown_outcomes_rejected(self):
        self.expected["expected_failures"] *= 2
        with self.assertRaisesRegex(ValueError, "duplicate expected"):
            self.gate()
        self.expected["expected_failures"] = self.expected["expected_failures"][:1]
        self.report["cases"][0]["outcome"] = "skipped"
        with self.assertRaisesRegex(ValueError, "unknown fixture outcome"):
            self.gate()


class DiagnosticIntegrityContracts(unittest.TestCase):
    def test_duplicate_json_fields_and_nonfinite_numbers_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            for raw in ('{"cases":[],"cases":[]}', '{"seconds":NaN}'):
                path.write_text(raw)
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    module.read_json(path)

    def test_assumptions_remain_explicit_and_runtime_credit_is_false(self):
        helper = ConsensusGateTests()
        helper.setUp()
        try:
            for case in helper.report["cases"]:
                for marker in module.MARKERS:
                    case[marker] = True
            result = helper.gate()
            self.assertTrue(all(result[marker] == 2 for marker in module.MARKERS))
            self.assertFalse(result["runtime_synchronization_verified"])
            del helper.report["cases"][0][module.MARKERS[0]]
            with self.assertRaisesRegex(ValueError, "assumption marker"):
                helper.gate()
        finally:
            helper.doCleanups()


if __name__ == "__main__":
    unittest.main()
